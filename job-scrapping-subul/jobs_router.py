"""
jobs_router.py — Module Sélection des Jobs depuis PostgreSQL
=============================================================
Responsabilités :
  - GET  /jobs/{user_id}        → stream SSE des jobs en cache DB
  - GET  /api/matches/{user_id} → liste des jobs matchés (dashboard)
  - POST /api/matches            → matching avec filtres (role, location, score)
  - POST /api/gap                → analyse skills gap utilisateur
  - POST /api/roadmap            → roadmap d'apprentissage personnalisée

Importé dans main.py :
    from jobs_router import jobs_router
    app.include_router(jobs_router)
"""

import json
import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from database import get_jobs_for_user
from job_analyzer_agent import (
    CandidateProfile,
    MarketAnalysis,
    compute_gap,
    match_jobs,
    generate_roadmap,
    LEARNING_META,
)

logger      = logging.getLogger(__name__)
jobs_router = APIRouter(tags=["Jobs"])

# ── Référence partagée vers l'objet MarketAnalysis (initialisé dans main.py) ──
# Doit être assignée depuis main.py après le chargement :
#   import jobs_router as jr
#   jr.market_analysis = MarketAnalysis(jobs)
market_analysis: MarketAnalysis | None = None


# ═══════════════════════════════════════════════════════════════════════════════
#  Pydantic models
# ═══════════════════════════════════════════════════════════════════════════════

class ProfileIn(BaseModel):
    name:                str       = ""
    target_role:         str       = ""
    experience_years:    int       = 0
    skills:              list[str] = []
    preferred_locations: list[str] = []
    open_to_remote:      bool      = True
    salary_expectation:  str       = ""
    user_id:             str       = ""


# ═══════════════════════════════════════════════════════════════════════════════
#  Utilitaires
# ═══════════════════════════════════════════════════════════════════════════════

def sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _to_candidate_profile(p: ProfileIn) -> CandidateProfile:
    d = p.model_dump()
    d.pop("user_id", None)
    return CandidateProfile(**d)


def _job_to_result(j: dict, skills_list: list[str] | None = None) -> dict:
    """Convertit une ligne DB job (issue de get_jobs_for_user) en dict résultat API unifié.

    Mapping champs DB (via get_jobs_for_user) :
      match_score  FLOAT 0.0-1.0  → multiplié x100 pour affichage %
      cosine       float 0.0-1.0  → cosine_score DB (remappé sous 'cosine')
      gap_missing  list[str]      → issu de skills_gap (JSON) en DB
      gap_total    int            → nb de skills must_have
      gap_coverage float          → calculé dans get_jobs_for_user
      skills_req   str            → must_have DB (remappé sous 'skills_req')
      skills_bon   str            → nice_to_have DB (remappé sous 'skills_bon')
      experience   str            → seniority DB (remappé sous 'experience')
      industry     str            → industry DB (= entreprise/secteur)
      ABSENT: pub_date (n'existe PAS dans la table jobs → utiliser created_at)
    """
    # match_score : 0.0-1.0 en DB, -1 si non calculé
    raw_score = j.get("match_score", 0)
    if raw_score is None or raw_score < 0:
        raw_score = 0.0
    ai_score  = round(float(raw_score) * 100, 1)
    score_int = int(ai_score)

    gap_missing = j.get("gap_missing", [])

    # matched_skills depuis must_have (retourné sous clé skills_req par get_jobs_for_user)
    matched_skills = []
    if skills_list:
        matched_skills = [
            s for s in (j.get("skills_req") or "").split(",")
            if s.strip() and s.strip().lower() in {sk.lower() for sk in skills_list}
        ]

    # cosine_score DB est remappé sous clé "cosine" par get_jobs_for_user (0.0-1.0)
    cosine_pct = round(float(j.get("cosine", 0) or 0) * 100, 1)

    # pub_date n'existe PAS dans la table jobs → on utilise created_at
    date_posted = str(j.get("created_at", "")) if j.get("created_at") else ""

    return {
        "title":        j.get("title", ""),
        "company":      j.get("industry", ""),       # industry DB = entreprise/secteur
        "location":     j.get("location", ""),
        "salary":       j.get("salary", ""),
        "url":          j.get("url", ""),
        "source":       j.get("source", ""),
        "date_posted":  date_posted,                  # created_at DB (pub_date n'existe pas)
        "total":        ai_score,
        "skill_pct":    ai_score,
        "loc_pct":      0,
        "title_pct":    0,
        "matched":      matched_skills,
        "missing":      gap_missing[:10],
        "verdict":      "Strong" if score_int >= 70 else ("Good" if score_int >= 50 else "Partial"),
        "description":  (j.get("description") or "")[:2000],
        "cosine":       cosine_pct,
        "match_score":  ai_score,
        "gap_coverage": j.get("gap_coverage", 0),
        "gap_missing":  gap_missing,
        "remote":       j.get("remote", ""),
        "contract":     j.get("contract", ""),
        "experience":   j.get("experience", ""),      # seniority DB remappé 'experience'
        "seniority":    j.get("experience", ""),      # alias pour cohérence
        "must_have":    j.get("skills_req", ""),      # must_have DB remappé 'skills_req'
        "nice_to_have": j.get("skills_bon", ""),      # nice_to_have DB remappé 'skills_bon'
    }


def _apply_filters(
    results: list[dict],
    role: str = "",
    location: str = "",
    min_fit: int = 0,
    top_n: int = 20,
) -> list[dict]:
    """Filtre et trie une liste de résultats par rôle, location, score minimum."""
    filtered = []
    for r in results:
        if r["total"] < min_fit:
            continue
        if role:
            txt = (r.get("title", "") + " " + r.get("description", "")).lower()
            if not any(w in txt for w in role.lower().split()):
                continue
        if location:
            loc = r.get("location", "").lower()
            if location.lower() not in loc and "remote" not in loc:
                continue
        filtered.append(r)
    filtered.sort(key=lambda x: x["total"], reverse=True)
    return filtered[:top_n]


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes FastAPI
# ═══════════════════════════════════════════════════════════════════════════════

@jobs_router.get("/jobs/{user_id}")
async def stream_cached_jobs(user_id: int):
    """
    SSE stream des jobs depuis PostgreSQL pour un user_id.
    Utilisé par le dashboard pour afficher les jobs en cache.
    """
    logger.info(f"[/jobs] GET /jobs/{user_id} — fetching from DB…")

    async def _stream():
        uid  = int(user_id)
        jobs = await get_jobs_for_user(uid)
        logger.info(f"[/jobs] user_id={uid} → {len(jobs)} jobs")

        if not jobs:
            logger.warning(f"[/jobs] No jobs for user_id={uid}")
            yield sse({"event": "no_cache", "user_id": uid})
            return

        yield sse({"event": "cached", "total": len(jobs), "user_id": uid})
        for job in jobs:
            yield sse(job)
        yield sse({"event": "done", "total": len(jobs), "from_cache": True})

    return StreamingResponse(
        _stream(),
        media_type = "text/event-stream",
        headers    = {
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@jobs_router.get("/api/matches/{user_id}")
async def api_matches_get(user_id: int):
    """
    GET /api/matches/{user_id}
    Retourne les jobs depuis PostgreSQL pour le dashboard Next.js.
    Appelé directement avec le user_id dans l'URL.
    """
    logger.info(f"[GET /api/matches/{user_id}] fetching from DB…")
    db_jobs = await get_jobs_for_user(user_id)
    logger.info(f"[GET /api/matches/{user_id}] → {len(db_jobs)} jobs")

    results = [_job_to_result(j) for j in db_jobs]
    return {"matches": results, "count": len(results), "source": "db"}


@jobs_router.post("/api/matches")
async def api_matches(
    data:     ProfileIn,
    top_n:    int = 20,
    min_fit:  int = 0,
    role:     str = "",
    location: str = "",
):
    """
    POST /api/matches
    Matching avec filtres — priorité DB si user_id fourni, sinon MarketAnalysis.
    """
    skills_list = [s.strip() for s in (data.skills or [])]

    # ── Priorité : jobs depuis la DB ──────────────────────────────────────────
    if data.user_id:
        try:
            uid = int(data.user_id)
            logger.info(f"[POST /api/matches] user_id={uid} — fetching from DB")
            db_jobs = await get_jobs_for_user(uid)
            logger.info(f"[POST /api/matches] → {len(db_jobs)} jobs from DB")
        except Exception as e:
            logger.warning(f"[POST /api/matches] DB fetch failed: {e}")
            db_jobs = []

        if db_jobs:
            raw     = [_job_to_result(j, skills_list) for j in db_jobs]
            results = _apply_filters(raw, role, location, min_fit, top_n)
            return {"matches": results, "count": len(results), "source": "db"}

    # ── Fallback : MarketAnalysis (jobs en mémoire) ───────────────────────────
    if market_analysis is None:
        raise HTTPException(503, "MarketAnalysis not initialized")

    prof = _to_candidate_profile(data)
    ms   = match_jobs(market_analysis, prof, top_n=top_n)

    results = []
    for m in ms:
        j = m["job"]
        results.append({
            "title":       j.get("title", ""),
            "company":     j.get("company", ""),
            "location":    j.get("location", ""),
            "salary":      j.get("salary", ""),
            "url":         j.get("url", ""),
            "source":      j.get("source", ""),
            "date_posted": j.get("date_posted", ""),
            "total":       m["total"],
            "skill_pct":   m["skill_pct"],
            "loc_pct":     m["loc_pct"],
            "title_pct":   m["title_pct"],
            "matched":     m["matched"],
            "missing":     m["missing"],
            "verdict":     m["verdict"],
            "explanation": m.get("explanation", {}),
            "description": j.get("description", "")[:2000],
        })

    results = _apply_filters(results, role, location, min_fit, top_n)
    return {"matches": results, "count": len(results), "source": "market"}


@jobs_router.post("/api/gap")
async def api_gap(data: ProfileIn):
    """
    POST /api/gap
    Analyse le gap de compétences entre le profil utilisateur
    et les compétences demandées sur le marché.
    """
    if market_analysis is None:
        raise HTTPException(503, "MarketAnalysis not initialized")

    prof = _to_candidate_profile(data)
    if not prof.skills:
        raise HTTPException(400, "No skills provided")

    gap = compute_gap(market_analysis, prof.skills_set())
    return {
        "coverage":            gap["coverage"],
        "matched":             gap["matched"][:25],
        "missing":             gap["missing"][:25],
        "total_market_skills": gap["total_market_skills"],
    }


@jobs_router.post("/api/roadmap")
async def api_roadmap(data: ProfileIn, top_n: int = 15):
    """
    POST /api/roadmap
    Génère une roadmap d'apprentissage personnalisée basée sur
    les compétences manquantes les plus demandées sur le marché.
    """
    if market_analysis is None:
        raise HTTPException(503, "MarketAnalysis not initialized")

    prof  = _to_candidate_profile(data)
    gap   = compute_gap(market_analysis, prof.skills_set())
    miss  = gap["missing"][:top_n]
    user_skills_lower = {s.lower() for s in prof.skills}

    phases: dict[str, list] = {"beginner": [], "intermediate": [], "advanced": []}

    for rank, (skill, count) in enumerate(miss, 1):
        meta    = LEARNING_META.get(skill, {})
        d       = meta.get("d", "Intermediate").lower()
        prereqs = meta.get("pre", [])
        prereqs_met     = [p for p in prereqs if p.lower() in user_skills_lower]
        prereqs_missing = [p for p in prereqs if p.lower() not in user_skills_lower]

        why_parts = [f"Ranked #{rank} because {count} job listings require this skill."]
        if d == "beginner":
            why_parts.append("Classified as Beginner — foundational skill, learn it first.")
        elif d == "advanced":
            why_parts.append("Classified as Advanced — build intermediate skills first.")
        else:
            why_parts.append("Classified as Intermediate — core industry skill.")
        if prereqs_met:
            why_parts.append(f"You already have prerequisites: {', '.join(prereqs_met)}.")
        if prereqs_missing:
            why_parts.append(f"You'll need to learn first: {', '.join(prereqs_missing)}.")
        if not prereqs:
            why_parts.append("No prerequisites — you can start immediately.")
        impact = round(count / market_analysis.total * 100, 1) if market_analysis.total else 0
        why_parts.append(f"Learning this opens up {count} jobs ({impact}% of market).")

        entry = {
            "skill":         skill,
            "jobs_count":    count,
            "difficulty":    meta.get("d", "Intermediate"),
            "weeks":         meta.get("w", 4),
            "tip":           meta.get("tip", "Official docs + projects"),
            "prerequisites": prereqs,
            "xai": {
                "rank":              rank,
                "reason":            " ".join(why_parts),
                "market_impact_pct": impact,
                "prereqs_met":       prereqs_met,
                "prereqs_missing":   prereqs_missing,
            },
        }
        phases.get(d, phases["intermediate"]).append(entry)

    total_weeks = sum(LEARNING_META.get(s, {}).get("w", 4) for s, _ in miss)
    return {
        "phases":      phases,
        "total_weeks": total_weeks,
        "coverage":    gap["coverage"],
    }