"""
main.py — JobScan + Career Assistant  ·  Backend unifié
========================================================

ARCHITECTURE :
  Projet 1 (toi)   → pipeline scan SSE, scraping 6 sources, cosine filter,
                      matching fine-tuné, skills gap, DB PostgreSQL asyncpg
  Projet 2 (collègue) → chatbot keyword, skills gap market, roadmap,
                         market analytics, PDF report, chat history

FLOW UTILISATEUR :
  GET  /                        → login.html (saisir user_id)
  GET  /api/user/{id}           → existe ? → redirect dashboard | onboarding
  POST /api/onboarding          → summary → LLM → embedding → scan SSE
  GET  /jobs/{user_id}          → SSE stream jobs depuis DB (cache)
  POST /scan                    → SSE pipeline complet (nouveau scan)
  GET  /cv                      → index.html (dashboard projet 1)

  -- Routes projet 2 (dashboard Next.js) --
  POST /api/login               → authentification
  GET  /api/profile             → profil utilisateur
  POST /api/profile             → sauvegarder profil
  POST /api/matches             → matching jobs depuis DB
  POST /api/gap                 → skills gap marché
  POST /api/roadmap             → roadmap apprentissage
  GET  /api/market              → analytics marché
  POST /api/chat                → chatbot keyword
  GET  /api/chat/history        → historique chat
  POST /api/report              → rapport markdown
  POST /api/report/pdf          → rapport PDF
  GET  /api/status              → statut API
  GET  /profile/{user_id}       → GET profil (compat projet 1)
  PATCH /profile/{user_id}      → MAJ profil (compat projet 1)
"""

import asyncio
import io
import json
import logging
import os
import statistics
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from openai import AsyncAzureOpenAI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

import matcher as mtch
from database import (
    init_db,
    close_pool,
    upsert_user,
    insert_job,
    get_jobs_for_user,
    update_user_profile,
    get_user,
)
from llm_extractor import extract_with_llm
from scraper import (
    scrape_aijobs,
    scrape_emploitic,
    scrape_remoteok,
    scrape_tanitjobs,
    scrape_greenhouse,
    scrape_eluta,
)
from job_analyzer_agent import (
    CandidateProfile,
    MarketAnalysis,
    load_all_jobs,
    compute_gap,
    match_jobs,
    generate_roadmap,
    generate_report,
    LEARNING_META,
)

# ── Fonctions DB chat + profil (asyncpg unifié — database.py) ────────────────
from database import (
    save_chat_message as _save_chat_msg,
    load_chat_history as _load_chat_history,
)
_DB2_AVAILABLE = True


logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ═══════════════════════════════════════════════════════════════════════════════
#  App FastAPI
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="JobScan · Career Assistant", version="3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("[startup] PostgreSQL DB initialized OK")
    await _refresh_market_analysis()


async def _refresh_market_analysis():
    """
    Recharge market_analysis depuis la DB PostgreSQL.
    Appelé au startup et après chaque scan terminé.
    """
    global market_analysis
    try:
        from database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT title, description, must_have, nice_to_have, "
                "requirements, industry, location, salary, source, remote "
                "FROM jobs ORDER BY created_at DESC LIMIT 5000"
            )
        jobs_for_market = []
        for r in rows:
            jobs_for_market.append({
                "title":       r["title"] or "",
                "description": " ".join(filter(None, [
                    r["description"], r["must_have"],
                    r["nice_to_have"], r["requirements"],
                ])),
                "tags":        [t.strip() for t in (r["requirements"] or "").split(",") if t.strip()],
                "source":      r["source"] or "unknown",
                "location":    r["location"] or "",
                "company":     r["industry"] or "",
                "salary":      r["salary"] or "",
            })
        market_analysis = MarketAnalysis(jobs_for_market)
        logger.info(f"[market] Refreshed — {market_analysis.total} jobs from DB ✓")
    except Exception as e:
        logger.warning(f"[market] Refresh from DB failed, keeping current: {e}")


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


# ═══════════════════════════════════════════════════════════════════════════════
#  Chargement modèles (au démarrage)
# ═══════════════════════════════════════════════════════════════════════════════

logger.info("Loading sentence-transformer (multilingual cosine filter)...")
EMBED_MODEL = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
logger.info("Sentence-transformer ready ✓")

logger.info("Loading fine-tuned matching model...")
MATCH_MODEL, MATCH_TOKENIZER = mtch.load_model()
if MATCH_MODEL is None:
    logger.warning(
        "⚠  Fine-tuned model not found.\n"
        "   Place files in:\n"
        "     jobscan_model/finetuned_model.pt\n"
        "     jobscan_model/tokenizer/\n"
    )
else:
    logger.info("Fine-tuned model ready ✓")

# Chargement des jobs pour le MarketAnalysis (projet 2)
logger.info("Loading jobs for MarketAnalysis (project 2)...")
_jobs_for_market = load_all_jobs(BASE_DIR)
market_analysis  = MarketAnalysis(_jobs_for_market)
logger.info(f"MarketAnalysis ready — {market_analysis.total} jobs ✓")

PROFILE_FILE = "candidate_profile.json"
PROFILE_PATH = BASE_DIR / PROFILE_FILE

# ═══════════════════════════════════════════════════════════════════════════════
#  Config pipeline
# ═══════════════════════════════════════════════════════════════════════════════

COSINE_THRESHOLD           = 0.60
COSINE_THRESHOLD_EMPLOITIC = 0.60
MAX_AGE_DAYS               = 45
LLM_CONCURRENCY            = 4
NUM_SOURCES                = 6

SHARED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ═══════════════════════════════════════════════════════════════════════════════
#  Pydantic models
# ═══════════════════════════════════════════════════════════════════════════════

class ScanRequest(BaseModel):
    cv_text: str
    user_id: int = 0


class OnboardingRequest(BaseModel):
    """Nouveau user : saisit son résumé de profil → on lance le scan."""
    summary: str
    user_id: int


class ProfileRequest(BaseModel):
    """PATCH /profile/{user_id} — projet 1 compat"""
    first_name: str = None
    last_name:  str = None
    email:      str = None
    linkedin:   str = None


class ProfileIn(BaseModel):
    """Corps des routes projet 2"""
    name:                str       = ""
    target_role:         str       = ""
    experience_years:    int       = 0
    skills:              list[str] = []
    preferred_locations: list[str] = []
    open_to_remote:      bool      = True
    salary_expectation:  str       = ""
    user_id:             str       = ""


class ChatIn(BaseModel):
    message: str
    profile: ProfileIn | None = None
    user_id: str = ""


class UserLogin(BaseModel):
    user_id: str


# ═══════════════════════════════════════════════════════════════════════════════
#  Utilitaires
# ═══════════════════════════════════════════════════════════════════════════════

def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    n = np.linalg.norm(a) * np.linalg.norm(b)
    return float(np.dot(a, b) / n) if n > 0 else 0.0


def pct(score: float) -> str:
    return f"{score * 100:.2f}"


def sse(payload: dict) -> str:
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _azure_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        api_key        = os.getenv("AZURE_OPENAI_API_KEY",  ""),
        api_version    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )


def _to_candidate_profile(p: ProfileIn) -> CandidateProfile:
    d = p.model_dump()
    d.pop("user_id", None)
    return CandidateProfile(**d)


def _load_profile_file() -> CandidateProfile | None:
    if PROFILE_PATH.exists():
        try:
            return CandidateProfile.load(PROFILE_PATH)
        except Exception:
            pass
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Pages HTML (projet 1)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def serve_login():
    """Page 1 : saisie user_id."""
    return HTMLResponse((BASE_DIR / "login.html").read_text(encoding="utf-8"))


@app.get("/cv")
async def serve_cv():
    """Page 2 : dashboard projet 1 (index.html)."""
    cv_html = BASE_DIR / "cv.html"
    html    = BASE_DIR / "index.html"
    return HTMLResponse((cv_html if cv_html.exists() else html).read_text(encoding="utf-8"))


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — User / Auth (projet 1 + 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/user/{user_id}")
async def check_user(user_id: int):
    """
    Vérifie si le user existe.
    Utilisé par le frontend Next.js pour décider :
      → existe      → redirect /dashboard
      → n'existe pas → redirect /onboarding
    """
    user = await get_user(user_id)
    if user is None:
        return {"exists": False, "user_id": user_id}
    return {
        "exists": True,
        "user_id": user_id,
        "role":        user.get("role", ""),
        "name":        " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or f"User {user_id}",
        "skills":      user.get("skills", ""),
        "seniority":   user.get("seniority", ""),
        "summary":     user.get("summary", ""),
    }


@app.post("/api/login")
async def login(data: UserLogin):
    """Authentification projet 2."""
    uid = data.user_id.strip()
    if not uid:
        raise HTTPException(400, "User ID is required")
    try:
        user = await get_user(int(uid))
    except Exception:
        raise HTTPException(400, "User ID must be numeric")

    if not user:
        raise HTTPException(404, f"User ID '{uid}' not found")

    return {
        "authenticated": True,
        "user_id": uid,
        "user": {
            "name":     " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or f"User {uid}",
            "role":     user.get("role", ""),
            "skills":   user.get("skills", ""),
            "summary":  user.get("summary", ""),
            "industry": user.get("industry", ""),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Profil (projet 1 + 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/profile/{user_id}")
async def get_profile_p1(user_id: int):
    """Profil complet — compat projet 1."""
    user = await get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return {"user_id": user_id, **user}


@app.patch("/profile/{user_id}")
async def patch_profile_p1(user_id: int, req: ProfileRequest):
    """MAJ profil (first_name, last_name, email, linkedin) — compat projet 1."""
    ok = await update_user_profile(
        user_id,
        first_name=req.first_name,
        last_name=req.last_name,
        email=req.email,
        linkedin=req.linkedin,
    )
    if not ok:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found or update failed")
    return {"user_id": user_id, "updated": req.model_dump(exclude_none=True), "status": "ok"}


@app.get("/api/profile")
async def api_get_profile(user_id: str = ""):
    """Profil — route projet 2."""
    if user_id:
        try:
            user = await get_user(int(user_id))
        except Exception:
            user = None
        if user:
            skills_list = [s.strip() for s in (user.get("skills") or "").split(",") if s.strip()]
            return {
                "exists":             True,
                "user_id":            user_id,
                "name":               " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or f"User {user_id}",
                "target_role":        user.get("role", ""),
                "experience_years":   int(user.get("years_experience") or 0),
                "skills":             skills_list,
                "preferred_locations": [],
                "open_to_remote":     True,
                "salary_expectation": "",
                "seniority":          user.get("seniority", ""),
                "industry":           user.get("industry", ""),
                "education":          user.get("education", ""),
                "summary":            user.get("summary", ""),
            }

    # Fallback fichier JSON local (projet 2)
    p = _load_profile_file()
    if not p:
        return {"exists": False}
    return {"exists": True, **p.__dict__}


@app.post("/api/profile")
async def api_save_profile(data: ProfileIn):
    """Sauvegarder profil — route projet 2."""
    prof = _to_candidate_profile(data)
    prof.save(PROFILE_PATH)

    if data.user_id:
        try:
            await upsert_user(int(data.user_id), {
                "role":             data.target_role,
                "skills":           ", ".join(data.skills),
                "years_experience": str(data.experience_years),
                "summary":          data.name,
            })
        except Exception as e:
            logger.warning(f"[api/profile] upsert_user failed: {e}")

    gap = compute_gap(market_analysis, prof.skills_set()) if prof.skills else None
    return {
        "saved":                True,
        "coverage":             gap["coverage"] if gap else 0,
        "matched_skills":       len(gap["matched"]) if gap else 0,
        "total_market_skills":  gap["total_market_skills"] if gap else 0,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Onboarding nouveau user
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/onboarding")
async def onboarding(req: OnboardingRequest):
    """
    Nouveau user : résumé profil → LLM extrait données + titre →
    embedding → lance le scan SSE.
    Le frontend Next.js doit ensuite appeler POST /scan avec le cv_text.
    Ici on retourne juste les données extraites pour confirmation.
    """
    if not req.user_id or req.user_id <= 0:
        raise HTTPException(400, "user_id invalide")
    if not req.summary.strip():
        raise HTTPException(400, "summary vide")

    logger.info(f"[onboarding] user_id={req.user_id} summary_len={len(req.summary)}")

    # LLM extrait titre + structure
    cv_title      = await extract_cv_title(req.summary)
    cv_structured = await structure_cv_for_model(cv_title, req.summary)

    # Sauvegarder en DB
    ok = await upsert_user(req.user_id, cv_structured)
    if not ok:
        raise HTTPException(500, "Erreur sauvegarde DB")

    return {
        "user_id":      req.user_id,
        "cv_title":     cv_title,
        "role":         cv_structured.get("role", cv_title),
        "seniority":    cv_structured.get("seniority", ""),
        "skills":       cv_structured.get("skills", ""),
        "summary":      cv_structured.get("summary", ""),
        "ready_to_scan": True,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Jobs SSE (projet 1)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs/{user_id}")
async def stream_cached_jobs(user_id: int):
    """SSE stream des jobs depuis PostgreSQL pour ce user_id (cache)."""
    logger.info(f"[/jobs] GET /jobs/{user_id}")

    async def _stream():
        jobs = await get_jobs_for_user(user_id)
        if not jobs:
            yield sse({"event": "no_cache", "user_id": user_id})
            return
        yield sse({"event": "cached", "total": len(jobs), "user_id": user_id})
        for job in jobs:
            yield sse(job)
        yield sse({"event": "done", "total": len(jobs), "from_cache": True})

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


@app.post("/scan")
async def scan(req: ScanRequest):
    """Lance le pipeline complet SSE."""
    logger.info(f"[/scan] POST — user_id={req.user_id} cv_len={len(req.cv_text)}")
    return StreamingResponse(
        pipeline(req.cv_text.strip(), req.user_id),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Analytics marché (projet 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/status")
def api_status():
    return {
        "total_jobs":   market_analysis.total,
        "sources":      dict(market_analysis.sources.most_common()),
        "remote_ratio": round(market_analysis.remote_ratio, 3),
    }


@app.get("/api/market")
def api_market():
    top_skills     = [{"skill": s, "count": c} for s, c in market_analysis.skill_counts.most_common(30)]
    top_locations  = [{"location": l, "count": c} for l, c in market_analysis.locations.most_common(15)]
    top_companies  = [{"company": co, "count": c} for co, c in market_analysis.companies.most_common(15)]

    salaries = {}
    for cur, vals in sorted(market_analysis.salary_by_currency.items()):
        if len(vals) >= 2:
            salaries[cur] = {
                "count":  len(vals),
                "min":    round(min(vals)),
                "median": round(statistics.median(vals)),
                "max":    round(max(vals)),
            }

    return {
        "total_jobs":    market_analysis.total,
        "sources":       dict(market_analysis.sources.most_common()),
        "remote_ratio":  round(market_analysis.remote_ratio, 3),
        "top_skills":    top_skills,
        "top_locations": top_locations,
        "top_companies": top_companies,
        "salaries":      salaries,
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Matching / Gap / Roadmap (projet 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/matches")
async def api_matches(
    data: ProfileIn,
    top_n:    int = 20,
    min_fit:  int = 0,
    role:     str = "",
    location: str = "",
):
    """
    Matching jobs.
    Priorité : jobs DB de l'user (scores fins-tuné) → fallback MarketAnalysis.
    """
    results = []

    # Priorité 1 — jobs déjà scrapés pour cet user depuis la DB
    if data.user_id:
        try:
            db_jobs = await get_jobs_for_user(int(data.user_id))
        except Exception:
            db_jobs = []

        for j in db_jobs:
            score = int(float(j.get("combined_score", 0)) * 100)
            if score < min_fit:
                continue
            if role:
                txt = (j.get("title", "") + " " + j.get("description", "")).lower()
                if not any(w in txt for w in role.lower().split()):
                    continue
            if location:
                jl = j.get("location", "").lower()
                if location.lower() not in jl and "remote" not in jl:
                    continue

            gap_missing = j.get("gap_missing", [])
            skills_list = [s.strip() for s in (data.skills or [])]
            matched_skills = [
                s for s in (j.get("skills_req") or "").split(",")
                if s.strip() and s.strip().lower() in {sk.lower() for sk in skills_list}
            ]

            results.append({
                "title":       j.get("title", ""),
                "company":     j.get("industry", ""),
                "location":    j.get("location", ""),
                "salary":      j.get("salary", ""),
                "url":         j.get("url", ""),
                "source":      j.get("source", ""),
                "date_posted": j.get("pub_date", ""),
                "total":       score,
                "skill_pct":   score,
                "loc_pct":     0,
                "title_pct":   0,
                "matched":     matched_skills,
                "missing":     gap_missing[:10],
                "verdict":     "Strong" if score >= 70 else ("Good" if score >= 50 else "Partial"),
                "description": (j.get("description") or "")[:2000],
                "cosine":      round(float(j.get("cosine", 0)) * 100, 2),
                "match_score": round(float(j.get("match_score", 0) or 0) * 100, 2),
            })

        if results:
            results.sort(key=lambda x: x["total"], reverse=True)
            return {"matches": results[:top_n], "count": len(results), "source": "db"}

    # Fallback — MarketAnalysis (projet 2)
    prof = _to_candidate_profile(data)
    ms   = match_jobs(market_analysis, prof, top_n=top_n)

    for m in ms:
        if m["total"] < min_fit:
            continue
        j = m["job"]
        if role:
            txt = (j.get("title", "") + " " + j.get("description", "")).lower()
            if not any(w in txt for w in role.lower().split()):
                continue
        if location:
            jl = j.get("location", "").lower()
            if location.lower() not in jl and "remote" not in jl:
                continue

        results.append({
            "title":        j.get("title", ""),
            "company":      j.get("company", ""),
            "location":     j.get("location", ""),
            "salary":       j.get("salary", ""),
            "url":          j.get("url", ""),
            "source":       j.get("source", ""),
            "date_posted":  j.get("date_posted", ""),
            "total":        m["total"],
            "skill_pct":    m["skill_pct"],
            "loc_pct":      m["loc_pct"],
            "title_pct":    m["title_pct"],
            "matched":      m["matched"],
            "missing":      m["missing"],
            "verdict":      m["verdict"],
            "explanation":  m.get("explanation", {}),
            "description":  j.get("description", "")[:2000],
        })

    return {"matches": results, "count": len(results), "source": "market"}


@app.post("/api/gap")
def api_gap(data: ProfileIn):
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


@app.post("/api/roadmap")
def api_roadmap(data: ProfileIn, top_n: int = 15):
    prof = _to_candidate_profile(data)
    gap  = compute_gap(market_analysis, prof.skills_set())
    miss = gap["missing"][:top_n]
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
                "rank":               rank,
                "reason":             " ".join(why_parts),
                "market_impact_pct":  impact,
                "prereqs_met":        prereqs_met,
                "prereqs_missing":    prereqs_missing,
            },
        }
        phases.get(d, phases["intermediate"]).append(entry)

    total_weeks = sum(LEARNING_META.get(s, {}).get("w", 4) for s, _ in miss)
    return {"phases": phases, "total_weeks": total_weeks, "coverage": gap["coverage"]}


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Chatbot keyword (projet 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/chat")
async def api_chat(data: ChatIn):
    """
    Chatbot keyword — logique projet 2.
    Enrichi avec les jobs réels de l'user depuis la DB si user_id fourni.
    """
    prof = _to_candidate_profile(data.profile) if data.profile else (
        _load_profile_file() or CandidateProfile()
    )

    # Enrichir le profil avec les données DB si possible
    if data.user_id:
        try:
            db_user = await get_user(int(data.user_id))
            if db_user and not prof.skills:
                skills_from_db = [s.strip() for s in (db_user.get("skills") or "").split(",") if s.strip()]
                if skills_from_db:
                    prof.skills = skills_from_db
                if not prof.target_role:
                    prof.target_role = db_user.get("role", "")
                if not prof.name:
                    prof.name = " ".join(filter(None, [db_user.get("first_name"), db_user.get("last_name")])) or "there"
        except Exception as e:
            logger.warning(f"[chat] DB enrichment failed: {e}")

    gap = compute_gap(market_analysis, prof.skills_set()) if prof.skills else None
    ms  = match_jobs(market_analysis, prof, top_n=15) if prof.skills else []
    m   = data.message.lower()

    # Sauvegarder message user
    if data.user_id and _DB2_AVAILABLE:
        try:
            await _save_chat_msg(data.user_id, "user", data.message)
        except Exception:
            pass

    response, intent = "", "unknown"

    if any(w in m for w in ["match", "job", "find", "opening", "position", "offre", "emploi"]):
        if data.user_id:
            try:
                db_jobs = await get_jobs_for_user(int(data.user_id))
                if db_jobs:
                    lines = [f"Here are your **top matched jobs**, {prof.name or 'there'}:\n"]
                    for i, j in enumerate(db_jobs[:10], 1):
                        score = round(float(j.get("combined_score", 0)) * 100)
                        lines.append(
                            f"**{i}. {j.get('title','')}** at {j.get('industry','')}\n"
                            f"> {score}% match · {j.get('location','') or 'Remote'}\n"
                        )
                    response, intent = "\n".join(lines), "matches"
                    ms = []  # skip market fallback
            except Exception:
                pass

        if not response:
            if not ms:
                response, intent = "No matches found. Try broadening your skills or target role.", "matches"
            else:
                lines = [f"Here are your **top 10 job matches**, {prof.name or 'there'}:\n"]
                for i, mt in enumerate(ms[:10], 1):
                    j = mt["job"]
                    lines.append(
                        f"**{i}. {j.get('title','')}** at {j.get('company','')}\n"
                        f"> {mt['total']}% fit ({mt['verdict']}) · {j.get('location','') or 'N/A'}\n"
                    )
                response, intent = "\n".join(lines), "matches"

    elif any(w in m for w in ["gap", "missing", "lack", "need", "manque", "skill"]):
        if not gap:
            response, intent = "Enter your skills first so I can analyse your gap.", "gap"
        else:
            lines = [f"**Your market coverage: {gap['coverage']:.0%}**\n"]
            for i, (s, c) in enumerate(gap["missing"][:10], 1):
                lines.append(f"{i}. **{s}** — {c} jobs need this")
            response, intent = "\n".join(lines), "gap"

    elif any(w in m for w in ["road", "learn", "path", "plan", "study", "apprend"]):
        if not gap:
            response, intent = "I need your skills to build a roadmap.", "roadmap"
        else:
            lines = [f"**Learning Roadmap for {prof.name or 'you'}:**\n"]
            for s, c in gap["missing"][:10]:
                meta = LEARNING_META.get(s, {})
                lines.append(f"- **{s}** (~{meta.get('w',4)} wks) — *{meta.get('tip','Docs + projects')}*")
            response, intent = "\n".join(lines), "roadmap"

    elif any(w in m for w in ["salary", "pay", "earn", "money", "salaire"]):
        lines = ["**Salary insights:**\n"]
        for cur, vals in sorted(market_analysis.salary_by_currency.items()):
            if len(vals) >= 2:
                lines.append(
                    f"**{cur}** ({len(vals)} jobs): "
                    f"{min(vals):,.0f} – {max(vals):,.0f} "
                    f"(median {statistics.median(vals):,.0f})"
                )
        response, intent = "\n".join(lines), "salary"

    elif any(w in m for w in ["market", "overview", "trend", "demand", "marché"]):
        top10 = market_analysis.skill_counts.most_common(10)
        lines = [f"**Market:** {market_analysis.total} jobs, {len(market_analysis.sources)} sources\n"]
        for i, (s, c) in enumerate(top10, 1):
            lines.append(f"{i}. **{s}** — {c} mentions")
        response, intent = "\n".join(lines), "market"

    elif any(w in m for w in ["competi", "strong", "coverage", "chance", "profil"]):
        if not gap:
            response, intent = "Complete your profile first.", "competitive"
        else:
            cov = gap["coverage"]
            v   = "strong" if cov >= 0.5 else ("competitive" if cov >= 0.25 else "building")
            response, intent = (
                f"You're **{v}** at **{cov:.0%}** coverage. "
                f"Skills matched: {len(gap['matched'])} / {gap['total_market_skills']}",
                "competitive"
            )

    elif any(w in m for w in ["help", "what can", "menu", "aide"]):
        response, intent = (
            "Ask me about: **jobs**, **skills gap**, **roadmap**, **salary**, **market**, **competitiveness**",
            "help"
        )

    elif any(w in m for w in ["hi", "hello", "hey", "bonjour", "salut"]):
        response, intent = (
            f"Hey {prof.name or 'there'}! Ask me about jobs, skills, salaries, or your career path.",
            "greeting"
        )

    else:
        response, intent = (
            "Try asking about: jobs, skills gap, roadmap, salary, market, or competitiveness.",
            "unknown"
        )

    # Sauvegarder réponse assistant
    if data.user_id and _DB2_AVAILABLE:
        try:
            await _save_chat_msg(data.user_id, "assistant", response)
        except Exception:
            pass

    return {"response": response, "intent": intent}


@app.get("/api/chat/history")
async def api_chat_history(user_id: str = ""):
    if not user_id or not _DB2_AVAILABLE:
        return {"messages": []}
    try:
        msgs = await _load_chat_history(int(user_id))
    except Exception:
        msgs = []
    return {"messages": msgs}


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Report (projet 2)
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/report")
def api_report(data: ProfileIn):
    prof = _to_candidate_profile(data)
    gap  = compute_gap(market_analysis, prof.skills_set())
    ms   = match_jobs(market_analysis, prof, top_n=20)
    rm   = generate_roadmap(gap, prof)
    md   = generate_report(market_analysis, prof, gap, ms, rm)
    return {"markdown": md}


@app.post("/api/report/pdf")
def api_report_pdf(data: ProfileIn):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (
        HRFlowable, PageBreak, SimpleDocTemplate, Spacer, Table, TableStyle, Paragraph,
    )

    prof    = _to_candidate_profile(data)
    gap     = compute_gap(market_analysis, prof.skills_set())
    matches = match_jobs(market_analysis, prof, top_n=20)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm,  bottomMargin=2*cm,
        title=f"Career Report – {prof.name}",
    )

    # Couleurs thème projet 1
    BRAND       = colors.HexColor("#7b61ff")
    BRAND_GREEN = colors.HexColor("#00e5a0")
    GRAY        = colors.HexColor("#888888")
    WHITE       = colors.white

    styles  = getSampleStyleSheet()
    s_title = ParagraphStyle("T", parent=styles["Title"], fontSize=22, textColor=BRAND,
                              spaceAfter=6, fontName="Helvetica-Bold")
    s_sub   = ParagraphStyle("S", parent=styles["Normal"], fontSize=10, textColor=GRAY, spaceAfter=14)
    s_h2    = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=14, textColor=BRAND,
                              spaceBefore=16, spaceAfter=8, fontName="Helvetica-Bold")
    s_body  = ParagraphStyle("B", parent=styles["Normal"], fontSize=10, spaceAfter=4, leading=14)
    s_small = ParagraphStyle("Sm", parent=styles["Normal"], fontSize=9, textColor=GRAY,
                              spaceAfter=2, leading=12)
    s_center = ParagraphStyle("Ctr", parent=styles["Normal"], fontSize=10,
                               alignment=TA_CENTER, textColor=GRAY, spaceAfter=2)

    story = []

    # Cover
    story.append(Spacer(1, 2*cm))
    story.append(Paragraph("Career Analysis Report", s_title))
    story.append(Paragraph(
        f"Prepared for <b>{prof.name}</b> | {datetime.now().strftime('%B %d, %Y')}", s_sub))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND, spaceAfter=10))

    # 1. Profil
    story.append(Paragraph("1. Your Profile", s_h2))
    pdata = [
        ["Name",              prof.name],
        ["Target Role",       prof.target_role],
        ["Experience",        f"{prof.experience_years} years"],
        ["Skills",            ", ".join(prof.skills) or "—"],
        ["Open to Remote",    "Yes" if prof.open_to_remote else "No"],
        ["Salary Expectation",prof.salary_expectation or "Not specified"],
    ]
    t = Table(pdata, colWidths=[4*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",  (0, 0), (0, -1), colors.HexColor("#f3f0ff")),
        ("TEXTCOLOR",   (0, 0), (0, -1), BRAND),
        ("FONTNAME",    (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 10),
        ("GRID",        (0, 0), (-1, -1), 0.5, colors.HexColor("#e0daf5")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING",  (0, 0), (-1, -1), 6),
    ]))
    story.append(t)

    # 2. Market overview
    story.append(Paragraph("2. Market Overview", s_h2))
    ov = [[
        Paragraph(f"<b>{market_analysis.total}</b><br/><font size=8 color='#888'>Total Jobs</font>", s_center),
        Paragraph(f"<b>{len(market_analysis.sources)}</b><br/><font size=8 color='#888'>Sources</font>", s_center),
        Paragraph(f"<b>{market_analysis.remote_ratio:.0%}</b><br/><font size=8 color='#888'>Remote</font>", s_center),
    ]]
    t = Table(ov, colWidths=[5.5*cm, 5.5*cm, 5.5*cm])
    t.setStyle(TableStyle([
        ("ALIGN",   (0,0), (-1,-1), "CENTER"),
        ("VALIGN",  (0,0), (-1,-1), "MIDDLE"),
        ("BOX",     (0,0), (-1,-1), 1, BRAND),
        ("TOPPADDING",    (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 10),
    ]))
    story.append(t)
    story.append(Spacer(1, 5*mm))

    # 3. Competitiveness
    story.append(Paragraph("3. Your Competitiveness", s_h2))
    cov   = gap["coverage"]
    level = "Strong Candidate" if cov >= 0.5 else ("Competitive" if cov >= 0.25 else "Building Profile")
    story.append(Paragraph(
        f"Coverage: <b>{cov:.0%}</b> | "
        f"Matched: <b>{len(gap['matched'])} / {gap['total_market_skills']}</b> | "
        f"Level: <b>{level}</b>", s_body))
    story.append(Spacer(1, 4*mm))

    if gap["missing"]:
        ms_h = [["#", "Missing Skill", "Jobs Requiring It"]]
        ms_r = [[str(i), s, str(c)] for i, (s, c) in enumerate(gap["missing"][:20], 1)]
        t = Table(ms_h + ms_r, colWidths=[1.5*cm, 9*cm, 6*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#ef4444")),
            ("TEXTCOLOR",  (0,0), (-1,0), WHITE),
            ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",   (0,0), (-1,-1), 9),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [WHITE, colors.HexColor("#fef2f2")]),
            ("GRID",       (0,0), (-1,-1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING",(0,0), (-1,-1), 8),
            ("TOPPADDING", (0,0), (-1,-1), 4),
        ]))
        story.append(t)

    # 4. Best job matches
    story.append(PageBreak())
    story.append(Paragraph("4. Best Job Matches", s_h2))
    for rank, match in enumerate(matches[:15], 1):
        j   = match["job"]
        clr = "#00e5a0" if match["total"] >= 70 else ("#eab308" if match["total"] >= 40 else "#ef4444")
        story.append(Paragraph(
            f"<font color='{clr}'><b>[{rank}]</b></font> "
            f"<b>{j.get('title','N/A')}</b> — "
            f"<font color='{clr}'>{match['total']}% ({match['verdict']})</font>", s_body))
        story.append(Paragraph(
            f"<font color='#888'>Company:</font> {j.get('company','N/A')} | "
            f"<font color='#888'>Location:</font> {j.get('location','') or 'N/A'} | "
            f"<font color='#888'>Salary:</font> {j.get('salary','') or 'N/A'}", s_small))
        if match["matched"]:
            story.append(Paragraph(
                f"<font color='#00e5a0'>✓ Matching:</font> {', '.join(match['matched'][:10])}", s_small))
        if match["missing"]:
            story.append(Paragraph(
                f"<font color='#ef4444'>✗ To learn:</font> {', '.join(match['missing'][:8])}", s_small))
        story.append(Spacer(1, 3*mm))

    # Footer
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=GRAY, spaceAfter=6))
    story.append(Paragraph(
        f"<font color='#888' size=8>JobScan Career Report · Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"{market_analysis.total} jobs from {len(market_analysis.sources)} sources</font>", s_center))

    doc.build(story)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=career_report_{prof.name or 'report'}.pdf"},
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  LLM helpers (pipeline projet 1)
# ═══════════════════════════════════════════════════════════════════════════════

async def detect_and_translate_cv(cv_text: str) -> tuple[str, str, str]:
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
    excerpt    = cv_text.strip()[:600]
    try:
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model=deployment, max_tokens=8, temperature=0,
                messages=[
                    {"role": "system", "content": (
                        "Detect the language of the text. "
                        "Reply with ONLY the language name in English. "
                        "Examples: English, French, Spanish, Arabic, German"
                    )},
                    {"role": "user", "content": excerpt},
                ],
            )
        detected_lang = resp.choices[0].message.content.strip().strip(".").strip()
        logger.info(f"[lang] Detected: '{detected_lang}'")
    except Exception as e:
        logger.error(f"[lang] Detection failed: {e}")
        return cv_text, "Unknown", "no"

    if detected_lang.lower() in ("english", "en"):
        return cv_text, "English", "no"

    logger.info(f"[lang] Translating from {detected_lang} to English...")
    try:
        chunk_size       = 2000
        chunks           = [cv_text[i:i+chunk_size] for i in range(0, len(cv_text), chunk_size)]
        translated_parts = []
        for chunk in chunks:
            async with _azure_client() as az:
                resp = await az.chat.completions.create(
                    model=deployment, max_tokens=1000, temperature=0,
                    messages=[
                        {"role": "system", "content": (
                            f"Translate the following {detected_lang} resume text to English. "
                            "Preserve all structure. Reply with ONLY the translated text."
                        )},
                        {"role": "user", "content": chunk},
                    ],
                )
            translated_parts.append(resp.choices[0].message.content.strip())
        return "\n".join(translated_parts), detected_lang, "yes"
    except Exception as e:
        logger.error(f"[lang] Translation failed: {e}")
        return cv_text, detected_lang, "error"


async def extract_cv_title(cv_text: str) -> str:
    """Extrait le titre principal du CV via LLM."""
    try:
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
        excerpt    = cv_text.strip()[:800]
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model=deployment, max_tokens=15, temperature=0,
                messages=[
                    {"role": "system", "content": (
                        "Extract the main job title from the resume. "
                        "Reply with ONLY the title (1-5 words, English). "
                        "Examples: 'Data Engineer', 'ML Engineer', 'Backend Developer'"
                    )},
                    {"role": "user", "content": f"Resume:\n{excerpt}"},
                ],
            )
        title = resp.choices[0].message.content.strip().strip('"\'')
        logger.info(f"[cv_title] '{title}'")
        return title or "Software Engineer"
    except Exception as e:
        logger.error(f"[cv_title] failed: {e}")
        for line in cv_text.split("\n"):
            line = line.strip()
            if 3 <= len(line) <= 60:
                return line
        return "Software Engineer"


async def structure_cv_for_model(cv_title: str, cv_text: str) -> dict:
    """Structure le CV en format training via LLM."""
    try:
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
        excerpt    = cv_text.strip()[:2000]
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model=deployment, max_tokens=400, temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": (
                        "You extract structured fields from a resume. "
                        "Respond ONLY with a valid JSON object. "
                        "For 'skills': list EVERY technical skill mentioned ANYWHERE."
                    )},
                    {"role": "user", "content": f"""Extract these fields as JSON:
{{
  "role":             "main job title",
  "seniority":        "Junior | Mid | Senior | Lead",
  "years_experience": "number only",
  "industry":         "sector",
  "education":        "highest degree",
  "skills":           "ALL technical skills comma-separated",
  "summary":          "1 sentence professional summary",
  "bullets":          "2-3 key achievements"
}}

Resume:
{excerpt}"""},
                ],
            )
        data = json.loads(resp.choices[0].message.content.strip())
        data["role"] = cv_title

        # Normalise skills : le LLM retourne parfois une list au lieu d'une str
        raw_skills = data.get("skills", "")
        if isinstance(raw_skills, list):
            data["skills"] = ", ".join(s.strip() for s in raw_skills if s.strip())
        elif not isinstance(raw_skills, str):
            data["skills"] = str(raw_skills)

        # Normalise bullets : même problème possible
        raw_bullets = data.get("bullets", "")
        if isinstance(raw_bullets, list):
            data["bullets"] = " | ".join(s.strip() for s in raw_bullets if s.strip())
        elif not isinstance(raw_bullets, str):
            data["bullets"] = str(raw_bullets)

        logger.info(
            f"[cv_struct] role={data.get('role')} | "
            f"seniority={data.get('seniority')} | "
            f"skills={str(data.get('skills',''))[:80]}"
        )
        return data
    except Exception as e:
        logger.error(f"[cv_struct] failed: {e}")
        return {
            "role": cv_title, "seniority": "Mid",
            "years_experience": "3", "industry": "Technology",
            "education": "Bachelor", "skills": "",
            "summary": cv_title, "bullets": "",
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  Pipeline principal (SSE) — projet 1 complet
# ═══════════════════════════════════════════════════════════════════════════════

async def pipeline(cv_text: str, user_id: int = 0):
    """
    Pipeline complet :
      0. Detect language → translate
      1. Extract CV title
      2. Structure CV → LLM
      3. Encode cv_title → embedding
      4. Scrape 6 sources en parallèle
      5. Per job : cosine filter → enrich → AI score → skills gap → SSE
      6. Save to DB si user_id > 0
    """
    cutoff    = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    llm_sem   = asyncio.Semaphore(LLM_CONCURRENCY)
    has_model = MATCH_MODEL is not None

    logger.info(f"[pipeline] START — user_id={user_id} save_to_db={user_id > 0}")

    # 0. Langue
    if cv_text:
        yield sse({"event": "lang_detecting"})
        cv_text, detected_lang, translated = await detect_and_translate_cv(cv_text)
        yield sse({"event": "lang_ready", "lang": detected_lang, "translated": translated})

    # 1. Titre CV
    cv_title = await extract_cv_title(cv_text) if cv_text else "Software Engineer"
    yield sse({"event": "cv_title", "title": cv_title})

    # 2. Structuration CV
    cv_structured: dict = {}
    if cv_text:
        yield sse({"event": "cv_structuring"})
        cv_structured = await structure_cv_for_model(cv_title, cv_text)
        yield sse({
            "event":     "cv_ready",
            "role":      cv_structured.get("role", cv_title),
            "seniority": cv_structured.get("seniority", ""),
            "skills":    cv_structured.get("skills", ""),
        })

        if user_id > 0:
            logger.info(f"[pipeline] Saving user to DB: user_id={user_id}")
            ok = await upsert_user(user_id, cv_structured)
            if ok:
                yield sse({"event": "user_saved", "user_id": user_id})
                logger.info(f"[pipeline] ✅ user_id={user_id} saved")
            else:
                logger.error(f"[pipeline] ❌ Failed to save user_id={user_id}")
        else:
            logger.info("[pipeline] user_id=0 → anonymous scan")

    # 3. Encode cv_title
    cv_vec: np.ndarray = await asyncio.to_thread(
        lambda: EMBED_MODEL.encode(cv_title, convert_to_numpy=True)
    )

    # Shared state
    result_q   = asyncio.Queue()
    src_done_q = asyncio.Queue()
    pending    = {"n": 0}
    scrapers   = {"done": 0}
    all_done   = {"v": False}

    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(headers=SHARED_HEADERS, connector=connector) as session:

        # 4. Cosine filter par job
        async def handle_job(job: dict, source: str):
            job["source"] = source
            threshold = COSINE_THRESHOLD_EMPLOITIC if source == "emploitic" else COSINE_THRESHOLD
            job_vec = await asyncio.to_thread(
                lambda: EMBED_MODEL.encode(job["title"], convert_to_numpy=True)
            )
            cosine = cosine_sim(cv_vec, job_vec)
            if cosine < threshold:
                logger.info(
                    f"  [filter] SKIP cosine={cosine:.2f} < {threshold} | "
                    f"{source} | {job['title'][:50]}"
                )
                return
            logger.info(
                f"  [filter] PASS cosine={cosine:.2f} | "
                f"{source} | {job['title'][:50]}"
            )
            pending["n"] += 1
            asyncio.create_task(enrich(job, cosine))

        # 5. Enrichissement LLM + scores
        async def enrich(job: dict, cosine: float):
            async with llm_sem:
                try:
                    source = job.get("source", "")

                    if source == "emploitic":
                        from scraper import _scrape_emploitic_fetch_one
                        full_job = await _scrape_emploitic_fetch_one(job["url"], session)
                        if full_job is None:
                            return
                        details = {
                            "title":       full_job.get("title", "") or job.get("title", ""),
                            "industry":    full_job.get("company", ""),
                            "location":    full_job.get("location", ""),
                            "remote":      full_job.get("remote", ""),
                            "salary":      full_job.get("salary", "Non spécifié"),
                            "contract":    full_job.get("_emp_contract", ""),
                            "experience":  full_job.get("_emp_experience", ""),
                            "education":   full_job.get("_emp_education", ""),
                            "pub_date":    full_job.get("time_ago", ""),
                            "expired":     full_job.get("_emp_status", ""),
                            "description": full_job.get("_emp_description", ""),
                            "skills_req":  "",
                            "skills_bon":  "",
                            "all_skills":  ", ".join(full_job.get("_emp_skills", []) or []),
                            "tags":        ", ".join(full_job.get("_emp_tags", []) or []),
                        }

                    elif source == "eluta":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("remote", ""),
                            "salary":      job.get("salary", "Not specified"),
                            "contract":    job.get("_eluta_contract", ""),
                            "experience":  job.get("_eluta_experience", ""),
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_eluta_description", ""),
                            "skills_req":  job.get("_eluta_skills", ""),
                            "skills_bon":  "",
                            "all_skills":  job.get("_eluta_skills", ""),
                            "tags":        "",
                        }

                    elif source == "greenhouse":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("_gh_remote", ""),
                            "salary":      job.get("_gh_salary", "Not specified"),
                            "contract":    job.get("_gh_contract", ""),
                            "experience":  job.get("_gh_experience", ""),
                            "education":   job.get("_gh_education", ""),
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_gh_description", ""),
                            "skills_req":  job.get("_gh_skills", ""),
                            "skills_bon":  job.get("_gh_bonus", ""),
                            "all_skills":  job.get("_gh_skills", ""),
                            "tags":        job.get("_gh_tags", ""),
                        }

                    elif source == "tanitjobs":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("remote", ""),
                            "salary":      job.get("salary", "Non spécifié"),
                            "contract":    job.get("_tnj_contract", ""),
                            "experience":  job.get("_tnj_experience", ""),
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_tnj_description", ""),
                            "skills_req":  "",
                            "skills_bon":  "",
                            "all_skills":  job.get("_tnj_all_skills", ""),
                            "tags":        "",
                        }

                    else:
                        # aijobs + remoteok → extraction LLM
                        details = await extract_with_llm(
                            url=job["url"],
                            session=session,
                            cutoff=cutoff,
                        )
                        if details is None:
                            logger.warning(f"  [enrich] SKIP extract_with_llm=None | {job['url'][:60]}")
                            return

                    # b. AI match score
                    match_score = -1.0
                    if has_model and cv_structured:
                        match_score = await asyncio.to_thread(
                            lambda: mtch.predict(MATCH_MODEL, MATCH_TOKENIZER, cv_structured, details)
                        )

                    # c. Skills gap
                    gap = {"missing": [], "matched": [], "coverage": 1.0, "total": 0}
                    if cv_structured:
                        gap = await asyncio.to_thread(
                            lambda: mtch.compute_skills_gap(cv_structured, details)
                        )

                    # d. Score combiné
                    combined_score = mtch.compute_combined_score(match_score, gap)

                    logger.info(
                        f"  ✦ {job['title'][:35]:35s} "
                        f"[{source:9s}] "
                        f"cos={cosine:.2f} ai={match_score:.2f} "
                        f"comb={combined_score:.2f} "
                        f"cov={gap['coverage']:.0%} "
                        f"miss={len(gap['missing'])}/{gap['total']}"
                    )

                    # e. Card SSE
                    card = {
                        "event":   "job",
                        "url":     job["url"],
                        "source":  source,
                        "title":    details.get("title")    or job.get("title", ""),
                        "industry": details.get("industry") or job.get("company", ""),
                        "location": details.get("location") or job.get("location", ""),
                        "remote":   details.get("remote")   or job.get("remote", ""),
                        "salary":   details.get("salary")   or job.get("salary", "Not specified"),
                        "time_ago": job.get("time_ago", ""),
                        "cosine":              cosine,
                        "cosine_display":      pct(cosine),
                        "match_score":         match_score,
                        "match_score_display": pct(match_score) if match_score >= 0 else "—",
                        "combined_score":         combined_score,
                        "combined_score_display": pct(combined_score),
                        "gap_missing":  gap["missing"],
                        "gap_matched":  gap["matched"],
                        "gap_coverage": gap["coverage"],
                        "gap_total":    gap["total"],
                        "contract":    details.get("contract", "")    or job.get("_emp_contract", ""),
                        "experience":  details.get("experience", "")  or job.get("_emp_experience", ""),
                        "education":   details.get("education", "")   or job.get("_emp_education", ""),
                        "pub_date":    details.get("pub_date", ""),
                        "expired":     details.get("expired", "")     or job.get("_emp_status", ""),
                        "description": details.get("description", "") or job.get("_emp_description", ""),
                        "skills_req":  details.get("skills_req", ""),
                        "skills_bon":  details.get("skills_bon", ""),
                        "all_skills":  details.get("all_skills", ""),
                        "tags":        details.get("skills_req", "") or details.get("tags", ""),
                    }
                    await result_q.put(card)

                    # f. Save to DB
                    if user_id > 0:
                        ok = await insert_job(user_id, card)
                        if ok:
                            logger.info(f"[pipeline] ✅ job saved — user={user_id}")
                        else:
                            logger.error(f"[pipeline] ❌ job save failed — user={user_id}")

                except Exception as e:
                    logger.error(f"  [enrich] {job.get('url', '')[:60]}: {e}", exc_info=True)
                finally:
                    pending["n"] -= 1
                    if all_done["v"] and pending["n"] <= 0:
                        await result_q.put(None)

        # Scrapers 6 sources en parallèle
        async def run_source(name: str, scrape_fn, session_):
            try:
                jobs = await scrape_fn(cv_title, session_)
                for job in jobs:
                    await handle_job(job, name)
            except Exception as e:
                logger.error(f"[{name}] scraper error: {e}")
            finally:
                await src_done_q.put(sse({"event": "source_done", "source": name}))

        asyncio.create_task(run_source("aijobs",     scrape_aijobs,     session))
        asyncio.create_task(run_source("remoteok",   scrape_remoteok,   session))
        asyncio.create_task(run_source("emploitic",  scrape_emploitic,  session))
        asyncio.create_task(run_source("tanitjobs",  scrape_tanitjobs,  session))
        asyncio.create_task(run_source("greenhouse", scrape_greenhouse, session))
        asyncio.create_task(run_source("eluta",      scrape_eluta,      session))

        # 6. Streaming loop
        job_count = 0
        while True:
            while not src_done_q.empty():
                yield src_done_q.get_nowait()
                scrapers["done"] += 1

            while not result_q.empty():
                item = result_q.get_nowait()
                if item is None:
                    logger.info(f"[pipeline] END — {job_count} jobs (user_id={user_id})")
                    yield sse({"event": "done", "total": job_count})
                    asyncio.create_task(_refresh_market_analysis())
                    return
                job_count += 1
                yield sse(item)

            if scrapers["done"] >= NUM_SOURCES:
                all_done["v"] = True
                if pending["n"] <= 0:
                    logger.info(f"[pipeline] END — {job_count} jobs (user_id={user_id})")
                    yield sse({"event": "done", "total": job_count})
                    asyncio.create_task(_refresh_market_analysis())
                    return

            await asyncio.sleep(0.05)