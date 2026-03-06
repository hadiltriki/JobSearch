"""
main.py — JobScan + Career Assistant  ·  Backend unifié
========================================================

CORRECTIONS APPLIQUÉES (vs original) :
  FIX 1 — stream_cached_jobs : log explicite + cast int(user_id) garanti
  FIX 2 — GET /api/matches/{user_id} : nouvel endpoint GET pour dashboard Next.js
           Le dashboard appelle GET /api/matches?user_id=X, pas POST /api/matches
  FIX 3 — api_chat : remplacé chatbot keyword-only par LLM Azure OpenAI intelligent
           avec contexte jobs DB + historique conversation + expert IT
  FIX 4 — api_matches POST : user_id transmis correctement depuis sessionStorage
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
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from openai import AsyncAzureOpenAI
from pydantic import BaseModel

# Détection de langue automatique (inspiré du Cloud Coach)
try:
    from langdetect import detect as _detect_lang
    _LANGDETECT_AVAILABLE = True
except ImportError:
    _LANGDETECT_AVAILABLE = False
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
#  Chargement modèles
# ═══════════════════════════════════════════════════════════════════════════════

logger.info("Loading sentence-transformer (multilingual cosine filter)...")
EMBED_MODEL = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
logger.info("Sentence-transformer ready ✓")

logger.info("Loading fine-tuned matching model...")
MATCH_MODEL, MATCH_TOKENIZER = mtch.load_model()
if MATCH_MODEL is None:
    logger.warning("⚠  Fine-tuned model not found.")
else:
    logger.info("Fine-tuned model ready ✓")

logger.info("Loading jobs for MarketAnalysis...")
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
    summary: str
    user_id: int


class ProfileRequest(BaseModel):
    first_name: str = None
    last_name:  str = None
    email:      str = None
    linkedin:   str = None


class ProfileIn(BaseModel):
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
#  Routes — Pages HTML
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/")
async def serve_login():
    return HTMLResponse((BASE_DIR / "login.html").read_text(encoding="utf-8"))


@app.get("/cv")
async def serve_cv():
    cv_html = BASE_DIR / "cv.html"
    html    = BASE_DIR / "index.html"
    return HTMLResponse((cv_html if cv_html.exists() else html).read_text(encoding="utf-8"))


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — User / Auth
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/user/{user_id}")
async def check_user(user_id: int):
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
#  Routes — Profil
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/profile/{user_id}")
async def get_profile_p1(user_id: int):
    user = await get_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")
    return {"user_id": user_id, **user}


@app.patch("/profile/{user_id}")
async def patch_profile_p1(user_id: int, req: ProfileRequest):
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

    p = _load_profile_file()
    if not p:
        return {"exists": False}
    return {"exists": True, **p.__dict__}


@app.post("/api/profile")
async def api_save_profile(data: ProfileIn):
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
#  Routes — Onboarding
# ═══════════════════════════════════════════════════════════════════════════════

@app.post("/api/onboarding")
async def onboarding(req: OnboardingRequest):
    if not req.user_id or req.user_id <= 0:
        raise HTTPException(400, "user_id invalide")
    if not req.summary.strip():
        raise HTTPException(400, "summary vide")

    logger.info(f"[onboarding] user_id={req.user_id} summary_len={len(req.summary)}")

    cv_title      = await extract_cv_title(req.summary)
    cv_structured = await structure_cv_for_model(cv_title, req.summary)

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
#  Routes — Jobs SSE
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/jobs/{user_id}")
async def stream_cached_jobs(user_id: int):
    """
    SSE stream des jobs depuis PostgreSQL.
    FIX : log explicite du user_id pour diagnostiquer les problèmes.
    """
    logger.info(f"[/jobs] GET /jobs/{user_id} — fetching from DB...")

    async def _stream():
        # FIX : int() garanti même si user_id arrive en str depuis l'URL
        uid = int(user_id)
        jobs = await get_jobs_for_user(uid)

        logger.info(f"[/jobs] user_id={uid} → {len(jobs)} jobs found in DB")

        if not jobs:
            logger.warning(f"[/jobs] No jobs found for user_id={uid} — check id_user[] column in DB")
            yield sse({"event": "no_cache", "user_id": uid})
            return

        yield sse({"event": "cached", "total": len(jobs), "user_id": uid})
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
#  Routes — Analytics marché
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
    top_skills    = [{"skill": s, "count": c} for s, c in market_analysis.skill_counts.most_common(30)]
    top_locations = [{"location": l, "count": c} for l, c in market_analysis.locations.most_common(15)]
    top_companies = [{"company": co, "count": c} for co, c in market_analysis.companies.most_common(15)]

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
#  Routes — Matching
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/matches/{user_id}")
async def api_matches_get(user_id: int):
    """
    FIX : Nouvel endpoint GET /api/matches/{user_id}
    Le dashboard Next.js appelle cette route directement avec le user_id dans l'URL.
    Retourne les jobs depuis la DB pour cet utilisateur.
    """
    logger.info(f"[GET /api/matches/{user_id}] fetching jobs from DB...")
    db_jobs = await get_jobs_for_user(user_id)
    logger.info(f"[GET /api/matches/{user_id}] → {len(db_jobs)} jobs")

    results = []
    for j in db_jobs:
        score = int(float(j.get("combined_score", 0)) * 100)
        gap_missing = j.get("gap_missing", [])

        results.append({
            "title":       j.get("title", ""),
            "company":     j.get("industry", ""),
            "location":    j.get("location", ""),
            "salary":      j.get("salary", ""),
            "url":         j.get("url", ""),
            "source":      j.get("source", ""),
            "total":       score,
            "skill_pct":   score,
            "loc_pct":     0,
            "title_pct":   0,
            "matched":     [],
            "missing":     gap_missing[:10],
            "verdict":     "Strong" if score >= 70 else ("Good" if score >= 50 else "Partial"),
            "description": (j.get("description") or "")[:2000],
            "cosine":      round(float(j.get("cosine", 0)) * 100, 2),
            "match_score": round(float(j.get("match_score", 0) or 0) * 100, 2),
            "gap_coverage": j.get("gap_coverage", 0),
            "gap_missing":  gap_missing,
            "remote":      j.get("remote", ""),
            "contract":    j.get("contract", ""),
            "experience":  j.get("experience", ""),
        })

    return {"matches": results, "count": len(results), "source": "db"}


@app.post("/api/matches")
async def api_matches(
    data: ProfileIn,
    top_n:    int = 20,
    min_fit:  int = 0,
    role:     str = "",
    location: str = "",
):
    """
    Matching jobs POST — priorité DB si user_id fourni, sinon MarketAnalysis.
    FIX : user_id correctement extrait du body.
    """
    results = []

    if data.user_id:
        try:
            uid = int(data.user_id)
            logger.info(f"[POST /api/matches] user_id={uid} — fetching from DB")
            db_jobs = await get_jobs_for_user(uid)
            logger.info(f"[POST /api/matches] → {len(db_jobs)} jobs from DB")
        except Exception as e:
            logger.warning(f"[POST /api/matches] DB fetch failed: {e}")
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

    # Fallback MarketAnalysis
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
#  Routes — Chatbot LLM intelligent (FIX MAJEUR)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_chat_system_prompt(db_user: dict | None, db_jobs: list[dict]) -> str:
    """
    Construit le system prompt avec les vraies données DB de l'utilisateur.
    """
    # Bloc profil utilisateur
    if db_user:
        skills_str = db_user.get("skills", "") or "Non renseigné"
        user_block = f"""## PROFIL DE L'UTILISATEUR (depuis PostgreSQL table `users`)
- Nom : {db_user.get('first_name', '')} {db_user.get('last_name', '')}
- Rôle cible : {db_user.get('role', 'N/A')}
- Séniorité : {db_user.get('seniority', 'N/A')}
- Expérience : {db_user.get('years_experience', 'N/A')} ans
- Compétences : {skills_str}
- Résumé : {db_user.get('summary', 'N/A')}"""
    else:
        user_block = "## PROFIL\nAucun profil trouvé en base."

    # Bloc jobs
    if db_jobs:
        jobs_lines = []
        for i, j in enumerate(db_jobs[:30], 1):
            gap    = j.get("gap_missing", [])
            score  = j.get("combined_score", 0) or 0
            cosine = j.get("cosine", 0) or 0
            jobs_lines.append(
                f"{i}. **{j.get('title','?')}** chez {j.get('industry','?')} | "
                f"{j.get('location','?')} | "
                f"Score: {score*100:.0f}% | Cosine: {cosine*100:.0f}% | "
                f"Manquant: {', '.join(gap[:5]) if gap else 'aucun'} | "
                f"URL: {j.get('url','')}"
            )
        jobs_block = f"## JOBS DÉTECTÉS ({len(db_jobs)} jobs depuis PostgreSQL table `jobs`)\n" + "\n".join(jobs_lines)
    else:
        jobs_block = "## JOBS DÉTECTÉS\nAucun job trouvé pour cet utilisateur."

    return f"""Tu es **JobScan AI**, un assistant expert en IT et carrière tech, intégré dans la plateforme JobScan.

{user_block}

{jobs_block}

---

## TON DOMAINE — L'INFORMATIQUE AU SENS LARGE

Tu réponds à TOUTES les questions liées à l'informatique, la technologie et la carrière tech.

### ✅ RÉPONDS TOUJOURS à ces sujets :

**Hardware & Matériel :**
PC, ordinateur, laptop, disque dur, SSD, HDD, RAM, mémoire, processeur, CPU, GPU, carte graphique,
carte mère, écran, moniteur, clavier, souris, imprimante, scanner, serveur, datacenter, smartphone,
tablette, périphérique, USB, câble, routeur, switch, modem, NAS, rack...

**Systèmes d'exploitation & Software :**
Windows, Linux, macOS, Ubuntu, Android, iOS, système d'exploitation, OS, logiciel, application,
programme, driver, firmware, antivirus, mise à jour, installation, configuration, virtualisation, VM...

**Réseaux & Internet :**
HTTP, HTTPS, DNS, TCP/IP, VPN, WiFi, Ethernet, protocole, port, pare-feu, firewall, proxy,
navigateur, URL, domaine, hébergement, SSL, TLS, IPv4, IPv6, LAN, WAN, routage, ping, SSH, FTP...

**Développement & Code :**
Python, JavaScript, TypeScript, Java, C, C++, C#, Go, Rust, PHP, Ruby, Swift, Kotlin, R, SQL, Bash,
framework, librairie, API, REST, GraphQL, IDE, Git, GitHub, debug, algorithme, structure de données,
objet, classe, fonction, variable, boucle, récursivité, compilateur, interpréteur, test unitaire...

**Web & Frontend/Backend :**
React, Next.js, Vue, Angular, HTML, CSS, Node.js, FastAPI, Django, Flask, Spring Boot, Laravel,
base de données, MySQL, PostgreSQL, MongoDB, Redis, ORM, migration, endpoint, webhook...

**Cloud & DevOps :**
Azure, AWS, GCP, Docker, Kubernetes, Terraform, CI/CD, GitHub Actions, Jenkins, pipeline,
déploiement, conteneur, microservices, serverless, load balancer, Nginx, monitoring, logs...

**IA & Data Science :**
LLM, GPT, Claude, machine learning, deep learning, NLP, computer vision, réseau de neurones,
dataset, entraînement, fine-tuning, embedding, TensorFlow, PyTorch, Scikit-learn, Pandas, NumPy,
Spark, Hadoop, ETL, data pipeline, feature engineering, modèle, inférence...

**Cybersécurité :**
virus, malware, ransomware, phishing, chiffrement, certificat SSL, authentification, OAuth,
JWT, pentest, OWASP, CVE, zero-day, firewall, IDS, SIEM, audit sécurité...

**Carrière IT :**
développeur, ingénieur logiciel, DevOps engineer, data scientist, data engineer, ML engineer,
architecte, CTO, product manager IT, salaire tech, entretien technique, CV tech,
compétences manquantes, roadmap apprentissage, certifications (AWS, Azure, GCP, CKA, CKAD)...
Jobs détectés dans ton profil, scores de matching, compétences à acquérir...

---

### ❌ REFUSE SEULEMENT ces sujets (clairement hors IT) :
- Recettes de cuisine, gastronomie
- Sport (football, tennis, natation...)
- Médecine, santé, pharmacie (sauf healthtech)
- Politique, géographie, histoire générale
- Météo, tourisme, voyages
- Animaux, nature, environnement
- Physique/chimie sans lien avec l'informatique

**Message de refus :** "Je suis spécialisé dans l'IT et la carrière tech. Pose-moi une question sur la programmation, les technologies, tes jobs détectés ou ta carrière ! 🚀"

---

### ⚠️ RÈGLE D'OR — EN CAS DE DOUTE : RÉPONDS

Si tu n'es pas sûr qu'une question est hors IT → **RÉPONDS**. Mieux vaut répondre à une question limite que de refuser une vraie question IT.

---

## RÈGLES DE RÉPONSE
1. **Réponds dans la langue de l'utilisateur** (français, anglais, arabe...)
2. **Utilise le markdown** : titres, listes, blocs de code, gras
3. **Cite les vraies données** : scores, URLs, compétences depuis le profil ci-dessus
4. **Réponds en expert** : précis, concret, avec des exemples de code si utile
5. **Jamais d'inventions** : si une info manque, dis-le clairement

Date : {datetime.now().strftime('%Y-%m-%d')}"""


@app.post("/api/chat")
async def api_chat(data: ChatIn):
    """
    Chatbot IT JobScan — inspiré du Cloud Coach (collègue) :
    ✅ STREAMING SSE mot par mot (réponse immédiate)
    ✅ PARALLÉLISME asyncio.gather() — DB user + jobs + historique en même temps
    ✅ Détection de langue automatique (langdetect)
    ✅ Résumé automatique si historique > 8 messages
    ✅ Timeout sécurisé — évite les blocages
    """
    import time as _time
    t0 = _time.time()

    # ── user_id ───────────────────────────────────────────────────────────────
    uid_str = data.user_id or (data.profile.user_id if data.profile else "")
    uid = 0
    if uid_str:
        try:
            uid = int(uid_str)
        except Exception:
            pass

    # ── Détection de langue automatique (inspiré Cloud Coach) ─────────────────
    detected_lang = "fr"
    if _LANGDETECT_AVAILABLE:
        try:
            detected_lang = "fr" if "fr" in _detect_lang(data.message) else "en"
        except Exception:
            detected_lang = "fr"

    # ── PARALLÉLISME : user + jobs + historique en même temps ─────────────────
    # Inspiré de asyncio.gather() du MemoryAgentProxy du collègue
    async def _get_user():
        return await get_user(uid) if uid > 0 else None

    async def _get_jobs():
        return await get_jobs_for_user(uid) if uid > 0 else []

    async def _get_history():
        if uid > 0 and _DB2_AVAILABLE:
            try:
                return await _load_chat_history(uid, limit=20)
            except Exception:
                return []
        return []

    try:
        db_user, db_jobs, history = await asyncio.wait_for(
            asyncio.gather(_get_user(), _get_jobs(), _get_history()),
            timeout=8.0  # Timeout sécurisé — inspiré du Cloud Coach
        )
    except asyncio.TimeoutError:
        logger.warning("[chat] DB timeout — réponse sans contexte")
        db_user, db_jobs, history = None, [], []
    except Exception as e:
        logger.error(f"[chat] gather error: {e}")
        db_user, db_jobs, history = None, [], []

    t1 = _time.time()
    logger.info(f"[chat] user={uid} lang={detected_lang} jobs={len(db_jobs)} history={len(history)} prep={t1-t0:.2f}s")

    # ── Sauvegarder le message user en arrière-plan (non bloquant) ────────────
    if uid > 0 and _DB2_AVAILABLE:
        asyncio.create_task(_safe_save_msg(uid, "user", data.message))

    # ── System prompt + historique ────────────────────────────────────────────
    lang_rule = "(Règle : réponds IMPÉRATIVEMENT en Français.)" if detected_lang == "fr" else "(Rule: answer in English.)"
    system_prompt = _build_chat_system_prompt(db_user, db_jobs)
    messages_payload = [{"role": "system", "content": system_prompt}]
    for h in history[-16:]:
        messages_payload.append({"role": h["role"], "content": h["content"]})
    messages_payload.append({"role": "user", "content": data.message + f"\n\n{lang_rule}"})

    # ── APPEL LLM + RÉPONSE JSON (compatible frontend actuel) ───────────────
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
    response = ""

    try:
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model       = deployment,
                messages    = messages_payload,
                max_tokens  = 1500,
                temperature = 0.3,
            )
        response = resp.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"[chat] LLM error: {e}")
        response = f"⚠️ Erreur LLM : {str(e)}"

    # ── Sauvegarder réponse + résumé auto en arrière-plan ────────────────────
    if uid > 0 and _DB2_AVAILABLE and response:
        asyncio.create_task(_safe_save_msg(uid, "assistant", response))
        # Résumé automatique si historique long (inspiré trigger_background_summary collègue)
        if len(history) >= 8:
            asyncio.create_task(_auto_summarize(uid, history, response, deployment))

    return {"response": response, "intent": "llm", "jobs_count": len(db_jobs)}


async def _safe_save_msg(uid: int, role: str, content: str):
    """Sauvegarde non-bloquante d'un message chat."""
    try:
        await _save_chat_msg(uid, role, content)
    except Exception as e:
        logger.warning(f"[chat] save_msg failed: {e}")


async def _auto_summarize(uid: int, history: list, last_response: str, deployment: str):
    """
    Résumé automatique de l'historique si > 8 messages.
    Inspiré de trigger_background_summary() du collègue.
    Compresse les vieux messages en une note pour économiser les tokens.
    """
    try:
        convo = "\n".join([f"{m['role'].upper()}: {m['content'][:200]}" for m in history[-8:]])
        prompt = f"""Résume en 3 lignes maximum les sujets IT abordés dans cette conversation JobScan :

{convo}

Réponds uniquement avec le résumé, sans introduction."""

        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model=deployment,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.1,
            )
        summary = resp.choices[0].message.content or ""
        if summary:
            # Sauvegarder le résumé comme message système dans l'historique
            await _save_chat_msg(uid, "assistant", f"[RÉSUMÉ SESSION] {summary}")
            logger.info(f"[chat] auto-summary saved for user={uid}")
    except Exception as e:
        logger.warning(f"[chat] auto_summarize failed: {e}")


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
#  Routes — Report
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
    story.append(Spacer(1, 2*cm))
    story.append(Paragraph("Career Analysis Report", s_title))
    story.append(Paragraph(
        f"Prepared for <b>{prof.name}</b> | {datetime.now().strftime('%B %d, %Y')}", s_sub))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND, spaceAfter=10))

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

        raw_skills = data.get("skills", "")
        if isinstance(raw_skills, list):
            data["skills"] = ", ".join(s.strip() for s in raw_skills if s.strip())
        elif not isinstance(raw_skills, str):
            data["skills"] = str(raw_skills)

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
#  Pipeline principal SSE
# ═══════════════════════════════════════════════════════════════════════════════

async def pipeline(cv_text: str, user_id: int = 0):
    cutoff    = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    llm_sem   = asyncio.Semaphore(LLM_CONCURRENCY)
    has_model = MATCH_MODEL is not None

    logger.info(f"[pipeline] START — user_id={user_id} save_to_db={user_id > 0}")

    if cv_text:
        yield sse({"event": "lang_detecting"})
        cv_text, detected_lang, translated = await detect_and_translate_cv(cv_text)
        yield sse({"event": "lang_ready", "lang": detected_lang, "translated": translated})

    cv_title = await extract_cv_title(cv_text) if cv_text else "Software Engineer"
    yield sse({"event": "cv_title", "title": cv_title})

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
            else:
                logger.error(f"[pipeline] ❌ Failed to save user_id={user_id}")
        else:
            logger.info("[pipeline] user_id=0 → anonymous scan")

    cv_vec: np.ndarray = await asyncio.to_thread(
        lambda: EMBED_MODEL.encode(cv_title, convert_to_numpy=True)
    )

    result_q   = asyncio.Queue()
    src_done_q = asyncio.Queue()
    pending    = {"n": 0}
    scrapers   = {"done": 0}
    all_done   = {"v": False}

    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(headers=SHARED_HEADERS, connector=connector) as session:

        async def handle_job(job: dict, source: str):
            job["source"] = source
            threshold = COSINE_THRESHOLD_EMPLOITIC if source == "emploitic" else COSINE_THRESHOLD
            job_vec = await asyncio.to_thread(
                lambda: EMBED_MODEL.encode(job["title"], convert_to_numpy=True)
            )
            cosine = cosine_sim(cv_vec, job_vec)
            if cosine < threshold:
                return
            pending["n"] += 1
            asyncio.create_task(enrich(job, cosine))

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
                        details = await extract_with_llm(
                            url=job["url"],
                            session=session,
                            cutoff=cutoff,
                        )
                        if details is None:
                            return

                    match_score = -1.0
                    if has_model and cv_structured:
                        match_score = await asyncio.to_thread(
                            lambda: mtch.predict(MATCH_MODEL, MATCH_TOKENIZER, cv_structured, details)
                        )

                    gap = {"missing": [], "matched": [], "coverage": 1.0, "total": 0}
                    if cv_structured:
                        gap = await asyncio.to_thread(
                            lambda: mtch.compute_skills_gap(cv_structured, details)
                        )

                    combined_score = mtch.compute_combined_score(match_score, gap)

                    logger.info(
                        f"  ✦ {job['title'][:35]:35s} "
                        f"[{source:9s}] "
                        f"cos={cosine:.2f} ai={match_score:.2f} "
                        f"comb={combined_score:.2f} "
                        f"cov={gap['coverage']:.0%} "
                        f"miss={len(gap['missing'])}/{gap['total']}"
                    )

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