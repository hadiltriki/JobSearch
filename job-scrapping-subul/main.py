"""
main.py — JobScan · Career Assistant  ·  Point d'entrée principal
=================================================================

Architecture modulaire :
┌──────────────────────────────────────────────────────────────┐
│  main.py             → CORS, auth, profil, market, report    │
│  chat_router.py      → chatbot IT (/api/chat, /api/chat/history) │
│  scraping_pipeline.py → scan CV + pipeline SSE (/scan)       │
│  jobs_router.py      → jobs DB, matching, gap, roadmap       │
└──────────────────────────────────────────────────────────────┘
"""

import io
import logging
import os
import statistics
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from openai import AsyncAzureOpenAI
from pydantic import BaseModel, field_validator

from database import (
    init_db, close_pool, upsert_user, get_user, get_jobs_for_user, update_user_profile,
)
from job_analyzer_agent import (
    CandidateProfile, MarketAnalysis, load_all_jobs,
    compute_gap, match_jobs, generate_roadmap, generate_report,
    order_missing_skills_by_prerequisites, LEARNING_META, sanitize_learning_tip,
)

# ── Import des 3 modules ──────────────────────────────────────────────────────
from chat_router       import chat_router
from scraping_pipeline import scraping_router
from jobs_router       import jobs_router
from voice_router      import voice_router
import jobs_router as _jr

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

PROFILE_FILE = "candidate_profile.json"
PROFILE_PATH = BASE_DIR / PROFILE_FILE


# ═══════════════════════════════════════════════════════════════════════════════
#  App FastAPI
# ═══════════════════════════════════════════════════════════════════════════════

app = FastAPI(title="JobScan · Career Assistant", version="4.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

app.include_router(chat_router)
app.include_router(scraping_router)
app.include_router(jobs_router)
app.include_router(voice_router)


# ═══════════════════════════════════════════════════════════════════════════════
#  MarketAnalysis (chargé une fois au démarrage)
# ═══════════════════════════════════════════════════════════════════════════════

logger.info("Loading jobs for MarketAnalysis…")
_jobs_for_market = load_all_jobs(BASE_DIR)
market_analysis  = MarketAnalysis(_jobs_for_market)
_jr.market_analysis = market_analysis
logger.info(f"MarketAnalysis ready — {market_analysis.total} jobs ✓")


@app.on_event("startup")
async def startup():
    await init_db()
    logger.info("[startup] PostgreSQL DB initialized ✓")
    await _refresh_market_analysis()


@app.on_event("shutdown")
async def shutdown():
    await close_pool()


async def _refresh_market_analysis():
    """Rafraîchit MarketAnalysis depuis PostgreSQL."""
    global market_analysis
    try:
        from collections import Counter
        from database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT title, description, must_have, nice_to_have, "
                "requirements, industry, location, salary, source, remote "
                "FROM jobs ORDER BY created_at DESC LIMIT 5000"
            )
        jobs_list = []
        requirement_counts: Counter = Counter()
        for r in rows:
            jobs_list.append({
                "title":       r["title"] or "",
                "description": " ".join(filter(None, [
                    r["description"], r["must_have"], r["nice_to_have"], r["requirements"],
                ])),
                "tags":    [t.strip() for t in (r["requirements"] or "").split(",") if t.strip()],
                "source":  r["source"] or "unknown",
                "location": r["location"] or "",
                "company":  r["industry"] or "",
                "salary":   r["salary"] or "",
            })
            # Count skills from job requirements (must_have) so job-card gaps appear in Skills Gap tab
            for part in (r["must_have"] or "").split(","):
                s = part.strip()
                if s:
                    requirement_counts[s] += 1
        market_analysis    = MarketAnalysis(jobs_list, requirement_counts)
        _jr.market_analysis = market_analysis
        logger.info(f"[market] Refreshed — {market_analysis.total} jobs ✓")
    except Exception as e:
        logger.warning(f"[market] Refresh failed: {e}")


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

    @field_validator("user_id", mode="before")
    @classmethod
    def user_id_to_str(cls, v):
        if v is None: return ""
        if isinstance(v, (int, float)): return str(int(v))
        return str(v) if v else ""


class ProfileRequest(BaseModel):
    first_name: str = None
    last_name:  str = None
    email:      str = None
    linkedin:   str = None


class OnboardingRequest(BaseModel):
    summary: str
    user_id: int


class UserLogin(BaseModel):
    user_id: str


# ═══════════════════════════════════════════════════════════════════════════════
#  Utilitaires
# ═══════════════════════════════════════════════════════════════════════════════

def _azure_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        api_key        = os.getenv("AZURE_OPENAI_API_KEY",  ""),
        api_version    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )


def _to_candidate_profile(p: ProfileIn) -> CandidateProfile:
    d = p.model_dump(); d.pop("user_id", None)
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
#  Routes — Auth / User
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/user/{user_id}")
async def check_user(user_id: int):
    user = await get_user(user_id)
    if user is None:
        return {"exists": False, "user_id": user_id}
    return {
        "exists": True, "user_id": user_id,
        "role":      user.get("role", ""),
        "name":      " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or f"User {user_id}",
        "skills":    user.get("skills", ""),
        "seniority": user.get("seniority", ""),
        "summary":   user.get("summary", ""),
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
        "authenticated": True, "user_id": uid,
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
        raise HTTPException(404, f"User {user_id} not found")
    return {"user_id": user_id, **user}


@app.patch("/profile/{user_id}")
async def patch_profile_p1(user_id: int, req: ProfileRequest):
    ok = await update_user_profile(
        user_id, first_name=req.first_name, last_name=req.last_name,
        email=req.email, linkedin=req.linkedin,
    )
    if not ok:
        raise HTTPException(404, f"User {user_id} not found or update failed")
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
                "exists": True, "user_id": user_id,
                "name":                " ".join(filter(None, [user.get("first_name"), user.get("last_name")])) or f"User {user_id}",
                "target_role":         user.get("role", ""),
                "experience_years":    int(user.get("years_experience") or 0),
                "skills":              skills_list,
                "preferred_locations": [],
                "open_to_remote":      True,
                "salary_expectation":  "",
                "seniority":           user.get("seniority", ""),
                "industry":            user.get("industry", ""),
                "education":           user.get("education", ""),
                "summary":             user.get("summary", ""),
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
                "role": data.target_role, "skills": ", ".join(data.skills),
                "years_experience": str(data.experience_years), "summary": data.name,
            })
        except Exception as e:
            logger.warning(f"[api/profile] upsert_user failed: {e}")
    gap = compute_gap(market_analysis, prof.skills_set()) if prof.skills else None
    return {
        "saved": True,
        "coverage":            gap["coverage"] if gap else 0,
        "matched_skills":      len(gap["matched"]) if gap else 0,
        "total_market_skills": gap["total_market_skills"] if gap else 0,
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
    from scraping_pipeline import extract_cv_title, structure_cv_for_model
    cv_title      = await extract_cv_title(req.summary)
    cv_structured = await structure_cv_for_model(cv_title, req.summary)
    ok            = await upsert_user(req.user_id, cv_structured)
    if not ok:
        raise HTTPException(500, "Erreur sauvegarde DB")
    return {
        "user_id": req.user_id, "cv_title": cv_title,
        "role":          cv_structured.get("role", cv_title),
        "seniority":     cv_structured.get("seniority", ""),
        "skills":        cv_structured.get("skills", ""),
        "summary":       cv_structured.get("summary", ""),
        "ready_to_scan": True,
    }


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
async def api_market(user_id: str | None = None):
    top_skills    = [{"skill": s, "count": c} for s, c in market_analysis.skill_counts.most_common(30)]
    top_locations = [{"location": l, "count": c} for l, c in market_analysis.locations.most_common(15)]
    top_companies = [{"company": co, "count": c} for co, c in market_analysis.companies.most_common(15)]
    salaries = {}
    for cur, vals in sorted(market_analysis.salary_by_currency.items()):
        if len(vals) >= 2:
            salaries[cur] = {
                "count": len(vals), "min": round(min(vals)),
                "median": round(statistics.median(vals)), "max": round(max(vals)),
            }
    out = {
        "total_jobs": market_analysis.total,
        "sources":    dict(market_analysis.sources.most_common()),
        "remote_ratio": round(market_analysis.remote_ratio, 3),
        "top_skills": top_skills, "top_locations": top_locations,
        "top_companies": top_companies, "salaries": salaries,
    }
    # User-specific: avg AI match score and score breakdown (Excellent / Good / etc.)
    if user_id and user_id.strip():
        try:
            uid = int(user_id.strip())
            jobs = await get_jobs_for_user(uid)
            scores = []
            for j in jobs:
                s = j.get("match_score")
                if isinstance(s, (int, float)) and s >= 0:
                    scores.append(float(s))
            if scores:
                out["avg_ai_score"] = round(statistics.mean(scores) * 100, 1)
                out["score_breakdown"] = {
                    "excellent": sum(1 for s in scores if s >= 0.75),
                    "good":      sum(1 for s in scores if 0.55 <= s < 0.75),
                    "moderate":  sum(1 for s in scores if 0.40 <= s < 0.55),
                    "low":       sum(1 for s in scores if s < 0.40),
                }
            else:
                out["avg_ai_score"] = None
                out["score_breakdown"] = {"excellent": 0, "good": 0, "moderate": 0, "low": 0}
        except (ValueError, Exception):
            out["avg_ai_score"] = None
            out["score_breakdown"] = {"excellent": 0, "good": 0, "moderate": 0, "low": 0}
    return out


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes — Report
# ═══════════════════════════════════════════════════════════════════════════════

async def _user_jobs_to_market_analysis(user_id: int) -> MarketAnalysis:
    """Build a MarketAnalysis from the jobs saved in this user's list (personalized)."""
    from collections import Counter
    jobs_raw = await get_jobs_for_user(user_id)
    jobs_list = []
    requirement_counts = Counter()
    for j in jobs_raw:
        desc_parts = [
            j.get("description") or "",
            j.get("skills_req") or "",
            j.get("skills_bon") or "",
            j.get("tags") or "",
        ]
        if isinstance(desc_parts[-1], list):
            desc_parts[-1] = " ".join(desc_parts[-1])
        tags_raw = j.get("tags") or ""
        tags_list = tags_raw if isinstance(tags_raw, list) else [t.strip() for t in str(tags_raw).split(",") if t.strip()]
        jobs_list.append({
            "title":       j.get("title") or "",
            "description": " ".join(filter(None, desc_parts)),
            "tags":        tags_list,
            "source":      j.get("source") or "unknown",
            "location":    j.get("location") or "",
            "company":     j.get("industry") or "",
            "industry":    j.get("industry") or "",
            "salary":      j.get("salary") or "",
            "url":         j.get("url") or "",
            "skills_req":  j.get("skills_req") or "",
            "must_have":   j.get("skills_req") or "",
            "skills_bon":  j.get("skills_bon") or "",
            "nice_to_have": j.get("skills_bon") or "",
        })
        for part in (j.get("skills_req") or "").split(","):
            s = part.strip()
            if s:
                requirement_counts[s] += 1
    return MarketAnalysis(jobs_list, requirement_counts)


def _sanitize_display_company(s: str) -> str:
    """Use for report/PDF: avoid showing salary or long text as company name."""
    if not s or not isinstance(s, str):
        return "—"
    s = s.strip()
    if not s:
        return "—"
    if s.lower().startswith("salary") or "$" in s or "€" in s:
        return "—"
    if len(s) > 60:
        return "—"
    return s


def _sanitize_display_location(s: str) -> str:
    """Avoid showing salary or description as location."""
    if not s or not isinstance(s, str):
        return "—"
    s = s.strip()
    if "$" in s or "€" in s or s.lower().startswith("salary") or "years" in s.lower():
        return "—"
    if len(s) > 80:
        return s[:77] + "…"
    return s


async def _user_report_matches(user_id: int, prof: CandidateProfile, top_n: int = 20) -> list:
    """
    Build report matches from the user's job list using stored DB scores
    so the report matches what the user sees in the Matches tab.
    The Matches tab displays match_score (AI BiEncoder), not combined_score,
    so we use match_score for the percentage and for sorting.
    Sanitizes company/location so we don't show salary or garbage as company name.
    """
    jobs_raw = await get_jobs_for_user(user_id)
    prof_skills_lower = prof.skills_set()
    out = []
    for j in jobs_raw:
        # Use match_score (biencoder) like the Matches tab; fallback to combined_score
        raw_score = j.get("match_score")
        if raw_score is None or raw_score < 0:
            raw_score = j.get("combined_score")
        if raw_score is not None and raw_score >= 0:
            total = round(float(raw_score) * 100)
        else:
            total = 0
        if total >= 75:
            verdict = "Strong match"
        elif total >= 55:
            verdict = "Good"
        elif total >= 40:
            verdict = "Worth applying"
        elif total >= 25:
            verdict = "Moderate"
        else:
            verdict = "Low match"
        skills_req_str = j.get("skills_req") or ""
        req_skills = [s.strip() for s in skills_req_str.split(",") if s.strip()]
        matched = [s for s in req_skills if s.lower() in prof_skills_lower]
        missing = list(j.get("gap_missing") or [])
        company = _sanitize_display_company(j.get("industry") or "")
        location = _sanitize_display_location(j.get("location") or "")
        salary = (j.get("salary") or "").strip()
        if salary and ("$" in salary or "€" in salary) and len(salary) > 50:
            salary = salary[:47] + "…"
        job_display = {
            "title":       j.get("title") or "",
            "company":     company,
            "industry":    company,
            "location":    location,
            "salary":      salary or "Not specified",
            "url":         j.get("url") or "",
            "experience":  j.get("experience") or "",
            "contract":   j.get("contract") or "",
            "remote":     j.get("remote") or "",
        }
        out.append({
            "job":     job_display,
            "total":   total,
            "verdict": verdict,
            "matched": matched,
            "missing": missing,
        })
    out.sort(key=lambda x: x["total"], reverse=True)
    return out[:top_n]


async def _report_profile(data: ProfileIn):
    """Load profile from DB when only user_id is sent (like gap/roadmap)."""
    if data.user_id and not data.skills:
        try:
            user = await get_user(int(data.user_id))
            if user:
                raw = user.get("skills") or ""
                data.skills = [s.strip() for s in raw.split(",") if s.strip()]
                if not data.target_role:
                    data.target_role = user.get("role") or ""
                name = " ".join(filter(None, [user.get("first_name"), user.get("last_name")])).strip()
                if not data.name and name:
                    data.name = name
        except Exception:
            pass
    return _to_candidate_profile(data)


@app.post("/api/report")
async def api_report(data: ProfileIn):
    prof = await _report_profile(data)
    if not prof.skills:
        raise HTTPException(400, "No skills provided. Run a CV scan first.")
    if data.user_id and data.user_id.strip():
        try:
            uid = int(data.user_id.strip())
            analysis = await _user_jobs_to_market_analysis(uid)
            ms = await _user_report_matches(uid, prof, top_n=20)
        except (ValueError, Exception):
            analysis = market_analysis
            ms = match_jobs(analysis, prof, top_n=20)
    else:
        analysis = market_analysis
        ms = match_jobs(analysis, prof, top_n=20)
    gap  = compute_gap(analysis, prof.skills_set())
    rm   = generate_roadmap(gap, prof)
    md   = generate_report(analysis, prof, gap, ms, rm)
    return {"report": md, "markdown": md}


@app.post("/api/report/pdf")
async def api_report_pdf(data: ProfileIn):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (
        HRFlowable, PageBreak, SimpleDocTemplate,
        Spacer, Table, TableStyle, Paragraph,
    )
    prof = await _report_profile(data)
    if not prof.skills:
        raise HTTPException(400, "No skills provided. Run a CV scan first.")
    if data.user_id and data.user_id.strip():
        try:
            uid = int(data.user_id.strip())
            analysis = await _user_jobs_to_market_analysis(uid)
            matches = await _user_report_matches(uid, prof, top_n=20)
        except (ValueError, Exception):
            analysis = market_analysis
            matches = match_jobs(analysis, prof, top_n=20)
    else:
        analysis = market_analysis
        matches = match_jobs(analysis, prof, top_n=20)
    gap = compute_gap(analysis, prof.skills_set())
    user_skills_lower = {s.lower() for s in prof.skills}
    miss = gap["missing"][:15]
    miss = order_missing_skills_by_prerequisites(miss, user_skills_lower)
    phases = {"beginner": [], "intermediate": [], "advanced": []}
    for skill, count in miss:
        meta = LEARNING_META.get(skill, {})
        d = meta.get("d", "Intermediate").lower()
        tip = sanitize_learning_tip(meta.get("tip", "Official docs + projects"))
        entry = {"skill": skill, "weeks": meta.get("w", 4), "tip": tip, "impact": round(count / analysis.total * 100, 1) if analysis.total else 0}
        phases.get(d, phases["intermediate"]).append(entry)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=1.8*cm, rightMargin=1.8*cm, topMargin=1.5*cm, bottomMargin=1.5*cm,
        title=f"Career Report – {prof.name or 'Candidate'}",
    )
    BRAND   = colors.HexColor("#7b61ff")
    GRAY    = colors.HexColor("#6b7280")
    DARK    = colors.HexColor("#1f2937")
    WHITE   = colors.white
    GREEN   = colors.HexColor("#10b981")
    AMBER   = colors.HexColor("#f59e0b")
    RED     = colors.HexColor("#ef4444")
    BG_LIGHT= colors.HexColor("#f8fafc")
    styles  = getSampleStyleSheet()
    s_title = ParagraphStyle("T", parent=styles["Title"], fontSize=24, textColor=BRAND, spaceAfter=4, fontName="Helvetica-Bold", alignment=TA_CENTER)
    s_sub   = ParagraphStyle("S", parent=styles["Normal"], fontSize=10, textColor=GRAY, spaceAfter=16, alignment=TA_CENTER)
    s_h2    = ParagraphStyle("H2", parent=styles["Heading2"], fontSize=13, textColor=BRAND, spaceBefore=14, spaceAfter=6, fontName="Helvetica-Bold")
    s_h3    = ParagraphStyle("H3", parent=styles["Normal"], fontSize=11, textColor=DARK, spaceBefore=10, spaceAfter=4, fontName="Helvetica-Bold")
    s_body  = ParagraphStyle("B", parent=styles["Normal"], fontSize=9, spaceAfter=3, leading=12)
    s_small = ParagraphStyle("Sm", parent=styles["Normal"], fontSize=8, textColor=GRAY, spaceAfter=2, leading=10)
    s_center= ParagraphStyle("Ctr", parent=styles["Normal"], fontSize=9, alignment=TA_CENTER, textColor=GRAY, spaceAfter=2)
    story   = []

    # Cover / header
    story.append(Spacer(1, 1.2*cm))
    story.append(Paragraph("Career Analysis Report", s_title))
    story.append(Paragraph(f"Prepared for <b>{prof.name or 'Candidate'}</b>", s_sub))
    story.append(Paragraph(datetime.now().strftime("%B %d, %Y"), s_center))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND, spaceAfter=12))
    story.append(Paragraph("1. Your Profile", s_h2))
    skills_val = ", ".join(prof.skills[:20]) or "—"
    if len(prof.skills) > 20:
        skills_val += " …"
    # Use Paragraph so skills wrap inside the table cell (no truncation)
    skills_cell = Paragraph(skills_val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), s_body)
    t = Table([
        ["Name", prof.name or "—"], ["Target role", prof.target_role or "—"],
        ["Experience", f"{prof.experience_years} years"],
        ["Skills", skills_cell],
        ["Open to remote", "Yes" if prof.open_to_remote else "No"],
        ["Salary expectation", prof.salary_expectation or "Not specified"],
    ], colWidths=[4*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(0,-1), colors.HexColor("#f3f0ff")),
        ("TEXTCOLOR",(0,0),(0,-1), BRAND), ("FONTNAME",(0,0),(0,-1), "Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 9), ("GRID",(0,0),(-1,-1), 0.4, colors.HexColor("#e5e7eb")),
        ("LEFTPADDING",(0,0),(-1,-1), 8), ("TOPPADDING",(0,0),(-1,-1), 5), ("BOTTOMPADDING",(0,0),(-1,-1), 5),
    ]))
    story.append(t)

    # 2. Market overview + insights
    story.append(Paragraph("2. Market Overview", s_h2))
    ov = [[
        Paragraph(f"<b>{analysis.total}</b><br/><font size=7>Total jobs</font>", s_center),
        Paragraph(f"<b>{len(analysis.sources)}</b><br/><font size=7>Sources</font>", s_center),
        Paragraph(f"<b>{analysis.remote_ratio:.0%}</b><br/><font size=7>Remote-friendly</font>", s_center),
    ]]
    t = Table(ov, colWidths=[5*cm, 5*cm, 5*cm])
    t.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("BOX",(0,0),(-1,-1), 1, BRAND), ("TOPPADDING",(0,0),(-1,-1), 10), ("BOTTOMPADDING",(0,0),(-1,-1), 10),
    ]))
    story.append(t)
    story.append(Spacer(1, 4*mm))
    story.append(Paragraph("Top skills in demand", s_h3))
    top_skills = analysis.skill_counts.most_common(12)
    t = Table([["Skill", "Jobs"]] + [[s, str(c)] for s, c in top_skills], colWidths=[10*cm, 5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), BG_LIGHT), ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8), ("GRID",(0,0),(-1,-1), 0.3, GRAY),
        ("LEFTPADDING",(0,0),(-1,-1), 6), ("TOPPADDING",(0,0),(-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph("Top companies hiring", s_h3))
    top_co = list(analysis.companies.most_common(10))
    t = Table([["Company", "Jobs"]] + [[c, str(n)] for c, n in top_co], colWidths=[10*cm, 5*cm]) if top_co else Table([["—", "—"]], colWidths=[10*cm, 5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), BG_LIGHT), ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8), ("GRID",(0,0),(-1,-1), 0.3, GRAY),
        ("LEFTPADDING",(0,0),(-1,-1), 6), ("TOPPADDING",(0,0),(-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ]))
    story.append(t)
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph("Top locations", s_h3))
    top_loc = list(analysis.locations.most_common(8))
    t = Table([["Location", "Jobs"]] + [[loc, str(n)] for loc, n in top_loc], colWidths=[10*cm, 5*cm]) if top_loc else Table([["—", "—"]], colWidths=[10*cm, 5*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), BG_LIGHT), ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1), 8), ("GRID",(0,0),(-1,-1), 0.3, GRAY),
        ("LEFTPADDING",(0,0),(-1,-1), 6), ("TOPPADDING",(0,0),(-1,-1), 3), ("BOTTOMPADDING",(0,0),(-1,-1), 3),
    ]))
    story.append(t)

    # 3. Skills gap
    story.append(PageBreak())
    story.append(Paragraph("3. Your Skills Gap", s_h2))
    cov = gap["coverage"]
    level = "Strong candidate" if cov >= 0.5 else ("Competitive" if cov >= 0.25 else "Building profile")
    story.append(Paragraph(
        f"Market coverage: <b>{cov:.0%}</b> · Matched skills: <b>{len(gap['matched'])}</b> / <b>{gap['total_market_skills']}</b> · <b>{level}</b>", s_body))
    story.append(Spacer(1, 4*mm))
    if gap["missing"]:
        t = Table(
            [["#", "Missing skill", "Jobs", "Impact"]] +
            [[str(i), s, str(c), f"{round(c/analysis.total*100,1)}%" if analysis.total else "—"]
             for i, (s, c) in enumerate(gap["missing"][:15], 1)],
            colWidths=[1.2*cm, 8*cm, 3*cm, 3*cm])
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0), BRAND), ("TEXTCOLOR",(0,0),(-1,0), WHITE), ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1), 8),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, colors.HexColor("#fef2f2")]),
            ("GRID",(0,0),(-1,-1), 0.3, GRAY),
            ("LEFTPADDING",(0,0),(-1,-1), 6), ("TOPPADDING",(0,0),(-1,-1), 4), ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ]))
        story.append(t)

    # 4. Learning roadmap
    story.append(Paragraph("4. Learning Roadmap", s_h2))
    total_weeks = sum(e["weeks"] for phase_list in phases.values() for e in phase_list)
    story.append(Paragraph(f"Estimated total: <b>~{total_weeks} weeks</b> (skills can be learned in parallel).", s_body))
    story.append(Spacer(1, 3*mm))
    for phase_name, phase_list in [("Beginner — foundations", phases["beginner"]), ("Intermediate — core skills", phases["intermediate"]), ("Advanced — specialization", phases["advanced"])]:
        if not phase_list:
            continue
        story.append(Paragraph(phase_name, s_h3))
        for e in phase_list:
            story.append(Paragraph(
                f"<b>{e['skill']}</b> · ~{e['weeks']}w · {e['impact']}% of jobs", s_small))
            story.append(Paragraph(f"Tip: {e['tip'][:120]}{'…' if len(e['tip'])>120 else ''}", s_small))
        story.append(Spacer(1, 2*mm))
    story.append(PageBreak())
    story.append(Paragraph("5. Best Job Matches", s_h2))
    for rank, match in enumerate(matches[:15], 1):
        j = match["job"]
        company = _sanitize_display_company(j.get("industry") or j.get("company") or "") or "N/A"
        location = _sanitize_display_location(j.get("location") or "") or "N/A"
        salary_display = (j.get("salary") or "").strip()
        if salary_display and len(salary_display) > 50:
            salary_display = salary_display[:47] + "…"
        if not salary_display:
            salary_display = "—"
        clr = "#10b981" if match["total"] >= 55 else ("#f59e0b" if match["total"] >= 40 else "#ef4444")
        story.append(Paragraph(
            f"<font color='{clr}'><b>[{rank}]</b></font> <b>{j.get('title','N/A')}</b> — "
            f"<font color='{clr}'>{match['total']}% ({match['verdict']})</font>", s_body))
        story.append(Paragraph(
            f"<font color='#6b7280'>Company:</font> {company} · "
            f"<font color='#6b7280'>Location:</font> {location} · "
            f"<font color='#6b7280'>Salary:</font> {salary_display}", s_small))
        if match["matched"]:
            story.append(Paragraph(f"✓ Matching: {', '.join(match['matched'][:8])}", s_small))
        if match["missing"]:
            story.append(Paragraph(f"✗ To learn: {', '.join(match['missing'][:6])}", s_small))
        story.append(Spacer(1, 3*mm))
    story.append(Spacer(1, 8*mm))
    story.append(HRFlowable(width="100%", thickness=1, color=GRAY, spaceAfter=4))
    story.append(Paragraph(
        f"<font color='#6b7280' size=7>JobScan Career Report · Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"{analysis.total} jobs from {len(analysis.sources)} sources</font>", s_center))
    doc.build(story)
    buf.seek(0)
    safe_name = (prof.name or "report").replace(" ", "_").replace("/", "-")[:50]
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=career_report_{safe_name}.pdf"})