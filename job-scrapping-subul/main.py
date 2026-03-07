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
from pydantic import BaseModel

from database import (
    init_db, close_pool, upsert_user, get_user, update_user_profile,
)
from job_analyzer_agent import (
    CandidateProfile, MarketAnalysis, load_all_jobs,
    compute_gap, match_jobs, generate_roadmap, generate_report,
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
        from database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT title, description, must_have, nice_to_have, "
                "requirements, industry, location, salary, source, remote "
                "FROM jobs ORDER BY created_at DESC LIMIT 5000"
            )
        jobs_list = []
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
        market_analysis    = MarketAnalysis(jobs_list)
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
def api_market():
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
    return {
        "total_jobs": market_analysis.total,
        "sources":    dict(market_analysis.sources.most_common()),
        "remote_ratio": round(market_analysis.remote_ratio, 3),
        "top_skills": top_skills, "top_locations": top_locations,
        "top_companies": top_companies, "salaries": salaries,
    }


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
        HRFlowable, PageBreak, SimpleDocTemplate,
        Spacer, Table, TableStyle, Paragraph,
    )
    prof    = _to_candidate_profile(data)
    gap     = compute_gap(market_analysis, prof.skills_set())
    matches = match_jobs(market_analysis, prof, top_n=20)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm, topMargin=2*cm, bottomMargin=2*cm,
        title=f"Career Report – {prof.name}",
    )
    BRAND = colors.HexColor("#7b61ff")
    GRAY  = colors.HexColor("#888888")
    WHITE = colors.white
    styles   = getSampleStyleSheet()
    s_title  = ParagraphStyle("T",  parent=styles["Title"],   fontSize=22, textColor=BRAND, spaceAfter=6,  fontName="Helvetica-Bold")
    s_sub    = ParagraphStyle("S",  parent=styles["Normal"],  fontSize=10, textColor=GRAY,  spaceAfter=14)
    s_h2     = ParagraphStyle("H2", parent=styles["Heading2"],fontSize=14, textColor=BRAND, spaceBefore=16, spaceAfter=8, fontName="Helvetica-Bold")
    s_body   = ParagraphStyle("B",  parent=styles["Normal"],  fontSize=10, spaceAfter=4,   leading=14)
    s_small  = ParagraphStyle("Sm", parent=styles["Normal"],  fontSize=9,  textColor=GRAY, spaceAfter=2,   leading=12)
    s_center = ParagraphStyle("Ctr",parent=styles["Normal"],  fontSize=10, alignment=TA_CENTER, textColor=GRAY, spaceAfter=2)
    story = []
    story.append(Spacer(1, 2*cm))
    story.append(Paragraph("Career Analysis Report", s_title))
    story.append(Paragraph(f"Prepared for <b>{prof.name}</b> | {datetime.now().strftime('%B %d, %Y')}", s_sub))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND, spaceAfter=10))
    story.append(Paragraph("1. Your Profile", s_h2))
    t = Table([
        ["Name", prof.name], ["Target Role", prof.target_role],
        ["Experience", f"{prof.experience_years} years"],
        ["Skills", ", ".join(prof.skills) or "—"],
        ["Open to Remote", "Yes" if prof.open_to_remote else "No"],
        ["Salary Expectation", prof.salary_expectation or "Not specified"],
    ], colWidths=[4*cm, 12*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(0,-1),colors.HexColor("#f3f0ff")),
        ("TEXTCOLOR",(0,0),(0,-1),BRAND),("FONTNAME",(0,0),(0,-1),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),10),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#e0daf5")),
        ("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),6),
    ]))
    story.append(t)
    story.append(Paragraph("2. Market Overview", s_h2))
    ov = [[
        Paragraph(f"<b>{market_analysis.total}</b><br/><font size=8>Total Jobs</font>", s_center),
        Paragraph(f"<b>{len(market_analysis.sources)}</b><br/><font size=8>Sources</font>", s_center),
        Paragraph(f"<b>{market_analysis.remote_ratio:.0%}</b><br/><font size=8>Remote</font>", s_center),
    ]]
    t = Table(ov, colWidths=[5.5*cm,5.5*cm,5.5*cm])
    t.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("BOX",(0,0),(-1,-1),1,BRAND),("TOPPADDING",(0,0),(-1,-1),10),("BOTTOMPADDING",(0,0),(-1,-1),10),
    ]))
    story.append(t); story.append(Spacer(1, 5*mm))
    story.append(Paragraph("3. Your Competitiveness", s_h2))
    cov   = gap["coverage"]
    level = "Strong Candidate" if cov >= 0.5 else ("Competitive" if cov >= 0.25 else "Building Profile")
    story.append(Paragraph(
        f"Coverage: <b>{cov:.0%}</b> | Matched: <b>{len(gap['matched'])} / {gap['total_market_skills']}</b> | Level: <b>{level}</b>", s_body))
    story.append(Spacer(1, 4*mm))
    if gap["missing"]:
        t = Table(
            [["#","Missing Skill","Jobs Requiring It"]] +
            [[str(i),s,str(c)] for i,(s,c) in enumerate(gap["missing"][:20],1)],
            colWidths=[1.5*cm,9*cm,6*cm]
        )
        t.setStyle(TableStyle([
            ("BACKGROUND",(0,0),(-1,0),colors.HexColor("#ef4444")),
            ("TEXTCOLOR",(0,0),(-1,0),WHITE),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),9),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[WHITE,colors.HexColor("#fef2f2")]),
            ("GRID",(0,0),(-1,-1),0.4,colors.HexColor("#ddd")),
            ("LEFTPADDING",(0,0),(-1,-1),8),("TOPPADDING",(0,0),(-1,-1),4),
        ]))
        story.append(t)
    story.append(PageBreak())
    story.append(Paragraph("4. Best Job Matches", s_h2))
    for rank, match in enumerate(matches[:15], 1):
        j   = match["job"]
        clr = "#00e5a0" if match["total"] >= 70 else ("#eab308" if match["total"] >= 40 else "#ef4444")
        story.append(Paragraph(
            f"<font color='{clr}'><b>[{rank}]</b></font> <b>{j.get('title','N/A')}</b> — "
            f"<font color='{clr}'>{match['total']}% ({match['verdict']})</font>", s_body))
        story.append(Paragraph(
            f"<font color='#888'>Company:</font> {j.get('company','N/A')} | "
            f"<font color='#888'>Location:</font> {j.get('location','') or 'N/A'} | "
            f"<font color='#888'>Salary:</font> {j.get('salary','') or 'N/A'}", s_small))
        if match["matched"]:
            story.append(Paragraph(f"<font color='#00e5a0'>✓ Matching:</font> {', '.join(match['matched'][:10])}", s_small))
        if match["missing"]:
            story.append(Paragraph(f"<font color='#ef4444'>✗ To learn:</font> {', '.join(match['missing'][:8])}", s_small))
        story.append(Spacer(1, 3*mm))
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width="100%", thickness=1, color=GRAY, spaceAfter=6))
    story.append(Paragraph(
        f"<font color='#888' size=8>JobScan Career Report · "
        f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"{market_analysis.total} jobs from {len(market_analysis.sources)} sources</font>", s_center))
    doc.build(story)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=career_report_{prof.name or 'report'}.pdf"})