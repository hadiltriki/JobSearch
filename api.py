#!/usr/bin/env python3
"""
Career Assistant — FastAPI Backend
====================================
Run:  uvicorn api:app --reload --port 8000
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import io
import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from job_analyzer_agent import (
    CandidateProfile,
    MarketAnalysis,
    load_all_jobs,
    compute_gap,
    match_jobs,
    generate_roadmap,
    generate_report,
    PROFILE_FILE,
    LEARNING_META,
)
import db as database

# ── Data loading ────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).resolve().parent
PROFILE_PATH = DATA_DIR / PROFILE_FILE

_jobs = load_all_jobs(DATA_DIR)
analysis = MarketAnalysis(_jobs)

# ── App ─────────────────────────────────────────────────────────────────

app = FastAPI(title="Career Assistant API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup():
    database.init_db()

# ── Pydantic models ─────────────────────────────────────────────────────

class UserLogin(BaseModel):
    user_id: str


class ProfileIn(BaseModel):
    name: str = ""
    target_role: str = ""
    experience_years: int = 0
    skills: list[str] = []
    preferred_locations: list[str] = []
    open_to_remote: bool = True
    salary_expectation: str = ""
    user_id: str = ""


class ChatIn(BaseModel):
    message: str
    profile: ProfileIn | None = None
    user_id: str = ""


def _load_profile_file() -> CandidateProfile | None:
    if PROFILE_PATH.exists():
        try:
            return CandidateProfile.load(PROFILE_PATH)
        except Exception:
            pass
    return None


def _to_profile(p: ProfileIn) -> CandidateProfile:
    d = p.model_dump()
    d.pop("user_id", None)
    return CandidateProfile(**d)


# ═══════════════════════════════════════════════════════════════════════
#  Endpoints
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/login")
def login(data: UserLogin):
    uid = data.user_id.strip()
    if not uid:
        raise HTTPException(400, "User ID is required")
    if database.DSN:
        user = database.find_user(uid)
        if not user:
            raise HTTPException(404, f"User ID '{uid}' not found")
        return {
            "authenticated": True,
            "user_id": uid,
            "user": user,
        }
    return {"authenticated": True, "user_id": uid}


@app.get("/api/status")
def status():
    return {
        "total_jobs": analysis.total,
        "sources": dict(analysis.sources.most_common()),
        "remote_ratio": round(analysis.remote_ratio, 3),
    }


@app.get("/api/profile")
def get_profile(user_id: str = ""):
    if user_id and database.DSN:
        db_prof = database.load_profile(user_id)
        if db_prof:
            return {"exists": True, **db_prof}
    p = _load_profile_file()
    if not p:
        return {"exists": False}
    return {"exists": True, **p.__dict__}


@app.post("/api/profile")
def save_profile(data: ProfileIn):
    prof = _to_profile(data)
    prof.save(PROFILE_PATH)
    if data.user_id and database.DSN:
        database.save_profile(data.user_id, data.model_dump())
    gap = compute_gap(analysis, prof.skills_set()) if prof.skills else None
    return {
        "saved": True,
        "coverage": gap["coverage"] if gap else 0,
        "matched_skills": len(gap["matched"]) if gap else 0,
        "total_market_skills": gap["total_market_skills"] if gap else 0,
    }


@app.post("/api/matches")
def get_matches(data: ProfileIn, top_n: int = 20, min_fit: int = 0,
                role: str = "", location: str = ""):
    prof = _to_profile(data)
    ms = match_jobs(analysis, prof, top_n=top_n)

    results = []
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
            "title": j.get("title", ""),
            "company": j.get("company", ""),
            "location": j.get("location", ""),
            "salary": j.get("salary", ""),
            "url": j.get("url", ""),
            "source": j.get("source", ""),
            "date_posted": j.get("date_posted", ""),
            "total": m["total"],
            "skill_pct": m["skill_pct"],
            "loc_pct": m["loc_pct"],
            "title_pct": m["title_pct"],
            "matched": m["matched"],
            "missing": m["missing"],
            "verdict": m["verdict"],
            "explanation": m.get("explanation", {}),
            "description": j.get("description", "")[:2000],
        })
    return {"matches": results, "count": len(results)}


@app.post("/api/gap")
def get_gap(data: ProfileIn):
    prof = _to_profile(data)
    if not prof.skills:
        raise HTTPException(400, "No skills provided")
    gap = compute_gap(analysis, prof.skills_set())
    return {
        "coverage": gap["coverage"],
        "matched": gap["matched"][:25],
        "missing": gap["missing"][:25],
        "total_market_skills": gap["total_market_skills"],
    }


@app.post("/api/roadmap")
def get_roadmap(data: ProfileIn, top_n: int = 15):
    prof = _to_profile(data)
    gap = compute_gap(analysis, prof.skills_set())
    miss = gap["missing"][:top_n]
    user_skills_lower = {s.lower() for s in prof.skills}
    phases: dict[str, list] = {"beginner": [], "intermediate": [], "advanced": []}
    for rank, (skill, count) in enumerate(miss, 1):
        meta = LEARNING_META.get(skill, {})
        d = meta.get("d", "Intermediate").lower()
        prereqs = meta.get("pre", [])
        prereqs_met = [p for p in prereqs if p.lower() in user_skills_lower]
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

        impact = round(count / analysis.total * 100, 1) if analysis.total else 0
        why_parts.append(f"Learning this opens up {count} jobs ({impact}% of market).")

        entry = {
            "skill": skill,
            "jobs_count": count,
            "difficulty": meta.get("d", "Intermediate"),
            "weeks": meta.get("w", 4),
            "tip": meta.get("tip", "Official docs + projects"),
            "prerequisites": prereqs,
            "xai": {
                "rank": rank,
                "reason": " ".join(why_parts),
                "market_impact_pct": impact,
                "prereqs_met": prereqs_met,
                "prereqs_missing": prereqs_missing,
            },
        }
        phases.get(d, phases["intermediate"]).append(entry)
    total_weeks = sum(LEARNING_META.get(s, {}).get("w", 4) for s, _ in miss)
    return {"phases": phases, "total_weeks": total_weeks, "coverage": gap["coverage"]}


@app.get("/api/market")
def get_market():
    top_skills = [{"skill": s, "count": c} for s, c in analysis.skill_counts.most_common(30)]
    top_locations = [{"location": l, "count": c} for l, c in analysis.locations.most_common(15)]
    top_companies = [{"company": co, "count": c} for co, c in analysis.companies.most_common(15)]

    salaries = {}
    for cur, vals in sorted(analysis.salary_by_currency.items()):
        if len(vals) >= 2:
            salaries[cur] = {
                "count": len(vals),
                "min": round(min(vals)),
                "median": round(statistics.median(vals)),
                "max": round(max(vals)),
            }

    return {
        "total_jobs": analysis.total,
        "sources": dict(analysis.sources.most_common()),
        "remote_ratio": round(analysis.remote_ratio, 3),
        "top_skills": top_skills,
        "top_locations": top_locations,
        "top_companies": top_companies,
        "salaries": salaries,
    }


@app.post("/api/chat")
def chat(data: ChatIn):
    prof = _to_profile(data.profile) if data.profile else _load_profile_file() or CandidateProfile()
    gap = compute_gap(analysis, prof.skills_set()) if prof.skills else None
    ms = match_jobs(analysis, prof, top_n=15) if prof.skills else []
    m = data.message.lower()

    if data.user_id and database.DSN:
        database.save_chat_message(data.user_id, "user", data.message)

    response, intent = "", "unknown"

    if any(w in m for w in ["match", "job", "find", "opening", "position"]):
        if not ms:
            response, intent = "No matches found. Try broadening your skills or target role.", "matches"
        else:
            lines = [f"Here are your **top 10 job matches**, {prof.name}:\n"]
            for i, mt in enumerate(ms[:10], 1):
                j = mt["job"]
                lines.append(
                    f"**{i}. {j.get('title','')}** at {j.get('company','')}\n"
                    f"> {mt['total']}% fit ({mt['verdict']}) · "
                    f"{j.get('location','') or 'N/A'}\n"
                )
            response, intent = "\n".join(lines), "matches"

    elif any(w in m for w in ["gap", "missing", "lack", "need"]):
        if not gap:
            response, intent = "Enter your skills first so I can analyse your gap.", "gap"
        else:
            lines = [f"**Your market coverage: {gap['coverage']:.0%}**\n"]
            for i, (s, c) in enumerate(gap["missing"][:10], 1):
                lines.append(f"{i}. **{s}** — {c} jobs need this")
            response, intent = "\n".join(lines), "gap"

    elif any(w in m for w in ["road", "learn", "path", "plan", "study"]):
        if not gap:
            response, intent = "I need your skills to build a roadmap.", "roadmap"
        else:
            lines = [f"**Learning Roadmap for {prof.name}:**\n"]
            for s, c in gap["missing"][:10]:
                meta = LEARNING_META.get(s, {})
                lines.append(f"- **{s}** (~{meta.get('w',4)} wks) — *{meta.get('tip','Docs + projects')}*")
            response, intent = "\n".join(lines), "roadmap"

    elif any(w in m for w in ["salary", "pay", "earn", "money"]):
        lines = ["**Salary insights:**\n"]
        for cur, vals in sorted(analysis.salary_by_currency.items()):
            if len(vals) >= 2:
                lines.append(f"**{cur}** ({len(vals)} jobs): {min(vals):,.0f} – {max(vals):,.0f} (median {statistics.median(vals):,.0f})")
        response, intent = "\n".join(lines), "salary"

    elif any(w in m for w in ["market", "overview", "trend", "demand"]):
        top10 = analysis.skill_counts.most_common(10)
        lines = [f"**Market:** {analysis.total} jobs, {len(analysis.sources)} sources\n"]
        for i, (s, c) in enumerate(top10, 1):
            lines.append(f"{i}. **{s}** — {c} mentions")
        response, intent = "\n".join(lines), "market"

    elif any(w in m for w in ["competi", "strong", "coverage", "chance"]):
        if not gap:
            response, intent = "Complete your profile first.", "competitive"
        else:
            cov = gap["coverage"]
            v = "strong" if cov >= 0.5 else ("competitive" if cov >= 0.25 else "building")
            response, intent = f"You're **{v}** at **{cov:.0%}** coverage. Skills matched: {len(gap['matched'])} / {gap['total_market_skills']}", "competitive"

    elif any(w in m for w in ["help", "what can", "menu"]):
        response, intent = "Ask me about: **jobs**, **skills gap**, **roadmap**, **salary**, **market**, **competitiveness**", "help"

    elif any(w in m for w in ["hi", "hello", "hey"]):
        response, intent = f"Hey {prof.name or 'there'}! Ask me about jobs, skills, salaries, or your career path.", "greeting"

    else:
        response, intent = "Try asking about: jobs, skills gap, roadmap, salary, market, or competitiveness.", "unknown"

    if data.user_id and database.DSN:
        database.save_chat_message(data.user_id, "assistant", response)

    return {"response": response, "intent": intent}


@app.get("/api/chat/history")
def chat_history(user_id: str = ""):
    if not user_id or not database.DSN:
        return {"messages": []}
    msgs = database.load_chat_history(user_id)
    return {"messages": msgs}


@app.post("/api/report")
def generate_report_endpoint(data: ProfileIn):
    prof = _to_profile(data)
    gap = compute_gap(analysis, prof.skills_set())
    ms = match_jobs(analysis, prof, top_n=20)
    rm = generate_roadmap(gap, prof)
    md = generate_report(analysis, prof, gap, ms, rm)
    return {"markdown": md}


# ═══════════════════════════════════════════════════════════════════════
#  PDF Generation
# ═══════════════════════════════════════════════════════════════════════

def _build_pdf(prof: CandidateProfile, analysis: MarketAnalysis) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm, mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        HRFlowable, PageBreak,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2 * cm, rightMargin=2 * cm,
        topMargin=2 * cm, bottomMargin=2 * cm,
        title=f"Career Report – {prof.name}",
    )

    BRAND = colors.HexColor("#6c63ff")
    BRAND_LIGHT = colors.HexColor("#a78bfa")
    DARK_BG = colors.HexColor("#1e1e2e")
    GRAY = colors.HexColor("#888888")
    WHITE = colors.white
    BLACK = colors.black

    styles = getSampleStyleSheet()
    s_title = ParagraphStyle("RTitle", parent=styles["Title"], fontSize=24,
                             textColor=BRAND, spaceAfter=6, fontName="Helvetica-Bold")
    s_subtitle = ParagraphStyle("RSub", parent=styles["Normal"], fontSize=10,
                                textColor=GRAY, spaceAfter=18)
    s_h2 = ParagraphStyle("RH2", parent=styles["Heading2"], fontSize=15,
                          textColor=BRAND, spaceBefore=18, spaceAfter=8,
                          fontName="Helvetica-Bold")
    s_h3 = ParagraphStyle("RH3", parent=styles["Heading3"], fontSize=12,
                          textColor=BRAND_LIGHT, spaceBefore=12, spaceAfter=6,
                          fontName="Helvetica-Bold")
    s_body = ParagraphStyle("RBody", parent=styles["Normal"], fontSize=10,
                            spaceAfter=4, leading=14)
    s_small = ParagraphStyle("RSmall", parent=styles["Normal"], fontSize=9,
                             textColor=GRAY, spaceAfter=2, leading=12)
    s_center = ParagraphStyle("RCenter", parent=styles["Normal"], fontSize=10,
                              alignment=TA_CENTER, textColor=GRAY, spaceAfter=2)

    gap = compute_gap(analysis, prof.skills_set())
    matches = match_jobs(analysis, prof, top_n=20)
    roadmap_data = []
    miss = gap["missing"][:15]
    phases: dict[str, list] = {"beginner": [], "intermediate": [], "advanced": []}
    for skill, count in miss:
        meta = LEARNING_META.get(skill, {})
        d = meta.get("d", "Intermediate").lower()
        entry = {
            "skill": skill, "jobs_count": count,
            "difficulty": meta.get("d", "Intermediate"),
            "weeks": meta.get("w", 4),
            "tip": meta.get("tip", "Official docs + projects"),
            "prerequisites": meta.get("pre", []),
        }
        phases.get(d, phases["intermediate"]).append(entry)
    total_weeks = sum(LEARNING_META.get(s, {}).get("w", 4) for s, _ in miss)

    story: list = []

    # ── Cover ────────────────────────────────────────────────────────
    story.append(Spacer(1, 3 * cm))
    story.append(Paragraph("Career Analysis Report", s_title))
    story.append(Paragraph(
        f"Prepared for <b>{prof.name}</b> &nbsp;|&nbsp; "
        f"{datetime.now().strftime('%B %d, %Y')}", s_subtitle))
    story.append(HRFlowable(width="100%", thickness=2, color=BRAND, spaceAfter=12))

    # ── 1. Profile ───────────────────────────────────────────────────
    story.append(Paragraph("1 &nbsp; Your Profile", s_h2))
    profile_data = [
        ["Name", Paragraph(prof.name, s_body)],
        ["Target Role", Paragraph(prof.target_role, s_body)],
        ["Experience", Paragraph(f"{prof.experience_years} years", s_body)],
        ["Skills", Paragraph(", ".join(prof.skills) or "—", s_body)],
        ["Preferred Locations", Paragraph(", ".join(prof.preferred_locations) or "Anywhere", s_body)],
        ["Open to Remote", Paragraph("Yes" if prof.open_to_remote else "No", s_body)],
        ["Salary Expectation", Paragraph(prof.salary_expectation or "Not specified", s_body)],
    ]
    t = Table(profile_data, colWidths=[4.5 * cm, 12 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f0ff")),
        ("TEXTCOLOR", (0, 0), (0, -1), BRAND),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e0daf5")),
    ]))
    story.append(t)

    # ── 2. Market Overview ───────────────────────────────────────────
    story.append(Paragraph("2 &nbsp; Market Overview", s_h2))
    overview_data = [
        [Paragraph(f"<b>{analysis.total}</b><br/><font size=8 color='#888'>Total Jobs</font>", s_center),
         Paragraph(f"<b>{len(analysis.sources)}</b><br/><font size=8 color='#888'>Sources</font>", s_center),
         Paragraph(f"<b>{analysis.remote_ratio:.0%}</b><br/><font size=8 color='#888'>Remote</font>", s_center)],
    ]
    t = Table(overview_data, colWidths=[5.5 * cm, 5.5 * cm, 5.5 * cm])
    t.setStyle(TableStyle([
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOX", (0, 0), (-1, -1), 1, BRAND),
        ("INNERGRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e0daf5")),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    story.append(t)
    story.append(Spacer(1, 6 * mm))

    src_header = [Paragraph("<b>Source</b>", s_body), Paragraph("<b>Jobs</b>", s_body)]
    src_rows = [[s, str(c)] for s, c in analysis.sources.most_common()]
    t = Table([src_header] + src_rows, colWidths=[10 * cm, 6.5 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BRAND),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f8f6ff")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)

    # ── 3. Competitiveness ───────────────────────────────────────────
    story.append(Paragraph("3 &nbsp; Your Competitiveness", s_h2))
    cov = gap["coverage"]
    level = "Strong Candidate" if cov >= 0.5 else ("Competitive" if cov >= 0.25 else "Building Profile")
    story.append(Paragraph(
        f"Market coverage: <b>{cov:.0%}</b> &nbsp;·&nbsp; "
        f"Skills matched: <b>{len(gap['matched'])} / {gap['total_market_skills']}</b> &nbsp;·&nbsp; "
        f"Level: <b>{level}</b>", s_body))
    story.append(Spacer(1, 4 * mm))

    story.append(Paragraph("Skills You Have (In Demand)", s_h3))
    if gap["matched"]:
        sk_header = [Paragraph("<b>Skill</b>", s_body), Paragraph("<b>Jobs Requiring It</b>", s_body)]
        sk_rows = [[s, str(c)] for s, c in gap["matched"][:20]]
        t = Table([sk_header] + sk_rows, colWidths=[10 * cm, 6.5 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#22c55e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f0fdf4")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)

    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph("Top Missing Skills", s_h3))
    if gap["missing"]:
        ms_header = [
            Paragraph("<b>#</b>", s_body),
            Paragraph("<b>Skill</b>", s_body),
            Paragraph("<b>Jobs Requiring It</b>", s_body),
        ]
        ms_rows = [[str(i), s, str(c)] for i, (s, c) in enumerate(gap["missing"][:20], 1)]
        t = Table([ms_header] + ms_rows, colWidths=[1.5 * cm, 9 * cm, 6 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#ef4444")),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#fef2f2")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)

    # ── 4. Job Matches ───────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("4 &nbsp; Best Job Matches", s_h2))
    for rank, m in enumerate(matches[:15], 1):
        j = m["job"]
        fit_color = "#22c55e" if m["total"] >= 70 else ("#eab308" if m["total"] >= 40 else "#ef4444")
        story.append(Paragraph(
            f"<font color='{fit_color}'><b>[{rank}]</b></font> &nbsp;"
            f"<b>{j.get('title', 'N/A')}</b> &nbsp;— &nbsp;"
            f"<font color='{fit_color}'>{m['total']}% fit ({m['verdict']})</font>", s_body))
        story.append(Paragraph(
            f"<font color='#888'>Company:</font> {j.get('company', 'N/A')} &nbsp;|&nbsp; "
            f"<font color='#888'>Location:</font> {j.get('location', '') or 'N/A'} &nbsp;|&nbsp; "
            f"<font color='#888'>Salary:</font> {j.get('salary', '') or 'N/A'}", s_small))
        if m["matched"]:
            story.append(Paragraph(
                f"<font color='#22c55e'>✓ Matching:</font> {', '.join(m['matched'][:10])}", s_small))
        if m["missing"]:
            story.append(Paragraph(
                f"<font color='#ef4444'>✗ To learn:</font> {', '.join(m['missing'][:8])}", s_small))
        story.append(Spacer(1, 3 * mm))

    # ── 5. Salary Insights ───────────────────────────────────────────
    story.append(Paragraph("5 &nbsp; Salary Insights", s_h2))
    sal_rows_data = []
    for cur, vals in sorted(analysis.salary_by_currency.items()):
        if len(vals) < 2:
            continue
        sal_rows_data.append([
            cur, str(len(vals)),
            f"{min(vals):,.0f}", f"{statistics.median(vals):,.0f}", f"{max(vals):,.0f}"
        ])
    if sal_rows_data:
        sal_header = [
            Paragraph("<b>Currency</b>", s_body),
            Paragraph("<b>Jobs</b>", s_body),
            Paragraph("<b>Min</b>", s_body),
            Paragraph("<b>Median</b>", s_body),
            Paragraph("<b>Max</b>", s_body),
        ]
        t = Table([sal_header] + sal_rows_data,
                  colWidths=[3 * cm, 2.5 * cm, 3.5 * cm, 3.5 * cm, 4 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f8f6ff")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)
    else:
        story.append(Paragraph("No salary data available.", s_body))

    # ── 6. Top Locations & Companies ─────────────────────────────────
    story.append(Paragraph("6 &nbsp; Top Locations", s_h2))
    loc_header = [Paragraph("<b>Location</b>", s_body), Paragraph("<b>Jobs</b>", s_body)]
    loc_rows = [[loc, str(c)] for loc, c in analysis.locations.most_common(15)]
    if loc_rows:
        t = Table([loc_header] + loc_rows, colWidths=[12 * cm, 4.5 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f8f6ff")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)

    story.append(Paragraph("7 &nbsp; Top Companies", s_h2))
    co_header = [Paragraph("<b>Company</b>", s_body), Paragraph("<b>Job Listings</b>", s_body)]
    co_rows = [[co, str(c)] for co, c in analysis.companies.most_common(15)]
    if co_rows:
        t = Table([co_header] + co_rows, colWidths=[12 * cm, 4.5 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f8f6ff")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(t)

    # ── 8. Most In-Demand Skills ─────────────────────────────────────
    story.append(Paragraph("8 &nbsp; Most In-Demand Skills", s_h2))
    skill_header = [
        Paragraph("<b>#</b>", s_body),
        Paragraph("<b>Skill</b>", s_body),
        Paragraph("<b>Job Mentions</b>", s_body),
        Paragraph("<b>Status</b>", s_body),
    ]
    my_skills_lower = {s.lower() for s in prof.skills}
    sk_rows2 = []
    for i, (sk, cnt) in enumerate(analysis.skill_counts.most_common(30), 1):
        have = sk.lower() in my_skills_lower
        status = "<font color='#22c55e'>✓ You have it</font>" if have else "<font color='#ef4444'>✗ To learn</font>"
        sk_rows2.append([str(i), sk, str(cnt), Paragraph(status, s_body)])
    t = Table([skill_header] + sk_rows2, colWidths=[1.5 * cm, 7 * cm, 4 * cm, 4 * cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), BRAND),
        ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#f8f6ff")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(t)

    # ── 9. Learning Roadmap ──────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("9 &nbsp; Learning Roadmap", s_h2))
    story.append(Paragraph(
        f"Estimated total: <b>~{total_weeks} weeks</b> (many can be learned in parallel — cut by 40–60%)",
        s_body))
    story.append(Spacer(1, 4 * mm))

    phase_labels = [("beginner", "Foundations", "#22c55e"),
                    ("intermediate", "Core Skills", "#eab308"),
                    ("advanced", "Specialisation", "#ef4444")]
    for phase_key, label, clr in phase_labels:
        items = phases.get(phase_key, [])
        if not items:
            continue
        story.append(Paragraph(f"<font color='{clr}'><b>{label}</b></font>", s_h3))
        rm_header = [
            Paragraph("<b>Skill</b>", s_body),
            Paragraph("<b>Jobs</b>", s_body),
            Paragraph("<b>Weeks</b>", s_body),
            Paragraph("<b>Tip</b>", s_body),
        ]
        rm_rows = [[it["skill"], str(it["jobs_count"]), f"~{it['weeks']}", it["tip"]] for it in items]
        t = Table([rm_header] + rm_rows, colWidths=[4 * cm, 2.5 * cm, 2 * cm, 8 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(clr)),
            ("TEXTCOLOR", (0, 0), (-1, 0), WHITE),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, colors.HexColor("#fafafa")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#ddd")),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(t)
        story.append(Spacer(1, 4 * mm))

    # ── Footer ───────────────────────────────────────────────────────
    story.append(Spacer(1, 1 * cm))
    story.append(HRFlowable(width="100%", thickness=1, color=GRAY, spaceAfter=6))
    story.append(Paragraph(
        f"<font color='#888' size=8>Career Assistant Report · Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"Analysed {analysis.total} jobs from {len(analysis.sources)} sources</font>", s_center))

    doc.build(story)
    buf.seek(0)
    return buf.read()


@app.post("/api/report/pdf")
def generate_pdf_endpoint(data: ProfileIn):
    prof = _to_profile(data)
    pdf_bytes = _build_pdf(prof, analysis)
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=career_report_{prof.name or 'report'}.pdf"},
    )
