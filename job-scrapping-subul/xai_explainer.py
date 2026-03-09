"""
xai_explainer.py — LLM-as-judge for explainable match scores

Uses Azure OpenAI to generate short, natural-language explanations of the
BiEncoder (AI match) score and combined score, grounded in the actual
numbers (cosine, match_score, combined_score, skills gap).

Output is attached to job["xai"] for the frontend "Explain scores (XAI)" panel.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from openai import AsyncAzureOpenAI

# Load .env from module dir and from cwd (so uvicorn started from repo root still finds .env)
load_dotenv()
load_dotenv(Path(__file__).parent / ".env")

logger = logging.getLogger(__name__)

AZURE_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "").strip()
AZURE_API_KEY  = os.getenv("AZURE_OPENAI_API_KEY", "").strip()
AZURE_VERSION  = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")
DEPLOYMENT     = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
# Default: enable when Azure is configured; set EXPLAINABLE_AI_ENABLED=0/false to disable
_explicit = os.getenv("EXPLAINABLE_AI_ENABLED", "").strip().lower()
if _explicit in ("1", "true", "yes"):
    EXPLAINABLE_AI_ENABLED = True
elif _explicit in ("0", "false", "no"):
    EXPLAINABLE_AI_ENABLED = False
else:
    EXPLAINABLE_AI_ENABLED = bool(AZURE_ENDPOINT and AZURE_API_KEY)


def _client() -> AsyncAzureOpenAI | None:
    if not AZURE_ENDPOINT or not AZURE_API_KEY:
        return None
    return AsyncAzureOpenAI(
        azure_endpoint=AZURE_ENDPOINT,
        api_key=AZURE_API_KEY,
        api_version=AZURE_VERSION,
    )


def get_roadmap_xai_status() -> dict:
    """For debugging: whether Azure/LLM is configured for roadmap courses."""
    client = _client()
    return {
        "explainable_ai_enabled": EXPLAINABLE_AI_ENABLED,
        "azure_endpoint_set": bool(AZURE_ENDPOINT),
        "azure_key_set": bool(AZURE_API_KEY),
        "azure_client_ok": client is not None,
        "deployment": DEPLOYMENT or "(default)",
    }


async def explain_job_match(
    *,
    job_title: str,
    job_skills_req: str,
    gap_matched: list[str],
    gap_missing: list[str],
    gap_coverage: float,
    gap_total: int,
    cv_role: str,
    cv_skills_summary: str,
    cosine: float,
    match_score: float,
    combined_score: float,
) -> dict | None:
    """
    Call LLM to produce explainable AI fields for a job card.

    Returns a dict with:
      - interpretation: "excellent" | "good" | "moderate" | "low"
      - score_formula: one-line description of combined score
      - explanations: list of 2–4 short sentences
      - cosine_score: same as input (for frontend)
      - match_score: same as input (for frontend)

    Returns None if disabled, client missing, or LLM call fails.
    """
    if not EXPLAINABLE_AI_ENABLED:
        return None

    client = _client()
    if not client:
        logger.debug("[xai] No Azure client — skip explanation")
        return None

    cos_pct = round(cosine * 100, 1)
    ai_pct  = round(match_score * 100, 1) if match_score >= 0 else 0
    comb_pct = round(combined_score * 100, 1)
    cov_pct = round(gap_coverage * 100, 0) if gap_total else 100
    missing_preview = ", ".join(gap_missing[:6])
    if len(gap_missing) > 6:
        missing_preview += f" (+{len(gap_missing) - 6} more)"
    matched_preview = ", ".join(gap_matched[:5]) if gap_matched else "—"

    system = (
        "You are a helpful career coach. You explain job match scores in plain language for the candidate. "
        "Be concise, honest, and encouraging. Use ONLY the provided numbers and facts; do not invent numbers. "
        "Output valid JSON only, no markdown or extra text."
    )
    user = f"""Job: "{job_title}"
Required skills (matched): {matched_preview}
Missing skills: {missing_preview}
Candidate role: {cv_role}
Candidate skills (summary): {cv_skills_summary[:300]}

Scores (0–100):
- Title match (cosine): {cos_pct}%
- AI match (BiEncoder): {ai_pct}%
- Combined (AI × √skills coverage): {comb_pct}%
- Skills coverage: {cov_pct}% ({gap_total - len(gap_missing)}/{gap_total} required skills matched)

Return a JSON object with exactly these keys:
- "interpretation": one of "excellent", "good", "moderate", "low"
- "score_formula": one short line (e.g. "Combined = AI match × √(skills coverage)")
- "explanations": array of 2–4 short sentences: why this score, what fits, what's missing. First sentence = overall fit; then title match, skills gap, or key missing skills. Tone: clear and constructive.
- "tip": one short, actionable tip for this specific role (e.g. what to add to the CV, what to highlight in the cover letter, or one skill to learn). Max 1 sentence. If the match is excellent, tip can be "Lead with your relevant experience in the first paragraph."
- "strength": one strength of the candidate for this job to highlight in the application (e.g. "Your X and Y experience align well with this role"). Max 1 sentence. If there are no clear strengths, say "Your profile has overlap with the role — tailor your summary to this title." """

    try:
        async with client as c:
            resp = await c.chat.completions.create(
                model=DEPLOYMENT,
                max_tokens=500,
                temperature=0.25,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code block if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        data = json.loads(raw)
        data["cosine_score"] = cosine
        data["match_score"] = match_score
        if "explanations" not in data or not isinstance(data["explanations"], list):
            data["explanations"] = []
        if "tip" not in data or not isinstance(data["tip"], str):
            data["tip"] = ""
        if "strength" not in data or not isinstance(data["strength"], str):
            data["strength"] = ""
        logger.info("[xai] LLM — %s", job_title[:50])
        return data
    except json.JSONDecodeError as e:
        logger.warning("[xai] JSON parse error: %s → fallback", e)
        return _fallback_xai(cosine, match_score, combined_score, gap_coverage, gap_total)
    except Exception as e:
        logger.warning("[xai] LLM failed: %s → fallback", e)
        return _fallback_xai(cosine, match_score, combined_score, gap_coverage, gap_total)


def _fallback_xai(
    cosine: float,
    match_score: float,
    combined_score: float,
    gap_coverage: float,
    gap_total: int,
) -> dict:
    """When LLM is unavailable, return a minimal xai dict with formula and interpretation only."""
    interp = "low"
    if combined_score >= 0.75:
        interp = "excellent"
    elif combined_score >= 0.55:
        interp = "good"
    elif combined_score >= 0.40:
        interp = "moderate"

    formula = "Combined = AI match × √(skills coverage)"
    if gap_total == 0:
        formula = "Combined = AI match (no required skills listed)"

    return {
        "interpretation": interp,
        "score_formula": formula,
        "explanations": [],
        "tip": "",
        "strength": "",
        "cosine_score": cosine,
        "match_score": match_score,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Roadmap LLM enrichment
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_roadmap_with_llm(
    *,
    user_role: str,
    user_skills_summary: str,
    coverage_pct: float,
    skills_in_order: list[dict],
) -> dict | None:
    """
    Call LLM to generate in-app "courses" per skill: personalized tip + small project ideas.
    No references to external platforms (Coursera, Udemy, etc.).
    Returns {"message": str, "courses": [{"skill": str, "tip": str, "project_ideas": [str, str, str]}, ...]} or None.
    """
    if not EXPLAINABLE_AI_ENABLED:
        logger.info("[xai] Roadmap courses: EXPLAINABLE_AI_ENABLED is false")
        return None
    client = _client()
    if not client:
        logger.warning(
            "[xai] Roadmap courses: no Azure client — check AZURE_OPENAI_ENDPOINT (%s) and AZURE_OPENAI_API_KEY (set=%s)",
            AZURE_ENDPOINT or "(empty)",
            bool(AZURE_API_KEY),
        )
        return None
    if not skills_in_order:
        return None

    skills_blob = "\n".join(
        f"- {e.get('skill', '')} ({e.get('difficulty', '')}, {e.get('jobs_count', 0)} jobs)"
        for e in skills_in_order[:15]
    )
    system = (
        "You are a career coach. You generate in-app learning content only. "
        "Do NOT mention any external learning platforms (no Coursera, Udemy, LinkedIn Learning, "
        "DataCamp, edX, Codecademy, etc.). Suggest official docs, practice, and building projects only. "
        "Be concise. Output valid JSON only, no markdown."
    )
    user = f"""Candidate role: {user_role}
Candidate current skills (summary): {user_skills_summary[:400]}
Market coverage: {coverage_pct:.0f}%
Top missing skills to learn (in priority order):
{skills_blob}

Return a JSON object with:
- "message": 1-2 short sentences for this candidate: why this roadmap, what to focus on first. Max 2 sentences. No platform names.
- "courses": an array with ONE object per skill in the SAME ORDER as the list above. Each object has:
  - "skill": "<exact skill name from the list>"
  - "tip": "1-2 sentences: how to start learning this skill using official documentation and practice only. No external platforms."
  - "project_ideas": ["Small project idea 1 (one line)", "Small project idea 2", "Small project idea 3"] — 2 to 3 concrete mini-project ideas to practice this skill (e.g. 'Build a CLI that lists files in a directory', 'Create a REST API that returns the current time'). Be specific and doable."""

    try:
        async with client as c:
            resp = await c.chat.completions.create(
                model=DEPLOYMENT,
                max_tokens=4000,
                temperature=0.3,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        data = json.loads(raw)
        msg = (data.get("message") or "").strip()
        courses = data.get("courses") or []
        if not isinstance(courses, list):
            courses = []
        # If model returned old "insights" format, convert to courses (tip from insight, no project_ideas)
        if not courses and data.get("insights"):
            insights = data.get("insights") or []
            skill_order = [e.get("skill", "") for e in skills_in_order[:15]]
            for obj in insights:
                sk = (obj.get("skill") or "").strip()
                if not sk:
                    continue
                insight_text = (obj.get("insight") or "").strip()
                courses.append({"skill": sk, "tip": insight_text or "Practice with official docs.", "project_ideas": []})
            # Preserve order by skill list
            by_skill = {c["skill"]: c for c in courses}
            courses = [by_skill[s] for s in skill_order if s in by_skill]
            logger.info("[xai] Roadmap: converted %s insights to courses (no project_ideas)", len(courses))
        if not courses:
            logger.info("[xai] Roadmap LLM returned 0 courses. Top-level keys: %s. Raw (first 600 chars): %s", list(data.keys()), (raw[:600] if raw else "") + ("..." if len(raw) > 600 else ""))
        # Normalize each course: ensure skill, tip, project_ideas
        out_courses = []
        for c in courses:
            if not c.get("skill"):
                continue
            out_courses.append({
                "skill": str(c.get("skill", "")).strip(),
                "tip": (c.get("tip") or "").strip() or "Practice with official docs and hands-on projects.",
                "project_ideas": [x.strip() for x in (c.get("project_ideas") or []) if isinstance(x, str) and x.strip()][:3],
            })
        logger.info("[xai] Roadmap LLM — message + %s courses", len(out_courses))
        return {"message": msg, "courses": out_courses}
    except (json.JSONDecodeError, Exception) as e:
        logger.warning("[xai] Roadmap LLM failed: %s", e, exc_info=True)
        return None
