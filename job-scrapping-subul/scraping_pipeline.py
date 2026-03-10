"""
scraping_pipeline.py — Scraping & SSE Pipeline Module
======================================================
Responsibilities:
  - Scrape job sources (aijobs, remoteok, emploitic,
    tanitjobs, greenhouse, eluta, whatjobs)
  - Enrich each job (details, cosine scores, AI match, skills gap)
  - Save each job to DB (table `jobs`)
  - Stream results via SSE to the frontend
  - CV helpers: language detection, title extraction, structuring

Exported functions:
    pipeline(cv_text, user_id)          → async generator SSE
    detect_and_translate_cv(cv_text)    → (text, lang, translated)
    extract_cv_title(cv_text)           → str
    structure_cv_for_model(title, text) → dict

Routes registered in main.py via:
    from scraping_pipeline import scraping_router
    app.include_router(scraping_router)
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import numpy as np
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from openai import AsyncAzureOpenAI
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer

import matcher as mtch
from database import upsert_user, insert_job
from llm_extractor import extract_with_llm
from scraper_utils import extract_tech_from_description
# Import scrapers directly to avoid circular imports.
# (scraper.py imports scraper_whatjobs.py, so importing scrape_whatjobs
#  via scraper.py from within scraping_pipeline.py creates a cycle.)
# --- Commented out (enable when re-adding these sources) ---
#from scraper_remoteok   import scrape_remoteok                                 # ✅ ACTIVE
# from scraper_emploitic import scrape_emploitic, _scrape_emploitic_fetch_one  # commenté
# from scraper_whatjobs  import scrape_whatjobs                                # commenté
# from scraper_aijobs    import scrape_aijobs                                  # commenté
# from scraper_tanitjobs import scrape_tanitjobs
# from scraper_greenhouse import scrape_greenhouse                              # commenté
# from scraper_eluta     import scrape_eluta                                   # commenté
# --- Active: Indeed, LinkedIn, Lever, WTTJ ---
from scraper_indeed   import scrape_indeed
from scraper_linkedin import scrape_linkedin
from scraper_lever    import scrape_lever
from scraper_wttj     import scrape_wttj

try:
    from xai_explainer import explain_job_match, EXPLAINABLE_AI_ENABLED
except Exception:
    explain_job_match = None
    EXPLAINABLE_AI_ENABLED = False

logger           = logging.getLogger(__name__)
scraping_router  = APIRouter(tags=["Scraping"])

# ── Config ────────────────────────────────────────────────────────────────────
COSINE_THRESHOLD           = 0.60
COSINE_THRESHOLD_EMPLOITIC = 0.60
MAX_AGE_DAYS               = 45
LLM_CONCURRENCY            = 4
NUM_SOURCES                = 3   # Indeed, LinkedIn, WTTJ (Lever commented out — was blocking WTTJ)

SHARED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Modèles ML chargés une seule fois au démarrage ───────────────────────────
logger.info("Loading sentence-transformer…")
EMBED_MODEL = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
logger.info("Sentence-transformer ready ✓")

logger.info("Loading fine-tuned matching model…")
MATCH_MODEL, MATCH_TOKENIZER = mtch.load_model()
if MATCH_MODEL is None:
    logger.warning("⚠ Fine-tuned model not found — cosine-only scoring.")
else:
    logger.info("Fine-tuned model ready ✓")


# ── Pydantic models ───────────────────────────────────────────────────────────
class ScanRequest(BaseModel):
    cv_text: str
    user_id: int = 0


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


def _job_dedupe_key(title: str, company: str) -> str:
    """Normalize title + company so the same role from Indeed vs LinkedIn gets one row."""
    t = (title or "").strip().lower()
    c = (company or "").strip().lower()
    # collapse spaces and ignore minor punctuation so "Acme, Inc." matches "Acme Inc"
    t = " ".join(t.split())
    c = " ".join(c.split())
    for suffix in (", inc.", " inc.", ", inc", " inc", ", llc.", " llc.", " llc"):
        if c.endswith(suffix):
            c = c[: -len(suffix)].strip()
    return f"{t}|{c}"


def _azure_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        api_key        = os.getenv("AZURE_OPENAI_API_KEY", ""),
        api_version    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers CV
# ═══════════════════════════════════════════════════════════════════════════════

async def detect_and_translate_cv(cv_text: str) -> tuple[str, str, str]:
    """Détecte la langue du CV et le traduit en anglais si besoin."""
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
    excerpt    = cv_text.strip()[:600]
    try:
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model       = deployment,
                max_tokens  = 8,
                temperature = 0,
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

    logger.info(f"[lang] Translating from {detected_lang} to English…")
    try:
        chunk_size       = 2000
        chunks           = [cv_text[i:i+chunk_size] for i in range(0, len(cv_text), chunk_size)]
        translated_parts = []
        for chunk in chunks:
            async with _azure_client() as az:
                resp = await az.chat.completions.create(
                    model       = deployment,
                    max_tokens  = 1000,
                    temperature = 0,
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
    """Extrait le titre de poste principal depuis le CV."""
    try:
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
        excerpt    = cv_text.strip()[:800]
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model       = deployment,
                max_tokens  = 15,
                temperature = 0,
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
    """Extrait les champs structurés du CV via LLM (JSON)."""
    try:
        deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
        excerpt    = cv_text.strip()[:2000]
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model           = deployment,
                max_tokens      = 400,
                temperature     = 0,
                response_format = {"type": "json_object"},
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
    """
    Pipeline complet : CV → scraping → enrichissement → scoring → DB → SSE stream.

    Étapes :
    1. Détection/traduction langue du CV
    2. Extraction titre + structuration CV via LLM
    3. Sauvegarde profil utilisateur en DB
    4. Encodage CV en vecteur (cosine similarity)
    5. Scraping parallèle des 6 sources
    6. Filtrage cosine, enrichissement LLM, scoring AI + skills gap
    7. Sauvegarde job en DB + envoi SSE vers le frontend
    """
    cutoff   = datetime.now() - timedelta(days=MAX_AGE_DAYS)
    llm_sem  = asyncio.Semaphore(LLM_CONCURRENCY)
    has_model = MATCH_MODEL is not None

    logger.info(f"[pipeline] START — user_id={user_id} save_to_db={user_id > 0}")

    # ── Étape 1 : Détection langue ────────────────────────────────────────────
    if cv_text:
        yield sse({"event": "lang_detecting"})
        cv_text, detected_lang, translated = await detect_and_translate_cv(cv_text)
        yield sse({"event": "lang_ready", "lang": detected_lang, "translated": translated})

    # ── Étape 2 : Titre + structuration CV ───────────────────────────────────
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

        # ── Étape 3 : Sauvegarde profil DB ───────────────────────────────────
        if user_id > 0:
            logger.info(f"[pipeline] Saving user to DB: user_id={user_id}")
            ok = await upsert_user(user_id, cv_structured)
            if ok:
                yield sse({"event": "user_saved", "user_id": user_id})
            else:
                logger.error(f"[pipeline] ❌ Failed to save user_id={user_id}")
        else:
            logger.info("[pipeline] user_id=0 → anonymous scan")

    # ── Étape 4 : Encodage vecteur CV ─────────────────────────────────────────
    cv_vec: np.ndarray = await asyncio.to_thread(
        lambda: EMBED_MODEL.encode(cv_title, convert_to_numpy=True)
    )

    # ── Étape 5 : Scraping + enrichissement parallèle ─────────────────────────
    result_q      = asyncio.Queue()
    src_done_q    = asyncio.Queue()
    pending       = {"n": 0}
    scrapers      = {"done": 0}
    all_done      = {"v": False}
    seen_dedupe   = set()  # (user_id, dedupe_key) → one row per (title, company)
    dedupe_lock   = asyncio.Lock()

    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(headers=SHARED_HEADERS, connector=connector) as session:

        async def handle_job(job: dict, source: str):
            """Filtre par cosine threshold puis lance l'enrichissement."""
            job["source"] = source
            threshold = COSINE_THRESHOLD_EMPLOITIC if source == "emploitic" else COSINE_THRESHOLD
            job_vec = await asyncio.to_thread(
                lambda: EMBED_MODEL.encode(job["title"], convert_to_numpy=True)
            )
            cosine = cosine_sim(cv_vec, job_vec)
            if cosine < threshold:
                return
            pending["n"] += 1
            asyncio.create_task(enrich(job, cosine, source))

        async def enrich(job: dict, cosine: float, source: str):
            async with llm_sem:
                try:
                   

                    # ── Extraction détails selon la source ────────────────────
                    if source == "emploitic":
                        from scraper_emploitic import _scrape_emploitic_fetch_one
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
                            "tags":        ", ".join(full_job.get("_emp_tags",   []) or []),
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

                    elif source == "whatjobs":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("remote", ""),
                            "salary":      job.get("salary", "Not specified"),
                            "contract":    job.get("_wj_contract", ""),
                            "experience":  job.get("_wj_experience", ""),
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_wj_description", ""),
                            "skills_req":  job.get("_wj_skills", ""),
                            "skills_bon":  "",
                            "all_skills":  job.get("_wj_skills", ""),
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

                    elif source == "indeed":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("_indeed_remote", ""),
                            "salary":      job.get("_indeed_salary", "Not specified"),
                            "contract":    job.get("_indeed_contract", ""),
                            "experience":  "",
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_indeed_description", ""),
                            "skills_req":  job.get("_indeed_skills", ""),
                            "skills_bon":  "",
                            "all_skills":  job.get("_indeed_skills", ""),
                            "tags":        "",
                        }

                    elif source == "linkedin":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("_linkedin_remote", ""),
                            "salary":      job.get("_linkedin_salary", "Not specified"),
                            "contract":    job.get("_linkedin_contract", ""),
                            "experience":  "",
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_linkedin_description", ""),
                            "skills_req":  job.get("_linkedin_skills", ""),
                            "skills_bon":  "",
                            "all_skills":  job.get("_linkedin_skills", ""),
                            "tags":        "",
                        }

                    elif source == "lever":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("remote", ""),
                            "salary":      "Not specified",
                            "contract":    job.get("_lever_contract", ""),
                            "experience":  "",
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_lever_description", ""),
                            "skills_req":  "",
                            "skills_bon":  "",
                            "all_skills":  job.get("_lever_categories", ""),
                            "tags":        job.get("_lever_categories", ""),
                        }

                    elif source == "wttj":
                        details = {
                            "title":       job.get("title", ""),
                            "industry":    job.get("company", ""),
                            "location":    job.get("location", ""),
                            "remote":      job.get("remote", ""),
                            "salary":      "Not specified",
                            "contract":    "",
                            "experience":  "",
                            "education":   "",
                            "pub_date":    job.get("time_ago", ""),
                            "expired":     "",
                            "description": job.get("_wttj_description", ""),
                            "skills_req":  job.get("_wttj_skills", ""),
                            "skills_bon":  "",
                            "all_skills":  job.get("_wttj_skills", ""),
                            "tags":        "",
                        }

                    else:
                        # aijobs / remoteok → extraction LLM générique
                        details = await extract_with_llm(
                            url     = job["url"],
                            session = session,
                            cutoff  = cutoff,
                        )
                        if details is None:
                            return

                    # Backfill skills from description when source provides no structured skills
                    # (for gap computation and "skills in demand" on job card)
                    if not details.get("skills_req") and not details.get("all_skills") and details.get("description"):
                        backfill = extract_tech_from_description(details["description"])
                        if backfill:
                            details["skills_req"] = backfill
                            details["all_skills"] = backfill

                    # ── Scoring AI (BiEncoder) ────────────────────────────────
                    match_score = -1.0
                    if has_model and cv_structured:
                        match_score = await asyncio.to_thread(
                            lambda: mtch.predict(
                                MATCH_MODEL, MATCH_TOKENIZER, cv_structured, details
                            )
                        )

                    # ── Skills gap ────────────────────────────────────────────
                    gap = {"missing": [], "matched": [], "coverage": 1.0, "total": 0}
                    if cv_structured:
                        gap = await asyncio.to_thread(
                            lambda: mtch.compute_skills_gap(cv_structured, details)
                        )

                    logger.info(
                        f"  ✦ {job['title'][:35]:35s} [{source:9s}] "
                        f"cos={cosine:.2f} ai={match_score:.2f} "
                        f"cov={gap['coverage']:.0%} "
                        f"miss={len(gap['missing'])}/{gap['total']}"
                    )

                    # Combined score (AI × √skills coverage) for report & frontend
                    combined_score = mtch.compute_combined_score(match_score, gap)

                    # XAI: LLM explanation when enabled (for "Explain scores" panel & report)
                    xai_val = None
                    if EXPLAINABLE_AI_ENABLED and explain_job_match and cv_structured:
                        try:
                            xai_val = await explain_job_match(
                                job_title=details.get("title") or job.get("title", ""),
                                job_skills_req=details.get("skills_req", "") or "",
                                gap_matched=gap["matched"],
                                gap_missing=gap["missing"],
                                gap_coverage=gap["coverage"],
                                gap_total=gap["total"],
                                cv_role=cv_structured.get("role", "") or "",
                                cv_skills_summary=(cv_structured.get("skills", "") or cv_structured.get("summary", ""))[:500],
                                cosine=cosine,
                                match_score=match_score,
                                combined_score=combined_score,
                            )
                        except Exception as xai_err:
                            logger.debug("[pipeline] XAI explain skip: %s", xai_err)

                    # ── Dedupe: one row per (title, company); first source wins ─
                    title_ = details.get("title") or job.get("title", "")
                    company_ = details.get("industry") or job.get("company", "")
                    dedupe_key = _job_dedupe_key(title_, company_)
                    async with dedupe_lock:
                        if user_id > 0 and (user_id, dedupe_key) in seen_dedupe:
                            return  # same role from another source (e.g. Indeed+LinkedIn) → skip
                        if user_id > 0:
                            seen_dedupe.add((user_id, dedupe_key))

                    # ── Carte job complète ────────────────────────────────────
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
                        "combined_score_display": pct(combined_score) if match_score >= 0 else "—",
                        "gap_missing":  gap["missing"],
                        "gap_matched":  gap["matched"],
                        "gap_coverage": gap["coverage"],
                        "gap_total":    gap["total"],
                        "contract":    details.get("contract", "")   or job.get("_emp_contract", ""),
                        "experience":  details.get("experience", "") or job.get("_emp_experience", ""),
                        "education":   details.get("education", "")  or job.get("_emp_education", ""),
                        "pub_date":    details.get("pub_date", ""),
                        "expired":     details.get("expired", "")    or job.get("_emp_status", ""),
                        "description": details.get("description", "") or job.get("_emp_description", ""),
                        "skills_req":  details.get("skills_req", ""),
                        "skills_bon":  details.get("skills_bon", ""),
                        "all_skills":  details.get("all_skills", ""),
                        "tags":        details.get("skills_req", "") or details.get("tags", ""),
                        "xai":         xai_val,
                    }
                    await result_q.put(card)

                    # ── Sauvegarde DB ─────────────────────────────────────────
                    if user_id > 0:
                        ok = await insert_job(user_id, card)
                        if ok:
                            logger.info(f"[pipeline] ✅ job saved — user={user_id}")
                        else:
                            logger.error(f"[pipeline] ❌ job save failed — user={user_id}")

                except Exception as e:
                    logger.error(f"  [enrich] {job.get('url','')[:60]}: {e}", exc_info=True)
                finally:
                    pending["n"] -= 1
                    if all_done["v"] and pending["n"] <= 0:
                        await result_q.put(None)

        async def run_source(name: str, scrape_fn, session_):
            """
            Lance un scraper et traite chaque job EN TEMPS RÉEL dès qu'il est détecté.
            Recherche mondiale — aucun filtre de localisation appliqué.
            Chaque job est notifié immédiatement au frontend via SSE 'job_found',
            puis traité par handle_job() (cosine → enrich → score → SSE 'job').
            """
            job_count_source = 0
            try:
                jobs = await scrape_fn(cv_title, session_)
                for job in jobs:
                    job_count_source += 1
                    # ── SSE immédiat : job détecté (avant filtre cosine) ──────
                    await result_q.put({
                        "event":    "job_found",
                        "source":   name,
                        "title":    job.get("title", ""),
                        "company":  job.get("company", ""),
                        "url":      job.get("url", ""),
                        "time_ago": job.get("time_ago", ""),
                        "count":    job_count_source,
                    })
                    # ── Cosine + enrich en parallèle ──────────────────────────
                    await handle_job(job, name)
            except Exception as e:
                logger.error(f"[{name}] scraper error: {e}")
            finally:
                await src_done_q.put(sse({
                    "event":  "source_done",
                    "source": name,
                    "found":  job_count_source,
                }))

        # ── Active sources ────────────────────────────────────────────────────
        # Dedupe: same (title, company) from multiple sources → one row (first wins).
        # Start LinkedIn first so it’s more likely to be the “original” when duplicated.
        #
        # asyncio.create_task(run_source("aijobs",     scrape_aijobs,     session))
        # asyncio.create_task(run_source("remoteok",   scrape_remoteok,   session))
        # asyncio.create_task(run_source("tanitjobs",  scrape_tanitjobs,  session))
        # asyncio.create_task(run_source("greenhouse", scrape_greenhouse, session))
        # asyncio.create_task(run_source("eluta",      scrape_eluta,      session))
        # asyncio.create_task(run_source("whatjobs",   scrape_whatjobs,   session))
        # asyncio.create_task(run_source("emploitic",  scrape_emploitic,  session))
        # --- Active: Indeed, LinkedIn, WTTJ (Lever commented — was blocking WTTJ) ---
        asyncio.create_task(run_source("linkedin", scrape_linkedin, session))
        asyncio.create_task(run_source("indeed",   scrape_indeed,  session))
        # asyncio.create_task(run_source("lever",    scrape_lever,    session))
        asyncio.create_task(run_source("wttj",     scrape_wttj,    session))

        # ── Boucle SSE principale ─────────────────────────────────────────────
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
                    return
                job_count += 1
                yield sse(item)

            if scrapers["done"] >= NUM_SOURCES:
                all_done["v"] = True
                if pending["n"] <= 0:
                    logger.info(f"[pipeline] END — {job_count} jobs (user_id={user_id})")
                    yield sse({"event": "done", "total": job_count})
                    return

            await asyncio.sleep(0.05)


# ═══════════════════════════════════════════════════════════════════════════════
#  Route FastAPI
# ═══════════════════════════════════════════════════════════════════════════════

@scraping_router.post("/scan")
async def scan(req: ScanRequest):
    """Lance le pipeline de scan et stream les résultats en SSE."""
    logger.info(f"[/scan] POST — user_id={req.user_id} cv_len={len(req.cv_text)}")
    return StreamingResponse(
        pipeline(req.cv_text.strip(), req.user_id),
        media_type = "text/event-stream",
        headers    = {
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
        },
    )