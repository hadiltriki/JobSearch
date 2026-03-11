"""
scraper_whatjobs.py — Scraper WhatJobs via SerpApi (Google Web Search)

Strategy: site:whatjobs.com <query>
  - gl=us, hl=en, tbs=qdr:m2 (last 2 months)
  - STOP as soon as a job > MAX_AGE_DAYS is detected
  - Filtre les pages SEO (titres "What jobs are in...", "Qué trabajos...")
    pour ne garder que les vraies fiches de poste

Exported functions:
    scrape_whatjobs(query, session) -> list[dict]
"""

import asyncio
import os
import re
from datetime import datetime, timedelta

import aiohttp
from dotenv import load_dotenv

from scraper_utils import (
    MAX_AGE_DAYS,
    _age_label, _too_old, _parse_date,
)

load_dotenv()

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")
try:
    from serpapi import GoogleSearch as _SerpApiSearch
    _SERPAPI_AVAILABLE = True
except ImportError:
    _SERPAPI_AVAILABLE = False

WHATJOBS_BASE = "https://www.whatjobs.com"

_WJ_SKILLS_RE = re.compile(
    r'\b(?:python|java|javascript|typescript|scala|go|rust|sql|nosql|'
    r'r\b|c\+\+|c#|bash|'
    r'tensorflow|pytorch|keras|scikit.learn|xgboost|langchain|openai|'
    r'spark|kafka|airflow|mlflow|docker|kubernetes|terraform|ansible|'
    r'aws|azure|gcp|databricks|snowflake|dbt|'
    r'postgresql|mysql|mongodb|redis|elasticsearch|'
    r'tableau|power\s*bi|looker|grafana|'
    r'llm|nlp|computer\s*vision|rag|deep\s*learning|machine\s*learning|'
    r'data\s*science|devops|mlops|git|linux|fastapi|flask|django|'
    r'react|node\.js|angular|vue|pandas|numpy|excel)\b',
    re.I
)

# ── Filtre pages SEO ──────────────────────────────────────────────────────────
# WhatJobs génère des pages SEO, ex :
#   "What Data Engineer Jobs Are in South Africa?"
#   "Data Engineer Jobs in the Philippines"
#   "Sr Data Engineer Jobs in the Philippines"
# On les rejette : ce ne sont PAS des offres d'emploi individuelles.
# NOTE : si _wj_is_job_url() fonctionne bien (ID numérique obligatoire),
# ces pages SEO sont déjà rejetées au niveau URL — ce filtre est une sécurité supplémentaire.

_SEO_TITLE_START_RE = re.compile(
    r'^(?:'
    r'what\s|which\s|how\s+(?:many|much|to\s)|find\s|top\s+\d|best\s+\d|'
    r'les?\s+\d|quels?\s+|welche\s+|que\s+(?:trabajo|empleo)|'
    r'comment\s+trouver|combien|quanto|wie\s+viele?|'
    r'\d+\s+(?:job|emploi|offre|stelle)'
    r')',
    re.I
)
# "[Role] Jobs in/near/at [Location]" — pattern central des pages SEO WhatJobs
_SEO_JOBS_IN_RE = re.compile(
    r'\bjobs?\s+(?:in|near|at|for|available\s+in)\b.{2,}$',
    re.I
)
_SEO_PHRASE_RE = re.compile(
    r'jobs?\s+(?:are|is)\s+(?:near|in|available)\b'
    r'|(?:near|cerca\s+de|près\s+de)\s+me\b'
    r'|jobs?\s+available\s+in\b'
    r'|trabajos?\s+(?:de|en|hay)\b'
    r'|hay\s+cerca\s+de\s+m[íi]\b',
    re.I
)

def _is_real_job_title(title: str) -> bool:
    """
    True  = vraie offre individuelle → garder
    False = page SEO agrégée WhatJobs → rejeter
    """
    if not title or len(title) < 3:
        return False
    t = title.strip()
    if _SEO_TITLE_START_RE.match(t):
        return False
    if _SEO_JOBS_IN_RE.search(t):       # ex: "Data Engineer Jobs in Philippines"
        return False
    if _SEO_PHRASE_RE.search(t):
        return False
    if len(t) > 120:
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  Date helpers  (inchangés par rapport à l'original)
# ══════════════════════════════════════════════════════════════════════════════

def _wj_parse_date(text: str) -> datetime | None:
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

    if any(w in low for w in ("today", "just now", "this morning", "hour")):
        return now
    if "yesterday" in low:
        return now - timedelta(days=1)

    m = re.search(r"(\d+)\+?\s*day",   low);
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\+?\s*week",  low);
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\+?\s*month", low);
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"(\d+)\+?\s*(?:hr|hour)", low);
    if m: return now

    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except (ValueError, TypeError):
            pass

    return _parse_date(s)


def _wj_extract_date(item: dict) -> tuple[datetime | None, str]:
    pub_dt, time_ago = None, ""

    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        for key in ("date_posted", "posted_at", "date", "published_date"):
            val = str(ext.get(key, "") or "").strip()
            if val:
                pub_dt = _wj_parse_date(val)
                if pub_dt:
                    return pub_dt, _age_label(pub_dt)

    raw = str(item.get("date", "") or "").strip()
    if raw:
        pub_dt = _wj_parse_date(raw)
        if pub_dt:
            return pub_dt, _age_label(pub_dt)
        time_ago = raw

    snippet = str(item.get("snippet", "") or "")
    DATE_PATS = [
        r"\d+\+?\s*(?:day|week|month|hour)s?\s*ago",
        r"posted\s+\d+\s*(?:day|week|month)s?\s*ago",
        r"today|just now|yesterday",
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
        r"\d{4}-\d{2}-\d{2}",
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,\s+\d{4}",
    ]
    for pat in DATE_PATS:
        dm = re.search(pat, snippet, re.I)
        if dm:
            pub_dt = _wj_parse_date(dm.group(0))
            if pub_dt:
                return pub_dt, _age_label(pub_dt)
            time_ago = time_ago or dm.group(0)

    return pub_dt, time_ago


# ══════════════════════════════════════════════════════════════════════════════
#  Info extraction helpers  (inchangés par rapport à l'original)
# ══════════════════════════════════════════════════════════════════════════════

def _wj_is_job_url(url: str) -> bool:
    """
    True  = fiche individuelle WhatJobs (contient un ID numérique)
             ex: /jobs?id=382392185  ou  /jobs/titre/ville/382392185
    False = page de liste / recherche (pas d'ID)
             ex: /jobs/data-engineer/philippines  ou  /jobs?q=...
    """
    if not url or "whatjobs.com" not in url:
        return False
    # Format principal : ?id=NNNNNNN
    if re.search(r'[?&]id=\d{5,}', url):
        return True
    # Format alternatif : /chemin/NNNNNNN en fin d'URL
    if re.search(r'/\d{6,}(?:[?#]|$)', url):
        return True
    # Tout le reste = page de liste
    return False


def _wj_extract_info(item: dict) -> dict:
    snippet = str(item.get("snippet", "") or "")
    title   = str(item.get("title",   "") or "")

    # Nettoyage du titre
    # 1. Supprimer branding WhatJobs
    title = re.sub(
        r"\s*[\|·\-–—]\s*(?:whatjobs(?:\.com)?|job[s]?\s+search)[^|]*$",
        "", title, flags=re.I
    ).strip()
    # 2. Supprimer suffixe " Jobs in [Location]" si présent
    title = re.sub(
        r"\s+jobs?\s+(?:in|near|at|for)\s+.+$",
        "", title, flags=re.I
    ).strip()
    # 3. Couper aux séparateurs
    for sep in (" | ", " - ", " – ", " — "):
        if title.count(sep) >= 1:
            title = title.split(sep)[0].strip()
            break
    # 4. Supprimer mots parasites finaux
    title = re.sub(
        r"\s+(?:jobs?|employment|careers?|hiring|openings?)\s*$",
        "", title, flags=re.I
    ).strip()

    company  = ""
    location = ""

    # 1. rich_snippet detected_extensions
    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        if not company  and ext.get("company"):
            company  = str(ext["company"]).strip()
        if not location and ext.get("location"):
            location = str(ext["location"]).strip()

    # 2. Fallback: parse first sentence of snippet
    if not company or not location:
        first = snippet.split(".")[0].split("\n")[0]
        for sep in (" - ", " – ", " · ", " | "):
            if sep in first:
                parts = first.split(sep, 1)
                cand = parts[0].strip()
                if cand and not re.search(r"\d+\s+(?:day|week|month)", cand, re.I):
                    if not company:
                        company = cand
                if len(parts) > 1 and not location:
                    loc_cand = parts[1].strip()
                    loc_cand = re.sub(
                        r"\s*[·\-]\s*(?:posted|\d+\s+(?:day|week|month)).*$",
                        "", loc_cand, flags=re.I
                    ).strip()
                    location = loc_cand
                break

    if company:
        company = re.sub(
            r"\s*[·\-]\s*(?:posted|\d+\s+(?:day|week|month)).*$",
            "", company, flags=re.I
        ).strip()

    if not location:
        m = re.search(r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*[A-Z][a-z]+)", snippet)
        if m:
            location = m.group(1).strip()

    # Salary
    salary = ""
    sm = re.search(
        r"\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*/\s*(?:hr|hour|year|yr|an))?"
        r"|\d[\d\s,.]*\s*(?:CAD|USD|EUR|GBP|€|£)\s*(?:[-–]\s*\d[\d\s,.]*\s*(?:CAD|USD|EUR|GBP|€|£))?"
        r"|\d+k\s*(?:[-–]\s*\d+k)?",
        snippet, re.I
    )
    if sm:
        salary = sm.group(0).strip()
    salary = salary or "Not specified"

    # Remote
    remote = ""
    if re.search(r"\b(full[- ]?remote|100%\s*remote|fully remote|work from home|\bwfh\b)\b", snippet, re.I):
        remote = "Full Remote 🌍"
    elif re.search(r"\b(hybrid|hybride|partial remote)\b", snippet, re.I):
        remote = "Hybrid 🏠🏢"
    elif re.search(r"\bremote\b", snippet, re.I):
        remote = "Remote 🌍"

    # Contract
    contract = ""
    for kw, label in [
        (r"\bpermanent\b|\bcdi\b",               "Permanent / Full-time"),
        (r"\bfull[- ]?time\b",                   "Full-time"),
        (r"\bpart[- ]?time\b",                   "Part-time"),
        (r"\bcontract\b|\bcdd\b|fixed.term",     "Contract"),
        (r"\bfreelance\b|\bcontractor\b",        "Freelance"),
        (r"\binternship\b|\bstage\b|\bintern\b", "Internship"),
        (r"\balternance\b|\bapprenticeship\b",   "Alternance"),
    ]:
        if re.search(kw, snippet, re.I):
            contract = label
            break

    # Experience
    experience = ""
    for pat in [
        r"(\d+\+?\s*[-–to]\s*\d+\s*years?\s*(?:of\s*)?(?:experience)?)",
        r"(\d+\+?\s*years?\s*(?:of\s*)?experience)",
        r"(\d+\+?\s*yrs?\b)",
        r"(entry[- ]level|mid[- ]level|senior|junior|lead|principal)",
    ]:
        m = re.search(pat, snippet, re.I)
        if m:
            experience = m.group(1).strip()
            break

    # Skills
    found_skills = _WJ_SKILLS_RE.findall(snippet)
    skills = ", ".join(dict.fromkeys(s.lower() for s in found_skills))

    return {
        "title":       title,
        "company":     company,
        "location":    location,
        "salary":      salary,
        "remote":      remote,
        "contract":    contract,
        "experience":  experience,
        "skills":      skills,
        "description": snippet[:1500],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Public entry point
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_whatjobs(query: str, session: aiohttp.ClientSession) -> list[dict]:
    try:
        from serpapi_google_web_common import scrape_google_web, looks_like_placeholder_api_key
    except ImportError:
        print("  [whatjobs] serpapi_google_web_common.py introuvable — skip")
        return []

    if not SERPAPI_API_KEY or looks_like_placeholder_api_key(SERPAPI_API_KEY):
        print("  [whatjobs] SERPAPI_API_KEY manquante ou invalide — skip")
        return []

    import tempfile
    from pathlib import Path

    search_query = f"site:whatjobs.com {query}"
    print(f"  [whatjobs] Query: '{search_query}'")
    print(f"  [whatjobs] Auto-STOP at {MAX_AGE_DAYS} days")

    listings:  list[dict] = []
    seen_urls: set[str]   = set()

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            raw_results, _ = await asyncio.to_thread(
                lambda: scrape_google_web(
                    q               = search_query,
                    output_dir      = Path(tmpdir),
                    source_tag      = "whatjobs",
                    api_key         = SERPAPI_API_KEY,
                    url_allow_regex = r"whatjobs\.com",
                    derive_fields   = False,
                    max_results     = 100,
                    hl              = "en",
                    gl              = "us",
                    delay_s         = 0.5,
                )
            )
        except Exception as e:
            print(f"  [whatjobs] scrape_google_web error: {e}")
            return []

    for item in raw_results:
        raw_url = str(item.get("url", "") or "")
        if not _wj_is_job_url(raw_url):
            continue

        id_match = re.search(r'[?&](id=\d+)', raw_url)
        clean_url = (f"{raw_url.split('?')[0].rstrip('/')}?{id_match.group(1)}"
                     if id_match else raw_url.split("?")[0].rstrip("/"))

        if clean_url in seen_urls:
            continue
        seen_urls.add(clean_url)

        # Reconstruire un item compatible _wj_extract_date / _wj_extract_info
        compat = {
            "link":    raw_url,
            "title":   item.get("title", ""),
            "snippet": item.get("description", ""),
        }
        pub_dt, time_ago = _wj_extract_date(compat)
        if pub_dt is not None and _too_old(pub_dt):
            print(f"  [whatjobs] STOP cutoff: {(datetime.now()-pub_dt).days}d > {MAX_AGE_DAYS}d")
            break

        info = _wj_extract_info(compat)
        if not _is_real_job_title(info["title"]):
            continue

        listings.append({
            "title":    info["title"],
            "url":      clean_url,
            "company":  info["company"],
            "location": info["location"],
            "salary":   info["salary"],
            "remote":   info["remote"],
            "time_ago": time_ago,
            "source":   "whatjobs",
            "_wj_contract":    info["contract"],
            "_wj_experience":  info["experience"],
            "_wj_skills":      info["skills"],
            "_wj_description": info["description"],
        })

    print(f"[whatjobs] TOTAL: {len(listings)}")
    return listings