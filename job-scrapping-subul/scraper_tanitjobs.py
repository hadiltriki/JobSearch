"""
scraper_tanitjobs.py — Scraper tanitjobs.com via SerpApi Google Web Search

Stratégie : scrape_google_web (urllib direct, pas de lib serpapi)
  - query : site:tanitjobs.com/job/ {cv_title}
  - Filtre les URLs ne contenant pas /job/
  - Retourne une liste de dicts compatibles avec scraping_pipeline.py

Fonctions exportées :
    scrape_tanitjobs(query, session) → list[dict]
"""

import asyncio
import logging
import os
import re
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from scraper_utils import MAX_AGE_DAYS, _age_label, _too_old

load_dotenv()

logger = logging.getLogger(__name__)

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")


# ── Helpers date (inchangés) ──────────────────────────────────────────────────

def _tnj_parse_date(text: str):
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

    if any(w in low for w in ("aujourd", "today", "ce jour")):
        return now
    if any(w in low for w in ("hier", "yesterday")):
        return now - timedelta(days=1)

    m = re.search(r"il\s+y\s+a\s+(\d+)\s+jour",   low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+semaine", low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+mois",    low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)

    m = re.search(r"(\d+)\s+day",   low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s+week",  low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\s+month", low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"(\d+)\s+hour",  low)
    if m: return now

    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except (ValueError, TypeError):
            pass

    return None


def _tnj_extract_date_from_snippet(snippet: str):
    DATE_PATS = [
        r"il\s+y\s+a\s+\d+\s+(?:jour|semaine|mois|an)s?",
        r"\d+\s+(?:day|week|month|hour)s?\s+ago",
        r"aujourd['']?hui|today",
        r"hier|yesterday",
        r"\d{4}-\d{2}-\d{2}",
    ]
    for pat in DATE_PATS:
        m = re.search(pat, snippet, re.I)
        if m:
            pub_dt = _tnj_parse_date(m.group(0))
            label  = _age_label(pub_dt) if pub_dt else m.group(0)
            return pub_dt, label
    return None, ""


def _tnj_is_job_url(url: str) -> bool:
    if not url or "tanitjobs.com" not in url:
        return False
    return bool(re.search(r"/job/\d+", url))


def _clean_title(title: str) -> str:
    """Nettoie le titre retourné par SerpApi (enlève '| TanitJobs', séparateurs…)"""
    t = re.sub(
        r"\s*[\|·\-–—]\s*(?:TanitJobs|Offres?\s+d['\u2019]emploi).*$",
        "", title, flags=re.I
    ).strip()
    for sep in (" | ", " - ", " – ", " — "):
        if sep in t:
            parts = t.split(sep)
            t = max(parts, key=len).strip()
            break
    return t


# ── Point d'entrée public ─────────────────────────────────────────────────────

async def scrape_tanitjobs(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Scrape TanitJobs via scrape_google_web (SerpApi urllib direct).
    Compatible avec scraping_pipeline.py : retourne list[dict] avec
    les clés : title, url, company, location, salary, remote, time_ago,
               _tnj_contract, _tnj_experience, _tnj_description, _tnj_all_skills
    """
    if not SERPAPI_API_KEY:
        logger.warning("  [tanitjobs] SERPAPI_API_KEY manquant — skip")
        return []

    # Import local (le fichier est dans le même dossier)
    try:
        from serpapi_google_web_common import scrape_google_web, looks_like_placeholder_api_key
    except ImportError:
        logger.error("  [tanitjobs] serpapi_google_web_common.py introuvable dans le projet")
        return []

    if looks_like_placeholder_api_key(SERPAPI_API_KEY):
        logger.warning("  [tanitjobs] SERPAPI_API_KEY invalide (placeholder) — skip")
        return []

    # Query : site:tanitjobs.com/job/ + titre CV
    q = f"site:tanitjobs.com/job/ {query}".strip()
    logger.info(f"  [tanitjobs] Query: {q}")
    logger.info(f"  [tanitjobs] Démarrage — STOP à {MAX_AGE_DAYS}j")

    # On utilise un dossier temporaire pour les outputs de scrape_google_web
    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            results, _ = await asyncio.to_thread(
                scrape_google_web,
                output_dir      = Path(tmp_dir),
                source_tag      = "tanitjobs",
                q               = q,
                location        = "",
                domain          = "tanitjobs.com",
                url_allow_regex = r"/job/\d+",   # garde seulement les URLs de jobs
                derive_fields   = True,
                max_results     = 300,
                hl              = "fr",
                gl              = "tn",
                delay_s         = 0.5,
                num_per_page    = 10,
                api_key         = SERPAPI_API_KEY,
            )
        except RuntimeError as e:
            logger.error(f"  [tanitjobs] scrape_google_web error: {e}")
            return []
        except Exception as e:
            logger.error(f"  [tanitjobs] unexpected error: {type(e).__name__}: {e}")
            return []

    listings: list[dict] = []
    seen_urls: set[str]  = set()
    stopped              = False

    for item in results:
        if stopped:
            break

        raw_url = str(item.get("url", "") or "")
        if not _tnj_is_job_url(raw_url):
            continue

        clean_url = raw_url.split("?")[0].rstrip("/")
        if clean_url in seen_urls:
            continue
        seen_urls.add(clean_url)

        snippet  = str(item.get("description", "") or "")
        pub_dt, time_ago = _tnj_extract_date_from_snippet(snippet)

        if pub_dt is not None and _too_old(pub_dt):
            days = (datetime.now() - pub_dt).days
            logger.info(f"  [tanitjobs] STOP cutoff: {days}j > {MAX_AGE_DAYS}j")
            stopped = True
            break

        title    = _clean_title(str(item.get("title", "") or ""))
        company  = str(item.get("company",  "") or "")
        location = str(item.get("location", "") or "Tunisie")
        salary   = str(item.get("salary",   "") or "Non spécifié")

        if not title or len(title) < 3:
            continue

        # Inférence remote depuis snippet
        remote = ""
        snip_low = snippet.lower()
        if any(w in snip_low for w in ("télétravail", "remote", "full remote", "travail à distance")):
            remote = "Remote 🌍"
        elif any(w in snip_low for w in ("hybrid", "hybride")):
            remote = "Hybrid 🏠🏢"

        # Contrat + expérience depuis snippet
        contract = ""
        m = re.search(r"\b(CDI|CDD|Stage|Freelance|Intérim|SIVP|Alternance)\b", snippet, re.I)
        if m:
            contract = m.group(1).strip()

        experience = ""
        m = re.search(
            r"(\d+\s*(?:an[s]?|ann[eé]e[s]?|mois|year[s]?)\s*d['\u2019]exp[eé]rience"
            r"|(?:D[eé]butant|Junior|Confirm[eé]|Exp[eé]riment[eé]|Senior)[^\.,\n]{0,30})",
            snippet, re.I
        )
        if m:
            experience = m.group(1).strip()

        listings.append({
            "title":              title,
            "url":                clean_url,
            "company":            company,
            "location":           location,
            "salary":             salary,
            "remote":             remote,
            "time_ago":           time_ago,
            "_tnj_contract":      contract,
            "_tnj_experience":    experience,
            "_tnj_description":   snippet[:1500],
            "_tnj_all_skills":    "",
        })

    logger.info(f"[tanitjobs] TOTAL: {len(listings)} jobs")
    return listings