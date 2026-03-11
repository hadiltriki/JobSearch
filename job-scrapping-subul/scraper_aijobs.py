"""
scraper_aijobs.py — Scraper aijobs.ai

Deux modes :
  1. scrape_google_web  (si SERPAPI_API_KEY disponible) — urllib direct, pas de lib externe
  2. HTTP direct (fallback) — avec Accept-Encoding: gzip,deflate (pas de Brotli)

Fonctions exportées :
    scrape_aijobs(query, session) → list[dict]
"""

import asyncio
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from scraper_utils import (
    MAX_AGE_DAYS, MAX_RETRIES, RETRY_WAIT,
    DELAY_BETWEEN_PAGES, WARMUP_DELAY, HTTP_TIMEOUT,
    BROWSER_HEADERS,
    _parse_date, _age_label, _too_old, _infer_remote,
    _cv_title_to_tags, _extract_tech_keywords,
)

load_dotenv()

SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")
AIJOBS_BASE    = "https://aijobs.ai"

# ── Headers sans Brotli pour éviter l'erreur de décodage ──────────────────────
_HEADERS_NO_BR = {
    **BROWSER_HEADERS,
    "Accept-Encoding": "gzip, deflate",   # pas de br
}


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers communs
# ══════════════════════════════════════════════════════════════════════════════

def _looks_like_placeholder(key: str) -> bool:
    return not key or key.startswith("your_") or key == "YOUR_SERPAPI_KEY"


def _parse_listing(item: dict) -> dict | None:
    """Convertit un résultat SerpApi/scrape_google_web en job dict."""
    raw_url = item.get("link", "")
    if "aijobs.ai/job/" not in raw_url:
        return None
    try:
        slug = raw_url.split("aijobs.ai/job/")[1].split("?")[0].rstrip("/")
    except IndexError:
        return None
    if not slug:
        return None

    clean_url = f"{AIJOBS_BASE}/job/{slug}"

    title = item.get("title", "")
    for sep in (" - ", " | ", " – ", " — "):
        if sep in title:
            title = title.split(sep)[0].strip()
            break
    if not title:
        title = slug.replace("-", " ").title()

    snippet  = item.get("snippet", "")
    time_ago, pub_dt = "", None

    dm = re.search(
        r'(\d+\s+(?:day|week|month|hour)s?\s+ago'
        r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,\s+\d{4}'
        r'|\d{4}-\d{2}-\d{2})',
        snippet, re.I
    )
    if dm:
        pub_dt   = _parse_date(dm.group(0))
        time_ago = _age_label(pub_dt) if pub_dt else dm.group(0)
    if pub_dt is None and item.get("date"):
        pub_dt   = _parse_date(str(item["date"]))
        time_ago = _age_label(pub_dt) if pub_dt else str(item["date"])

    if pub_dt and _too_old(pub_dt):
        return None   # trop ancien

    company = ""
    cm = re.search(r'\bat\s+([A-Z][^\.\n,]{2,40})', snippet)
    if cm:
        company = cm.group(1).strip()

    return {
        "title":   title,
        "url":     clean_url,
        "company": company,
        "location": "",
        "salary":  "Not specified",
        "time_ago": time_ago,
        "remote":  _infer_remote(snippet),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Mode scrape_google_web (urllib, pas de lib serpapi)
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_aijobs_google_web(query: str) -> list[dict]:
    """
    Utilise scrape_google_web (serpapi_google_web_common.py) pour chercher
    des offres aijobs.ai via l'API SerpApi sans dépendance externe.
    """
    try:
        from serpapi_google_web_common import scrape_google_web, looks_like_placeholder_api_key
    except ImportError:
        print("  [aijobs/google_web] serpapi_google_web_common.py introuvable")
        return []

    if looks_like_placeholder_api_key(SERPAPI_API_KEY):
        print("  [aijobs/google_web] SERPAPI_API_KEY non configurée")
        return []

    tech_kw = _extract_tech_keywords(query)
    queries_to_try = [
        f"site:aijobs.ai/job/ {tech_kw}",
        f"site:aijobs.ai/job/ {query}",
    ]

    listings:   list[dict] = []
    seen_slugs: set[str]   = set()

    for search_query in queries_to_try:
        print(f"  [aijobs/google_web] query: '{search_query}'")
        with tempfile.TemporaryDirectory() as tmpdir:
            try:
                # scrape_google_web retourne (list[dict], Path)
                raw_results, _ = await asyncio.to_thread(
                    lambda q=search_query, d=tmpdir: scrape_google_web(
                        q              = q,
                        output_dir     = Path(d),
                        source_tag     = "aijobs",
                        api_key        = SERPAPI_API_KEY,
                        url_allow_regex= r"aijobs\.ai/job/",
                        derive_fields  = False,
                        max_results    = 50,
                    )
                )
            except Exception as e:
                print(f"  [aijobs/google_web] exception: {e}")
                continue

        # raw_results est déjà une list[dict] normalisés par scrape_google_web
        # On les re-mappe au format attendu par _parse_listing
        items = [
            {
                "link":    r.get("url", ""),
                "title":   r.get("title", ""),
                "snippet": r.get("description", ""),
            }
            for r in raw_results
        ]
        print(f"  [aijobs/google_web] {len(items)} resultats bruts")

        stopped_early = False
        for item in items:
            # Stop si la date de publication > MAX_AGE_DAYS
            snippet  = item.get("snippet", "")
            raw_date = str(item.get("date", ""))
            dm = re.search(
                r'(\d+\s+(?:day|week|month|hour)s?\s+ago'
                r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,\s+\d{4}'
                r'|\d{4}-\d{2}-\d{2})',
                snippet or raw_date, re.I
            )
            pub_dt = _parse_date(dm.group(0)) if dm else (_parse_date(raw_date) if raw_date else None)
            if pub_dt and _too_old(pub_dt):
                days_old = (datetime.now() - pub_dt).days
                print(f"  [aijobs/google_web] STOP cutoff ({days_old}d > {MAX_AGE_DAYS}d)")
                stopped_early = True
                break

            job = _parse_listing(item)
            if job is None:
                continue
            slug = job["url"].split("/job/")[1]
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)
            listings.append(job)

        if stopped_early:
            break

    print(f"[aijobs] TOTAL (google_web): {len(listings)}")
    return listings


# ══════════════════════════════════════════════════════════════════════════════
#  Mode HTTP direct (fallback) — sans Brotli
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_aijobs_page(url: str) -> str | None:
    """Fetch HTTP avec Accept-Encoding sans br pour éviter l'erreur Brotli."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connector = aiohttp.TCPConnector(ssl=False, force_close=True)
            async with aiohttp.ClientSession(
                connector=connector,
                headers=_HEADERS_NO_BR          # ← pas de br ici
            ) as s:
                async with s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    if resp.status == 429:
                        wait = RETRY_WAIT * attempt
                        print(f"  [aijobs/direct] 429 → attente {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status in (403, 404):
                        return None
                    return None
        except Exception as e:
            print(f"  [aijobs/direct] error ({attempt}): {e}")
            if attempt < MAX_RETRIES:
                await asyncio.sleep(10)
    return None


def _parse_job_links(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    job_links = soup.find_all("a", href=re.compile(r'^/job/[^/"\\s]+'))
    if not job_links:
        job_links = [a for a in soup.find_all("a", href=True)
                     if a.get("href", "").startswith(f"{AIJOBS_BASE}/job/")]

    SKIP = {"post a job","home","jobs","companies","pricing","blog","sign in",
            "post job","full time","part time","contract","remote","search",
            "filter","load more","back","see more","apply now","job details",
            "freelance","internship"}

    results: list[dict] = []
    seen:    set[str]   = set()

    for link in job_links:
        href = link.get("href", "")
        slug = (href.rstrip("/").split("/job/")[-1].split("?")[0]
                if href.startswith("http")
                else href.replace("/job/", "").split("?")[0].rstrip("/"))
        if not slug or slug in seen:
            continue
        seen.add(slug)
        job_url = f"{AIJOBS_BASE}/job/{slug}"

        title = ""
        for tag in ("h2","h3","h4","h1","span","p","div"):
            el = link.find(tag)
            if el:
                t = el.get_text(separator=" ", strip=True)
                if t and len(t) > 3 and t.lower() not in SKIP:
                    title = t[:120]
                    break
        if not title:
            title = " ".join(link.get_text(separator=" ", strip=True).split())[:120]
        if not title or len(title) < 3 or title.lower() in SKIP:
            continue

        company = time_ago = ""
        parent  = link.parent
        if parent:
            for node_text in parent.find_all(string=True):
                t = node_text.strip()
                if not t or t == title:
                    continue
                if re.search(r'\d+[dwmhDWMH]|\bday|\bweek|\bmonth|\btoday|\byesterday|\bhour', t, re.I):
                    if not time_ago:
                        time_ago = t
                elif not company and 2 < len(t) < 60 and t.lower() not in SKIP:
                    company = t

        if not time_ago:
            for attr in ("data-date","data-time","datetime","data-posted","data-age"):
                val = link.get(attr, "")
                if val:
                    time_ago = str(val).strip()
                    break

        results.append({
            "title":    title,
            "url":      job_url,
            "company":  company,
            "location": "",
            "salary":   "Not specified",
            "time_ago": time_ago,
            "remote":   _infer_remote(link.get_text(" ", strip=True)),
        })
    return results


async def _scrape_aijobs_direct(session: aiohttp.ClientSession) -> list[dict]:
    listings: list[dict] = []
    seen:     set[str]   = set()

    print(f"  [aijobs/direct] warm-up {AIJOBS_BASE} ...")
    html = await _fetch_aijobs_page(AIJOBS_BASE)
    print(f"  [aijobs/direct] warm-up {'OK' if html else 'failed'}")
    await asyncio.sleep(WARMUP_DELAY)

    page_num = 0
    while True:
        page_num += 1
        url = f"{AIJOBS_BASE}/jobs" if page_num == 1 else f"{AIJOBS_BASE}/jobs?page={page_num}"
        if page_num > 1:
            await asyncio.sleep(DELAY_BETWEEN_PAGES)

        html = await _fetch_aijobs_page(url)
        if not html:
            print(f"  [aijobs/direct] page {page_num}: fetch failed")
            break

        page_jobs = _parse_job_links(html)
        if not page_jobs:
            print(f"  [aijobs/direct] page {page_num}: 0 jobs")
            break

        new_count     = 0
        stopped_early = False

        for job in page_jobs:
            slug = job["url"].split("/job/")[1]
            if slug in seen:
                continue
            seen.add(slug)
            pub_dt = _parse_date(job.get("time_ago", ""))
            if pub_dt and _too_old(pub_dt):
                print(f"  [aijobs/direct] STOP {(datetime.now()-pub_dt).days}d > {MAX_AGE_DAYS}d")
                stopped_early = True
                break
            if pub_dt:
                job["time_ago"] = _age_label(pub_dt)
            listings.append(job)
            new_count += 1

        print(f"  [aijobs/direct] page {page_num}: +{new_count} (total {len(listings)})")
        if stopped_early:
            break

    return listings


# ══════════════════════════════════════════════════════════════════════════════
#  Point d'entrée public
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_aijobs(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Priorité :
      1. scrape_google_web via SerpApi (urllib, pas de lib externe)
      2. HTTP direct fallback (sans Brotli)
    """
    if SERPAPI_API_KEY and not _looks_like_placeholder(SERPAPI_API_KEY):
        print(f"  [aijobs] Mode: scrape_google_web")
        listings = await _scrape_aijobs_google_web(query)
        if listings:
            return listings
        print(f"  [aijobs] google_web 0 résultats → fallback HTTP direct")

    print(f"  [aijobs] Mode: HTTP direct")
    listings = await _scrape_aijobs_direct(session)
    print(f"[aijobs] TOTAL (direct): {len(listings)}")
    return listings