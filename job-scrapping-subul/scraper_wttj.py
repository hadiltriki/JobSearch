"""
scraper_wttj.py — Welcome to the Jungle via SerpApi Google search
===================================================================
SerpApi Google search: site:welcometothejungle.com/en/jobs + query.
Parses organic_results into pipeline job shape + _wttj_*.
"""

import os
import re
from urllib.parse import urlencode

import aiohttp

from scraper_utils import _age_label, _parse_date, _too_old

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
DEFAULT_MAX_JOBS = 40


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _normalize_result(entry: dict) -> dict | None:
    if not isinstance(entry, dict):
        return None
    title = _clean(str(entry.get("title") or ""))
    link = _clean(str(entry.get("link") or ""))
    if not title or "welcometothejungle.com" not in link.lower():
        return None
    # Skip generic listing pages (keep only company job pages)
    if "/companies/" not in link:
        return None
    snippet = _clean(str(entry.get("snippet") or ""))[:3000]
    pub_dt = _parse_date(snippet)
    if _too_old(pub_dt):
        return None
    time_ago = _age_label(pub_dt)
    # Extract company from URL: .../companies/company-name/jobs/... or use displayed_link
    company = "WTTJ"
    if "/companies/" in link:
        m = re.search(r"/companies/([^/]+)", link)
        if m:
            company = m.group(1).replace("-", " ").strip().title() or company
    if company == "WTTJ":
        company = _clean(str(entry.get("displayed_link") or "").split("/")[0] or company)
    return {
        "title": title,
        "url": link,
        "company": company,
        "location": "",
        "salary": "Not specified",
        "remote": "",
        "time_ago": time_ago,
        "_wttj_description": snippet,
        "_wttj_skills": "",
        "_wttj_snippet": snippet,
    }


async def scrape_wttj(query: str, session: aiohttp.ClientSession) -> list[dict]:
    api_key = (os.getenv("SERPAPI_API_KEY") or "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        print("  [wttj] SERPAPI_API_KEY missing → skip")
        return []
    q = f"site:welcometothejungle.com/en {query or 'software engineer'} jobs"
    params = {
        "engine": "google",
        "api_key": api_key,
        "q": q,
        "num": 40,
    }
    url = f"{SERPAPI_ENDPOINT}?{urlencode(params)}"
    results: list[dict] = []
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                print(f"  [wttj] HTTP {resp.status}")
                return []
            data = await resp.json(content_type=None)
    except Exception as e:
        print(f"  [wttj] fetch error: {e}")
        return []
    organic = data.get("organic_results") if isinstance(data, dict) else []
    if not isinstance(organic, list):
        return []
    for entry in organic:
        if len(results) >= DEFAULT_MAX_JOBS:
            break
        job = _normalize_result(entry)
        if job:
            results.append(job)
    print(f"  [wttj] TOTAL: {len(results)} jobs")
    return results
