"""
scraper_linkedin.py — LinkedIn via SerpApi Google Jobs
=========================================================
Uses SerpApi Google Jobs API, keeps only results with linkedin.com in link.
SERPAPI_API_KEY from .env. Same job shape as pipeline + _linkedin_*.
"""

import os
import re
from urllib.parse import urlencode

import aiohttp

from scraper_utils import _age_label, _parse_date, _too_old

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
DEFAULT_MAX_JOBS = 50


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _extract_salary(job: dict) -> str:
    ext = job.get("detected_extensions") or {}
    if isinstance(ext, dict):
        s = _clean(str(ext.get("salary") or ""))
        if s:
            return s
    return _clean(str(job.get("salary") or ""))


def _extract_apply_url(job: dict) -> str:
    for k in ("job_google_link", "link", "url"):
        v = job.get(k)
        if isinstance(v, str):
            u = _clean(v)
            if "linkedin.com" in u.lower():
                return u
    opts = job.get("apply_options") or []
    for o in opts:
        if isinstance(o, dict):
            link = _clean(str(o.get("link") or ""))
            if "linkedin.com" in link.lower():
                return link
    return ""


def _is_linkedin(job: dict) -> bool:
    for k in ("job_google_link", "link", "url"):
        v = job.get(k)
        if isinstance(v, str) and "linkedin.com" in (v or "").lower():
            return True
    for o in (job.get("apply_options") or []):
        if isinstance(o, dict):
            link = _clean(str(o.get("link") or ""))
            if "linkedin.com" in link.lower():
                return True
    return False


def _extract_remote_contract(extensions: list, detected: dict) -> tuple[str, str]:
    remote, contract = "", ""
    for x in (extensions or []):
        if isinstance(x, str):
            t = _clean(x)
            low = t.lower()
            if "remote" in low or "hybrid" in low:
                remote = t
            if "full-time" in low or "part-time" in low or "contract" in low:
                contract = t
    if isinstance(detected, dict):
        remote = remote or _clean(str(detected.get("work_from_home") or ""))
        contract = contract or _clean(str(detected.get("schedule_type") or ""))
    return remote, contract


def _normalize_job(job: dict) -> dict | None:
    title = _clean(str(job.get("title") or ""))
    if not title:
        return None
    url = _extract_apply_url(job)
    if not url:
        return None
    company = _clean(str(job.get("company_name") or job.get("company") or ""))
    location = _clean(str(job.get("location") or ""))
    description = _clean(str(job.get("description") or ""))[:5000]
    salary = _extract_salary(job)
    ext = job.get("extensions") or []
    det = job.get("detected_extensions") or {}
    remote, contract = _extract_remote_contract(ext, det)
    posted = _clean(str((det if isinstance(det, dict) else {}).get("posted_at") or ""))
    for x in ext:
        if isinstance(x, str) and "ago" in (x or "").lower():
            posted = _clean(x)
            break
    pub_dt = _parse_date(posted)
    time_ago = _age_label(pub_dt)
    if _too_old(pub_dt):
        return None
    return {
        "title": title,
        "url": url,
        "company": company or "Company",
        "location": location,
        "salary": salary or "Not specified",
        "remote": remote,
        "time_ago": time_ago,
        "_linkedin_description": description,
        "_linkedin_skills": "",
        "_linkedin_contract": contract,
        "_linkedin_remote": remote,
        "_linkedin_salary": salary,
    }


async def scrape_linkedin(query: str, session: aiohttp.ClientSession) -> list[dict]:
    api_key = (os.getenv("SERPAPI_API_KEY") or "").strip()
    if not api_key or api_key.lower().startswith("your_"):
        print("  [linkedin] SERPAPI_API_KEY missing → skip")
        return []
    params = {
        "engine": "google_jobs",
        "api_key": api_key,
        "q": query or "software engineer",
    }
    url = f"{SERPAPI_ENDPOINT}?{urlencode(params)}"
    results: list[dict] = []
    next_token = None
    while len(results) < DEFAULT_MAX_JOBS:
        if next_token:
            url = f"{SERPAPI_ENDPOINT}?{urlencode({**params, 'next_page_token': next_token})}"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                if resp.status != 200:
                    break
                data = await resp.json(content_type=None)
        except Exception as e:
            print(f"  [linkedin] fetch error: {e}")
            break
        jobs_raw = data.get("jobs_results") if isinstance(data, dict) else None
        if not isinstance(jobs_raw, list):
            break
        for j in jobs_raw:
            if not isinstance(j, dict) or not _is_linkedin(j):
                continue
            norm = _normalize_job(j)
            if norm:
                results.append(norm)
            if len(results) >= DEFAULT_MAX_JOBS:
                break
        next_token = None
        if isinstance(data, dict):
            sp = data.get("serpapi_pagination") or {}
            if isinstance(sp, dict):
                next_token = _clean(str(sp.get("next_page_token") or "")) or None
        if not next_token:
            break
    print(f"  [linkedin] TOTAL: {len(results)} jobs")
    return results
