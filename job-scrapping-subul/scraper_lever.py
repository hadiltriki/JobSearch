"""
scraper_lever.py — Lever.co public postings API
=================================================
GET https://api.lever.co/v0/postings/{slug}?mode=json
Same job shape as pipeline + _lever_*.
"""

import asyncio
import re
from datetime import datetime

import aiohttp

from scraper_utils import _age_label, _too_old

LEVER_API_BASE = "https://api.lever.co/v0/postings"
LEVER_BOARDS = [
    "netflix",
    "stripe",
    "dropbox",
    "spotify",
    "atlassian",
    "twilio",
    "canva",
    "figma",
    "notion",
    "airtable",
    "discord",
    "robinhood",
    "coinbase",
    "square",
    "shopify",
    "slack",
    "salesforce",
    "adobe",
    "servicenow",
    "snowflake",
]
LEVER_DELAY = 0.15


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _lever_pub_dt(job: dict) -> datetime | None:
    for key in ("createdAt", "updatedAt", "publishedAt"):
        raw = job.get(key)
        if not raw:
            continue
        try:
            s = str(raw).replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=None)
        except (ValueError, TypeError):
            pass
    return None


def _lever_description(job: dict) -> str:
    desc = job.get("description") or job.get("descriptionPlain") or ""
    if isinstance(desc, str):
        return _clean(desc)[:5000]
    return ""


def _lever_categories(job: dict) -> str:
    cats = job.get("categories") or {}
    if not isinstance(cats, dict):
        return ""
    parts = []
    for k in ("department", "team", "commitment", "location"):
        v = cats.get(k)
        if isinstance(v, str) and _clean(v):
            parts.append(_clean(v))
    return ", ".join(parts)


def _normalize_job(slug: str, job: dict) -> dict | None:
    if not isinstance(job, dict):
        return None
    title = _clean(str(job.get("title") or ""))
    if not title:
        return None
    url = _clean(str(job.get("hostedUrl") or job.get("applyUrl") or ""))
    location = _clean(str((job.get("categories") or {}).get("location") or ""))
    desc = _lever_description(job)
    pub_dt = _lever_pub_dt(job)
    if pub_dt and _too_old(pub_dt):
        return None
    time_ago = _age_label(pub_dt)
    contract = _clean(str((job.get("categories") or {}).get("commitment") or ""))
    return {
        "title": title,
        "url": url or f"https://jobs.lever.co/{slug}",
        "company": slug.capitalize(),
        "location": location,
        "salary": "Not specified",
        "remote": "Remote" if "remote" in location.lower() else "",
        "time_ago": time_ago,
        "_lever_description": desc,
        "_lever_contract": contract,
        "_lever_categories": _lever_categories(job),
    }


async def scrape_lever(query: str, session: aiohttp.ClientSession) -> list[dict]:
    results: list[dict] = []
    for i, slug in enumerate(LEVER_BOARDS):
        await asyncio.sleep(i * LEVER_DELAY)
        url = f"{LEVER_API_BASE}/{slug}?mode=json"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status != 200:
                    # 404 = no Lever board for this slug; skip quietly
                    if resp.status != 404:
                        print(f"  [lever/{slug}] HTTP {resp.status}")
                    continue
                data = await resp.json(content_type=None)
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            print(f"  [lever/{slug}] error: {err_msg}")
            continue
        postings = data if isinstance(data, list) else (data.get("postings") or data.get("data") or [])
        if not isinstance(postings, list):
            continue
        for raw in postings:
            job = _normalize_job(slug, raw)
            if job:
                results.append(job)
    print(f"  [lever] TOTAL: {len(results)} jobs")
    return results
