"""
scraper_remoteok.py — Scraper remoteok.com

Deux modes :
  1. HTML /remote-{tag}-jobs (par tag technique)
  2. API JSON /api (fallback)

Fonctions exportées :
    scrape_remoteok(query, session) → list[dict]
"""

import re
from datetime import datetime, timedelta, timezone

import aiohttp
from bs4 import BeautifulSoup

from scraper_utils import (
    MAX_AGE_DAYS, HTTP_TIMEOUT, BROWSER_HEADERS,
    _age_label, _too_old,
    _cv_title_to_tags,
)

REMOTEOK_BASE = "https://remoteok.com"
REMOTEOK_API  = "https://remoteok.com/api"


# ── Epoch UTC correct (évite le bug de fuseau local) ─────────────────────────

def _ro_epoch_to_dt(epoch) -> datetime | None:
    """
    Epoch → datetime UTC naive.
    Utilise tz=UTC EXPLICITEMENT pour corriger le bug "9j affiché comme 45j"
    (fromtimestamp sans tz utilise le fuseau local du serveur = +1h/+2h off).
    """
    if not epoch:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).replace(tzinfo=None)
    except (ValueError, TypeError, OSError):
        return None


# ── Parser HTML remoteok ──────────────────────────────────────────────────────

def _parse_remoteok_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    jobs = []
    for tr in soup.find_all("tr", class_=re.compile(r'\bjob\b')):
        title_el = (tr.find(itemprop="title") or
                    tr.find(class_=re.compile(r'title|position', re.I)) or
                    tr.find(["h2","h3"]))
        if not title_el: continue
        title = title_el.get_text(strip=True)
        if not title: continue

        link    = tr.find("a", href=re.compile(r'/[^/]+-\d+'))
        job_url = ""
        if link:
            href    = link.get("href", "")
            job_url = f"{REMOTEOK_BASE}{href}" if href.startswith("/") else href
        if not job_url: continue

        company_el = tr.find(itemprop="name") or tr.find(class_=re.compile(r'company', re.I))
        company    = company_el.get_text(strip=True) if company_el else ""

        pub_dt_n = None
        date_el  = tr.find("time")
        if date_el:
            try:
                pub_dt   = datetime.fromisoformat(date_el.get("datetime","").replace("Z","+00:00"))
                pub_dt_n = pub_dt.replace(tzinfo=None)
            except Exception:
                pass

        if pub_dt_n is None:
            row_text = tr.get_text(" ", strip=True)
            for pattern, delta_fn in [
                (r'\b(\d+)\s*d\b', lambda n: timedelta(days=n)),
                (r'\b(\d+)\s*w\b', lambda n: timedelta(weeks=n)),
                (r'\b(\d+)\s*m\b', lambda n: timedelta(days=n*30)),
            ]:
                m = re.search(pattern, row_text)
                if m:
                    pub_dt_n = datetime.now() - delta_fn(int(m.group(1))); break

        if pub_dt_n and _too_old(pub_dt_n):
            print(f"  [remoteok/html] STOP {(datetime.now()-pub_dt_n).days}d > {MAX_AGE_DAYS}d (trié newest→oldest)")
            break

        salary_el = tr.find(class_=re.compile(r'salary', re.I))
        salary    = salary_el.get_text(strip=True) if salary_el else "Not specified"
        epoch     = int(pub_dt_n.timestamp()) if pub_dt_n else 0

        jobs.append({
            "position": title, "company": company, "url": job_url,
            "salary": salary, "epoch": epoch,
            "time_ago": _age_label(pub_dt_n),
            "tags": [t.get_text(strip=True) for t in tr.find_all(class_=re.compile(r'\btag\b', re.I))],
        })
    return jobs


# ══════════════════════════════════════════════════════════════════════════════
#  Point d'entrée public
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_remoteok(query: str, session: aiohttp.ClientSession) -> list[dict]:
    tags = _cv_title_to_tags(query)
    print(f"  [remoteok] tags: {tags}")

    urls_to_try: list[dict] = [
        {"url": f"{REMOTEOK_BASE}/remote-{t.replace(' ','-').lower()}-jobs", "is_json": False}
        for t in tags
    ]
    urls_to_try.append({"url": REMOTEOK_API, "is_json": True})

    raw_items: list = []
    used_url        = ""

    for entry in urls_to_try:
        api_url = entry["url"]
        is_json = entry["is_json"]
        try:
            headers = {
                "User-Agent": BROWSER_HEADERS["User-Agent"],
                "Accept":     "application/json" if is_json else "text/html",
                "Referer":    "https://remoteok.com",
            }
            async with session.get(api_url, headers=headers,
                                   timeout=aiohttp.ClientTimeout(total=25), ssl=False) as resp:
                if resp.status == 200:
                    if is_json:
                        data      = await resp.json(content_type=None)
                        raw_items = [j for j in (data[1:] if isinstance(data, list) else [])
                                     if isinstance(j, dict)]
                    else:
                        parsed = _parse_remoteok_html(await resp.text())
                        if parsed:
                            used_url = api_url
                            print(f"  [remoteok] {len(parsed)} jobs (HTML) de {api_url}")
                            print(f"[remoteok] TOTAL: {len(parsed)}")
                            return parsed
                    print(f"  [remoteok] {len(raw_items)} raw de {api_url}")
                    if raw_items:
                        used_url = api_url; break
                else:
                    print(f"  [remoteok] HTTP {resp.status} → {api_url}")
        except Exception as e:
            print(f"  [remoteok] error ({api_url}): {e}")

    listings:        list[dict] = []
    skipped_no_date: int        = 0
    MAX_NO_DATE_SKIP             = 5   # sécurité : si trop de jobs sans date → on arrête

    for item in raw_items:
        if not isinstance(item, dict): continue

        title   = str(item.get("position") or item.get("title") or "").strip()
        company = str(item.get("company") or "").strip()
        job_url = str(item.get("url")     or "").strip()
        if not title or not job_url: continue
        if job_url.startswith("/"): job_url = f"{REMOTEOK_BASE}{job_url}"

        pub_dt = _ro_epoch_to_dt(item.get("epoch"))
        if pub_dt is None:
            date_str = str(item.get("date") or "")
            m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
            if m:
                from scraper_utils import _parse_date
                pub_dt = _parse_date(m.group(1))
        if pub_dt is None:
            skipped_no_date += 1
            if skipped_no_date >= MAX_NO_DATE_SKIP:
                print(f"  [remoteok] STOP: {MAX_NO_DATE_SKIP} jobs consécutifs sans date")
                break
            continue

        skipped_no_date = 0   # reset : on a une date valide

        days_old = (datetime.now() - pub_dt).days
        if days_old > MAX_AGE_DAYS:
            print(f"  [remoteok] STOP cutoff {days_old}d > {MAX_AGE_DAYS}d"); break

        lo, hi, sal = item.get("salary_min"), item.get("salary_max"), item.get("salary","")
        if lo and hi:
            try:    salary = f"${int(float(lo)):,} – ${int(float(hi)):,} / yr"
            except: salary = sal or "Not specified"
        elif lo:
            try:    salary = f"${int(float(lo)):,}+ / yr"
            except: salary = sal or "Not specified"
        else:
            salary = sal or "Not specified"

        listings.append({
            "title":    title,      "url":      job_url,
            "company":  company,    "salary":   salary,
            "location": item.get("location") or "Worldwide / Remote",
            "remote":   "Full Remote 🌍",
            "time_ago": _age_label(pub_dt),
        })

    if skipped_no_date:
        print(f"  [remoteok] {skipped_no_date} jobs sans date ignorés")
    print(f"[remoteok] TOTAL: {len(listings)} (via: {used_url or 'none'})")
    return listings