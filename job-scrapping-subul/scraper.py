"""
scraper.py — Scrapers d'offres d'emploi

Sites supportés :
  1. aijobs.ai  — SerpApi Google Search (si SERPAPI_API_KEY) → fallback HTTP direct
  2. remoteok.com — HTML /remote-{tag}-jobs + fallback API JSON
  3. emploitic.com — Sitemap XML + JSON-LD JobPosting (offres Algérie)

Règles communes :
  - STOP immédiat dès qu'un job > MAX_AGE_DAYS (45j) est détecté
  - Date affichée en jours exacts ("11 days ago", "Aujourd'hui", "Il y a 3 jours")
  - Tous les scrapers retournent des dicts compatibles handle_job() de main.py
"""

import asyncio
import json as _json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ── SerpApi optionnel ─────────────────────────────────────────────────────────
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "")
try:
    from serpapi import GoogleSearch as _SerpApiSearch
    _SERPAPI_AVAILABLE = True
except ImportError:
    _SERPAPI_AVAILABLE = False

# ── Constantes globales ───────────────────────────────────────────────────────
AIJOBS_BASE       = "https://aijobs.ai"
REMOTEOK_BASE     = "https://remoteok.com"
REMOTEOK_API      = "https://remoteok.com/api"
EMPLOITIC_BASE    = "https://emploitic.com"

MAX_AGE_DAYS      = 45

# aijobs anti-429
DELAY_BETWEEN_PAGES = 8.0
WARMUP_DELAY        = 3.0
RETRY_WAIT          = 45
MAX_RETRIES         = 3

# emploitic : délai poli entre requêtes
EMPLOITIC_DELAY = 0.3   # réduit : filtre cosine pré-filtre les jobs inutiles avant le fetch

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)

BROWSER_HEADERS = {
    "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate, br",
    "Referer":                   "https://aijobs.ai/",
    "Connection":                "keep-alive",
    "sec-ch-ua":                 '"Chromium";v="122", "Google Chrome";v="122"',
    "sec-ch-ua-mobile":          "?0",
    "sec-ch-ua-platform":        '"Windows"',
    "sec-fetch-dest":            "document",
    "sec-fetch-mode":            "navigate",
    "sec-fetch-site":            "same-origin",
    "sec-fetch-user":            "?1",
    "upgrade-insecure-requests": "1",
    "Cache-Control":             "max-age=0",
}


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS DATE COMMUNS
# ══════════════════════════════════════════════════════════════════════════════

def _parse_date(text: str) -> datetime | None:
    """
    Parse tous les formats de date rencontrés sur les job boards :
      "7D" / "3W" / "2M"           (listing aijobs)
      "7 days ago" / "1 week ago"   (snippets SerpApi)
      "just now" / "today" / "yesterday"
      "2024-03-15T10:00:00Z"        (ISO)
      "March 15, 2024"
      1710500000                    (Unix epoch)
    """
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    m = re.match(r'^(\d+)\s*([DdWwMmHhYy])$', s)
    if m:
        n, unit = int(m.group(1)), m.group(2).upper()
        if unit == 'H': return now - timedelta(hours=n)
        if unit == 'D': return now - timedelta(days=n)
        if unit == 'W': return now - timedelta(weeks=n)
        if unit == 'M': return now - timedelta(days=n * 30)
        if unit == 'Y': return now - timedelta(days=n * 365)

    low = s.lower()
    if any(w in low for w in ('just now', 'today', 'hour', 'minute')): return now
    if 'yesterday' in low: return now - timedelta(days=1)

    m = re.search(r'\b(\d+)\s+day',   low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r'\b(\d+)\s+week',  low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r'\b(\d+)\s+month', low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)

    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",     "%Y-%m-%d",
        "%B %d, %Y",             "%b %d, %Y",
    ):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except (ValueError, TypeError):
            pass

    try:
        ts = int(float(s))
        if ts > 1_000_000_000:
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except (ValueError, TypeError):
        pass

    return None


def _age_label(dt: datetime | None) -> str:
    """Jours EXACTS : "11 days ago" et non "1 week ago"."""
    if dt is None:
        return ""
    days = max(0, (datetime.now() - dt).days)
    if days == 0: return "today"
    if days == 1: return "1 day ago"
    return f"{days} days ago"


def _too_old(dt: datetime | None) -> bool:
    return dt is not None and (datetime.now() - dt).days > MAX_AGE_DAYS


def _infer_remote(text: str) -> str:
    t = text.lower()
    if "fully remote" in t or "100% remote" in t: return "Full Remote 🌍"
    if "hybrid"  in t:                            return "Hybrid 🏠🏢"
    if "on-site" in t or "on site" in t:          return "On-site 🏢"
    if "remote"  in t or "worldwide" in t:        return "Remote 🌍"
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS TAG / KEYWORD EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

_SENIORITY  = {"senior", "junior", "mid", "lead", "staff", "principal",
               "associate", "head", "chief", "director", "vp", "manager", "sr", "jr"}
_ROLE_WORDS = {"engineer", "developer", "programmer", "specialist", "analyst",
               "architect", "consultant", "expert", "scientist", "researcher",
               "and", "or", "the", "of", "for", "a", "an", "in", "with"}

_TECH_MAP = {
    "python": "python", "javascript": "javascript", "typescript": "typescript",
    "react": "react", "node": "nodejs", "nodejs": "nodejs", "vue": "vue",
    "angular": "angular", "java": "java", "golang": "golang", "go": "golang",
    "rust": "rust", "ruby": "ruby", "php": "php", "scala": "scala",
    "kotlin": "kotlin", "swift": "swift", "dotnet": "dotnet", "devops": "devops",
    "aws": "aws", "gcp": "gcp", "azure": "azure", "docker": "docker",
    "kubernetes": "kubernetes", "k8s": "kubernetes", "terraform": "terraform",
    "mlops": "mlops", "ml": "machine-learning", "ai": "ai",
    "backend": "backend", "frontend": "frontend", "fullstack": "fullstack",
    "mobile": "mobile", "ios": "ios", "android": "android", "qa": "qa",
    "security": "security", "blockchain": "blockchain", "web3": "web3",
    "cloud": "cloud", "data": "data", "sql": "sql", "embedded": "embedded",
    "saas": "saas", "machine learning": "machine-learning",
    "data science": "data-science", "data scientist": "data-science",
    "data engineer": "data-engineer", "full stack": "fullstack",
    "full-stack": "fullstack", "ml engineer": "machine-learning", "ai engineer": "ai",
}


def _cv_title_to_tags(cv_title: str) -> list[str]:
    lower = cv_title.lower().strip()
    tags: list[str] = []
    for phrase, tag in _TECH_MAP.items():
        if " " in phrase and phrase in lower and tag not in tags:
            tags.append(tag)
            lower = lower.replace(phrase, " ")
    for word in re.findall(r'[a-z0-9#+.]+', lower):
        if word in _SENIORITY or word in _ROLE_WORDS:
            continue
        mapped = _TECH_MAP.get(word)
        if mapped and mapped not in tags:
            tags.append(mapped)
        elif len(word) >= 3 and word not in tags:
            tags.append(word)
    if not tags:
        for word in re.findall(r'[a-z]+', cv_title.lower()):
            if word not in _SENIORITY and word not in _ROLE_WORDS and len(word) >= 3:
                tags.append(word)
                break
    return tags[:3]


def _extract_tech_keywords(title: str) -> str:
    tags  = _cv_title_to_tags(title)
    extra = [
        w for w in re.findall(r'[a-zA-Z0-9#+.]+', title.lower())
        if w not in _SENIORITY and w not in _ROLE_WORDS
        and len(w) >= 3 and w not in tags
    ]
    combined = tags + extra[:2]
    return " ".join(combined[:5]) if combined else title


# ══════════════════════════════════════════════════════════════════════════════
#  1.  aijobs.ai
# ══════════════════════════════════════════════════════════════════════════════

async def _scrape_aijobs_serpapi(query: str) -> list[dict]:
    listings:     list[dict] = []
    seen_slugs:   set[str]   = set()
    stopped_early             = False

    tech_kw        = _extract_tech_keywords(query)
    queries_to_try = [
        f"site:aijobs.ai/job/ {tech_kw}",
        f"site:aijobs.ai/job/ {query}",
    ]

    for search_query in queries_to_try:
        if stopped_early:
            break
        print(f"  [aijobs/serpapi] query: '{search_query}'")

        page_num = -1
        while True:  # continue jusqu'au cutoff date
            page_num += 1
            if stopped_early:
                break
            try:
                params = {
                    "engine":  "google",
                    "q":       search_query,
                    "api_key": SERPAPI_API_KEY,
                    "num":     10,
                    "start":   page_num * 10,
                    "hl":      "en",
                    "gl":      "us",
                    "tbs":     "qdr:m3",
                }
                results = await asyncio.to_thread(
                    lambda p=params: _SerpApiSearch(p).get_dict()
                )
                items = results.get("organic_results", [])
                print(f"  [aijobs/serpapi] page {page_num+1}: {len(items)} raw")
                if not items:
                    break

                new_count = 0
                for item in items:
                    raw_url = item.get("link", "")
                    if "/job/" not in raw_url or "aijobs.ai" not in raw_url:
                        continue
                    try:
                        slug = raw_url.split("aijobs.ai/job/")[1].split("?")[0].rstrip("/")
                    except IndexError:
                        continue
                    if not slug or slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)

                    clean_url = f"{AIJOBS_BASE}/job/{slug}"
                    title     = item.get("title", "")
                    for sep in (" - ", " | ", " – "):
                        if sep in title:
                            title = title.split(sep)[0].strip(); break
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
                        print(f"  [aijobs/serpapi] STOP cutoff ({(datetime.now()-pub_dt).days}d)")
                        stopped_early = True
                        break

                    company = ""
                    cm = re.search(r'\bat\s+([A-Z][^\.\n,]{2,40})', snippet)
                    if cm:
                        company = cm.group(1).strip()

                    listings.append({
                        "title": title, "slug": slug, "url": clean_url,
                        "company": company, "location": "",
                        "salary": "Not specified", "time_ago": time_ago,
                        "remote": _infer_remote(snippet),
                    })
                    new_count += 1

                print(f"  [aijobs/serpapi] +{new_count} kept (total {len(listings)})")
                if not results.get("serpapi_pagination", {}).get("next"):
                    break
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"  [aijobs/serpapi] exception: {e}")
                return []

    return listings


async def _fetch_aijobs_page(url: str) -> str | None:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            connector = aiohttp.TCPConnector(ssl=False, force_close=True)
            async with aiohttp.ClientSession(connector=connector, headers=BROWSER_HEADERS) as s:
                async with s.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    if resp.status == 429:
                        wait = RETRY_WAIT * attempt
                        print(f"  [aijobs/direct] 429 → attente {wait}s")
                        await asyncio.sleep(wait); continue
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
        if not slug or slug in seen: continue
        seen.add(slug)
        job_url = f"{AIJOBS_BASE}/job/{slug}"

        title = ""
        for tag in ("h2","h3","h4","h1","span","p","div"):
            el = link.find(tag)
            if el:
                t = el.get_text(separator=" ", strip=True)
                if t and len(t) > 3 and t.lower() not in SKIP:
                    title = t[:120]; break
        if not title:
            title = " ".join(link.get_text(separator=" ", strip=True).split())[:120]
        if not title or len(title) < 3 or title.lower() in SKIP: continue

        company = time_ago = ""
        parent  = link.parent
        if parent:
            for node_text in parent.find_all(string=True):
                t = node_text.strip()
                if not t or t == title: continue
                if re.search(r'\d+[dwmhDWMH]|\bday|\bweek|\bmonth|\btoday|\byesterday|\bhour', t, re.I):
                    if not time_ago: time_ago = t
                elif not company and 2 < len(t) < 60 and t.lower() not in SKIP:
                    company = t

        if not time_ago:
            for attr in ("data-date","data-time","datetime","data-posted","data-age"):
                val = link.get(attr, "")
                if val: time_ago = str(val).strip(); break

        results.append({
            "title": title, "slug": slug, "url": job_url,
            "company": company, "location": "",
            "salary": "Not specified", "time_ago": time_ago,
            "remote": _infer_remote(link.get_text(" ", strip=True)),
        })
    return results


async def _scrape_aijobs_direct(session: aiohttp.ClientSession) -> list[dict]:
    listings: list[dict] = []
    seen:     set[str]   = set()

    print(f"  [aijobs/direct] warm-up {AIJOBS_BASE} ...")
    html   = await _fetch_aijobs_page(AIJOBS_BASE)
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
            print(f"  [aijobs/direct] page {page_num}: fetch failed"); break

        page_jobs = _parse_job_links(html)
        if not page_jobs:
            print(f"  [aijobs/direct] page {page_num}: 0 jobs"); break

        new_count     = 0
        stopped_early = False

        for job in page_jobs:
            slug = job["slug"]
            if slug in seen: continue
            seen.add(slug)
            pub_dt = _parse_date(job.get("time_ago", ""))
            if pub_dt and _too_old(pub_dt):
                print(f"  [aijobs/direct] STOP {(datetime.now()-pub_dt).days}d > {MAX_AGE_DAYS}d")
                stopped_early = True; break
            if pub_dt:
                job["time_ago"] = _age_label(pub_dt)
            listings.append(job)
            new_count += 1

        print(f"  [aijobs/direct] page {page_num}: +{new_count} (total {len(listings)})")
        if stopped_early: break

    return listings


async def scrape_aijobs(query: str, session: aiohttp.ClientSession) -> list[dict]:
    use_serpapi = _SERPAPI_AVAILABLE and bool(SERPAPI_API_KEY)
    if use_serpapi:
        print(f"  [aijobs] Mode: SerpApi")
        listings = await _scrape_aijobs_serpapi(query)
        if listings:
            print(f"[aijobs] TOTAL (serpapi): {len(listings)}")
            return listings
        print(f"  [aijobs] SerpApi 0 résultats → fallback HTTP direct")

    print(f"  [aijobs] Mode: HTTP direct")
    listings = await _scrape_aijobs_direct(session)
    print(f"[aijobs] TOTAL (direct): {len(listings)}")
    return listings


# ══════════════════════════════════════════════════════════════════════════════
#  2.  remoteok.com
# ══════════════════════════════════════════════════════════════════════════════

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

        if pub_dt_n and _too_old(pub_dt_n): continue

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
            if m: pub_dt = _parse_date(m.group(1))
        if pub_dt is None:
            skipped_no_date += 1; continue

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


# ══════════════════════════════════════════════════════════════════════════════
#  3.  emploitic.com
# ══════════════════════════════════════════════════════════════════════════════
#
#  Strategy:
#    1. Sitemap index (sitemap.xml) → find sitemap-jobs.xml
#    2. Parse the sitemap → list (url, lastmod) sorted from newest to oldest
#    3. For each URL:
#         a. If lastmod > 45 days → STOP (the following ones are older)
#         b. Fetch HTML → extract JSON-LD JobPosting
#         c. Normalize → dict compatible with handle_job()
#    4. EMPLOITIC_DELAY delay between requests
#    5. STOP only on cutoff date
#
#  Date format visible on the site (cf. screenshot):
#    "Today", "Yesterday", "3 days ago", "Confirmed / Experienced (3 To 5 Years)"

_EMP_HTML_HEADERS = {
    "User-Agent":      BROWSER_HEADERS["User-Agent"],
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Referer":         EMPLOITIC_BASE + "/",
    "Cache-Control":   "no-cache",
}
_EMP_XML_HEADERS = {
    "User-Agent":      BROWSER_HEADERS["User-Agent"],
    "Accept":          "application/xml,text/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


# ── Emploitic date helpers ────────────────────────────────────────────────────

def _emp_parse_date(text: str) -> datetime | None:
    """
    Parse emploitic.com date formats (French):
      "Aujourd'hui"               → now
      "Hier"                      → -1 day
      "Il y a 3 jours"            → -3 days
      "Il y a 2 semaines"         → -14 days
      "Il y a 1 mois"             → -30 days
      "12/01/2025"  "12-01-2025"  → FR format DD/MM/YYYY
      "2025-01-12"                → ISO
      "2025-01-12T10:00:00Z"      → ISO datetime (JSON-LD datePosted)
    """
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

    if "aujourd" in low:            return now
    if "hier"    in low:            return now - timedelta(days=1)

    m = re.search(r"il\s+y\s+a\s+(\d+)\s+jour",    low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+semaine",  low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+mois",     low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+an",       low)
    if m: return now - timedelta(days=int(m.group(1)) * 365)

    # FR format: DD/MM/YYYY or DD-MM-YYYY
    m = re.match(r'^(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})$', s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # ISO formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass

    return None


def _emp_age_label(dt: datetime | None) -> str:
    """English label (consistent with aijobs/remoteok)."""
    if dt is None:
        return ""
    days = max(0, (datetime.now() - dt).days)
    if days == 0: return "today"
    if days == 1: return "1 day ago"
    return f"{days} days ago"


# ── Emploitic fetch helpers ───────────────────────────────────────────────────

async def _emp_fetch(url: str, session: aiohttp.ClientSession,
                     is_xml: bool = False) -> str | None:
    """Fetch an emploitic URL, handles 429/retry, returns text or None."""
    headers = _EMP_XML_HEADERS if is_xml else _EMP_HTML_HEADERS
    for attempt in range(1, 4):
        try:
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    return await resp.text(encoding="utf-8", errors="replace")
                if resp.status == 429:
                    wait = 20 * attempt
                    print(f"  [emploitic] 429 → waiting {wait}s (attempt {attempt})")
                    await asyncio.sleep(wait); continue
                if resp.status in (403, 404):
                    print(f"  [emploitic] {resp.status} → {url[:70]}")
                    return None
                print(f"  [emploitic] HTTP {resp.status} → {url[:70]}")
                return None
        except Exception as e:
            print(f"  [emploitic] error (attempt {attempt}): {e}")
            if attempt < 3:
                await asyncio.sleep(5)
    return None


# ── Sitemap XML parsing ───────────────────────────────────────────────────────

def _emp_xml_tag(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def _emp_parse_sitemap_index(xml_text: str) -> list[str]:
    """Returns sub-sitemap URLs, with sitemap-jobs.xml first."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    locs: list[str] = []
    for child in root:
        if _emp_xml_tag(child.tag) != "sitemap":
            continue
        for node in child:
            if _emp_xml_tag(node.tag) == "loc" and (node.text or "").strip():
                locs.append(node.text.strip())
    # Prioritize "jobs" sitemaps
    locs.sort(key=lambda u: 0 if "jobs" in u.lower() else 1)
    return locs


def _emp_parse_job_sitemap(xml_text: str) -> list[tuple[str, datetime | None]]:
    """
    Parses a jobs sitemap.
    Filter: keeps only /offres-d-emploi/ URLs.
    Returns list of (url, lastmod) sorted newest to oldest.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []

    entries: list[tuple[str, datetime | None]] = []
    for child in root:
        loc_text = lastmod_text = ""
        for node in child:
            tag = _emp_xml_tag(node.tag)
            if tag == "loc":
                loc_text = (node.text or "").strip()
            elif tag == "lastmod":
                lastmod_text = (node.text or "").strip()

        if not loc_text or "/offres-d-emploi/" not in loc_text:
            continue

        lastmod = _emp_parse_date(lastmod_text) if lastmod_text else None
        entries.append((loc_text, lastmod))

    entries.sort(
        key=lambda x: x[1] if x[1] else datetime(1970, 1, 1),
        reverse=True,
    )
    return entries


# ── JSON-LD extraction (based on the provided reference script) ───────────────

def _emp_extract_jsonld_objects(html_text: str) -> list[dict]:
    scripts = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html_text, flags=re.S | re.I
    )
    out: list[dict] = []
    for s in scripts:
        s = (s or "").strip()
        if not s: continue
        try:
            data = _json.loads(s)
        except _json.JSONDecodeError:
            try:
                data = _json.loads(s.strip().strip("\ufeff"))
            except _json.JSONDecodeError:
                continue
        if isinstance(data, dict):
            out.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    out.append(item)
    return out


def _emp_find_jobposting(objs: list[dict]) -> dict | None:
    def is_jp(o: dict) -> bool:
        t = o.get("@type")
        if isinstance(t, str):   return t.lower() == "jobposting"
        if isinstance(t, list):  return any(isinstance(x,str) and x.lower()=="jobposting" for x in t)
        return False

    for o in objs:
        if is_jp(o): return o
        for node in o.get("@graph", []):
            if isinstance(node, dict) and is_jp(node): return node
    return None


def _emp_clean(s: str) -> str:
    """Cleans text: strip HTML tags, collapse whitespace."""
    import html as _html
    s = _html.unescape(s or "")
    s = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", s)
    s = re.sub(r"(?s)<[^>]*>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ── Field extractors (async port of the reference script) ────────────────────

def _emp_location(jp: dict) -> str:
    loc = jp.get("jobLocation")
    locs = [loc] if isinstance(loc, dict) else (loc if isinstance(loc, list) else [])
    for l in locs:
        if not isinstance(l, dict): continue
        addr = l.get("address", {})
        if not isinstance(addr, dict): continue
        parts = [str(addr.get(k) or "").strip()
                 for k in ("addressLocality", "addressRegion", "addressCountry")
                 if addr.get(k)]
        if parts: return ", ".join(parts)
    return ""


def _emp_salary(jp: dict) -> str:
    base = jp.get("baseSalary")
    if not isinstance(base, dict): return ""
    currency = str(base.get("currency") or "DZD").strip()
    val      = base.get("value")
    if not isinstance(val, dict): return ""
    lo, hi, unit = val.get("minValue"), val.get("maxValue"), str(val.get("unitText") or "").strip()
    if lo is not None and hi is not None:
        return f"{currency} {lo} – {hi} {unit}".strip()
    if lo is not None:
        return f"{currency} {lo}+ {unit}".strip()
    return ""


def _emp_date_posted(jp: dict, html_text: str) -> str:
    date_str = jp.get("datePosted") or jp.get("publishedAt") or ""
    if date_str:
        return str(date_str).strip()
    # meta tag
    m = re.search(
        r'<meta[^>]+(?:property|name)="(?:article:published_time|datePosted)"[^>]+content="([^"]+)"',
        html_text, re.I
    )
    if m: return m.group(1).strip()
    # "Published on: 12/01/2025"
    m = re.search(
        r'(?:publi[eé]\s*le\s*:?\s*)(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4})',
        html_text, re.I
    )
    if m: return m.group(1).strip()
    return ""


def _emp_status(jp: dict, html_text: str) -> str:
    status = jp.get("jobStatus") or jp.get("status") or ""
    if status: return str(status).strip()
    vt = jp.get("validThrough") or ""
    if vt:
        try:
            expiry = datetime.fromisoformat(str(vt).replace("Z", "+00:00"))
            return "Expired" if expiry < datetime.now(timezone.utc) else "Active"
        except Exception:
            pass
    if re.search(r'(expir[eé]|clôtur[eé]|fermé|closed|expired)', html_text, re.I):
        return "Expired"
    return "Active"


def _emp_contract(jp: dict, html_text: str) -> str:
    emp = jp.get("employmentType") or ""
    if isinstance(emp, list): emp = ", ".join(str(e) for e in emp)
    if emp: return str(emp).strip()
    m = re.search(
        r'(?:type de contrat|contrat)\s*:?\s*<[^>]*>?\s*([^<\n]{2,50})',
        html_text, re.I
    )
    if m: return _emp_clean(m.group(1))
    for ctype in ("CDI", "CDD", "Stage", "Freelance", "Intérim", "Alternance"):
        if re.search(r'\b' + re.escape(ctype) + r'\b', html_text, re.I):
            return ctype
    return ""


def _emp_remote(jp: dict, html_text: str) -> str:
    jlt = str(jp.get("jobLocationType") or "").lower()
    if "remote" in jlt or "telecommut" in jlt: return "Yes"
    if jp.get("applicantLocationRequirements"): return "Yes"
    if re.search(r'\b(t[eé]l[eé]travail|remote|travail\s+[àa]\s+distance)\b', html_text, re.I):
        return "Yes"
    return "No"


def _emp_experience(jp: dict, html_text: str) -> str:
    exp = jp.get("experienceRequirements") or jp.get("experience") or ""
    if isinstance(exp, dict):
        exp = exp.get("monthsOfExperience") or exp.get("description") or ""
    if exp: return str(exp).strip()
    m = re.search(
        r'(?:exp[eé]rience\s*(?:requise|souhait[eé]e)?)\s*:?\s*<[^>]*>?\s*([^<\n]{2,80})',
        html_text, re.I
    )
    if m: return _emp_clean(m.group(1))
    # "Confirmed / Experienced (3 To 5 Years)" visible in screenshot
    m = re.search(
        r'((?:D[eé]butant|Junior|Confirm[eé]|Exp[eé]riment[eé]|Senior)'
        r'(?:\s*/\s*(?:D[eé]butant|Junior|Confirm[eé]|Exp[eé]riment[eé]|Senior))?'
        r'(?:\s*[\(\[][^)\]\n]{2,30}[\)\]])?)',
        html_text, re.I
    )
    if m: return m.group(1).strip()
    # "3 years of experience"
    m = re.search(
        r"(\d+\s*(?:an[s]?|ann[eé]e[s]?|mois|year[s]?)\s*d['']exp[eé]rience)",
        html_text, re.I
    )
    if m: return m.group(1).strip()
    return ""


def _emp_education(jp: dict, html_text: str) -> str:
    edu = jp.get("educationRequirements") or jp.get("education") or ""
    if isinstance(edu, dict):
        edu = edu.get("credentialCategory") or edu.get("description") or ""
    if isinstance(edu, list):
        edu = ", ".join(str(e) for e in edu)
    if edu: return str(edu).strip()
    m = re.search(
        r"(?:[eé]ducation|formation|niveau d[''](?:[eé]tudes?)?|dipl[oô]me)\s*:?\s*<[^>]*>?\s*([^<\n]{2,100})",
        html_text, re.I
    )
    if m: return _emp_clean(m.group(1))
    return ""


def _emp_skills(jp: dict, html_text: str) -> list[str]:
    raw = jp.get("skills") or jp.get("competencies") or jp.get("qualifications") or []
    if isinstance(raw, str):
        raw = [s.strip() for s in re.split(r'[,;/]', raw) if s.strip()]
    elif isinstance(raw, list):
        raw = [_emp_clean(str(s)) for s in raw if s]
    if raw: return [s for s in raw if s]
    # Fallback: skills HTML section
    m = re.search(
        r'(?:comp[eé]tences?\s*(?:requises?)?|skills?)\s*:?\s*</?\w+[^>]*>\s*(.*?)(?:</(?:ul|ol|div|section|p)\b)',
        html_text, re.I | re.S
    )
    if m:
        items = re.findall(r'<li[^>]*>(.*?)</li>', m.group(1), re.S | re.I)
        if items: return [_emp_clean(i) for i in items if _emp_clean(i)]
    return []


def _emp_bonus(jp: dict, html_text: str) -> list[str]:
    raw = (jp.get("bonuses") or jp.get("niceToHave") or
           jp.get("preferredQualifications") or [])
    if isinstance(raw, str):
        raw = [s.strip() for s in re.split(r'[,;/]', raw) if s.strip()]
    elif isinstance(raw, list):
        raw = [_emp_clean(str(s)) for s in raw if s]
    if raw: return [s for s in raw if s]
    m = re.search(
        r'(?:atouts?|bonus|un plus|souhait[eé]|appr[eé]ci[eé]s?)\s*:?\s*</?\w+[^>]*>\s*(.*?)(?:</(?:ul|ol|div|section|p)\b)',
        html_text, re.I | re.S
    )
    if m:
        items = re.findall(r'<li[^>]*>(.*?)</li>', m.group(1), re.S | re.I)
        if items: return [_emp_clean(i) for i in items if _emp_clean(i)]
    return []


def _emp_tags(jp: dict) -> list[str]:
    tags: list[str] = []
    for key in ("employmentType", "industry", "occupationalCategory"):
        val = jp.get(key)
        if isinstance(val, str) and val.strip():
            tags.append(val.strip())
        elif isinstance(val, list):
            tags.extend(v.strip() for v in val if isinstance(v, str) and v.strip())
    return list(dict.fromkeys(t for t in tags if t))


def _emp_normalize(url: str, html_text: str, jp: dict) -> dict | None:
    """
    Builds a normalized dict compatible with handle_job() from the JSON-LD
    and emploitic HTML.

    Returned fields:
      title, url, company, salary, location, remote, time_ago  ← required by handle_job
      _emp_*   ← bonus fields passed directly in the job dict (avoids re-fetching)
    """
    # ── Title ──────────────────────────────────────────────────────────────
    title = _emp_clean(jp.get("title") or jp.get("name") or "")
    if not title:
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, re.S | re.I)
        if m: title = _emp_clean(m.group(1))
    if not title:
        m = re.search(r"<title[^>]*>(.*?)</title>", html_text, re.S | re.I)
        if m: title = re.sub(r"\|\s*Emploitic.*", "", _emp_clean(m.group(1))).strip()
    if not title:
        return None

    # ── Company ──────────────────────────────────────────────────────────────
    company = ""
    org = jp.get("hiringOrganization")
    if isinstance(org, dict):
        company = _emp_clean(org.get("name") or "")
    if not company:
        m = re.search(
            r'(?:soci[eé]t[eé]|entreprise|employeur)\s*[:\-]\s*([^<\n]{2,60})',
            html_text, re.I
        )
        if m: company = _emp_clean(m.group(1))

    # ── Location ──────────────────────────────────────────────────────────────
    location = _emp_location(jp)
    if not location:
        m = re.search(
            r'<[^>]*class=["\'][^"\']*(?:location|lieu|ville|localisa)[^"\']*["\'][^>]*>(.*?)</\w+>',
            html_text, re.S | re.I
        )
        if m: location = _emp_clean(m.group(1))
    if not location:
        location = "Algeria"

    # ── Salary ───────────────────────────────────────────────────────────────
    salary = _emp_salary(jp) or "not specified"

    # ── Publication date ───────────────────────────────────────────────────────
    date_str = _emp_date_posted(jp, html_text)
    pub_dt   = _emp_parse_date(date_str) if date_str else None

    # Fallback visible HTML: "Today", "Yesterday", "X days ago"
    if pub_dt is None:
        m = re.search(
            r"(Aujourd['']hui|Hier|Il\s+y\s+a\s+\d+\s+(?:jours?|semaines?|mois|ans?))",
            html_text, re.I
        )
        if m: pub_dt = _emp_parse_date(m.group(1))

    # ── Enriched fields ───────────────────────────────────────────────────────
    status     = _emp_status(jp, html_text)
    contract   = _emp_contract(jp, html_text)
    remote_val = _emp_remote(jp, html_text)
    experience = _emp_experience(jp, html_text)
    education  = _emp_education(jp, html_text)
    skills     = _emp_skills(jp, html_text)
    bonus      = _emp_bonus(jp, html_text)
    tags       = _emp_tags(jp)
    description= _emp_clean(str(jp.get("description") or ""))[:2000]

    return {
        # ── Fields required by handle_job() ────────────────────────────────
        "title":    title,
        "url":      url,
        "company":  company,
        "salary":   salary,
        "location": location,
        "remote":   remote_val,
        "time_ago": _emp_age_label(pub_dt),
        # ── Enriched fields directly available ───────────────────────────
        # (enrich() in main.py can use them without re-fetching)
        "_emp_pub_dt":    pub_dt,
        "_emp_status":    status,
        "_emp_contract":  contract,
        "_emp_experience":experience,
        "_emp_education": education,
        "_emp_skills":    skills,
        "_emp_bonus":     bonus,
        "_emp_tags":      tags,
        "_emp_description":description,
    }


# ── Emploitic entry point ──────────────────────────────────────────────────────


def _emp_title_from_slug(url: str) -> str:
    """
    Extracts an approximate title from the emploitic URL slug.
    Allows cosine filtering BEFORE fetching the page (avoids 1.5s × N useless fetches).

    Examples:
      /offres-d-emploi/data-engineer-chez-techcorp-123456  → "data engineer"
      /offres-d-emploi/developpeur-fullstack-react-nodejs-987654 → "developpeur fullstack react nodejs"
      /offres-d-emploi/ingenieur-machine-learning-456789  → "ingenieur machine learning"
    """
    try:
        slug = url.rstrip("/").split("/")[-1]
        # Remove trailing numeric ID (always present on emploitic)
        slug = re.sub(r"-\d+$", "", slug)
        # Remove "chez-..." (company name encoded in the slug)
        slug = re.sub(r"-chez-.+$", "", slug, flags=re.I)
        # Hyphens → spaces
        title = slug.replace("-", " ").strip()
        return title if len(title) >= 3 else ""
    except Exception:
        return ""


async def scrape_emploitic(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Scrapes emploitic.com via XML sitemap + JSON-LD JobPosting.

    Same logic as other scrapers:
      - Immediate STOP as soon as a job is > 45 days old (the following ones are older)
      - Polite 1.5s delay between requests
      - Limit of 50 jobs
      - Returns dicts compatible with handle_job() in main.py
    """
    print(f"  [emploitic] Starting for '{query}'")

    # ── 1. Sitemap index ──────────────────────────────────────────────────────
    index_text = await _emp_fetch(f"{EMPLOITIC_BASE}/sitemap.xml", session, is_xml=True)
    if not index_text:
        print(f"  [emploitic] Unable to read sitemap.xml")
        return []

    sub_sitemaps = _emp_parse_sitemap_index(index_text)
    if not sub_sitemaps:
        print(f"  [emploitic] No sub-sitemap found")
        return []
    print(f"  [emploitic] {len(sub_sitemaps)} sub-sitemaps: {sub_sitemaps[:2]}")

    # ── 2. Jobs sitemap ───────────────────────────────────────────────────────
    job_entries: list[tuple[str, datetime | None]] = []

    for sm_url in sub_sitemaps:
        await asyncio.sleep(0.5)
        sm_text = await _emp_fetch(sm_url, session, is_xml=True)
        if not sm_text:
            print(f"  [emploitic] Unable to read {sm_url[:60]}")
            continue
        entries = _emp_parse_job_sitemap(sm_text)
        job_entries.extend(entries)
        print(f"  [emploitic] {sm_url.split('/')[-1]}: {len(entries)} job URLs")
        if job_entries:
            break   # jobs sitemap found → stop here

    if not job_entries:
        print(f"  [emploitic] No /offres-d-emploi/ URL found")
        return []
    print(f"  [emploitic] {len(job_entries)} URLs (sorted newest → oldest)")

    # ── 3. Scrape job pages ───────────────────────────────────────────────────
    # Accelerated strategy:
    #   a) Extract title from URL slug (free, 0ms)
    #   b) Return a "lightweight" job with slug_title for cosine filter in main.py
    #   c) If cosine < emploitic threshold (0.55) → main.py skips without ever fetching the page
    #   d) Otherwise → main.py calls extract_with_llm which fetches the full HTML
    # Result: avoids 0.3s × each irrelevant job

    listings:        list[dict] = []
    skipped_no_date: int        = 0

    for idx, (url, lastmod) in enumerate(job_entries):
        # ── SKIP if lastmod is too old ─────────────────────────────────────
        if lastmod is not None:
            days_old = (datetime.now() - lastmod).days
            if days_old > MAX_AGE_DAYS:
                print(
                    f"  [emploitic] SKIP sitemap {days_old}d > {MAX_AGE_DAYS}d: "
                    f"{url.split('/')[-1][:40]}"
                )
                continue
        else:
            skipped_no_date += 1

        # ── Title from slug (0ms, no fetch) ───────────────────────────────
        # Allows cosine filter in main.py to decide BEFORE fetching
        slug_title = _emp_title_from_slug(url)
        if not slug_title:
            # Unreadable slug → fetch anyway
            slug_title = ""

        # Return a lightweight job: main.py will fetch via extract_with_llm
        # only if cosine(slug_title, cv_title) >= emploitic threshold (0.55)
        # _emp_* fields are empty here → filled by extract_with_llm
        job_light = {
            "title":            slug_title or url.split("/")[-1].replace("-", " "),
            "url":              url,
            "company":          "",
            "location":         "",
            "salary":           "Not specified",
            "remote":           "",
            "time_ago":         _emp_age_label(lastmod) if lastmod else "",
            "_emp_pub_dt":      lastmod,
            # Flag for main.py: emploitic = full fetch via extract_with_llm
            "_emp_needs_fetch": True,
        }
        listings.append(job_light)

    if skipped_no_date:
        print(f"  [emploitic] {skipped_no_date} jobs with no sitemap date skipped")
    print(f"[emploitic] TOTAL pre-filter: {len(listings)} (cosine filter 0.55 in main.py)")
    return listings


async def _scrape_emploitic_fetch_one(url: str, session: aiohttp.ClientSession) -> dict | None:
    """
    Fetches and extracts full data for one emploitic job.
    Called by enrich() in main.py ONLY if cosine >= 0.55.
    Replaces extract_with_llm for emploitic (no LLM needed,
    data is available in the page's JSON-LD).
    """
    html_text = await _emp_fetch(url, session, is_xml=False)
    if not html_text:
        return None
    jp  = _emp_find_jobposting(_emp_extract_jsonld_objects(html_text)) or {}
    job = _emp_normalize(url, html_text, jp)
    return job

# ══════════════════════════════════════════════════════════════════════════════
#  4.  tanitjobs.com
# ══════════════════════════════════════════════════════════════════════════════
#
#  TanitJobs = Cloudflare → HTTP direct impossible.
#
#  Stratégie : SerpApi Google Search paginé (équivalent à /jobs/?page=N)
#    Query   :  site:tanitjobs.com/job/
#    Tri     :  tbs=sbd:1,qdr:m2  → trié par date, 2 derniers mois
#    Pagination : start=0, 10, 20, 30 ... (même que page=1,2,3 sur le site)
#
#  Format snippet Google pour TanitJobs :
#    LIGNE 1 du snippet → "company - Ville, Tunisie. Il y a X jours. ..."
#    Exemple : "multinationale - Sousse, Tunisie. Il y a 4 semaines. ..."
#    => company  = tout ce qui est avant " - "
#    => location = ce qui est entre " - " et le premier "."
#    => date     = "Il y a X" ou "X days ago" dans le reste
#
#  Condition d'arrêt : date_pub > 45 jours → STOP
#  Filtre cosine     : géré par handle_job() dans main.py (seuil 0.40)
#
#  Réponse à la question cosine :
#    OUI — handle_job() encode job["title"] → compare avec cv_vec (titre CV)
#    Si cosine(job_title, cv_title) > 0.40 → job envoyé à enrich() → affiché
#    Sinon → SKIP silencieux
#
#  Bypass Cloudflare dans enrich() :
#    source=="tanitjobs" → utilise _tnj_* (snippet) directement
#    → aucun fetch vers tanitjobs.com

TANITJOBS_BASE      = "https://www.tanitjobs.com"
TANITJOBS_JOBS_URL  = f"{TANITJOBS_BASE}/jobs/"


def _tnj_parse_date(text: str) -> datetime | None:
    """
    Parse tous les formats de date vus dans les snippets Google de TanitJobs.
    FR : "il y a 3 jours", "il y a 2 semaines", "il y a 1 mois"
    EN : "3 days ago", "2 weeks ago", "1 month ago"
    ISO : "2024-03-15", "2024-03-15T10:00:00Z"
    FR date : "12/03/2024", "12-03-2024"
    Relatif : "aujourd'hui", "hier", "today", "yesterday"
    """
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

    # Relatifs
    if any(w in low for w in ("aujourd", "today", "ce jour")):
        return now
    if any(w in low for w in ("hier", "yesterday")):
        return now - timedelta(days=1)

    # FR : "il y a X jours/semaines/mois/ans"
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+jour",     low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+semaine",   low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+mois",      low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"il\s+y\s+a\s+(\d+)\s+an",        low)
    if m: return now - timedelta(days=int(m.group(1)) * 365)

    # EN : "X days/weeks/months ago"
    m = re.search(r"(\d+)\s+day",   low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s+week",  low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\s+month", low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"(\d+)\s+hour",  low)
    if m: return now  # même jour

    # Mois FR abrégés → EN
    FR_MONTHS = {
        "janv": "Jan", "fév": "Feb", "févr": "Feb", "mars": "Mar",
        "avr": "Apr",  "mai": "May", "juin": "Jun", "juil": "Jul",
        "août": "Aug", "sept": "Sep", "oct": "Oct",  "nov": "Nov", "déc": "Dec",
    }
    s_en = s
    for fr, en in FR_MONTHS.items():
        s_en = re.sub(r'\b' + fr + r'\b', en, s_en, flags=re.I)

    # Formats ISO et date FR
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y",
        "%B %d, %Y", "%b %d, %Y",
    ):
        try:
            candidate = s_en[:len(fmt)+5]
            return datetime.strptime(candidate[:len(fmt)], fmt).replace(tzinfo=None)
        except (ValueError, TypeError):
            pass

    return _parse_date(s_en)


def _tnj_extract_date_from_snippet(snippet: str) -> tuple[datetime | None, str]:
    """
    Extrait la date depuis le texte du snippet.
    Le snippet TanitJobs contient typiquement :
      "company - Ville, Tunisie. Il y a 4 semaines. Description..."
      "company - Ville. 3 days ago. ..."
    """
    # Patterns date dans l'ordre de priorité
    DATE_PATS = [
        r"il\s+y\s+a\s+\d+\s+(?:jour|semaine|mois|an)s?",     # FR relatif
        r"\d+\s+(?:day|week|month|hour)s?\s+ago",               # EN relatif
        r"aujourd['']?hui|today",                                # aujourd'hui
        r"hier|yesterday",                                       # hier
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",               # ISO datetime
        r"\d{4}-\d{2}-\d{2}",                                   # ISO date
        r"\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{4}",                  # JJ/MM/AAAA
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
        r"|janv?|févr?|mars|avr|juin?|juil?|août|sept?|oct|nov|déc)"
        r"\s+\d+,?\s+\d{4}",
    ]
    for pat in DATE_PATS:
        dm = re.search(pat, snippet, re.I)
        if dm:
            pub_dt = _tnj_parse_date(dm.group(0))
            label  = _age_label(pub_dt) if pub_dt else dm.group(0)
            return pub_dt, label
    return None, ""


def _tnj_extract_date(item: dict) -> tuple[datetime | None, str]:
    """
    Extrait la date de publication depuis un résultat SerpApi.
    Ordre de priorité :
      1. rich_snippet.detected_extensions (données structurées Google)
      2. item["date"] (champ SerpApi direct)
      3. Snippet texte (patterns date)
    """
    pub_dt, time_ago = None, ""

    # ── 1. rich_snippet structured data ──────────────────────────────────────
    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        for key in ("date_posted", "posted_at", "date", "published_date", "posting_date"):
            val = str(ext.get(key, "") or "").strip()
            if val:
                pub_dt = _tnj_parse_date(val)
                if pub_dt:
                    return pub_dt, _age_label(pub_dt)

    # ── 2. Champ item["date"] direct ──────────────────────────────────────────
    raw = str(item.get("date", "") or "").strip()
    if raw:
        pub_dt = _tnj_parse_date(raw)
        if pub_dt:
            return pub_dt, _age_label(pub_dt)
        # Garder le texte brut comme label si parse échoue
        time_ago = raw

    # ── 3. Snippet texte ──────────────────────────────────────────────────────
    snippet = str(item.get("snippet", "") or "")
    dt_snip, lbl = _tnj_extract_date_from_snippet(snippet)
    if dt_snip:
        return dt_snip, lbl
    if lbl and not time_ago:
        time_ago = lbl

    return pub_dt, time_ago


def _tnj_is_job_url(url: str) -> bool:
    """
    Retourne True si l'URL pointe vers une page de job individuel TanitJobs.
    URL valide  : tanitjobs.com/job/12345/titre
    URL rejetée : tanitjobs.com/jobs, /search, /company, /?cat=...
    """
    if not url or "tanitjobs.com" not in url:
        return False
    # Doit contenir /job/ suivi d'un ID numérique
    return bool(re.search(r"/job/\d+", url))


def _tnj_extract_info(item: dict) -> dict:
    """
    Extrait title, company, location, salary, remote, contract, experience
    depuis un résultat SerpApi TanitJobs.

    Format snippet TanitJobs (exemple capturé) :
      "multinationale - Sousse, Tunisie. Il y a 4 semaines. ..."
      "TechCorp · Tunis, Tunisie · 2 days ago · Developer..."
      "company name - Ariana, Tunisie. CDI. Expérience: 3 ans..."

    Structure :
      PARTIE 1 avant le 1er séparateur  → SOCIÉTÉ
      PARTIE 2 jusqu'au 1er "." ou date → LIEU
      Suite                             → description, contrat, exp...
    """
    snippet = str(item.get("snippet", "") or "")
    title   = str(item.get("title",   "") or "")

    # ── Titre : nettoyer ──────────────────────────────────────────────────────
    # Supprimer "| TanitJobs", "- TanitJobs", "Offres d'emploi", etc.
    title = re.sub(
        r"\s*[\|·\-–—]\s*(?:TanitJobs|Offres?\s+d['\u2019]emploi).*$",
        "", title, flags=re.I
    ).strip()
    # Nettoyer les séparateurs restants en fin de titre
    for sep in (" | ", " - ", " – ", " — "):
        if sep in title:
            parts = title.split(sep)
            # Garder la partie la plus longue (= le vrai titre)
            title = max(parts, key=len).strip()
            break

    # ── Société + Lieu depuis début du snippet ────────────────────────────────
    # Pattern : "company [-·|] city, country[. ...]"
    # Exemple  : "multinationale - Sousse, Tunisie. Il y a 4 semaines"
    # Exemple  : "Banque ABC · Tunis, Tunisie · Publié il y a 2 jours"
    company  = ""
    location = ""

    # Tentative 1 : rich_snippet structured data (plus fiable)
    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        if not company  and ext.get("company"):
            company  = str(ext["company"]).strip()
        if not location and ext.get("location"):
            location = str(ext["location"]).strip()

    # Tentative 2 : début du snippet "company - location"
    if not company or not location:
        # Séparateurs possibles : " - ", " – ", " · ", " | "
        sep_pat = r"\s*(?:[-–—·|])\s*"
        # Le snippet commence souvent par "company SEP location. date. ..."
        m = re.match(
            r"^(.+?)" + sep_pat +
            r"((?:[A-ZÀÂÇÉÈÊËÎÏÔÙÛÜ][^\.,·\-\n]{1,30},?\s*(?:Tunisie?|Tunisia|Alg[eé]rie?|Maroc|Morocco)[^\.·\-]*)?)"
            r"(?:[\.·]|$)",
            snippet, re.I
        )
        if m:
            if not company:
                company  = m.group(1).strip()
            if not location:
                location = m.group(2).strip()

        # Tentative 3 : pattern plus simple si le premier échoue
        if not company:
            first_line = snippet.split(".")[0].split("\n")[0]
            for sep in (" - ", " – ", " · ", " | "):
                if sep in first_line:
                    parts = first_line.split(sep, 1)
                    candidate = parts[0].strip()
                    # Vérifier que ce n'est pas une ville ou une date
                    if candidate and not re.search(r"\d+\s+(?:jour|week|day|mois)", candidate, re.I):
                        company = candidate
                    if len(parts) > 1 and not location:
                        location = parts[1].strip()
                    break

    # Nettoyer company : supprimer suffixes parasites
    if company:
        # Supprimer si company contient une ville tunisienne (ça serait une location)
        if re.search(r"\b(Tunis|Sfax|Sousse|Ariana|Bizerte|Nabeul|Monastir)\b", company, re.I):
            # Essayer de séparer company de location
            m2 = re.match(r"^(.+?)\s*,\s*(.+)$", company)
            if m2:
                company  = m2.group(1).strip()
                if not location:
                    location = m2.group(2).strip()
        # Supprimer dates parasites dans company
        company = re.sub(r"\s*[·\-]\s*(?:il\s+y\s+a|posted|\d+\s+(?:day|week|hour|month)).*$", "", company, flags=re.I).strip()

    # ── Salaire ───────────────────────────────────────────────────────────────
    salary = ""
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        if ext.get("salary"):
            salary = str(ext["salary"]).strip(); break
    if not salary:
        sm = re.search(
            r"(?:Salaire|Rémunération|Salary)\s*:?\s*"
            r"([\d\s]+(?:DT|TND|€|USD|\$|K)[^\.,\n]{0,20})",
            snippet, re.I
        )
        if sm: salary = sm.group(1).strip()
    salary = salary or "Non spécifié"

    # ── Remote ────────────────────────────────────────────────────────────────
    remote = ""
    if re.search(r"\b(t[eé]l[eé]travail|remote|full\s*remote|travail\s+[àa]\s+distance)\b", snippet, re.I):
        remote = "Remote 🌍"
    elif re.search(r"\bhybrid\b|\bhybride\b", snippet, re.I):
        remote = "Hybrid 🏠🏢"

    # ── Contrat ───────────────────────────────────────────────────────────────
    contract = ""
    m = re.search(r"\b(CDI|CDD|Stage|Freelance|Intérim|SIVP|Alternance|Temps\s+plein|Temps\s+partiel)\b", snippet, re.I)
    if m: contract = m.group(1).strip()

    # ── Expérience ────────────────────────────────────────────────────────────
    experience = ""
    m = re.search(
        r"(\d+\s*(?:an[s]?|ann[eé]e[s]?|mois|year[s]?)\s*d['\u2019]exp[eé]rience"
        r"|(?:D[eé]butant|Junior|Confirm[eé]|Exp[eé]riment[eé]|Senior)[^\.,\n]{0,30})",
        snippet, re.I
    )
    if m: experience = m.group(1).strip()

    # Lieu par défaut
    if not location:
        # Chercher ville tunisienne dans snippet
        TN_VILLES = (r"Tunis|Sfax|Sousse|Ariana|Ben\s+Arous|Monastir|Bizerte|Nabeul|"
                     r"Hammamet|Manouba|La\s+Marsa|Carthage|Gab[eè]s|Gafsa|Djerba|Médenine")
        m = re.search(r"\b(" + TN_VILLES + r")\b", snippet, re.I)
        if m: location = m.group(1).strip()
        else: location = "Tunisie"

    return {
        "title":       title,
        "company":     company,
        "location":    location,
        "salary":      salary,
        "remote":      remote,
        "contract":    contract,
        "experience":  experience,
        "description": snippet[:1500],  # snippet = mini-description pour skills gap
        "all_skills":  "",
    }


async def scrape_tanitjobs(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Parcourt tous les jobs tanitjobs.com récents via SerpApi Google Search.

    Équivalent à naviguer sur https://www.tanitjobs.com/jobs/?page=1,2,3,...
    SerpApi pagine avec start=0,10,20,... (même résultat que page=1,2,3)

    Paramètres Google :
      site:tanitjobs.com/job/ → uniquement les pages job individuelles (pas les listings)
      tbs=sbd:1,qdr:m2        → trié par date décroissante + 2 derniers mois
      gl=tn, hl=fr            → contexte Tunisie + français

    Filtre cosine (réponse à ta question) :
      OUI — handle_job() encode job["title"] et compare avec cv_vec (titre CV).
      Si cosine(job_title_vec, cv_title_vec) > 0.40 → job affiché.
      Ce filtre se passe dans main.py, PAS ici. Ici on retourne TOUS les jobs récents.

    STOP : dès que date_pub > 45 jours.
    """
    if not (_SERPAPI_AVAILABLE and bool(SERPAPI_API_KEY)):
        print("  [tanitjobs] SERPAPI_API_KEY requis — absent, skip")
        return []

    listings:  list[dict] = []
    seen_urls: set[str]   = set()
    stopped                = False

    # Query sans mots-clés : TOUS les jobs triés par date
    search_query = "site:tanitjobs.com/job/"
    print(f"  [tanitjobs] Démarrage — parcours tous les jobs récents (tri par date)")
    print(f"  [tanitjobs] Équivalent à /jobs/?page=1,2,3 ... → STOP à {MAX_AGE_DAYS}j")

    page_num = -1
    while True:
        page_num += 1
        if stopped:
            break
        try:
            params = {
                "engine":  "google",
                "q":       search_query,
                "api_key": SERPAPI_API_KEY,
                "num":     10,
                "start":   page_num * 10,   # page 0→start=0, page 1→start=10, ...
                "hl":      "fr",
                "gl":      "tn",
                "tbs":     "sbd:1,qdr:m2",  # trié par date + 2 derniers mois
            }
            results = await asyncio.to_thread(
                lambda p=params: _SerpApiSearch(p).get_dict()
            )

            err = results.get("error", "")
            if err:
                print(f"  [tanitjobs] SerpApi error: {err}")
                break

            items = results.get("organic_results", []) or []
            print(f"  [tanitjobs] page {page_num + 1} (start={page_num*10}): "
                  f"{len(items)} résultats")

            if not items:
                print(f"  [tanitjobs] Plus de résultats → fin")
                break

            new_count   = 0
            skip_count  = 0

            for item in items:
                raw_url = str(item.get("link", "") or "")

                # ── Rejeter les pages listing / search / company ──────────────
                if not _tnj_is_job_url(raw_url):
                    skip_count += 1
                    continue

                # ── Déduplication ─────────────────────────────────────────────
                clean_url = raw_url.split("?")[0].rstrip("/")
                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)

                # ── Extraire date ─────────────────────────────────────────────
                pub_dt, time_ago = _tnj_extract_date(item)

                # ── STOP si date > 45 jours ───────────────────────────────────
                if pub_dt is not None and _too_old(pub_dt):
                    days = (datetime.now() - pub_dt).days
                    print(
                        f"  [tanitjobs] STOP cutoff : {days}j > {MAX_AGE_DAYS}j "
                        f"(résultats triés par date → tous les suivants sont plus vieux)"
                    )
                    stopped = True
                    break

                # ── Extraire infos depuis snippet ─────────────────────────────
                info = _tnj_extract_info(item)

                if not info["title"] or len(info["title"]) < 3:
                    skip_count += 1
                    continue

                job = {
                    # Champs requis par handle_job() + filtre cosine
                    "title":    info["title"],
                    "url":      clean_url,
                    "company":  info["company"],
                    "location": info["location"],
                    "salary":   info["salary"],
                    "remote":   info["remote"],
                    "time_ago": time_ago,
                    # Champs _tnj_* → bypass extract_with_llm (403 Cloudflare)
                    "_tnj_contract":    info["contract"],
                    "_tnj_experience":  info["experience"],
                    "_tnj_description": info["description"],
                    "_tnj_all_skills":  info["all_skills"],
                }
                listings.append(job)
                new_count += 1

            print(f"  [tanitjobs] +{new_count} gardés, {skip_count} ignorés "
                  f"(total {len(listings)})")

            # Pas de page suivante → fin
            if not results.get("serpapi_pagination", {}).get("next"):
                print(f"  [tanitjobs] Fin de pagination Google")
                break

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"  [tanitjobs] exception: {type(e).__name__}: {e}")
            break

    print(f"[tanitjobs] TOTAL: {len(listings)}")
    return listings


# ══════════════════════════════════════════════════════════════════════════════
#  5.  greenhouse.io
# ══════════════════════════════════════════════════════════════════════════════
#
#  Greenhouse expose une API JSON publique, pas de Cloudflare.
#    - Endpoint : https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true
#    - Retourne tous les jobs d'un board en un seul appel (pas de pagination)
#    - La description HTML complète est incluse → extraction sans LLM
#
#  Stratégie :
#    1. Tous les boards fetchés en parallèle (asyncio.gather)
#    2. Filtre date : si updated_at / first_published > 45 jours → SKIP
#    3. Extraction complète inline (contrat, expérience, skills, remote...)
#    4. Champs _gh_* → bypass extract_with_llm dans enrich() de main.py
#    5. Filtre cosine (≥ 0.40 vs titre CV) géré par handle_job() dans main.py
#
#  Boards configurables dans GREENHOUSE_BOARDS.
#  Pour ajouter un board : récupérer le token dans l'URL
#    https://boards.greenhouse.io/{token}

GREENHOUSE_API_BASE = "https://boards-api.greenhouse.io/v1/boards"
GREENHOUSE_BOARDS   = [
    "airbnb",
    "stripe",
    "anthropic",
    "openai",
    "huggingface",
    "dataiku",
    "mistral",
    "alan",
    "doctolib",
    "contentsquare",
    "databricks",
    "scale",
    "cohere",
    "stability",
    "adyen",
]

# Délai entre boards pour être poli avec l'API
GREENHOUSE_DELAY = 0.2


# ── Helpers extraction (depuis description HTML) ──────────────────────────────

def _gh_strip_html(html_text: str) -> str:
    """Convertit le HTML Greenhouse en texte propre."""
    import html as _html_mod
    if not html_text:
        return ""
    text = _html_mod.unescape(str(html_text))
    text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]*>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _gh_published(job: dict) -> datetime | None:
    """
    Parse la date de publication depuis les champs Greenhouse.
    Priorité : updated_at → first_published → created_at
    Format : ISO 8601 avec timezone "2024-03-15T10:00:00.000Z"
    """
    for key in ("updated_at", "first_published", "created_at"):
        raw = job.get(key, "") or ""
        if not raw:
            continue
        try:
            # Format Greenhouse : "2024-03-15T10:00:00.000Z"
            dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            return dt.replace(tzinfo=None)
        except (ValueError, AttributeError):
            pass
    return None


def _gh_location(job: dict) -> str:
    loc = job.get("location", {}) or {}
    if isinstance(loc, dict):
        return str(loc.get("name", "") or "").strip()
    return str(loc).strip()


def _gh_tags(job: dict) -> str:
    """Extrait départements + bureaux comme tags."""
    tags = []
    for key in ("departments", "offices"):
        items = job.get(key) or []
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    name = str(item.get("name", "") or "").strip()
                    if name:
                        tags.append(name)
    return ", ".join(dict.fromkeys(tags))


def _gh_meta_value(job: dict, *keys: str) -> str:
    """Cherche une valeur dans les metadata Greenhouse par nom de clé."""
    metadata = job.get("metadata") or []
    if not isinstance(metadata, list):
        return ""
    keys_low = {k.lower() for k in keys}
    for item in metadata:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "").lower().strip()
        if any(k in name for k in keys_low):
            val = item.get("value")
            if isinstance(val, list):
                return ", ".join(str(v) for v in val if v)
            return str(val or "").strip()
    return ""


def _gh_contract(job: dict, description: str) -> str:
    val = _gh_meta_value(job, "employment type", "contract", "contrat", "type")
    if val:
        return val
    combined = (str(job.get("title", "") or "") + " " + description).lower()
    for kw, label in [
        (r"internship|stage|intern\b",          "Stage"),
        (r"alternance|apprentissage",            "Alternance"),
        (r"\bcdi\b|permanent|full.time",         "Full-time"),
        (r"\bcdd\b|fixed.term",                  "CDD"),
        (r"freelance|contractor",                "Freelance"),
        (r"part.time|temps partiel",             "Part-time"),
    ]:
        if re.search(kw, combined):
            return label
    return ""


def _gh_remote(job: dict, location: str, description: str) -> str:
    val = _gh_meta_value(job, "remote", "télétravail", "work from home")
    if val:
        return val
    combined = (location + " " + description).lower()
    if re.search(r"\b(full[- ]?remote|100\s*%\s*remote|fully remote)\b", combined):
        return "Full Remote 🌍"
    if re.search(r"\b(hybrid|hybride|télétravail partiel)\b", combined):
        return "Hybrid 🏠🏢"
    if re.search(r"\b(remote|télétravail|work from home)\b", combined):
        return "Remote 🌍"
    return ""


def _gh_experience(job: dict, description: str) -> str:
    val = _gh_meta_value(job, "experience", "years", "expérience")
    if val:
        return val
    m = re.search(
        r"(\d+\s*(?:\+|to|-|à)\s*\d*\s*(?:years?|ans?))\s*(?:of\s*)?(?:experience|expérience)",
        description, re.I
    )
    if m:
        return m.group(1).strip() + " exp."
    m = re.search(r"(\d+)\+?\s*(?:years?|ans?)\s*(?:of\s*)?(?:experience|expérience)", description, re.I)
    if m:
        return m.group(1) + "+ years exp."
    return ""


def _gh_education(job: dict, description: str) -> str:
    val = _gh_meta_value(job, "education", "degree", "diplôme")
    if val:
        return val
    for pat, label in [
        (r"ph\.?d|doctorat",                               "PhD / Doctorat"),
        (r"bac\s*\+\s*5|master|msc|m\.sc|ingénieur",      "Bac+5 / Master"),
        (r"bac\s*\+\s*3|bachelor|licence|bsc|b\.sc",      "Bac+3 / Bachelor"),
        (r"bac\s*\+\s*2|bts|dut|hnd",                     "Bac+2"),
    ]:
        if re.search(pat, description, re.I):
            return label
    return ""


def _gh_skills(description: str) -> str:
    """Extrait les compétences techniques depuis la description."""
    TECH_SKILLS = [
        "Python", "Java", "JavaScript", "TypeScript", r"C\+\+", r"C#", "Go", "Rust",
        "Scala", r"\bR\b", "SQL", "NoSQL", "MongoDB", "PostgreSQL", "MySQL", "Redis",
        "Elasticsearch", "Kafka", "Spark", "Hadoop",
        r"TensorFlow", "PyTorch", "Keras", r"scikit-learn", "Pandas", "NumPy",
        "OpenCV", "spaCy", "HuggingFace", "LangChain", "LlamaIndex",
        "Docker", "Kubernetes", "Terraform", "Ansible", "Jenkins", r"GitLab CI",
        r"\bAWS\b", r"\bGCP\b", r"\bAzure\b", "Linux",
        "React", r"Vue\.?js", "Angular", r"Node\.js", "FastAPI", "Flask", "Django",
        "Spring", r"REST\b", "GraphQL", r"gRPC",
        r"Machine Learning", r"Deep Learning", r"\bNLP\b", r"Computer Vision",
        r"\bMLOps\b", r"\bDevOps\b", r"\bLLM\b", r"\bRAG\b",
        "Airflow", "dbt", "Snowflake", "BigQuery", "Databricks",
        "PowerBI", r"Power BI", "Tableau", "Looker",
    ]
    found = []
    for skill in TECH_SKILLS:
        if re.search(r"\b" + skill + r"\b", description, re.I):
            clean = re.sub(r"\\b|\\", "", skill)
            if clean not in found:
                found.append(clean)
    return ", ".join(found)


def _gh_salary(job: dict, description: str) -> str:
    val = _gh_meta_value(job, "salary", "compensation", "rémunération", "pay")
    if val:
        return val
    m = re.search(
        r"\$[\d,]+\s*(?:[-–]\s*\$[\d,]+)?\s*(?:k|K|000)?"
        r"|\€[\d,]+\s*(?:[-–]\s*\€[\d,]+)?"
        r"|\d[\d\s,.]*(?:k|K)?\s*(?:€|EUR|USD|\$|£)\s*(?:[-–/]\s*\d[\d\s,.]*(?:k|K)?\s*(?:€|EUR|USD|\$|£))?",
        description, re.I
    )
    return m.group(0).strip() if m else "Not specified"


def _gh_bonus_skills(description: str) -> str:
    """Extrait les compétences 'nice-to-have' depuis la description."""
    m = re.search(
        r"(?:nice[- ]to[- ]have|bonus|preferred|plus|appreciated|ideally|souhaitable)"
        r"[s:\s]*(.{20,300}?)(?:\n\n|\Z|(?=\n[A-Z]))",
        description, re.I | re.S
    )
    if m:
        return re.sub(r"\s+", " ", m.group(1)[:200]).strip()
    return ""


def _gh_normalize(board_token: str, job: dict) -> dict | None:
    """
    Normalise un job brut de l'API Greenhouse.
    Retourne un dict compatible handle_job() de main.py,
    avec champs _gh_* pour le bypass extract_with_llm dans enrich().
    """
    if not isinstance(job, dict):
        return None

    title = str(job.get("title", "") or "").strip()
    url   = str(job.get("absolute_url", "") or job.get("url", "") or "").strip()
    if not title:
        return None

    location    = _gh_location(job)
    description = _gh_strip_html(str(job.get("content", "") or ""))
    pub_dt      = _gh_published(job)
    time_ago    = _age_label(pub_dt)

    skills     = _gh_skills(description)
    remote_val = _gh_remote(job, location, description)
    salary_val = _gh_salary(job, description)

    return {
        # ── Champs requis par handle_job() + filtre cosine ────────────────
        "title":    title,
        "url":      url or f"https://boards.greenhouse.io/{board_token}",
        "company":  board_token.capitalize(),
        "location": location,
        "salary":   salary_val,
        "remote":   remote_val,
        "time_ago": time_ago,
        # ── Champs _gh_* → bypass extract_with_llm dans enrich() ─────────
        "_gh_pub_dt":     pub_dt,
        "_gh_contract":   _gh_contract(job, description),
        "_gh_experience": _gh_experience(job, description),
        "_gh_education":  _gh_education(job, description),
        "_gh_skills":     skills,
        "_gh_bonus":      _gh_bonus_skills(description),
        "_gh_description": description[:3000],
        "_gh_salary":     salary_val,
        "_gh_remote":     remote_val,
        "_gh_tags":       _gh_tags(job),
    }


async def _fetch_greenhouse_board(
    board_token: str,
    session: aiohttp.ClientSession,
) -> list[dict]:
    """
    Fetche tous les jobs d'un board Greenhouse via l'API JSON.
    Endpoint : GET /v1/boards/{token}/jobs?content=true
    Retourne la liste normalisée filtrée sur MAX_AGE_DAYS.
    """
    url = f"{GREENHOUSE_API_BASE}/{board_token}/jobs?content=true"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            if resp.status != 200:
                print(f"  [greenhouse/{board_token}] HTTP {resp.status} → skip")
                return []
            data = await resp.json(content_type=None)
    except Exception as e:
        print(f"  [greenhouse/{board_token}] fetch error: {type(e).__name__}: {e}")
        return []

    jobs_raw = data.get("jobs") if isinstance(data, dict) else None
    if not isinstance(jobs_raw, list):
        print(f"  [greenhouse/{board_token}] format inattendu → skip")
        return []

    recent    = 0
    old_count = 0
    results   = []

    for raw in jobs_raw:
        job = _gh_normalize(board_token, raw)
        if job is None:
            continue

        pub_dt = job.get("_gh_pub_dt")

        # ── SKIP si date > 45 jours ──────────────────────────────────────
        if pub_dt is not None and _too_old(pub_dt):
            old_count += 1
            continue

        results.append(job)
        recent += 1

    print(
        f"  [greenhouse/{board_token}] {recent} récents / "
        f"{old_count} vieux (> {MAX_AGE_DAYS}j) / "
        f"{len(jobs_raw)} total"
    )
    return results


async def scrape_greenhouse(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Scrape tous les boards Greenhouse configurés dans GREENHOUSE_BOARDS.

    Stratégie :
      - Tous les boards fetchés EN PARALLÈLE (asyncio.gather)
      - API JSON publique, pas de Cloudflare
      - Filtre date : SKIP si updated_at > 45 jours
      - Filtre cosine géré par handle_job() dans main.py (seuil 0.40)
      - Toutes les données extraites inline → bypass extract_with_llm

    L'argument `query` (titre CV) est transmis pour les logs uniquement.
    Le filtre de pertinence est le cosine, pas un filtre keyword ici.
    """
    print(f"  [greenhouse] Démarrage — {len(GREENHOUSE_BOARDS)} boards en parallèle")
    print(f"  [greenhouse] Boards : {', '.join(GREENHOUSE_BOARDS)}")

    # Fetch tous les boards en parallèle avec un délai court entre eux
    tasks = []
    for i, token in enumerate(GREENHOUSE_BOARDS):
        # Petit délai pour éviter de flooder l'API
        async def _fetch_with_delay(t=token, d=i * GREENHOUSE_DELAY):
            await asyncio.sleep(d)
            return await _fetch_greenhouse_board(t, session)
        tasks.append(_fetch_with_delay())

    board_results = await asyncio.gather(*tasks, return_exceptions=True)

    listings: list[dict] = []
    for i, result in enumerate(board_results):
        token = GREENHOUSE_BOARDS[i]
        if isinstance(result, Exception):
            print(f"  [greenhouse/{token}] exception: {result}")
            continue
        listings.extend(result)

    print(f"[greenhouse] TOTAL: {len(listings)} jobs récents (< {MAX_AGE_DAYS}j)")
    return listings


# ══════════════════════════════════════════════════════════════════════════════
#  6.  eluta.ca
# ══════════════════════════════════════════════════════════════════════════════
#
#  Eluta est le moteur d'offres d'emploi canadien.
#  Certaines pages sont protégées → scraping HTTP direct non fiable.
#
#  Stratégie : SerpApi Google Search paginé (identique à TanitJobs)
#    Query   : site:eluta.ca/job/   → uniquement pages job individuelles
#    Tri     : tbs=sbd:1,qdr:m2     → trié par date, 2 derniers mois
#    Marché  : gl=ca (Canada), hl=en
#    Pagination : start=0,10,20,... → équivalent à page=1,2,3 sur le site
#
#  Format snippet Google pour eluta.ca :
#    "Company Name - City, Province. X days ago. Description..."
#    "Company · City, ON · Posted 2 weeks ago. Job description..."
#    => company  = tout avant le 1er séparateur [-–·]
#    => location = ville + province (ON, QC, BC, AB, etc.)
#    => date     = "X days ago", "1 week ago", "today" dans le reste
#
#  Condition d'arrêt : date_pub > 45 jours → STOP
#  Filtre cosine     : handle_job() dans main.py (seuil 0.40)
#  Bypass Cloudflare : _eluta_* stockés → enrich() bypass extract_with_llm

ELUTA_BASE = "https://www.eluta.ca"

# Provinces canadiennes pour extraction de lieu
_CA_PROVINCES = (
    r"Ontario|Quebec|British Columbia|Alberta|Saskatchewan|Manitoba|"
    r"Nova Scotia|New Brunswick|Newfoundland|Prince Edward Island|"
    r"\bON\b|\bQC\b|\bBC\b|\bAB\b|\bSK\b|\bMB\b|\bNS\b|\bNB\b|\bNL\b|\bPEI\b|\bPE\b|\bNT\b|\bYT\b|\bNU\b"
)

# Compétences tech à extraire depuis le snippet (marché CA = EN majoritairement)
_ELUTA_SKILLS_RE = re.compile(
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


def _eluta_is_job_url(url: str) -> bool:
    """
    Retourne True si l'URL pointe vers une page de job eluta.ca.
    URL valide  : eluta.ca/job/12345 ou eluta.ca/job/titre-poste-12345
    URL rejetée : eluta.ca/jobs, eluta.ca/search, eluta.ca/ (home)
    """
    if not url or "eluta.ca" not in url:
        return False
    # Doit contenir /job/ suivi d'au moins un caractère
    return bool(re.search(r"/job/\S+", url))


def _eluta_parse_date(text: str) -> datetime | None:
    """
    Parse tous les formats de date vus dans les snippets Google d'eluta.ca.
    EN : "3 days ago", "2 weeks ago", "1 month ago", "today", "yesterday"
    ISO : "2024-03-15", "2024-03-15T10:00:00Z"
    Relatif : "posted X days ago", "X+ days ago"
    """
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

    # Relatifs EN
    if any(w in low for w in ("today", "just now", "this morning", "hour")):
        return now
    if "yesterday" in low:
        return now - timedelta(days=1)

    m = re.search(r"(\d+)\+?\s*day",   low)
    if m: return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\+?\s*week",  low)
    if m: return now - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\+?\s*month", low)
    if m: return now - timedelta(days=int(m.group(1)) * 30)
    m = re.search(r"(\d+)\+?\s*(?:hr|hour)", low)
    if m: return now

    # ISO formats
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except (ValueError, TypeError):
            pass

    return _parse_date(s)


def _eluta_extract_date(item: dict) -> tuple[datetime | None, str]:
    """
    Extrait la date de publication depuis un résultat SerpApi eluta.ca.
    Ordre de priorité :
      1. rich_snippet.detected_extensions (structured data Google)
      2. item["date"] (champ SerpApi direct)
      3. Snippet texte : patterns date
    """
    pub_dt, time_ago = None, ""

    # ── 1. rich_snippet ───────────────────────────────────────────────────────
    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        for key in ("date_posted", "posted_at", "date", "published_date"):
            val = str(ext.get(key, "") or "").strip()
            if val:
                pub_dt = _eluta_parse_date(val)
                if pub_dt:
                    return pub_dt, _age_label(pub_dt)

    # ── 2. item["date"] ───────────────────────────────────────────────────────
    raw = str(item.get("date", "") or "").strip()
    if raw:
        pub_dt = _eluta_parse_date(raw)
        if pub_dt:
            return pub_dt, _age_label(pub_dt)
        time_ago = raw

    # ── 3. Snippet texte ──────────────────────────────────────────────────────
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
            pub_dt = _eluta_parse_date(dm.group(0))
            if pub_dt:
                return pub_dt, _age_label(pub_dt)
            time_ago = time_ago or dm.group(0)

    return pub_dt, time_ago


def _eluta_extract_info(item: dict) -> dict:
    """
    Extrait title, company, location, salary, remote, contract, experience,
    skills depuis un résultat SerpApi eluta.ca.

    Format snippet eluta.ca (exemples réels) :
      "Accenture - Toronto, ON. 3 days ago. We are looking for a Data Engineer..."
      "Royal Bank of Canada · Montreal, QC · Posted 1 week ago. Machine Learning..."
      "Shopify - Remote, Canada. 5 days ago. Senior Data Engineer II..."

    Structure :
      PARTIE 1 avant [-–·]  → SOCIÉTÉ
      PARTIE 2 ville+prov   → LIEU
      Suite                 → description, date, contrat, skills...
    """
    snippet = str(item.get("snippet", "") or "")
    title   = str(item.get("title",   "") or "")

    # ── Titre : nettoyer "| eluta.ca", "- Eluta", "Job Title - Company | eluta" ──
    title = re.sub(
        r"\s*[\|·\-–—]\s*(?:eluta(?:\.ca)?|job[s]?\s+search)[^|]*$",
        "", title, flags=re.I
    ).strip()
    # Si "Titre - Société" → garder uniquement le titre
    for sep in (" | ", " - ", " – ", " — "):
        if title.count(sep) >= 1:
            title = title.split(sep)[0].strip()
            break

    # ── Société + Lieu depuis début du snippet ────────────────────────────────
    company  = ""
    location = ""

    # Tentative 1 : rich_snippet structured data
    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        if not company  and ext.get("company"):
            company  = str(ext["company"]).strip()
        if not location and ext.get("location"):
            location = str(ext["location"]).strip()

    # Tentative 2 : Pattern début de snippet "Company [-·] City, Province"
    if not company or not location:
        # Les snippets eluta commencent souvent par "Société - Ville, Province."
        sep_pat = r"\s*[-–—·]\s*"
        m = re.match(
            r"^(.+?)" + sep_pat +
            r"([A-Z][^·\-\n.]{1,40}(?:" + _CA_PROVINCES + r")[^.·]*)"
            r"(?:[.·]|$)",
            snippet, re.I
        )
        if m:
            if not company:  company  = m.group(1).strip()
            if not location: location = m.group(2).strip()

    # Tentative 3 : première ligne, premier séparateur
    if not company:
        first = snippet.split(".")[0].split("\n")[0]
        for sep in (" - ", " – ", " · ", " | "):
            if sep in first:
                parts = first.split(sep, 1)
                cand = parts[0].strip()
                # Vérifier que ce n'est pas une date ou ville
                if cand and not re.search(r"\d+\s+(?:day|week|month)", cand, re.I):
                    company = cand
                if len(parts) > 1 and not location:
                    loc_cand = parts[1].strip()
                    # Nettoyer la date si présente
                    loc_cand = re.sub(
                        r"\s*[·\-]\s*(?:posted|il\s+y\s+a|\d+\s+(?:day|week|month)).*$",
                        "", loc_cand, flags=re.I
                    ).strip()
                    location = loc_cand
                break

    # Nettoyer company des parasites (date, tirets, etc.)
    if company:
        company = re.sub(
            r"\s*[·\-]\s*(?:posted|il\s+y\s+a|\d+\s+(?:day|week|month)).*$",
            "", company, flags=re.I
        ).strip()

    # ── Localisation par défaut : chercher province canadienne ───────────────
    if not location:
        m = re.search(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*(?:" + _CA_PROVINCES + r"))",
            snippet, re.I
        )
        if m:
            location = m.group(1).strip()
        else:
            # Chercher juste la province
            m = re.search(r"\b(" + _CA_PROVINCES + r")\b", snippet, re.I)
            if m: location = m.group(1).strip()

    # ── Salaire ───────────────────────────────────────────────────────────────
    salary = ""
    sm = re.search(
        r"\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*/\s*(?:hr|hour|year|yr|an))?"
        r"|\d[\d\s,.]*\s*(?:CAD|USD|€)\s*(?:[-–]\s*\d[\d\s,.]*\s*(?:CAD|USD|€))?"
        r"|\d+k\s*(?:[-–]\s*\d+k)?",
        snippet, re.I
    )
    if sm: salary = sm.group(0).strip()
    salary = salary or "Not specified"

    # ── Remote ────────────────────────────────────────────────────────────────
    remote = ""
    if re.search(r"\b(full[- ]?remote|100%\s*remote|fully remote|work from home|\bwfh\b)\b", snippet, re.I):
        remote = "Full Remote 🌍"
    elif re.search(r"\b(hybrid|hybride|partial remote)\b", snippet, re.I):
        remote = "Hybrid 🏠🏢"
    elif re.search(r"\bremote\b", snippet, re.I):
        remote = "Remote 🌍"

    # ── Contrat ───────────────────────────────────────────────────────────────
    contract = ""
    for kw, label in [
        (r"\bpermanent\b|\bcdi\b",           "Permanent / Full-time"),
        (r"\bfull[- ]?time\b",               "Full-time"),
        (r"\bpart[- ]?time\b",               "Part-time"),
        (r"\bcontract\b|\bcdd\b|fixed.term", "Contract"),
        (r"\bfreelance\b|\bcontractor\b",    "Freelance"),
        (r"\binternship\b|\bstage\b|\bintern\b", "Internship"),
        (r"\balternal?ce\b|\bapprenticeship\b",  "Alternance"),
    ]:
        if re.search(kw, snippet, re.I):
            contract = label; break

    # ── Expérience ────────────────────────────────────────────────────────────
    experience = ""
    for pat in [
        r"(\d+\+?\s*[-–to]\s*\d+\s*years?\s*(?:of\s*)?(?:experience)?)",
        r"(\d+\+?\s*years?\s*(?:of\s*)?experience)",
        r"(\d+\+?\s*yrs?\b)",
        r"(entry[- ]level|mid[- ]level|senior|junior|lead|principal)",
    ]:
        m = re.search(pat, snippet, re.I)
        if m: experience = m.group(1).strip(); break

    # ── Skills depuis snippet ─────────────────────────────────────────────────
    found_skills = _ELUTA_SKILLS_RE.findall(snippet)
    skills = ", ".join(dict.fromkeys(s.lower() for s in found_skills))

    return {
        "title":       title,
        "company":     company,
        "location":    location or "Canada",
        "salary":      salary,
        "remote":      remote,
        "contract":    contract,
        "experience":  experience,
        "skills":      skills,
        "description": snippet[:1500],
    }


async def scrape_eluta(query: str, session: aiohttp.ClientSession) -> list[dict]:
    """
    Parcourt tous les jobs eluta.ca récents via SerpApi Google Search.

    Stratégie identique à TanitJobs :
      - Query SANS mots-clés : site:eluta.ca/job/
      - tbs=sbd:1,qdr:m2 → trié par date décroissante + 2 derniers mois
      - gl=ca (Canada), hl=en
      - Filtre _eluta_is_job_url() → rejette pages listing
      - STOP dès que date > 45 jours
      - _eluta_* stockés → bypass extract_with_llm dans enrich()
      - Si SERPAPI_API_KEY absent → retourne [] immédiatement

    Filtre cosine (seuil 0.40) géré par handle_job() dans main.py.
    """
    if not (_SERPAPI_AVAILABLE and bool(SERPAPI_API_KEY)):
        print("  [eluta] SERPAPI_API_KEY requis — absent, skip")
        return []

    listings:  list[dict] = []
    seen_urls: set[str]   = set()
    stopped                = False

    search_query = "site:eluta.ca/job/"
    print(f"  [eluta] Démarrage — parcours tous les jobs récents (Canada)")
    print(f"  [eluta] Query: '{search_query}' | gl=ca | tbs=sbd:1,qdr:m2")
    print(f"  [eluta] STOP automatique à {MAX_AGE_DAYS} jours")

    page_num = -1
    while True:
        page_num += 1
        if stopped:
            break
        try:
            params = {
                "engine":  "google",
                "q":       search_query,
                "api_key": SERPAPI_API_KEY,
                "num":     10,
                "start":   page_num * 10,
                "hl":      "en",
                "gl":      "ca",            # Canada
                "tbs":     "sbd:1,qdr:m2",  # trié par date + 2 derniers mois
            }
            results = await asyncio.to_thread(
                lambda p=params: _SerpApiSearch(p).get_dict()
            )

            err = results.get("error", "")
            if err:
                print(f"  [eluta] SerpApi error: {err}")
                break

            items = results.get("organic_results", []) or []
            print(f"  [eluta] page {page_num + 1} (start={page_num*10}): "
                  f"{len(items)} résultats Google")

            if not items:
                print(f"  [eluta] Plus de résultats → fin")
                break

            new_count  = 0
            skip_count = 0

            for item in items:
                raw_url = str(item.get("link", "") or "")

                # ── Filtrer : uniquement pages job individuelles ──────────────
                if not _eluta_is_job_url(raw_url):
                    skip_count += 1
                    continue

                # ── Déduplication ─────────────────────────────────────────────
                clean_url = raw_url.split("?")[0].rstrip("/")
                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)

                # ── Extraire date ─────────────────────────────────────────────
                pub_dt, time_ago = _eluta_extract_date(item)

                # ── STOP si date > 45 jours ───────────────────────────────────
                if pub_dt is not None and _too_old(pub_dt):
                    days = (datetime.now() - pub_dt).days
                    print(
                        f"  [eluta] STOP cutoff : {days}j > {MAX_AGE_DAYS}j "
                        f"(résultats triés par date → fin du scraping)"
                    )
                    stopped = True
                    break

                # ── Extraire infos depuis snippet ─────────────────────────────
                info = _eluta_extract_info(item)

                if not info["title"] or len(info["title"]) < 3:
                    skip_count += 1
                    continue

                job = {
                    # ── Champs handle_job() + filtre cosine ───────────────────
                    "title":    info["title"],
                    "url":      clean_url,
                    "company":  info["company"],
                    "location": info["location"],
                    "salary":   info["salary"],
                    "remote":   info["remote"],
                    "time_ago": time_ago,
                    # ── Champs _eluta_* → bypass extract_with_llm ─────────────
                    "_eluta_contract":    info["contract"],
                    "_eluta_experience":  info["experience"],
                    "_eluta_skills":      info["skills"],
                    "_eluta_description": info["description"],
                }
                listings.append(job)
                new_count += 1

            print(f"  [eluta] +{new_count} gardés, {skip_count} ignorés "
                  f"(total {len(listings)})")

            if not results.get("serpapi_pagination", {}).get("next"):
                print(f"  [eluta] Fin de pagination Google")
                break

            await asyncio.sleep(0.5)

        except Exception as e:
            print(f"  [eluta] exception: {type(e).__name__}: {e}")
            break

    print(f"[eluta] TOTAL: {len(listings)}")
    return listings