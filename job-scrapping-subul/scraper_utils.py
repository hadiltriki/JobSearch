"""
scraper_utils.py — Helpers partagés entre tous les scrapers

Contient :
  - Constantes globales (MAX_AGE_DAYS, timeouts, headers…)
  - Parsers de dates (_parse_date, _age_label, _too_old)
  - Inférence remote (_infer_remote)
  - Extraction de tags / keywords depuis le titre CV
    (_cv_title_to_tags, _extract_tech_keywords)

Importé par :
  scraper_aijobs.py, scraper_remoteok.py, scraper_emploitic.py,
  scraper_tanitjobs.py, scraper_greenhouse.py, scraper_eluta.py
"""

import re
from datetime import datetime, timedelta, timezone

import aiohttp

# ── Constantes globales ───────────────────────────────────────────────────────
MAX_AGE_DAYS      = 45

# aijobs anti-429
DELAY_BETWEEN_PAGES = 8.0
WARMUP_DELAY        = 3.0
RETRY_WAIT          = 45
MAX_RETRIES         = 3

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=30)

BROWSER_HEADERS = {
    "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate",
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


def extract_tech_from_description(description: str, max_skills: int = 12) -> str:
    """
    Extract tech/skill keywords from job description for gap & "skills in demand" display.
    Used when the source does not provide structured skills (e.g. Indeed/LinkedIn snippets).
    Returns comma-separated string of normalized terms found in text.
    """
    if not description or not description.strip():
        return ""
    text = description.lower()
    found: list[str] = []
    # Multi-word phrases first (so "machine learning" wins over "machine" + "learning")
    for phrase, tag in sorted(_TECH_MAP.items(), key=lambda x: -len(x[0])):
        if " " in phrase and phrase in text and tag not in found:
            found.append(tag)
    for word in re.findall(r'[a-z0-9#+.]+', text):
        if word in _SENIORITY or word in _ROLE_WORDS or len(word) < 2:
            continue
        mapped = _TECH_MAP.get(word)
        if mapped and mapped not in found:
            found.append(mapped)
    return ", ".join(found[:max_skills]) if found else ""