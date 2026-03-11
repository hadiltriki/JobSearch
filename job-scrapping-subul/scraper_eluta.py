"""
scraper_eluta.py — Scraper eluta.ca (Canada)

Stratégie : SerpApi Google Search paginé
  - Cloudflare peut bloquer l'accès direct → SerpApi recommandé
  - Query : site:eluta.ca/job/
  - Tri    : tbs=sbd:1,qdr:m2 (par date, 2 derniers mois)
  - gl=ca (Canada), hl=en
  - STOP dès qu'un job > 45 jours est détecté

Fonctions exportées :
    scrape_eluta(query, session) → list[dict]
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

ELUTA_BASE = "https://www.eluta.ca"

# Provinces canadiennes pour extraction de lieu
_CA_PROVINCES = (
    r"Ontario|Quebec|British Columbia|Alberta|Saskatchewan|Manitoba|"
    r"Nova Scotia|New Brunswick|Newfoundland|Prince Edward Island|"
    r"\bON\b|\bQC\b|\bBC\b|\bAB\b|\bSK\b|\bMB\b|\bNS\b|\bNB\b|\bNL\b|\bPEI\b|\bPE\b|\bNT\b|\bYT\b|\bNU\b"
)

# Compétences tech à extraire depuis le snippet + titre
_ELUTA_SKILLS_RE = re.compile(
    r'\b(?:python|java|javascript|typescript|scala|go|rust|sql|nosql|'
    r'r\b|c\+\+|c#|bash|shell|'
    r'tensorflow|pytorch|keras|scikit[\-\s]?learn|xgboost|lightgbm|catboost|langchain|openai|'
    r'spark|apache\s*spark|kafka|apache\s*kafka|airflow|apache\s*airflow|mlflow|'
    r'docker|kubernetes|k8s|terraform|ansible|jenkins|ci[/\-]?cd|'
    r'aws|azure|gcp|google\s*cloud|databricks|snowflake|dbt|bigquery|redshift|'
    r'postgresql|postgres|mysql|mongodb|redis|elasticsearch|cassandra|dynamodb|'
    r'tableau|power\s*bi|powerbi|looker|grafana|metabase|qlik|'
    r'llm|nlp|natural\s*language\s*processing|computer\s*vision|rag|'
    r'deep\s*learning|machine\s*learning|reinforcement\s*learning|'
    r'data\s*science|data\s*engineering|data\s*warehouse|data\s*lake|'
    r'devops|mlops|sre|git|linux|unix|fastapi|flask|django|spring|'
    r'react|node\.js|angular|vue\.?js|pandas|numpy|excel|matlab|'
    r'hadoop|hive|pig|flink|beam|nifi|sagemaker|vertex\s*ai|kubeflow|'
    r'etl|elt|pipeline|api|rest\s*api|microservices)\b',
    re.I
)


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers dates
# ══════════════════════════════════════════════════════════════════════════════

def _eluta_parse_date(text: str) -> datetime | None:
    if not text:
        return None
    s   = str(text).strip()
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    low = s.lower()

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

    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt)
        except (ValueError, TypeError):
            pass

    return _parse_date(s)


def _eluta_extract_date(item: dict) -> tuple[datetime | None, str]:
    pub_dt, time_ago = None, ""

    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        for key in ("date_posted", "posted_at", "date", "published_date"):
            val = str(ext.get(key, "") or "").strip()
            if val:
                pub_dt = _eluta_parse_date(val)
                if pub_dt:
                    return pub_dt, _age_label(pub_dt)

    raw = str(item.get("date", "") or "").strip()
    if raw:
        pub_dt = _eluta_parse_date(raw)
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
            pub_dt = _eluta_parse_date(dm.group(0))
            if pub_dt:
                return pub_dt, _age_label(pub_dt)
            time_ago = time_ago or dm.group(0)

    return pub_dt, time_ago


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers extraction info
# ══════════════════════════════════════════════════════════════════════════════

def _eluta_is_job_url(url: str) -> bool:
    return bool(url and "eluta.ca" in url)


def _eluta_extract_info(item: dict) -> dict:
    snippet = str(item.get("snippet", "") or "")
    title   = str(item.get("title",   "") or "")

    title = re.sub(
        r"\s*[\|·\-–—]\s*(?:eluta(?:\.ca)?|job[s]?\s+search)[^|]*$",
        "", title, flags=re.I
    ).strip()
    for sep in (" | ", " - ", " – ", " — "):
        if title.count(sep) >= 1:
            title = title.split(sep)[0].strip()
            break

    company  = ""
    location = ""

    rs = item.get("rich_snippet", {}) or {}
    for section in ("top", "bottom"):
        ext = (rs.get(section, {}) or {}).get("detected_extensions", {}) or {}
        if not company  and ext.get("company"):
            company  = str(ext["company"]).strip()
        if not location and ext.get("location"):
            location = str(ext["location"]).strip()

    if not company or not location:
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

    if not company:
        first = snippet.split(".")[0].split("\n")[0]
        for sep in (" - ", " – ", " · ", " | "):
            if sep in first:
                parts = first.split(sep, 1)
                cand = parts[0].strip()
                if cand and not re.search(r"\d+\s+(?:day|week|month)", cand, re.I):
                    company = cand
                if len(parts) > 1 and not location:
                    loc_cand = parts[1].strip()
                    loc_cand = re.sub(
                        r"\s*[·\-]\s*(?:posted|il\s+y\s+a|\d+\s+(?:day|week|month)).*$",
                        "", loc_cand, flags=re.I
                    ).strip()
                    location = loc_cand
                break

    if company:
        company = re.sub(
            r"\s*[·\-]\s*(?:posted|il\s+y\s+a|\d+\s+(?:day|week|month)).*$",
            "", company, flags=re.I
        ).strip()

    if not location:
        m = re.search(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?,\s*(?:" + _CA_PROVINCES + r"))",
            snippet, re.I
        )
        if m:
            location = m.group(1).strip()
        else:
            m = re.search(r"\b(" + _CA_PROVINCES + r")\b", snippet, re.I)
            if m: location = m.group(1).strip()

    # Fallback : extraire depuis le titre SerpApi
    # Eluta title format : "Job Title - City, Province | Company - eluta.ca"
    if not location:
        raw_title_loc = str(item.get("title", "") or "")
        # Chercher "City, Province" dans le titre (ex: "Toronto, ON")
        m = re.search(
            r"[-–|]\s*([A-Z][a-zA-Z\s]+,?\s*(?:" + _CA_PROVINCES + r"))\s*(?:[|\-]|$)",
            raw_title_loc, re.I
        )
        if m:
            location = m.group(1).strip().rstrip(",")
        else:
            # Chercher province seule dans le titre
            m = re.search(r"\b(" + _CA_PROVINCES + r")\b", raw_title_loc, re.I)
            if m:
                location = m.group(1).strip()

    # Fallback : extraire depuis l'URL eluta
    # ex: /find-job-in-toronto-123  ou /top-job/data-engineer-toronto-on-456
    if not location:
        raw_url_loc = str(item.get("url", "") or "")
        # Pattern : "in-cityname" ou "city-province"
        m = re.search(r"find-job-in-([a-z-]+?)(?:-\d|$)", raw_url_loc, re.I)
        if m:
            city = m.group(1).replace("-", " ").title()
            if len(city) > 2:
                location = city + ", Canada"
        if not location:
            m = re.search(r"\b(" + _CA_PROVINCES + r")\b", raw_url_loc, re.I)
            if m:
                location = m.group(1).strip()

    salary = ""
    sm = re.search(
        r"\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?(?:\s*/\s*(?:hr|hour|year|yr|an))?"
        r"|\d[\d\s,.]*\s*(?:CAD|USD|€)\s*(?:[-–]\s*\d[\d\s,.]*\s*(?:CAD|USD|€))?"
        r"|\d+k\s*(?:[-–]\s*\d+k)?",
        snippet, re.I
    )
    if sm: salary = sm.group(0).strip()
    salary = salary or "Not specified"

    remote = ""
    if re.search(r"\b(full[- ]?remote|100%\s*remote|fully remote|work from home|\bwfh\b)\b", snippet, re.I):
        remote = "Full Remote 🌍"
    elif re.search(r"\b(hybrid|hybride|partial remote)\b", snippet, re.I):
        remote = "Hybrid 🏠🏢"
    elif re.search(r"\bremote\b", snippet, re.I):
        remote = "Remote 🌍"

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

    experience = ""
    for pat in [
        r"(\d+\+?\s*[-–to]\s*\d+\s*years?\s*(?:of\s*)?(?:experience)?)",
        r"(\d+\+?\s*years?\s*(?:of\s*)?experience)",
        r"(\d+\+?\s*yrs?\b)",
        r"(entry[- ]level|mid[- ]level|senior|junior|lead|principal)",
    ]:
        m = re.search(pat, snippet, re.I)
        if m: experience = m.group(1).strip(); break

    # Skills : chercher dans snippet + titre (snippet SerpApi est court ~150 chars)
    combined_text = title + " " + snippet
    found_skills  = _ELUTA_SKILLS_RE.findall(combined_text)
    # Normaliser pour correspondre aux noms canoniques attendus par matcher.py
    _SKILL_NORM = {
        "scikit learn": "scikit-learn", "scikit-learn": "scikit-learn",
        "apache spark": "Apache Spark",  "spark": "Apache Spark",
        "apache kafka": "Apache Kafka",  "kafka": "Apache Kafka",
        "apache airflow": "Apache Airflow", "airflow": "Apache Airflow",
        "machine learning": "Machine Learning", "ml": "Machine Learning",
        "deep learning": "Deep Learning",
        "natural language processing": "NLP", "nlp": "NLP",
        "computer vision": "Computer Vision",
        "power bi": "Power BI", "powerbi": "Power BI",
        "node.js": "Node.js", "nodejs": "Node.js",
        "vue.js": "Vue.js", "vuejs": "Vue.js",
        "google cloud": "GCP", "gcp": "GCP",
        "kubernetes": "Kubernetes", "k8s": "Kubernetes",
        "postgresql": "PostgreSQL", "postgres": "PostgreSQL",
        "mongodb": "MongoDB", "elasticsearch": "Elasticsearch",
        "tensorflow": "TensorFlow", "pytorch": "PyTorch",
        "rest api": "REST API", "etl": "ETL",
    }
    seen_skills, deduped = set(), []
    for s in found_skills:
        normed = _SKILL_NORM.get(s.lower().strip(), s.strip())
        key    = normed.lower()
        if key not in seen_skills:
            seen_skills.add(key)
            deduped.append(normed)
    skills = ", ".join(deduped)

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


# ══════════════════════════════════════════════════════════════════════════════
#  Point d'entrée public
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_eluta(query: str, session: aiohttp.ClientSession) -> list[dict]:
    try:
        from serpapi_google_web_common import scrape_google_web, looks_like_placeholder_api_key
    except ImportError:
        print("  [eluta] serpapi_google_web_common.py introuvable — skip")
        return []

    if not SERPAPI_API_KEY or looks_like_placeholder_api_key(SERPAPI_API_KEY):
        print("  [eluta] SERPAPI_API_KEY requis — absent ou invalide, skip")
        return []

    import tempfile
    from pathlib import Path

    search_query = f"site:eluta.ca {query}"
    print(f"  [eluta] Démarrage — Canada | Query: '{search_query}'")
    print(f"  [eluta] STOP automatique à {MAX_AGE_DAYS} jours")

    listings:  list[dict] = []
    seen_urls: set[str]   = set()

    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            raw_results, _ = await asyncio.to_thread(
                lambda: scrape_google_web(
                    q               = search_query,
                    output_dir      = Path(tmpdir),
                    source_tag      = "eluta",
                    api_key         = SERPAPI_API_KEY,
                    url_allow_regex = r"eluta\.ca",
                    derive_fields   = False,
                    max_results     = 100,
                    hl              = "en",
                    gl              = "us",
                    delay_s         = 0.5,
                )
            )
        except Exception as e:
            print(f"  [eluta] scrape_google_web error: {e}")
            return []

    for item in raw_results:
        raw_url = str(item.get("url", "") or "")
        if not _eluta_is_job_url(raw_url):
            continue

        clean_url = raw_url.split("?")[0].rstrip("/")
        if clean_url in seen_urls:
            continue
        seen_urls.add(clean_url)

        # Reconstruire un item compatible avec les helpers existants
        # Note : on passe aussi l'URL brute pour extraction de location depuis le slug
        raw_title = item.get("title", "") or ""
        raw_desc  = item.get("description", "") or ""
        compat = {
            "link":    raw_url,
            "url":     raw_url,    # utilisé par _eluta_extract_location_from_url
            "title":   raw_title,
            "snippet": raw_desc,
        }
        pub_dt, time_ago = _eluta_extract_date(compat)
        if pub_dt is not None and _too_old(pub_dt):
            print(f"  [eluta] STOP cutoff: {(datetime.now()-pub_dt).days}j > {MAX_AGE_DAYS}j")
            break

        info = _eluta_extract_info(compat)
        if not info["title"] or len(info["title"]) < 3:
            continue

        listings.append({
            "title":    info["title"],
            "url":      clean_url,
            "company":  info["company"],
            "location": info["location"],
            "salary":   info["salary"],
            "remote":   info["remote"],
            "time_ago": time_ago,
            "_eluta_contract":    info["contract"],
            "_eluta_experience":  info["experience"],
            "_eluta_skills":      info["skills"],
            "_eluta_description": info["description"],
        })

    print(f"[eluta] TOTAL: {len(listings)}")
    return listings