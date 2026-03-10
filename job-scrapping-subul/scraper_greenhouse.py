"""
scraper_greenhouse.py — Scraper Greenhouse.io

Stratégie : API JSON publique (pas de Cloudflare)
  - Endpoint : GET /v1/boards/{token}/jobs?content=true
  - Tous les boards fetchés EN PARALLÈLE (asyncio.gather)
  - Filtre date : SKIP si updated_at > 45 jours
  - Extraction complète inline → bypass extract_with_llm

Fonctions exportées :
    scrape_greenhouse(query, session) → list[dict]
"""

import asyncio
import re
from datetime import datetime

import aiohttp

from scraper_utils import (
    MAX_AGE_DAYS,
    _age_label, _too_old,
)

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

GREENHOUSE_DELAY = 0.2


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers extraction depuis description HTML
# ══════════════════════════════════════════════════════════════════════════════

def _gh_strip_html(html_text: str) -> str:
    import html as _html_mod
    if not html_text:
        return ""
    text = _html_mod.unescape(str(html_text))
    text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]*>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _gh_published(job: dict) -> datetime | None:
    for key in ("updated_at", "first_published", "created_at"):
        raw = job.get(key, "") or ""
        if not raw:
            continue
        try:
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
    m = re.search(
        r"(?:nice[- ]to[- ]have|bonus|preferred|plus|appreciated|ideally|souhaitable)"
        r"[s:\s]*(.{20,300}?)(?:\n\n|\Z|(?=\n[A-Z]))",
        description, re.I | re.S
    )
    if m:
        return re.sub(r"\s+", " ", m.group(1)[:200]).strip()
    return ""


def _gh_normalize(board_token: str, job: dict) -> dict | None:
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
        "title":    title,
        "url":      url or f"https://boards.greenhouse.io/{board_token}",
        "company":  board_token.capitalize(),
        "location": location,
        "salary":   salary_val,
        "remote":   remote_val,
        "time_ago": time_ago,
        "_gh_pub_dt":      pub_dt,
        "_gh_contract":    _gh_contract(job, description),
        "_gh_experience":  _gh_experience(job, description),
        "_gh_education":   _gh_education(job, description),
        "_gh_skills":      skills,
        "_gh_bonus":       _gh_bonus_skills(description),
        "_gh_description": description[:3000],
        "_gh_salary":      salary_val,
        "_gh_remote":      remote_val,
        "_gh_tags":        _gh_tags(job),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Fetch d'un board
# ══════════════════════════════════════════════════════════════════════════════

async def _fetch_greenhouse_board(
    board_token: str,
    session: aiohttp.ClientSession,
) -> list[dict]:
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


# ══════════════════════════════════════════════════════════════════════════════
#  Point d'entrée public
# ══════════════════════════════════════════════════════════════════════════════

async def scrape_greenhouse(query: str, session: aiohttp.ClientSession) -> list[dict]:
    print(f"  [greenhouse] Démarrage — {len(GREENHOUSE_BOARDS)} boards en parallèle")
    print(f"  [greenhouse] Boards : {', '.join(GREENHOUSE_BOARDS)}")

    tasks = []
    for i, token in enumerate(GREENHOUSE_BOARDS):
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