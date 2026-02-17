import json
import csv
import re
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


BASE_URL = "https://remoteok.com"
API_URL = f"{BASE_URL}/api"

OUTPUT_DIR = Path("outputs_remoteok")
RAW_API_JSON = OUTPUT_DIR / "raw_api.json"
OUTPUT_JSON = OUTPUT_DIR / "jobs.json"
OUTPUT_CSV = OUTPUT_DIR / "jobs.csv"
LOG_FILE = OUTPUT_DIR / "scrape.log"

MAX_JOBS = 30  # increase later


def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler()
        ],
    )


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _fetch_json(url: str, *, timeout_s: int = 60, retries: int = 3) -> Any:
    """
    Fetch JSON from a URL using stdlib only.
    RemoteOK can be a bit picky: send a browser-ish UA and accept JSON.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL + "/",
    }

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout_s) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8", errors="replace"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            wait_s = min(10, 1.5 ** attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to fetch JSON from {url}") from last_err


def _as_list(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [clean_text(str(x)) for x in v if clean_text(str(x))]
    # sometimes tags come in as a comma-separated string
    if isinstance(v, str):
        return [clean_text(x) for x in v.split(",") if clean_text(x)]
    return [clean_text(str(v))] if clean_text(str(v)) else []


def _normalize_job(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize RemoteOK API job fields into our schema.
    Skip non-job objects like the first 'legal' entry.
    """
    # The API's first element is usually metadata (e.g. {"legal": "..."})
    if not isinstance(item, dict):
        return None
    if "legal" in item and len(item.keys()) <= 2:
        return None

    title = clean_text(item.get("position") or item.get("title") or "")
    company = clean_text(item.get("company") or "")

    url = item.get("url") or ""
    if isinstance(url, str) and url.startswith("/"):
        url = BASE_URL + url
    url = clean_text(str(url)) if url else ""

    # Not always present; keep best-effort.
    location = clean_text(
        item.get("location")
        or item.get("region")
        or item.get("country")
        or ""
    )
    salary = clean_text(item.get("salary") or item.get("compensation") or "")
    tags = _as_list(item.get("tags"))
    description = clean_text(item.get("description") or item.get("description_raw") or "")

    if not title and not company and not url:
        return None

    return {
        "source": "remoteok",
        "url": url,
        "title": title,
        "company": company,
        "location": location,
        "salary": salary,
        "tags": tags,
        "description": description,
        "raw_html_file": "",  # API mode: no raw HTML saved
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def scrape_remoteok():
    setup_logging()
    logging.info("Starting RemoteOK scraping (API feed)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    data = _fetch_json(API_URL)
    RAW_API_JSON.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected API response shape: {type(data)}")

    jobs: List[Dict[str, Any]] = []
    for item in data:
        job = _normalize_job(item)
        if job:
            jobs.append(job)

    logging.info(f"API returned {len(jobs)} job objects.")
    jobs = jobs[:MAX_JOBS]
    logging.info(f"Will save {len(jobs)} jobs (MAX_JOBS={MAX_JOBS}).")

    # Save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

    # Save CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source", "url", "title", "company", "location", "salary",
                "tags", "description", "raw_html_file", "scraped_at_utc"
            ]
        )
        writer.writeheader()
        for j in jobs:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved JSON: {OUTPUT_JSON}")
    logging.info(f"Saved CSV : {OUTPUT_CSV}")
    logging.info(f"Raw API   : {RAW_API_JSON}")
    logging.info("Done ✅")


if __name__ == "__main__":
    scrape_remoteok()
