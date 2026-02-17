import argparse
import csv
import html
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


BOARDS_BASE = "https://boards.greenhouse.io"
API_BASE = "https://boards-api.greenhouse.io/v1/boards"

OUTPUT_DIR = Path("outputs_greenhouse")
LOG_FILE = OUTPUT_DIR / "scrape.log"

DEFAULT_MAX_JOBS = 30


def setup_logging():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _strip_tags(html_text: str) -> str:
    if not html_text:
        return ""
    html_text = html.unescape(html_text)
    html_text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", html_text)
    html_text = re.sub(r"(?s)<[^>]*>", " ", html_text)
    return clean_text(html_text)


def _fetch_json(url: str, *, timeout_s: int = 60, retries: int = 3) -> Any:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BOARDS_BASE + "/",
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
            wait_s = min(10, 1.5**attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to fetch JSON from {url}") from last_err


def _board_from_url(board_url: str) -> Optional[str]:
    """
    Accept:
    - https://boards.greenhouse.io/<board_token>
    - https://boards.greenhouse.io/<board_token>/
    - https://boards.greenhouse.io/<board_token>/jobs/<job_id>
    """
    try:
        u = urlparse(board_url)
    except Exception:
        return None
    if not u.netloc:
        return None
    if u.netloc.lower() not in {"boards.greenhouse.io", "www.boards.greenhouse.io"}:
        return None
    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None
    return parts[0]


def _as_tags(job: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    depts = job.get("departments")
    if isinstance(depts, list):
        for d in depts:
            if isinstance(d, dict):
                name = clean_text(str(d.get("name") or ""))
                if name:
                    tags.append(name)
    offices = job.get("offices")
    if isinstance(offices, list):
        for o in offices:
            if isinstance(o, dict):
                name = clean_text(str(o.get("name") or ""))
                if name:
                    tags.append(name)
    return list(dict.fromkeys(tags))


def _normalize_job(board_token: str, job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(job, dict):
        return None

    title = clean_text(job.get("title") or "")
    url = clean_text(job.get("absolute_url") or job.get("url") or "")
    location_name = ""
    loc = job.get("location")
    if isinstance(loc, dict):
        location_name = clean_text(loc.get("name") or "")

    tags = _as_tags(job)
    content_html = str(job.get("content") or "")
    description = _strip_tags(content_html)

    if not title and not url:
        return None

    return {
        "source": "greenhouse",
        "url": url,
        "title": title,
        "company": board_token,
        "location": location_name,
        "salary": "",
        "tags": tags,
        "description": description,
        "raw_html_file": "",
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def scrape_board(board_token: str, *, max_jobs: int) -> List[Dict[str, Any]]:
    board_token = clean_text(board_token).strip("/").lower()
    if not board_token:
        raise ValueError("Empty board token")

    api_url = f"{API_BASE}/{board_token}/jobs?content=true"
    logging.info(f"Fetching Greenhouse jobs for '{board_token}'")
    data = _fetch_json(api_url)

    board_dir = OUTPUT_DIR / board_token
    board_dir.mkdir(parents=True, exist_ok=True)
    raw_path = board_dir / "raw_jobs.json"
    raw_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    jobs_raw = None
    if isinstance(data, dict):
        jobs_raw = data.get("jobs")
    if not isinstance(jobs_raw, list):
        raise RuntimeError(f"Unexpected Greenhouse API response shape: {type(data)}")

    jobs: List[Dict[str, Any]] = []
    for j in jobs_raw:
        job = _normalize_job(board_token, j)
        if job:
            jobs.append(job)

    logging.info(f"'{board_token}': API returned {len(jobs)} jobs.")
    jobs = jobs[:max_jobs]

    out_json = board_dir / "jobs.json"
    out_csv = board_dir / "jobs.csv"
    out_json.write_text(json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8")

    with open(out_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source",
                "url",
                "title",
                "company",
                "location",
                "salary",
                "tags",
                "description",
                "raw_html_file",
                "scraped_at_utc",
            ],
        )
        writer.writeheader()
        for job in jobs:
            row = dict(job)
            row["tags"] = ", ".join(job.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved: {out_json}")
    logging.info(f"Saved: {out_csv}")
    logging.info(f"Raw  : {raw_path}")
    return jobs


def parse_targets(values: Iterable[str]) -> List[str]:
    targets: List[str] = []
    for v in values:
        v = v.strip()
        if not v:
            continue
        if v.startswith("http://") or v.startswith("https://"):
            token = _board_from_url(v)
            if not token:
                raise ValueError(f"Not a boards.greenhouse.io URL: {v}")
            targets.append(token)
        else:
            targets.append(v)
    return list(dict.fromkeys([t.strip("/").lower() for t in targets if t.strip("/")]))


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Greenhouse job boards via the public Job Board API."
    )
    parser.add_argument(
        "--board",
        "-b",
        action="append",
        default=[],
        help="Greenhouse board token (can repeat). Example: --board airbnb",
    )
    parser.add_argument(
        "--url",
        "-u",
        action="append",
        default=[],
        help="Greenhouse board URL (can repeat). Example: --url https://boards.greenhouse.io/airbnb",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=DEFAULT_MAX_JOBS,
        help=f"Max jobs per board to save (default: {DEFAULT_MAX_JOBS}).",
    )
    args = parser.parse_args()

    setup_logging()
    targets = parse_targets([*args.board, *args.url])
    if not targets:
        parser.error(
            "Provide at least one --board (token) or --url (boards.greenhouse.io URL). "
            "Example: --board airbnb or --url https://boards.greenhouse.io/airbnb"
        )

    for token in targets:
        try:
            scrape_board(token, max_jobs=args.max_jobs)
        except Exception as e:
            logging.exception(f"Failed scraping '{token}': {e}")

    logging.info("Done ✅")


if __name__ == "__main__":
    main()

