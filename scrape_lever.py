import argparse
import csv
import html
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


LEVER_HOSTED_BASE = "https://jobs.lever.co"
LEVER_API_BASE = "https://api.lever.co/v0/postings"

OUTPUT_DIR = Path("outputs_lever")
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
    # decode entities first so output is readable
    html_text = html.unescape(html_text)
    # remove script/style
    html_text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", html_text)
    # remove all tags
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
        "Referer": LEVER_HOSTED_BASE + "/",
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


def _company_from_board_url(board_url: str) -> Optional[str]:
    """
    Accept:
    - https://jobs.lever.co/<company>
    - https://jobs.lever.co/<company>/
    - https://jobs.lever.co/<company>/<posting-id>
    """
    try:
        u = urlparse(board_url)
    except Exception:
        return None
    if not u.netloc:
        return None
    if u.netloc.lower() not in {"jobs.lever.co", "www.jobs.lever.co"}:
        return None
    parts = [p for p in u.path.split("/") if p]
    if not parts:
        return None
    return parts[0]


def _as_tags(posting: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    categories = posting.get("categories") or {}
    if isinstance(categories, dict):
        for k in ("team", "department", "commitment", "location", "level"):
            v = clean_text(str(categories.get(k) or ""))
            if v:
                tags.append(v)
    workplace = clean_text(str(posting.get("workplaceType") or ""))
    if workplace:
        tags.append(workplace)
    # de-dup preserve order
    return list(dict.fromkeys(tags))


def _normalize_posting(company_slug: str, posting: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(posting, dict):
        return None

    title = clean_text(posting.get("text") or "")
    hosted_url = clean_text(posting.get("hostedUrl") or "")
    if not hosted_url:
        # fallback to hosted board URL pattern
        pid = clean_text(str(posting.get("id") or ""))
        if pid:
            hosted_url = f"{LEVER_HOSTED_BASE}/{company_slug}/{pid}"

    categories = posting.get("categories") or {}
    location = ""
    if isinstance(categories, dict):
        location = clean_text(str(categories.get("location") or ""))

    description_html = str(posting.get("description") or "")
    description = _strip_tags(description_html)

    # Lever gives us HTML chunks via: description, additional, and lists[] (not always).
    # We'll combine description + additional, and we add list text below in a best-effort way.
    additional = posting.get("additional") or ""
    if additional:
        description = clean_text(description + " " + _strip_tags(str(additional)))

    # Lists is usually a structured array; extract content if present.
    lists = posting.get("lists")
    if isinstance(lists, list):
        list_bits: List[str] = []
        for section in lists:
            if not isinstance(section, dict):
                continue
            header = clean_text(section.get("text") or "")
            content = _strip_tags(section.get("content") or "")
            if header and content:
                list_bits.append(f"{header}: {content}")
            elif content:
                list_bits.append(content)
        if list_bits:
            description = clean_text(description + " " + " ".join(list_bits))

    company_name = clean_text(posting.get("organization") or "") or company_slug
    tags = _as_tags(posting)

    if not title and not hosted_url:
        return None

    return {
        "source": "lever",
        "url": hosted_url,
        "title": title,
        "company": company_name,
        "location": location,
        "salary": "",
        "tags": tags,
        "description": description,
        "raw_html_file": "",
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def scrape_company(company_slug: str, *, max_jobs: int) -> List[Dict[str, Any]]:
    company_slug = clean_text(company_slug).strip("/").lower()
    if not company_slug:
        raise ValueError("Empty company slug")

    api_url = f"{LEVER_API_BASE}/{company_slug}?mode=json"
    logging.info(f"Fetching Lever postings for '{company_slug}'")
    data = _fetch_json(api_url)

    company_dir = OUTPUT_DIR / company_slug
    company_dir.mkdir(parents=True, exist_ok=True)
    raw_path = company_dir / "raw_postings.json"
    raw_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected Lever API response shape: {type(data)}")

    jobs: List[Dict[str, Any]] = []
    for posting in data:
        job = _normalize_posting(company_slug, posting)
        if job:
            jobs.append(job)

    logging.info(f"'{company_slug}': API returned {len(jobs)} jobs.")
    jobs = jobs[:max_jobs]

    out_json = company_dir / "jobs.json"
    out_csv = company_dir / "jobs.csv"

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
        for j in jobs:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved: {out_json}")
    logging.info(f"Saved: {out_csv}")
    logging.info(f"Raw  : {raw_path}")
    return jobs


def parse_targets(values: Iterable[str]) -> List[str]:
    companies: List[str] = []
    for v in values:
        v = v.strip()
        if not v:
            continue
        if v.startswith("http://") or v.startswith("https://"):
            slug = _company_from_board_url(v)
            if not slug:
                raise ValueError(f"Not a jobs.lever.co URL: {v}")
            companies.append(slug)
        else:
            companies.append(v)
    # de-dup preserve order
    return list(dict.fromkeys([c.strip("/").lower() for c in companies if c.strip("/")]))


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Lever job boards via Lever's public postings API."
    )
    parser.add_argument(
        "--company",
        "-c",
        action="append",
        default=[],
        help="Company slug on jobs.lever.co (can repeat). Example: --company lever",
    )
    parser.add_argument(
        "--url",
        "-u",
        action="append",
        default=[],
        help="Lever board URL (can repeat). Example: --url https://jobs.lever.co/lever",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=DEFAULT_MAX_JOBS,
        help=f"Max jobs per company to save (default: {DEFAULT_MAX_JOBS}).",
    )

    args = parser.parse_args()
    setup_logging()

    targets = parse_targets([*args.company, *args.url])
    if not targets:
        parser.error(
            "Provide at least one --company (slug) or --url (jobs.lever.co board URL). "
            "Example: --company leverdemo or --url https://jobs.lever.co/somecompany"
        )

    for slug in targets:
        try:
            scrape_company(slug, max_jobs=args.max_jobs)
        except Exception as e:
            logging.exception(f"Failed scraping '{slug}': {e}")

    logging.info("Done ✅")


if __name__ == "__main__":
    main()

