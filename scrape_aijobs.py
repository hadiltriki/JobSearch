import argparse
import csv
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = "https://aijobs.ai"
LIST_URL = f"{BASE_URL}/jobs"

OUTPUT_DIR = Path("outputs_aijobs")
RAW_DIR = OUTPUT_DIR / "raw_pages"
OUTPUT_JSON = OUTPUT_DIR / "jobs.json"
OUTPUT_CSV = OUTPUT_DIR / "jobs.csv"
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


def _strip_tags(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", s)
    s = re.sub(r"(?s)<[^>]*>", " ", s)
    return clean_text(s)


def _fetch_text(url: str, *, timeout_s: int = 60, retries: int = 3, referer: str = BASE_URL + "/") -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
    }

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout_s) as resp:
                data = resp.read()
            return data.decode("utf-8", errors="replace")
        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            wait_s = min(10, 1.5**attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to fetch HTML from {url}") from last_err


JOB_CARD_RE = re.compile(
    r'<a href="(?P<url>https://aijobs\.ai/job/[^"]+)"[^>]*class="[^"]*jobcardStyle1[^"]*"[^>]*>(?P<body>.*?)</a>',
    flags=re.S,
)


def _parse_job_cards(html: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []

    for m in JOB_CARD_RE.finditer(html):
        url = clean_text(m.group("url"))
        body = m.group("body")

        # Title
        title = ""
        m_title = re.search(
            r'<div class="tw-text-\[#18191C\]\s+tw-text-lg\s+tw-font-medium">\s*(?P<t>.*?)\s*</div>',
            body,
            flags=re.S,
        )
        if m_title:
            title = _strip_tags(m_title.group("t"))

        # "Posted" age (e.g. 2W)
        posted_ago = ""
        m_post = re.search(
            r'<div class="tw-text-sm\s+tw-text-\[#767F8C\][^"]*">\s*(?P<p>[^<]{1,20})\s*</div>',
            body,
            flags=re.S,
        )
        if m_post:
            posted_ago = clean_text(m_post.group("p"))

        # Tags like "Full Time", "Remote"
        tags: List[str] = []
        for t in re.findall(r'<span[^>]*tw-text-\[12px\][^>]*>(?P<x>.*?)</span>', body, flags=re.S):
            tt = _strip_tags(t)
            if tt:
                tags.append(tt)
        if posted_ago:
            tags.append(f"posted:{posted_ago}")

        # Company
        company = ""
        m_co = re.search(r'tw-card-title">(?P<c>[^<]+)<', body)
        if m_co:
            company = clean_text(m_co.group("c"))

        # Location
        location = ""
        m_loc = re.search(r'tw-location">(?P<l>[^<]+)<', body)
        if m_loc:
            location = clean_text(m_loc.group("l"))

        jobs.append(
            {
                "source": "aijobs_ai",
                "url": url,
                "title": title,
                "company": company,
                "location": location,
                "salary": "",
                "tags": list(dict.fromkeys([x for x in tags if x])),
                # robots.txt disallows /job/ so we don't fetch descriptions
                "description": "",
                "raw_html_file": "",
                "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )

    return jobs


def scrape_aijobs(*, max_jobs: int, request_delay_s: float) -> List[Dict[str, Any]]:
    setup_logging()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("Starting AIJobs.ai scraping (listing pages only).")
    logging.info("Note: AIJobs.ai robots.txt disallows /job/ so descriptions are not fetched.")

    all_jobs: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    # Page 1 (full HTML)
    html1 = _fetch_text(LIST_URL, referer=BASE_URL + "/")
    (RAW_DIR / "page_1.html").write_text(html1, encoding="utf-8")
    jobs1 = _parse_job_cards(html1)
    logging.info(f"Page 1: parsed {len(jobs1)} job cards.")

    for j in jobs1:
        if j["url"] and j["url"] not in seen_urls:
            seen_urls.add(j["url"])
            all_jobs.append(j)
            if len(all_jobs) >= max_jobs:
                break

    # Load-more pages: /jobs?loadmore=1&page=N
    page = 2
    while len(all_jobs) < max_jobs:
        time.sleep(max(0.0, request_delay_s))
        url = f"{LIST_URL}?loadmore=1&page={page}"
        frag = _fetch_text(url, referer=LIST_URL)
        (RAW_DIR / f"page_{page}.html").write_text(frag, encoding="utf-8")

        jobs_n = _parse_job_cards(frag)
        logging.info(f"Page {page}: parsed {len(jobs_n)} job cards.")
        if not jobs_n:
            break

        added = 0
        for j in jobs_n:
            if j["url"] and j["url"] not in seen_urls:
                seen_urls.add(j["url"])
                all_jobs.append(j)
                added += 1
                if len(all_jobs) >= max_jobs:
                    break

        if added == 0:
            # avoid looping forever if response repeats
            break

        page += 1

    # Save JSON
    OUTPUT_JSON.write_text(json.dumps(all_jobs, indent=2, ensure_ascii=False), encoding="utf-8")

    # Save CSV
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
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
        for j in all_jobs:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved JSON: {OUTPUT_JSON}")
    logging.info(f"Saved CSV : {OUTPUT_CSV}")
    logging.info(f"Raw HTML  : {RAW_DIR}")
    logging.info(f"Done ✅ (saved {len(all_jobs)} jobs)")
    return all_jobs


def main():
    parser = argparse.ArgumentParser(description="Scrape AIJobs.ai job listings (listing pages only).")
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between requests (seconds). Default 1.0")
    args = parser.parse_args()
    scrape_aijobs(max_jobs=args.max_jobs, request_delay_s=args.delay)


if __name__ == "__main__":
    main()

