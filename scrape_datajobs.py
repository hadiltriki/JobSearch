import argparse
import csv
import html as htmllib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen


BASE_URL = "https://datajobs.com"

OUTPUT_DIR = Path("outputs_datajobs")
RAW_DIR = OUTPUT_DIR / "raw_html"
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
    s = htmllib.unescape(s)
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


def _category_page_url(category: str, page: int) -> str:
    category = category.strip().lstrip("/")
    if page <= 1:
        return f"{BASE_URL}/{category}"
    return f"{BASE_URL}/{category}~{page}"


def _extract_job_links(listing_html: str) -> List[str]:
    hrefs = re.findall(r'href="(?P<h>/[^"]+Job~\d+)"', listing_html)
    hrefs = list(dict.fromkeys([h for h in hrefs if "Job~" in h]))
    return [urljoin(BASE_URL, h) for h in hrefs]


def _extract_job_fields(job_html: str) -> Tuple[str, str, str, str, str, List[str]]:
    """
    Returns: title, company, location, salary, description, tags
    """
    title = ""
    company = ""

    m = re.search(r"<h1[^>]*>(?P<h1>.*?)</h1>", job_html, flags=re.S | re.I)
    if m:
        title = _strip_tags(m.group("h1"))

    m = re.search(r"<h2[^>]*>(?P<h2>.*?)</h2>", job_html, flags=re.S | re.I)
    if m:
        company = _strip_tags(m.group("h2"))

    # Job Description cell
    description = ""
    m = re.search(
        r"<strong>Job Description</strong>.*?jobpost-table-cell-2[^>]*>(?P<d>.*?)</div>\s*</div>\s*</div>",
        job_html,
        flags=re.S | re.I,
    )
    if m:
        description = _strip_tags(m.group("d"))

    # Job Location cell
    location = ""
    m = re.search(
        r"<strong>Job Location</strong>.*?jobpost-table-cell-2[^>]*>(?P<l>.*?)</div>",
        job_html,
        flags=re.S | re.I,
    )
    if m:
        location = _strip_tags(m.group("l"))

    # Additional Job Details (often includes employment type and sometimes salary range)
    salary = ""
    tags: List[str] = []
    m = re.search(
        r"<strong>Additional Job Details</strong>.*?jobpost-table-cell-2[^>]*>(?P<a>.*?)</div>",
        job_html,
        flags=re.S | re.I,
    )
    if m:
        details = _strip_tags(m.group("a"))
        # Example detail strings sometimes contain both employment type + salary range:
        # "Employment Type: Full Time Salary range: $130,000 - $160,000"
        m_emp = re.search(r"Employment Type:\s*(?P<v>.+?)(?:\s+Salary range:|$)", details, flags=re.I)
        if m_emp:
            emp = clean_text(m_emp.group("v"))
            if emp:
                tags.append(emp)

        m_sal = re.search(r"Salary range:\s*(?P<s>.+)$", details, flags=re.I)
        if m_sal:
            salary = clean_text(m_sal.group("s"))

    if location:
        tags.append(location)

    tags = list(dict.fromkeys([t for t in tags if t]))
    return title, company, location, salary, description, tags


def scrape_datajobs(*, categories: List[str], max_jobs: int, delay_s: float, max_pages_per_category: int) -> List[Dict[str, Any]]:
    setup_logging()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Default categories if none provided
    if not categories:
        categories = ["Data-Science-Jobs"]

    jobs: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    logging.info(f"Starting DataJobs.com scraping. Categories={categories}")

    for cat in categories:
        page = 1
        while page <= max_pages_per_category and len(jobs) < max_jobs:
            list_url = _category_page_url(cat, page)
            logging.info(f"Listing: {list_url}")
            html_list = _fetch_text(list_url, referer=BASE_URL + "/")
            (RAW_DIR / f"{cat.replace('/', '_')}_page_{page}.html").write_text(html_list, encoding="utf-8")

            links = _extract_job_links(html_list)
            if not links:
                break

            new_links = [u for u in links if u not in seen]
            if not new_links:
                break

            for u in new_links:
                if len(jobs) >= max_jobs:
                    break
                seen.add(u)

                time.sleep(max(0.0, delay_s))
                try:
                    job_html = _fetch_text(u, referer=list_url)
                    # keep a small raw capture for debugging
                    slug = re.sub(r"[^a-zA-Z0-9]+", "_", u[-80:]).strip("_")
                    (RAW_DIR / f"job_{slug}.html").write_text(job_html, encoding="utf-8")

                    title, company, location, salary, description, tags = _extract_job_fields(job_html)
                    jobs.append(
                        {
                            "source": "datajobs",
                            "url": u,
                            "title": title,
                            "company": company,
                            "location": location,
                            "salary": salary,
                            "tags": tags,
                            "description": description,
                            "raw_html_file": "",
                            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                except Exception as e:
                    logging.exception(f"Failed scraping {u}: {e}")

            page += 1

    # Save outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(json.dumps(jobs, indent=2, ensure_ascii=False), encoding="utf-8")

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
        for j in jobs:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved JSON: {OUTPUT_JSON}")
    logging.info(f"Saved CSV : {OUTPUT_CSV}")
    logging.info(f"Raw HTML  : {RAW_DIR}")
    logging.info(f"Done ✅ (saved {len(jobs)} jobs)")
    return jobs


def main():
    parser = argparse.ArgumentParser(description="Scrape DataJobs.com job listings by category pages.")
    parser.add_argument(
        "--category",
        "-c",
        action="append",
        default=[],
        help="Category path, e.g. Data-Science-Jobs (can repeat).",
    )
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between job page requests (seconds).")
    parser.add_argument("--max-pages", type=int, default=10, help="Max pages per category to crawl.")
    args = parser.parse_args()

    scrape_datajobs(
        categories=args.category,
        max_jobs=args.max_jobs,
        delay_s=args.delay,
        max_pages_per_category=args.max_pages,
    )


if __name__ == "__main__":
    main()

