import argparse
import csv
import html as htmllib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


FEED_URL = "https://devitjobs.uk/job_feed.xml"

OUTPUT_DIR = Path("outputs_devitjobs")
RAW_FEED = OUTPUT_DIR / "raw_feed.xml"
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


def strip_tags(html_text: str) -> str:
    if not html_text:
        return ""
    html_text = htmllib.unescape(html_text)
    html_text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", html_text)
    html_text = re.sub(r"(?s)<[^>]*>", " ", html_text)
    return clean_text(html_text)


def _get_text(job: ET.Element, tag: str) -> str:
    el = job.find(tag)
    return clean_text(el.text or "") if el is not None else ""


def _fetch_feed_stream():
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/xml,text/xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://devitjobs.uk/",
    }
    req = Request(FEED_URL, headers=headers, method="GET")
    return urlopen(req, timeout=60)


def scrape_devitjobs(*, max_jobs: int, save_raw: bool, delay_s: float) -> List[Dict[str, Any]]:
    setup_logging()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("Starting DevITjobs scraping (devitjobs.uk job_feed.xml).")

    jobs: List[Dict[str, Any]] = []

    # Stream parse to keep memory low.
    # Optionally also save the raw feed to disk for debugging.
    try:
        with _fetch_feed_stream() as resp:
            if save_raw:
                # Save the raw feed first (read bytes), then parse from disk.
                raw_bytes = resp.read()
                RAW_FEED.write_bytes(raw_bytes)
                logging.info(f"Saved raw feed: {RAW_FEED} ({len(raw_bytes)} bytes)")
                source = RAW_FEED.open("rb")
                close_source = True
            else:
                source = resp  # file-like
                close_source = False

            try:
                context = ET.iterparse(source, events=("end",))
                for event, elem in context:
                    if elem.tag != "job":
                        continue

                    title = _get_text(elem, "title") or _get_text(elem, "name")
                    company = _get_text(elem, "company") or _get_text(elem, "company-name")
                    location = _get_text(elem, "location")
                    salary = _get_text(elem, "salary")
                    jobtype = _get_text(elem, "jobtype") or _get_text(elem, "job-type")
                    country = _get_text(elem, "country")
                    region = _get_text(elem, "region")
                    pubdate = _get_text(elem, "pubdate")

                    url = (
                        _get_text(elem, "url")
                        or _get_text(elem, "link")
                        or _get_text(elem, "apply_url")
                    )

                    description_html = _get_text(elem, "description")
                    description = strip_tags(description_html)

                    tags: List[str] = []
                    for t in (jobtype, country, region):
                        if t:
                            tags.append(t)
                    if pubdate:
                        tags.append(f"published:{pubdate}")
                    tags = list(dict.fromkeys(tags))

                    jobs.append(
                        {
                            "source": "devitjobs_uk",
                            "url": url,
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

                    elem.clear()

                    if len(jobs) >= max_jobs:
                        break

                logging.info(f"Parsed {len(jobs)} jobs (MAX_JOBS={max_jobs}).")
            finally:
                if close_source:
                    source.close()
    except (HTTPError, URLError, TimeoutError) as e:
        raise RuntimeError(f"Failed to fetch DevITjobs feed: {e}") from e

    time.sleep(max(0.0, delay_s))

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
    logging.info("Done ✅")
    return jobs


def main():
    parser = argparse.ArgumentParser(description="Scrape DevITjobs feed (devitjobs.uk).")
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--save-raw", action="store_true", help="Save raw XML feed to outputs_devitjobs/raw_feed.xml")
    parser.add_argument("--delay", type=float, default=0.0, help="Optional delay before saving outputs (seconds).")
    args = parser.parse_args()

    scrape_devitjobs(max_jobs=args.max_jobs, save_raw=args.save_raw, delay_s=args.delay)


if __name__ == "__main__":
    main()

