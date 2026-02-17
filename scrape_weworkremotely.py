import json
import csv
import re
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import xml.etree.ElementTree as ET


BASE_URL = "https://weworkremotely.com"
RSS_URL = f"{BASE_URL}/remote-jobs.rss"

OUTPUT_DIR = Path("outputs_weworkremotely")
RAW_RSS_XML = OUTPUT_DIR / "raw_rss.xml"
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
            logging.StreamHandler(),
        ],
    )


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _fetch_bytes(url: str, *, timeout_s: int = 60, retries: int = 3) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL + "/",
    }

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout_s) as resp:
                return resp.read()
        except (HTTPError, URLError, TimeoutError) as e:
            last_err = e
            wait_s = min(10, 1.5**attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to fetch RSS from {url}") from last_err


def _split_company_title(title: str) -> Tuple[str, str]:
    """
    WWR RSS titles are often 'Company: Role'.
    Fall back to putting everything in title if not splittable.
    """
    t = clean_text(title)
    if not t:
        return "", ""
    if ":" in t:
        left, right = t.split(":", 1)
        company = clean_text(left)
        role = clean_text(right)
        if company and role:
            return company, role
    return "", t


def _strip_tags(html: str) -> str:
    # Lightweight HTML-to-text for nicer CSV/JSON; keep it simple.
    # (We still keep the original HTML in raw_html_file = "")
    if not html:
        return ""
    # Remove script/style blocks
    html = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", html)
    # Remove all tags
    html = re.sub(r"(?s)<[^>]*>", " ", html)
    return clean_text(html)


def _find_text(elem: ET.Element, tag: str) -> str:
    child = elem.find(tag)
    return child.text if child is not None and child.text is not None else ""


def scrape_weworkremotely():
    setup_logging()
    logging.info("Starting WeWorkRemotely scraping (RSS feed)...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    rss_bytes = _fetch_bytes(RSS_URL)
    RAW_RSS_XML.write_bytes(rss_bytes)

    # ElementTree needs a decoded string for fromstring in some cases with bad bytes.
    rss_text = rss_bytes.decode("utf-8", errors="replace")
    root = ET.fromstring(rss_text)

    channel = root.find("channel")
    if channel is None:
        raise RuntimeError("RSS parse error: missing <channel>")

    items = channel.findall("item")
    logging.info(f"Found {len(items)} RSS items.")

    jobs: List[Dict[str, Any]] = []
    for item in items[:MAX_JOBS]:
        title_raw = _find_text(item, "title")
        link = clean_text(_find_text(item, "link"))
        desc_html = _find_text(item, "description")

        company, title = _split_company_title(title_raw)
        tags = [clean_text(c.text or "") for c in item.findall("category")]
        tags = [t for t in tags if t]
        tags = list(dict.fromkeys(tags))

        # WWR RSS doesn't consistently expose location/salary; leave blank.
        job = {
            "source": "weworkremotely",
            "url": link,
            "title": title,
            "company": company,
            "location": "",
            "salary": "",
            "tags": tags,
            # keep a readable text version (WWR description can be HTML-heavy)
            "description": _strip_tags(desc_html),
            "raw_html_file": "",  # RSS mode: no per-job HTML saved
            "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        # skip empty rows
        if not job["url"] and not job["title"]:
            continue

        jobs.append(job)

    logging.info(f"Will save {len(jobs)} jobs (MAX_JOBS={MAX_JOBS}).")

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)

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
    logging.info(f"Raw RSS   : {RAW_RSS_XML}")
    logging.info("Done ✅")


if __name__ == "__main__":
    scrape_weworkremotely()

