import argparse
import csv
import html as htmllib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


"""
Scrape Emploitic.com via its public sitemap(s) and JobPosting JSON-LD.

Emploitic exposes:
- https://emploitic.com/sitemap.xml  (sitemap index)
- https://emploitic.com/sitemap-jobs.xml  (job URLs)

Example:
  python scrape_emploitic.py --max-jobs 30 --delay 0.5 --save-raw
"""


BASE_URL = "https://emploitic.com"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap.xml"

OUTPUT_DIR = Path("outputs_emploitic")
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


def _fetch_bytes(url: str, *, timeout_s: int = 60, retries: int = 3, accept: str = "*/*") -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": accept,
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

    raise RuntimeError(f"Failed to fetch {url}") from last_err


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


def _xml_tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_sitemap_index(xml_bytes: bytes) -> List[str]:
    root = ET.fromstring(xml_bytes.decode("utf-8", errors="replace"))
    locs: List[str] = []
    for child in root:
        if _xml_tag_name(child.tag) != "sitemap":
            continue
        for node in child:
            if _xml_tag_name(node.tag) == "loc" and (node.text or "").strip():
                locs.append(node.text.strip())
    return locs


def _iter_sitemap_urls(xml_bytes: bytes) -> Iterable[str]:
    stream = BytesIO(xml_bytes)
    ctx = ET.iterparse(stream, events=("end",))
    for _, elem in ctx:
        if _xml_tag_name(elem.tag) == "loc" and (elem.text or "").strip():
            yield elem.text.strip()
        elem.clear()


def _job_sitemap_url_from_index(index_locs: List[str]) -> Optional[str]:
    for loc in index_locs:
        if "sitemap-jobs.xml" in (loc or ""):
            return loc
    return index_locs[0] if index_locs else None


def _extract_jsonld_objects(html_text: str) -> List[Dict[str, Any]]:
    scripts = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html_text,
        flags=re.S | re.I,
    )
    out: List[Dict[str, Any]] = []
    for s in scripts:
        s = (s or "").strip()
        if not s:
            continue
        try:
            data = json.loads(s)
        except json.JSONDecodeError:
            s2 = s.strip().strip("\ufeff")
            try:
                data = json.loads(s2)
            except json.JSONDecodeError:
                continue
        if isinstance(data, dict):
            out.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    out.append(item)
    return out


def _find_jobposting(objs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    def is_jobposting(o: Dict[str, Any]) -> bool:
        t = o.get("@type")
        if isinstance(t, str):
            return t.lower() == "jobposting"
        if isinstance(t, list):
            return any(isinstance(x, str) and x.lower() == "jobposting" for x in t)
        return False

    for o in objs:
        if is_jobposting(o):
            return o
        g = o.get("@graph")
        if isinstance(g, list):
            for node in g:
                if isinstance(node, dict) and is_jobposting(node):
                    return node
    return None


def _location_from_jobposting(jp: Dict[str, Any]) -> str:
    loc = jp.get("jobLocation")
    locs: List[Dict[str, Any]] = []
    if isinstance(loc, dict):
        locs = [loc]
    elif isinstance(loc, list):
        locs = [x for x in loc if isinstance(x, dict)]
    if not locs:
        return ""

    addr = locs[0].get("address")
    if not isinstance(addr, dict):
        return ""

    parts: List[str] = []
    for k in ("addressLocality", "addressRegion", "addressCountry"):
        v = clean_text(str(addr.get(k) or ""))
        if v:
            parts.append(v)
    return ", ".join(parts)


def _salary_from_jobposting(jp: Dict[str, Any]) -> str:
    base = jp.get("baseSalary")
    if not isinstance(base, dict):
        return ""
    currency = clean_text(str(base.get("currency") or ""))
    value = base.get("value")
    if not isinstance(value, dict):
        return ""
    minv = value.get("minValue")
    maxv = value.get("maxValue")
    unit = clean_text(str(value.get("unitText") or ""))
    if minv is None and maxv is None:
        return ""
    if minv is not None and maxv is not None:
        return clean_text(f"{currency} {minv} - {maxv} {unit}".strip())
    if minv is not None:
        return clean_text(f"{currency} {minv} {unit}".strip())
    return clean_text(f"{currency} {maxv} {unit}".strip())


def _tags_from_jobposting(jp: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    emp = jp.get("employmentType")
    if isinstance(emp, str) and clean_text(emp):
        tags.append(clean_text(emp))
    elif isinstance(emp, list):
        for x in emp:
            if isinstance(x, str) and clean_text(x):
                tags.append(clean_text(x))

    for k in ("industry", "occupationalCategory"):
        v = jp.get(k)
        if isinstance(v, str) and clean_text(v):
            tags.append(clean_text(v))
        elif isinstance(v, list):
            for x in v:
                if isinstance(x, str) and clean_text(x):
                    tags.append(clean_text(x))

    return list(dict.fromkeys([t for t in tags if t]))


def _fallback_title(html_text: str) -> str:
    m = re.search(r"<h1[^>]*>(?P<t>.*?)</h1>", html_text, flags=re.S | re.I)
    if m:
        return _strip_tags(m.group("t"))
    m = re.search(r"<title[^>]*>(?P<t>.*?)</title>", html_text, flags=re.S | re.I)
    if m:
        return clean_text(_strip_tags(m.group("t")).replace("| Emploitic", "").strip())
    return ""


def _normalize_job(url: str, html_text: str, jobposting: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    title = ""
    company = ""
    location = ""
    salary = ""
    tags: List[str] = []
    description = ""

    if isinstance(jobposting, dict):
        title = clean_text(jobposting.get("title") or jobposting.get("name") or "")
        org = jobposting.get("hiringOrganization")
        if isinstance(org, dict):
            company = clean_text(org.get("name") or "")
        location = _location_from_jobposting(jobposting)
        salary = _salary_from_jobposting(jobposting)
        tags = _tags_from_jobposting(jobposting)
        description = _strip_tags(str(jobposting.get("description") or ""))

    if not title:
        title = _fallback_title(html_text)

    if not description:
        description = clean_text(_strip_tags(html_text))[:1500]

    return {
        "source": "emploitic",
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


def scrape(*, max_jobs: int, delay_s: float, save_raw: bool) -> List[Dict[str, Any]]:
    setup_logging()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if save_raw:
        RAW_DIR.mkdir(parents=True, exist_ok=True)

    logging.info(f"Fetching sitemap index: {SITEMAP_INDEX_URL}")
    index_xml = _fetch_bytes(SITEMAP_INDEX_URL, accept="application/xml,text/xml;q=0.9,*/*;q=0.8")
    index_locs = _parse_sitemap_index(index_xml)
    job_sitemap = _job_sitemap_url_from_index(index_locs)
    if not job_sitemap:
        raise RuntimeError("Could not find job sitemap in sitemap index")

    logging.info(f"Fetching job sitemap: {job_sitemap}")
    job_xml = _fetch_bytes(job_sitemap, accept="application/xml,text/xml;q=0.9,*/*;q=0.8")

    job_urls: List[str] = []
    for loc in _iter_sitemap_urls(job_xml):
        if loc.startswith(BASE_URL) and "/offres-d-emploi/" in loc:
            job_urls.append(loc)
        if len(job_urls) >= max_jobs:
            break

    logging.info(f"Sitemap yielded {len(job_urls)} job URLs (max_jobs={max_jobs}).")

    jobs: List[Dict[str, Any]] = []
    for i, url in enumerate(job_urls, start=1):
        time.sleep(max(0.0, delay_s))
        try:
            logging.info(f"[{i}/{len(job_urls)}] Fetching: {url}")
            html_text = _fetch_text(url, referer=BASE_URL + "/")
            if save_raw:
                path = urlparse(url).path.strip("/").replace("/", "_")
                (RAW_DIR / f"{path}.html").write_text(html_text, encoding="utf-8")

            jp = _find_jobposting(_extract_jsonld_objects(html_text))
            job = _normalize_job(url, html_text, jp)
            if not job["title"] and not job["url"]:
                continue
            jobs.append(job)
        except Exception as e:
            logging.exception(f"Failed scraping {url}: {e}")

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
    if save_raw:
        logging.info(f"Raw HTML  : {RAW_DIR}")
    logging.info(f"Done ✅ (saved {len(jobs)} jobs)")
    return jobs


def main():
    parser = argparse.ArgumentParser(description="Scrape Emploitic via sitemap + JobPosting JSON-LD.")
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between job page requests (seconds).")
    parser.add_argument("--save-raw", action="store_true", help="Save per-job raw HTML files for debugging.")
    args = parser.parse_args()

    scrape(max_jobs=args.max_jobs, delay_s=args.delay, save_raw=args.save_raw)


if __name__ == "__main__":
    main()

