import argparse
import csv
import html as htmllib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


"""
Scrape CyberList / CyberSecJobs (Next.js SSR) by parsing __NEXT_DATA__.

Works with:
- https://cyberlist.co/
- https://cybersecjobs.io/

Examples:
  python scrape_cyberlist.py --domain cyberlist.co --path / --max-jobs 30
  python scrape_cyberlist.py --domain cybersecjobs.io --path /remote --max-jobs 30
"""


DEFAULT_DOMAIN = "cyberlist.co"
DEFAULT_PATH = "/"
DEFAULT_MAX_JOBS = 30

OUTPUT_DIR = Path("outputs_cyberlist")


def setup_logging(log_file: Path):
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
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


def _fetch_html(url: str, *, timeout_s: int = 60, retries: int = 3, referer: str = "") -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        headers["Referer"] = referer

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


def _extract_next_data(html: str) -> Dict[str, Any]:
    m = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.S,
    )
    if not m:
        raise RuntimeError("Could not find __NEXT_DATA__ in page HTML")
    return json.loads(m.group(1))


def _salary_from_base_salary(base_salary: Any) -> str:
    """
    baseSalary looks like Schema.org MonetaryAmount.
    Often empty on this board; best-effort string.
    """
    if not isinstance(base_salary, dict):
        return ""
    currency = clean_text(str(base_salary.get("currency") or ""))
    value = base_salary.get("value")
    if not isinstance(value, dict):
        return ""
    unit = clean_text(str(value.get("unitText") or ""))
    min_v = value.get("minValue")
    max_v = value.get("maxValue")
    amount = ""
    if min_v is not None and max_v is not None:
        amount = f"{min_v} - {max_v}"
    elif min_v is not None:
        amount = str(min_v)
    elif max_v is not None:
        amount = str(max_v)
    if not amount:
        return ""
    return clean_text(" ".join([currency, amount, unit]).strip())


def _normalize_job(domain: str, job_entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    attrs = job_entity.get("attributes") if isinstance(job_entity, dict) else None
    if not isinstance(attrs, dict):
        return None

    title = clean_text(str(attrs.get("title") or ""))
    slug = clean_text(str(attrs.get("slug") or ""))
    if not slug:
        return None

    company_name = ""
    company = attrs.get("company")
    try:
        company_name = clean_text(
            str(((company or {}).get("data") or {}).get("attributes", {}).get("name") or "")
        )
    except Exception:
        company_name = ""

    location = clean_text(str(attrs.get("location") or ""))
    description = _strip_tags(str(attrs.get("description") or ""))

    tags_raw = attrs.get("tags")
    tags: List[str] = []
    if isinstance(tags_raw, list):
        tags = [clean_text(str(t)) for t in tags_raw if clean_text(str(t))]
    tags = list(dict.fromkeys(tags))

    salary = _salary_from_base_salary(attrs.get("baseSalary"))

    url = f"https://{domain}/jobs/{slug}"
    source = domain.replace(".", "_")

    return {
        "source": source,
        "url": url,
        "title": title,
        "company": company_name,
        "location": location,
        "salary": salary,
        "tags": tags,
        "description": description,
        "raw_html_file": "",
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _build_page_url(domain: str, path: str, page: int) -> str:
    base = f"https://{domain}"
    if not path.startswith("/"):
        path = "/" + path
    if path == "/":
        url = base + "/"
    else:
        url = base + path
    if page > 1:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}page={page}"
    return url


def scrape(domain: str, path: str, *, max_jobs: int, delay_s: float) -> Tuple[List[Dict[str, Any]], Path]:
    safe_path = path.strip("/").replace("/", "_") or "home"
    run_dir = OUTPUT_DIR / domain / safe_path
    run_dir.mkdir(parents=True, exist_ok=True)
    log_file = run_dir / "scrape.log"
    setup_logging(log_file)

    jobs_out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    page = 1
    page_count: Optional[int] = None

    while len(jobs_out) < max_jobs and (page_count is None or page <= page_count):
        url = _build_page_url(domain, path, page)
        logging.info(f"Fetching: {url}")
        html = _fetch_html(url, referer=f"https://{domain}/")
        (run_dir / f"page_{page}.html").write_text(html, encoding="utf-8")

        data = _extract_next_data(html)
        page_props = ((data.get("props") or {}).get("pageProps") or {})
        jobs = (page_props.get("jobs") or {})
        job_entities = jobs.get("data") or []

        pag = ((jobs.get("meta") or {}).get("pagination") or {})
        if page_count is None:
            page_count = int(pag.get("pageCount") or 1)
            total = pag.get("total")
            logging.info(f"Pagination: total={total} pageCount={page_count}")

        added = 0
        if isinstance(job_entities, list):
            for je in job_entities:
                job = _normalize_job(domain, je)
                if not job:
                    continue
                if job["url"] in seen:
                    continue
                seen.add(job["url"])
                jobs_out.append(job)
                added += 1
                if len(jobs_out) >= max_jobs:
                    break

        logging.info(f"Page {page}: parsed {len(job_entities) if isinstance(job_entities, list) else 0}, added {added}")
        if added == 0:
            break

        page += 1
        time.sleep(max(0.0, delay_s))

    out_json = run_dir / "jobs.json"
    out_csv = run_dir / "jobs.csv"

    out_json.write_text(json.dumps(jobs_out, indent=2, ensure_ascii=False), encoding="utf-8")
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
        for j in jobs_out:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    logging.info(f"Saved JSON: {out_json}")
    logging.info(f"Saved CSV : {out_csv}")
    logging.info(f"Done ✅ (saved {len(jobs_out)} jobs)")
    return jobs_out, run_dir


def main():
    parser = argparse.ArgumentParser(description="Scrape cyberlist.co / cybersecjobs.io jobs via __NEXT_DATA__.")
    parser.add_argument("--domain", default=DEFAULT_DOMAIN, help="Domain, e.g. cyberlist.co or cybersecjobs.io")
    parser.add_argument("--path", default=DEFAULT_PATH, help="Path like /, /remote, /ciso, /pentester")
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between page fetches (seconds).")
    args = parser.parse_args()

    scrape(args.domain.strip().lower(), args.path.strip() or "/", max_jobs=args.max_jobs, delay_s=args.delay)


if __name__ == "__main__":
    main()

