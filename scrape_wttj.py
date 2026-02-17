import argparse
import csv
import html as htmllib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


BASE_URL = "https://www.welcometothejungle.com"

OUTPUT_DIR = Path("outputs_wttj")
LOG_FILE = OUTPUT_DIR / "scrape.log"

DEFAULT_MAX_JOBS = 30
DEFAULT_LANG = "en"


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
    html_text = htmllib.unescape(html_text)
    html_text = re.sub(r"(?is)<(script|style)\b.*?>.*?</\1>", " ", html_text)
    html_text = re.sub(r"(?s)<[^>]*>", " ", html_text)
    return clean_text(html_text)


def _fetch_text(url: str, *, timeout_s: int = 60, retries: int = 3) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": BASE_URL + "/",
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


def _company_from_url(company_url: str) -> Optional[str]:
    """
    Accept:
    - https://www.welcometothejungle.com/en/companies/<company>/jobs
    - https://www.welcometothejungle.com/en/companies/<company>
    """
    try:
        u = urlparse(company_url)
    except Exception:
        return None
    if not u.netloc:
        return None
    if u.netloc.lower() not in {"www.welcometothejungle.com", "welcometothejungle.com"}:
        return None
    parts = [p for p in u.path.split("/") if p]
    # expected: [lang, "companies", "<company>", ("jobs")? ...]
    if len(parts) >= 3 and parts[1] == "companies":
        return parts[2]
    return None


def _parse_company_jobs_page(html: str, *, company: str, lang: str) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    """
    Returns:
    - job_urls (absolute)
    - metadata_by_path: { "/en/companies/<company>/jobs/<job_slug>": {salary, remote, contract, location} }
    """
    # Find all job links.
    link_re = re.compile(r'href="(?P<path>/' + re.escape(lang) + r'/companies/' + re.escape(company) + r'/jobs/[^"]+)"')
    paths = [m.group("path") for m in link_re.finditer(html)]
    paths = list(dict.fromkeys(paths))

    meta: Dict[str, Dict[str, str]] = {}
    for path in paths:
        # window-based extraction (best-effort)
        idx = html.find(path)
        if idx == -1:
            continue
        window = html[idx: idx + 2500]

        # salary label appears as 'Salary:' on EN pages (may differ by language)
        salary = ""
        m = re.search(r"Salary:\s*([^<]{1,80})", window)
        if m:
            salary = clean_text(m.group(1))

        # contract / location / remote are near icon names
        contract = ""
        m = re.search(r'name="contract"[^>]*></i>\s*([^<]{1,80})', window)
        if m:
            contract = clean_text(m.group(1))

        location = ""
        m = re.search(r'name="location"[^>]*></i>.*?itestC">([^<]{1,80})<', window, flags=re.S)
        if m:
            location = clean_text(m.group(1))

        remote = ""
        m = re.search(r'name="remote"[^>]*></i>.*?<span>\s*([^<]{1,80})\s*</span>', window, flags=re.S)
        if m:
            remote = clean_text(m.group(1))

        meta[path] = {"salary": salary, "remote": remote, "contract": contract, "location": location}

    job_urls = [urljoin(BASE_URL, p) for p in paths]
    return job_urls, meta


def _extract_jsonld_objects(html: str) -> List[Dict[str, Any]]:
    scripts = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html,
        flags=re.S | re.I,
    )
    objs: List[Dict[str, Any]] = []
    for s in scripts:
        s = s.strip()
        if not s:
            continue
        try:
            data = json.loads(s)
        except json.JSONDecodeError:
            # sometimes there are stray characters; attempt a minimal cleanup
            s2 = s.strip().strip("\ufeff")
            try:
                data = json.loads(s2)
            except json.JSONDecodeError:
                continue

        if isinstance(data, dict):
            objs.append(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    objs.append(item)
    return objs


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
        # handle @graph
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
    # JSON-LD optional; keep best-effort human string.
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


def _normalize_job(
    *,
    company: str,
    url: str,
    jobposting: Dict[str, Any],
    meta: Dict[str, str],
) -> Dict[str, Any]:
    title = clean_text(jobposting.get("title") or jobposting.get("name") or "")
    org = jobposting.get("hiringOrganization")
    company_name = company
    if isinstance(org, dict):
        company_name = clean_text(org.get("name") or "") or company

    description_html = str(jobposting.get("description") or "")
    description = _strip_tags(description_html)

    location = meta.get("location") or _location_from_jobposting(jobposting)
    salary = meta.get("salary") or _salary_from_jobposting(jobposting)

    tags: List[str] = []
    emp = jobposting.get("employmentType")
    if isinstance(emp, str) and clean_text(emp):
        tags.append(clean_text(emp))
    elif isinstance(emp, list):
        for x in emp:
            if isinstance(x, str) and clean_text(x):
                tags.append(clean_text(x))

    if meta.get("contract"):
        tags.append(meta["contract"])
    if meta.get("remote"):
        tags.append(meta["remote"])
    tags = list(dict.fromkeys([t for t in tags if t]))

    return {
        "source": "wttj",
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


def parse_targets(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    for v in values:
        v = v.strip()
        if not v:
            continue
        if v.startswith("http://") or v.startswith("https://"):
            c = _company_from_url(v)
            if not c:
                raise ValueError(f"Not a Welcome to the Jungle company URL: {v}")
            out.append(c)
        else:
            out.append(v)
    return list(dict.fromkeys([x.strip("/").lower() for x in out if x.strip("/")]))


def scrape_company(company: str, *, lang: str, max_jobs: int) -> List[Dict[str, Any]]:
    company = clean_text(company).strip("/").lower()
    if not company:
        raise ValueError("Empty company slug")

    company_dir = OUTPUT_DIR / company
    company_dir.mkdir(parents=True, exist_ok=True)

    jobs_url = f"{BASE_URL}/{lang}/companies/{company}/jobs"
    logging.info(f"Fetching company jobs page: {jobs_url}")
    jobs_page_html = _fetch_text(jobs_url)
    (company_dir / "raw_company_jobs.html").write_text(jobs_page_html, encoding="utf-8")

    job_urls, meta_by_path = _parse_company_jobs_page(jobs_page_html, company=company, lang=lang)
    logging.info(f"Found {len(job_urls)} job URLs on listing page.")
    job_urls = job_urls[:max_jobs]
    logging.info(f"Will scrape {len(job_urls)} jobs (MAX_JOBS={max_jobs}).")

    jobs: List[Dict[str, Any]] = []
    raw_items: List[Dict[str, Any]] = []

    for i, job_url in enumerate(job_urls, start=1):
        try:
            logging.info(f"[{i}/{len(job_urls)}] Fetching: {job_url}")
            html_text = _fetch_text(job_url)

            path = urlparse(job_url).path
            meta = meta_by_path.get(path, {})

            objs = _extract_jsonld_objects(html_text)
            jp = _find_jobposting(objs)
            if not jp:
                logging.warning(f"No JobPosting JSON-LD found for {job_url}")
                continue

            raw_items.append({"url": job_url, "jobposting": jp})
            job = _normalize_job(company=company, url=job_url, jobposting=jp, meta=meta)
            jobs.append(job)

        except Exception as e:
            logging.exception(f"Failed scraping {job_url}: {e}")

    (company_dir / "raw_jobpostings.json").write_text(
        json.dumps(raw_items, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

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
    return jobs


def main():
    parser = argparse.ArgumentParser(
        description="Scrape Welcome to the Jungle company job pages (HTML + JobPosting JSON-LD)."
    )
    parser.add_argument(
        "--company",
        "-c",
        action="append",
        default=[],
        help="Company slug (can repeat). Example: --company wttj",
    )
    parser.add_argument(
        "--url",
        "-u",
        action="append",
        default=[],
        help="Company URL (can repeat). Example: --url https://www.welcometothejungle.com/en/companies/wttj/jobs",
    )
    parser.add_argument(
        "--lang",
        default=DEFAULT_LANG,
        help=f"Language prefix in URL (default: {DEFAULT_LANG}). Example: en, fr, cs",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=DEFAULT_MAX_JOBS,
        help=f"Max jobs per company to save (default: {DEFAULT_MAX_JOBS}).",
    )
    args = parser.parse_args()
    setup_logging()

    companies = parse_targets([*args.company, *args.url])
    if not companies:
        parser.error(
            "Provide at least one --company (slug) or --url (company URL). "
            "Example: --company wttj"
        )

    for c in companies:
        try:
            scrape_company(c, lang=args.lang, max_jobs=args.max_jobs)
        except Exception as e:
            logging.exception(f"Failed scraping '{c}': {e}")

    logging.info("Done ✅")


if __name__ == "__main__":
    main()

