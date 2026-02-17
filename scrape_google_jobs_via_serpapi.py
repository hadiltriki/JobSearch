import argparse
import csv
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen


"""
Generic "blocked site" fallback: fetch job listings via SerpApi Google Jobs API.

Use this when a job board is blocked (robots/403/Cloudflare). Instead of scraping the site,
we ask Google Jobs through SerpApi and optionally filter to results that link to a given domain.

Docs: https://serpapi.com/google-jobs-api

Examples (PowerShell):
  $env:SERPAPI_API_KEY="YOUR_REAL_KEY"

  # Tanitjobs (blocked directly) – best-effort via SerpApi
  python scrape_google_jobs_via_serpapi.py --q "site:tanitjobs.com développeur" --gl tn --hl fr --max-jobs 30

  # Filter results to those whose apply URL is on a specific domain
  python scrape_google_jobs_via_serpapi.py --q "python developer" --location "Tunisia" --gl tn --hl fr --domain tanitjobs.com --max-jobs 50

Notes:
- SerpApi results may not include full job descriptions; fields vary by job.
- We do NOT bypass Cloudflare or attempt to scrape blocked pages.
"""


SERPAPI_ENDPOINT = "https://serpapi.com/search.json"
OUTPUT_DIR = Path("outputs_serpapi_google_jobs")
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


def _redact_api_key(url: str) -> str:
    return re.sub(r"(api_key=)[^&]+", r"\1REDACTED", url)


def _looks_like_placeholder_api_key(api_key: str) -> bool:
    k = api_key.strip().lower()
    return (
        (not k)
        or k.startswith("your_")
        or k.startswith("paste_")
        or "paste_real_key_here" in k
        or "your_real_key" in k
        or k in {"changeme", "replace_me", "replace-this"}
    )


def _fetch_json(url: str, *, timeout_s: int = 60, retries: int = 3) -> Any:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }

    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout_s) as resp:
                data = resp.read()
            return json.loads(data.decode("utf-8", errors="replace"))
        except HTTPError as e:
            last_err = e
            if getattr(e, "code", None) == 401:
                raise RuntimeError(
                    "SerpApi request unauthorized (HTTP 401). "
                    "Your SERPAPI API key is missing/invalid.\n"
                    "Fix: set $env:SERPAPI_API_KEY='YOUR_REAL_KEY' or pass --api-key.\n"
                    f"Request (redacted): {_redact_api_key(url)}"
                ) from e
            wait_s = min(10, 1.5**attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)
        except (URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            wait_s = min(10, 1.5**attempt)
            logging.warning(f"Fetch failed (attempt {attempt}/{retries}): {e}. Sleeping {wait_s:.1f}s")
            time.sleep(wait_s)

    raise RuntimeError(f"Failed to fetch JSON from {_redact_api_key(url)}") from last_err


def _build_search_url(
    *,
    api_key: str,
    q: str,
    location: str,
    next_page_token: Optional[str],
    hl: Optional[str],
    gl: Optional[str],
) -> str:
    params: Dict[str, str] = {
        "engine": "google_jobs",
        "api_key": api_key,
        "q": q,
    }
    if location:
        params["location"] = location
    if next_page_token:
        params["next_page_token"] = next_page_token
    if hl:
        params["hl"] = hl
    if gl:
        params["gl"] = gl
    return SERPAPI_ENDPOINT + "?" + urlencode(params)


def _extract_serpapi_status(data: Dict[str, Any]) -> Tuple[str, str]:
    status = ""
    error_message = ""

    err = data.get("error")
    if isinstance(err, str) and clean_text(err):
        error_message = clean_text(err)

    md = data.get("search_metadata")
    if isinstance(md, dict):
        status = clean_text(str(md.get("status") or ""))
        if not error_message:
            em = md.get("error")
            if isinstance(em, str) and clean_text(em):
                error_message = clean_text(em)

    if not status:
        status = clean_text(str(data.get("status") or ""))
    if not error_message:
        error_message = clean_text(str(data.get("message") or data.get("error_message") or ""))

    return status, error_message


def _extract_salary(job: Dict[str, Any]) -> str:
    det = job.get("detected_extensions")
    if isinstance(det, dict):
        salary = clean_text(str(det.get("salary") or ""))
        if salary:
            return salary
    return clean_text(str(job.get("salary") or ""))


def _extract_tags(job: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    ext = job.get("extensions")
    if isinstance(ext, list):
        for x in ext:
            if isinstance(x, str):
                t = clean_text(x)
                if t:
                    tags.append(t)

    det = job.get("detected_extensions")
    if isinstance(det, dict):
        for k in ("schedule_type", "work_from_home", "posted_at"):
            v = det.get(k)
            if isinstance(v, str) and clean_text(v):
                tags.append(clean_text(v))

    via = clean_text(str(job.get("via") or ""))
    if via:
        tags.append(f"via:{via}")

    return list(dict.fromkeys(tags))


def _choose_apply_url(job: Dict[str, Any]) -> str:
    opts = job.get("apply_options")
    if isinstance(opts, list):
        for o in opts:
            if not isinstance(o, dict):
                continue
            link = clean_text(str(o.get("link") or ""))
            if link:
                return link
    for k in ("job_google_link", "link", "url"):
        v = job.get(k)
        if isinstance(v, str) and clean_text(v):
            return clean_text(v)
    return ""


def _url_domain(u: str) -> str:
    try:
        return (urlparse(u).netloc or "").lower()
    except Exception:
        return ""


def _domain_matches(domain: str, u: str) -> bool:
    if not domain:
        return True
    d = domain.strip().lower()
    if not d:
        return True
    netloc = _url_domain(u)
    if not netloc:
        return False
    return netloc == d or netloc.endswith("." + d)


def _job_matches_domain(domain: str, job: Dict[str, Any]) -> bool:
    if not domain:
        return True
    u = _choose_apply_url(job)
    if _domain_matches(domain, u):
        return True
    # also check apply_options publishers/links
    opts = job.get("apply_options")
    if isinstance(opts, list):
        for o in opts:
            if not isinstance(o, dict):
                continue
            link = clean_text(str(o.get("link") or ""))
            if link and _domain_matches(domain, link):
                return True
    return False


def _normalize_job(job: Dict[str, Any], *, q: str, domain: str) -> Dict[str, Any]:
    title = clean_text(str(job.get("title") or ""))
    company = clean_text(str(job.get("company_name") or job.get("company") or ""))
    location = clean_text(str(job.get("location") or ""))
    salary = _extract_salary(job)
    tags = _extract_tags(job)
    if domain:
        tags.append(f"domain:{domain.lower().strip()}")
    if q:
        tags.append("serpapi_google_jobs")
    tags = list(dict.fromkeys([t for t in tags if t]))
    description = clean_text(str(job.get("description") or ""))
    url = _choose_apply_url(job)

    return {
        "source": "serpapi_google_jobs",
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


def scrape(
    *,
    api_key: Optional[str],
    q: str,
    location: str,
    domain: str,
    max_jobs: int,
    hl: Optional[str],
    gl: Optional[str],
    delay_s: float,
) -> Tuple[List[Dict[str, Any]], Path]:
    api_key_from_cli = bool(api_key and api_key.strip())
    api_key = (api_key or os.environ.get("SERPAPI_API_KEY", "")).strip()
    if _looks_like_placeholder_api_key(api_key):
        raise RuntimeError(
            "Missing/placeholder SerpApi key.\n"
            "Set it first, e.g.:\n"
            "  powershell: $env:SERPAPI_API_KEY='YOUR_REAL_KEY'\n"
            "Or pass it directly:\n"
            "  --api-key YOUR_REAL_KEY\n"
        )

    logging.info(
        "Using SerpApi key from %s (length=%d)",
        ("--api-key" if api_key_from_cli else "SERPAPI_API_KEY env var"),
        len(api_key),
    )

    run_dir = OUTPUT_DIR / datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    jobs_out: List[Dict[str, Any]] = []
    raw_pages: List[Dict[str, Any]] = []

    next_token: Optional[str] = None
    while len(jobs_out) < max_jobs:
        url = _build_search_url(
            api_key=api_key,
            q=q,
            location=location,
            next_page_token=next_token,
            hl=hl,
            gl=gl,
        )
        data = _fetch_json(url)
        raw_pages.append(data if isinstance(data, dict) else {"data": data})

        if not isinstance(data, dict):
            (run_dir / "last_response.json").write_text(
                json.dumps({"data": data}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            raise RuntimeError(f"Unexpected SerpApi response type: {type(data)}")

        results = data.get("jobs_results")
        if not isinstance(results, list):
            status, err = _extract_serpapi_status(data)
            (run_dir / "last_response.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            if isinstance(err, str) and "hasn't returned any results" in err.lower():
                logging.warning("No Google Jobs results for this query/location. Saving empty outputs.")
                break
            raise RuntimeError(
                "Unexpected SerpApi response: missing jobs_results[].\n"
                f"- status: {status or 'unknown'}\n"
                f"- error: {err or 'n/a'}\n"
                f"- saved: {run_dir / 'last_response.json'}\n"
                f"- request: {_redact_api_key(url)}"
            )

        added = 0
        for job in results:
            if not isinstance(job, dict):
                continue
            if domain and not _job_matches_domain(domain, job):
                continue
            jobs_out.append(_normalize_job(job, q=q, domain=domain))
            added += 1
            if len(jobs_out) >= max_jobs:
                break
        logging.info(f"Got {len(results)} results, added {added} (kept={len(jobs_out)}/{max_jobs}).")

        next_token = None
        sp = data.get("serpapi_pagination")
        if isinstance(sp, dict):
            next_token = clean_text(str(sp.get("next_page_token") or "")) or None
        if not next_token:
            break
        time.sleep(max(0.0, delay_s))

    (run_dir / "raw_pages.json").write_text(
        json.dumps(raw_pages, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

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

    logging.info(f"Saved: {out_json}")
    logging.info(f"Saved: {out_csv}")
    logging.info(f"Raw  : {run_dir / 'raw_pages.json'}")
    return jobs_out, run_dir


def main():
    parser = argparse.ArgumentParser(description="Fetch jobs via SerpApi Google Jobs (blocked-site fallback).")
    parser.add_argument("--q", required=True, help="Search query, e.g. 'python developer' or 'site:tanitjobs.com python'")
    parser.add_argument("--location", default="", help="Location, e.g. 'Tunisia' or 'United States'")
    parser.add_argument("--domain", default="", help="Optional: keep only jobs whose apply URL is on this domain.")
    parser.add_argument("--api-key", default=None, help="SerpApi API key (optional). If omitted, uses SERPAPI_API_KEY env var.")
    parser.add_argument("--max-jobs", type=int, default=DEFAULT_MAX_JOBS)
    parser.add_argument("--hl", default=None, help="Language (Google param), e.g. en, fr")
    parser.add_argument("--gl", default=None, help="Country (Google param), e.g. us, fr, tn")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between page requests to SerpApi (seconds).")
    args = parser.parse_args()

    setup_logging()
    logging.info("Starting SerpApi Google Jobs scrape...")
    scrape(
        api_key=args.api_key,
        q=args.q,
        location=args.location,
        domain=args.domain,
        max_jobs=args.max_jobs,
        hl=args.hl,
        gl=args.gl,
        delay_s=args.delay,
    )
    logging.info("Done ✅")


if __name__ == "__main__":
    main()

