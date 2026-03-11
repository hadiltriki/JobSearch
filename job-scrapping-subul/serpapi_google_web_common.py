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

SERPAPI_ENDPOINT = "https://serpapi.com/search.json"


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _redact_api_key(url: str) -> str:
    return re.sub(r"(api_key=)[^&]+", r"\1REDACTED", url)


def looks_like_placeholder_api_key(api_key: str) -> bool:
    k = (api_key or "").strip().lower()
    return (
        (not k)
        or k.startswith("your_")
        or k.startswith("paste_")
        or "paste_real_key_here" in k
        or "your_real_key" in k
        or k in {"changeme", "replace_me", "replace-this"}
    )


def fetch_json(url: str, *, timeout_s: int = 60, retries: int = 3) -> Any:
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
                    "Fix: set SERPAPI_API_KEY in the environment or pass --api-key.\n"
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


def build_google_web_url(
    *,
    api_key: str,
    q: str,
    location: str,
    start: int,
    num: int,
    hl: Optional[str],
    gl: Optional[str],
) -> str:
    params: Dict[str, str] = {
        "engine": "google",
        "api_key": api_key,
        "q": q,
        "num": str(num),
    }
    if location:
        params["location"] = location
    if start:
        params["start"] = str(start)
    if hl:
        params["hl"] = hl
    if gl:
        params["gl"] = gl
    return SERPAPI_ENDPOINT + "?" + urlencode(params)


def extract_serpapi_status(data: Dict[str, Any]) -> Tuple[str, str]:
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


def _url_domain(u: str) -> str:
    try:
        return (urlparse(u).netloc or "").lower()
    except Exception:
        return ""


def domain_matches(domain: str, u: str) -> bool:
    if not domain:
        return True
    d = domain.strip().lower()
    if not d:
        return True
    netloc = _url_domain(u)
    if not netloc:
        return False
    return netloc == d or netloc.endswith("." + d)


def _derive_location_from_title(title: str) -> str:
    t = clean_text(title)
    if " | " in t:
        right = t.rsplit(" | ", 1)[-1].strip()
        if right and len(right) <= 64:
            return right
    return ""


def _derive_company_from_title(title: str) -> str:
    t = clean_text(title)
    if not t:
        return ""
    if " | " in t:
        t = t.rsplit(" | ", 1)[0].strip()
    for sep in (" - ", " – ", " — "):
        if sep in t:
            parts = [p.strip() for p in t.split(sep) if p.strip()]
            if len(parts) >= 2:
                cand = parts[-1]
                if not re.fullmatch(r"[\d\W_]+", cand or ""):
                    return cand[:120]
    return ""


def _derive_salary_from_snippet(snippet: str) -> str:
    s = clean_text(snippet)
    if not s:
        return ""

    m = re.search(r"\b(Salary|Compensation)\s*:\s*(.+?)(?:\.\s|;|\||$)", s, flags=re.I)
    if m:
        return clean_text(m.group(2))[:120]

    m = re.search(
        r"(\$\s?\d[\d,]*(?:\.\d{1,2})?\s*(?:-|to)\s*\$\s?\d[\d,]*(?:\.\d{1,2})?)",
        s,
        flags=re.I,
    )
    if m:
        return clean_text(m.group(1))[:120]

    m = re.search(
        r"(\$\s?\d[\d,]*(?:\.\d{1,2})?\s*(?:an?\s+hour|/hour|per\s+hour|/mth|per\s+year|/yr))",
        s,
        flags=re.I,
    )
    if m:
        return clean_text(m.group(1))[:120]

    return ""


def normalize_organic_result(
    item: Dict[str, Any], *, domain: str, source_tag: str, derive_fields: bool = False
) -> Optional[Dict[str, Any]]:
    title = clean_text(str(item.get("title") or ""))
    link = clean_text(str(item.get("link") or ""))
    snippet = clean_text(str(item.get("snippet") or ""))

    if not link:
        return None
    if domain and not domain_matches(domain, link):
        return None

    tags = [source_tag, "serpapi_google_web"]
    if domain:
        tags.append(f"domain:{domain.strip().lower()}")

    company = ""
    location = ""
    salary = ""
    if derive_fields:
        company = _derive_company_from_title(title)
        location = _derive_location_from_title(title)
        salary = _derive_salary_from_snippet(snippet)

    return {
        "source": source_tag,
        "url": link,
        "title": title,
        "company": company,
        "location": location,
        "salary": salary,
        "tags": list(dict.fromkeys([t for t in tags if t])),
        "description": snippet,
        "raw_html_file": "",
        "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def scrape_google_web(
    *,
    output_dir: Path,
    source_tag: str,
    q: str,
    location: str = "",
    domain: str = "",
    url_allow_regex: Optional[str] = None,
    url_deny_regex: Optional[str] = None,
    derive_fields: bool = False,
    max_results: int = 50,
    hl: Optional[str] = None,
    gl: Optional[str] = None,
    delay_s: float = 0.5,
    num_per_page: int = 10,
    api_key: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    api_key_from_cli = bool(api_key and api_key.strip())
    api_key = (api_key or os.environ.get("SERPAPI_API_KEY", "")).strip()
    if looks_like_placeholder_api_key(api_key):
        raise RuntimeError(
            "Missing/placeholder SerpApi key.\n"
            "Set SERPAPI_API_KEY in the environment or pass --api-key.\n"
        )

    logging.info(
        "Using SerpApi key from %s (length=%d)",
        ("--api-key" if api_key_from_cli else "SERPAPI_API_KEY env var"),
        len(api_key),
    )

    run_dir = output_dir / datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    out: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()
    raw_pages: List[Dict[str, Any]] = []

    allow_re = re.compile(url_allow_regex, flags=re.I) if url_allow_regex else None
    deny_re = re.compile(url_deny_regex, flags=re.I) if url_deny_regex else None

    start = 0
    while len(out) < max_results:
        url = build_google_web_url(
            api_key=api_key,
            q=q,
            location=location,
            start=start,
            num=num_per_page,
            hl=hl,
            gl=gl,
        )
        data = fetch_json(url)
        raw_pages.append(data if isinstance(data, dict) else {"data": data})

        if not isinstance(data, dict):
            (run_dir / "last_response.json").write_text(
                json.dumps({"data": data}, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            raise RuntimeError(f"Unexpected SerpApi response type: {type(data)}")

        organic = data.get("organic_results")

        # ✅ If Google returns no results on next pages, stop cleanly instead of crashing
        if not organic:
            status, err = extract_serpapi_status(data)
            (run_dir / "last_response.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            reason = err or data.get("search_information", {}).get("organic_results_state") or "No results"
            logging.warning(f"{source_tag}: No organic_results returned. Stopping pagination. Reason: {reason}")
            break

        # ✅ If organic_results exists but is not a list → still treat as unexpected
        if not isinstance(organic, list):
            status, err = extract_serpapi_status(data)
            (run_dir / "last_response.json").write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            raise RuntimeError(
                "Unexpected SerpApi response: organic_results is not a list.\n"
                f"- status: {status or 'unknown'}\n"
                f"- error: {err or 'n/a'}\n"
                f"- saved: {run_dir / 'last_response.json'}\n"
                f"- request: {_redact_api_key(url)}"
            )

        added = 0
        for item in organic:
            if not isinstance(item, dict):
                continue
            row = normalize_organic_result(item, domain=domain, source_tag=source_tag, derive_fields=derive_fields)
            if not row:
                continue

            u = clean_text(str(row.get("url") or ""))
            if not u:
                continue
            if allow_re and not allow_re.search(u):
                continue
            if deny_re and deny_re.search(u):
                continue
            if u in seen_urls:
                continue

            seen_urls.add(u)
            out.append(row)
            added += 1
            if len(out) >= max_results:
                break

        logging.info(f"{source_tag}: start={start} organic={len(organic)} added={added} kept={len(out)}/{max_results}")

        if added == 0:
            break

        start += num_per_page
        time.sleep(max(0.0, delay_s))

    (run_dir / "raw_pages.json").write_text(
        json.dumps(raw_pages, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    (run_dir / "jobs.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    with open(run_dir / "jobs.csv", "w", encoding="utf-8", newline="") as f:
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
        for j in out:
            row = dict(j)
            row["tags"] = ", ".join(j.get("tags", []))
            writer.writerow(row)

    return out, run_dir