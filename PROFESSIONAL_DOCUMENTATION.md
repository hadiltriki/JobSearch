# Professional Documentation — Job Scraping Project

## 1) Executive Summary
This project implements a set of Python scrapers that collect job postings from multiple job boards and ATS/job-board platforms.  
The system prioritizes **stable, low-maintenance sources** (official APIs, RSS/XML feeds, and embedded structured data) and outputs data in a **consistent schema** across sources.

Key deliverables:
- Per-source scrapers (Python scripts)
- Structured outputs: `jobs.json` and `jobs.csv`
- Optional raw snapshots (API responses, RSS XML, HTML, etc.) for traceability and debugging

---

## 2) Standard Data Structure (Unified Job Schema)
Across sources, each job is normalized to the following fields:

- **source**: short identifier for the data source (e.g., `remoteok`, `weworkremotely`, `lever`)
- **url**: canonical job posting URL
- **title**: job title
- **company**: company name (best-effort depending on availability)
- **location**: location text (may be empty)
- **salary**: salary text (may be empty)
- **tags**:
  - **JSON**: list of strings
  - **CSV**: single comma-separated string
- **description**: job description text (may be empty for some sources)
- **raw_html_file**: reserved for per-job raw HTML file path (often empty when scraping via API/RSS)
- **scraped_at_utc**: UTC ISO timestamp of scrape time

---

## 3) Scraped Sources (What, How, Where It Saves)

### 3.1 RemoteOK
- **Script**: `scrape_remoteok.py`
- **Method**: Official JSON feed
  - `GET https://remoteok.com/api`
- **Outputs**:
  - `outputs_remoteok/jobs.json`
  - `outputs_remoteok/jobs.csv`
  - `outputs_remoteok/raw_api.json` (raw API response)
- **Notes**:
  - This replaced an earlier Playwright/HTML approach which broke due to changing selectors.

### 3.2 We Work Remotely (WWR)
- **Script**: `scrape_weworkremotely.py`
- **Method**: Public RSS feed
  - `GET https://weworkremotely.com/remote-jobs.rss`
- **Outputs**:
  - `outputs_weworkremotely/jobs.json`
  - `outputs_weworkremotely/jobs.csv`
  - `outputs_weworkremotely/raw_rss.xml`
- **Notes**:
  - RSS does not always include salary/location; those fields may be blank.

### 3.3 Lever job boards
- **Script**: `scrape_lever.py`
- **Method**: Lever public postings API
  - `GET https://api.lever.co/v0/postings/<company>?mode=json`
- **Outputs**:
  - `outputs_lever/<company>/jobs.json`
  - `outputs_lever/<company>/jobs.csv`
  - `outputs_lever/<company>/raw_postings.json`
- **Usage**:
  - Accepts `--company <slug>` or `--url https://jobs.lever.co/<slug>`

### 3.4 Greenhouse boards
- **Script**: `scrape_greenhouse.py`
- **Method**: Greenhouse public job board API
  - `GET https://boards-api.greenhouse.io/v1/boards/<token>/jobs?content=true`
- **Outputs**:
  - `outputs_greenhouse/<token>/jobs.json`
  - `outputs_greenhouse/<token>/jobs.csv`
  - `outputs_greenhouse/<token>/raw_jobs.json`

### 3.5 Welcome to the Jungle (WTTJ)
- **Script**: `scrape_wttj.py`
- **Method**:
  - Company listings:
    - `https://www.welcometothejungle.com/en/companies/<company>/jobs`
  - Job detail extraction:
    - Parse `JobPosting` from `application/ld+json` on each job detail page
- **Outputs**:
  - `outputs_wttj/<company>/jobs.json`
  - `outputs_wttj/<company>/jobs.csv`
  - `outputs_wttj/<company>/raw_company_jobs.html`
  - `outputs_wttj/<company>/raw_jobpostings.json`
- **Notes**:
  - Salary/location can be extracted from JSON-LD and/or listing-page hints.

### 3.6 AIJobs.ai
- **Script**: `scrape_aijobs.py`
- **Method**:
  - Listing pages only:
    - `https://aijobs.ai/jobs`
    - pagination fragments: `https://aijobs.ai/jobs?loadmore=1&page=N`
- **Outputs**:
  - `outputs_aijobs/jobs.json`
  - `outputs_aijobs/jobs.csv`
  - `outputs_aijobs/raw_pages/page_*.html`
- **Important limitation**:
  - `https://aijobs.ai/robots.txt` includes `Disallow: /job/`
  - Therefore job **detail pages are not fetched** and `description` remains empty.

### 3.7 DataJobs.com
- **Script**: `scrape_datajobs.py`
- **Method**:
  - Crawl category pages (example):
    - `https://datajobs.com/Data-Science-Jobs`
    - `https://datajobs.com/Data-Science-Jobs~2` (pagination)
  - Fetch each job page and extract:
    - title/company from `h1/h2`
    - description from “Job Description”
    - location from “Job Location”
    - salary from “Salary range” (when present)
- **Outputs**:
  - `outputs_datajobs/jobs.json`
  - `outputs_datajobs/jobs.csv`
  - `outputs_datajobs/raw_html/*.html`

### 3.8 CyberSecJobs / Cyberlist (Next.js SSR)
- **Script**: `scrape_cyberlist.py`
- **Method**:
  - Fetch the page and parse embedded Next.js SSR payload (`__NEXT_DATA__`)
  - Supports both:
    - `cyberlist.co`
    - `cybersecjobs.io`
- **Outputs**:
  - `outputs_cyberlist/<domain>/<path>/jobs.json`
  - `outputs_cyberlist/<domain>/<path>/jobs.csv`
  - `outputs_cyberlist/<domain>/<path>/page_*.html`

### 3.9 DevITjobs (UK feed)
- **Script**: `scrape_devitjobs.py`
- **Method**: Public XML job feed
  - `GET https://devitjobs.uk/job_feed.xml`
- **Outputs**:
  - `outputs_devitjobs/jobs.json`
  - `outputs_devitjobs/jobs.csv`
  - `outputs_devitjobs/raw_feed.xml` (optional via `--save-raw`)

---

## 4) SerpApi Solution (Indeed + LinkedIn Without Their Official APIs)
Some platforms (notably Indeed and Glassdoor) are high-risk to scrape directly due to strong anti-bot protections and restrictive Terms.  
To avoid direct scraping, we use **SerpApi’s Google Jobs API** as a discovery/collection layer.

### 4.0 SerpApi API Key Setup (Operational Procedure)
SerpApi requires an API key for authentication.

- **Where the key comes from**: SerpApi account dashboard (Manage API Key)
- **How we provide the key to scripts**:
  - Preferred: `SERPAPI_API_KEY` environment variable (per terminal session)
  - Alternative: command-line flag `--api-key` (supported by `scrape_indeed_via_serpapi.py`)

**PowerShell (recommended approach)**:

```powershell
$env:SERPAPI_API_KEY="PASTE_REAL_KEY_HERE"
```

**Run example**:

```powershell
python "c:\Users\monta\job scrapping\scrape_indeed_via_serpapi.py" --q "software engineer" --location "United States" --gl us --hl en --max-jobs 30
```

**Security handling**:
- API keys must **not** be shared in reports, chats, screenshots, or logs.
- If a key is exposed, it must be treated as compromised and **rotated/regenerated** immediately in the SerpApi dashboard.
- The SerpApi scraper was implemented to **redact API keys** from error messages where possible.

### 4.1 Indeed via SerpApi (Implemented)
- **Script**: `scrape_indeed_via_serpapi.py`
- **Input**:
  - `--q "<search query>"`
  - `--location "<location>"`
  - optional: `--only-indeed` (filters results to those whose provider/apply options indicate Indeed)
  - optional: `--gl us --hl en` for consistent region/language behavior
- **Authentication**:
  - `SERPAPI_API_KEY` environment variable or `--api-key`
- **How it works**:
  1. Calls SerpApi endpoint:
     - `GET https://serpapi.com/search.json?engine=google_jobs&...`
  2. Reads results from `jobs_results[]`
  3. Normalizes into the unified schema
  4. Saves raw debug JSON (`raw_pages.json` and `last_response.json`) for traceability

**Field mapping (SerpApi → unified schema)**:
- `jobs_results[].title` → `title`
- `jobs_results[].company_name` → `company`
- `jobs_results[].location` → `location`
- `jobs_results[].description` → `description`
- `jobs_results[].detected_extensions.salary` (if present) → `salary`
- `jobs_results[].extensions` + `detected_extensions` + `via` → `tags`
- `apply_options[].link` (preferred) → `url`

### 4.2 LinkedIn via SerpApi (Design / Same Method)
LinkedIn can be handled using the **same Google Jobs via SerpApi** approach, without using LinkedIn’s official APIs:

1. Use `engine=google_jobs` with a LinkedIn-oriented query and location.
2. Filter results where:
   - `via` contains “LinkedIn”, **or**
   - `apply_options.publisher` contains “LinkedIn”, **or**
   - `apply_options.link` contains `linkedin.com`
3. Normalize results using the same schema (`source` could be `linkedin_serpapi`).

---


## 6) Operational Notes and Traceability
- Most scrapers save **raw source data** (API responses, RSS/XML, or HTML pages) to support debugging and verification.
- CSV outputs are intended for easy import into spreadsheets; JSON outputs preserve structured fields (notably `tags` as a list).

