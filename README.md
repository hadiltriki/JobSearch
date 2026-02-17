# Job Scraping Workspace

This workspace contains small Python scrapers for multiple job boards and ATS/job-board platforms.  
Each scraper writes `jobs.json` + `jobs.csv` using a **mostly consistent schema**.

## Common output schema (per job)

Most scrapers output the following fields:

- **source**: string identifier of the source (`remoteok`, `weworkremotely`, `lever`, etc.)
- **url**: job posting URL
- **title**: job title
- **company**: company name (best-effort depending on source)
- **location**: job location (string; may be empty)
- **salary**: salary text (string; may be empty)
- **tags**: list of tags (JSON) / comma-separated string (CSV)
- **description**: job description text (may be empty for some sources)
- **raw_html_file**: reserved for “saved raw HTML per job” (often empty in API/RSS-based scrapers)
- **scraped_at_utc**: ISO timestamp in UTC

## Sources we scraped

### 1) RemoteOK
- **Script**: `scrape_remoteok.py`
- **How it works**: uses RemoteOK’s official JSON feed: `https://remoteok.com/api`
- **Outputs**:
  - `outputs_remoteok/jobs.json`
  - `outputs_remoteok/jobs.csv`
  - `outputs_remoteok/raw_api.json` (raw API response for debugging)
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_remoteok.py"
```

---

### 2) We Work Remotely (WWR)
- **Script**: `scrape_weworkremotely.py`
- **How it works**: uses the public RSS feed: `https://weworkremotely.com/remote-jobs.rss`
- **Outputs**:
  - `outputs_weworkremotely/jobs.json`
  - `outputs_weworkremotely/jobs.csv`
  - `outputs_weworkremotely/raw_rss.xml`
- **Notes**:
  - RSS does not always contain salary/location fields, so those may be empty.
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_weworkremotely.py"
```

---

### 3) Lever job boards
- **Script**: `scrape_lever.py`
- **How it works**: uses Lever public postings API:
  - `https://api.lever.co/v0/postings/<company>?mode=json`
- **Outputs**:
  - `outputs_lever/<company>/jobs.json`
  - `outputs_lever/<company>/jobs.csv`
  - `outputs_lever/<company>/raw_postings.json`
- **Run (slug)**:

```powershell
python "c:\Users\monta\job scrapping\scrape_lever.py" --company leverdemo --max-jobs 30
```

- **Run (URL)**:

```powershell
python "c:\Users\monta\job scrapping\scrape_lever.py" --url "https://jobs.lever.co/<company>" --max-jobs 30
```

---

### 4) Greenhouse boards
- **Script**: `scrape_greenhouse.py`
- **How it works**: uses Greenhouse Job Board API:
  - `https://boards-api.greenhouse.io/v1/boards/<token>/jobs?content=true`
- **Outputs**:
  - `outputs_greenhouse/<token>/jobs.json`
  - `outputs_greenhouse/<token>/jobs.csv`
  - `outputs_greenhouse/<token>/raw_jobs.json`
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_greenhouse.py" --board airbnb --max-jobs 30
```

---

### 5) Welcome to the Jungle (WTTJ)
- **Script**: `scrape_wttj.py`
- **How it works**:
  - fetches the company jobs list page:
    - `https://www.welcometothejungle.com/en/companies/<company>/jobs`
  - then for each job detail page, extracts `JobPosting` from **JSON-LD** (`application/ld+json`).
- **Outputs**:
  - `outputs_wttj/<company>/jobs.json`
  - `outputs_wttj/<company>/jobs.csv`
  - `outputs_wttj/<company>/raw_company_jobs.html`
  - `outputs_wttj/<company>/raw_jobpostings.json`
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_wttj.py" --company wttj --max-jobs 30
```

---

### 6) AIJobs.ai
- **Script**: `scrape_aijobs.py`
- **How it works**:
  - scrapes listing pages only:
    - `https://aijobs.ai/jobs`
    - `https://aijobs.ai/jobs?loadmore=1&page=2` (and so on)
- **Important limitation**:
  - `https://aijobs.ai/robots.txt` includes `Disallow: /job/`
  - So we **do not fetch job detail pages** (no descriptions).
- **Outputs**:
  - `outputs_aijobs/jobs.json`
  - `outputs_aijobs/jobs.csv`
  - `outputs_aijobs/raw_pages/page_*.html`
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_aijobs.py" --max-jobs 30 --delay 0.5
```

---

### 7) DataJobs.com
- **Script**: `scrape_datajobs.py`
- **How it works**:
  - crawls category pages like:
    - `https://datajobs.com/Data-Science-Jobs`
    - `https://datajobs.com/Data-Science-Jobs~2` (pagination)
  - fetches each job page and extracts title/company/location/salary/description from the page HTML.
- **Outputs**:
  - `outputs_datajobs/jobs.json`
  - `outputs_datajobs/jobs.csv`
  - `outputs_datajobs/raw_html/*.html`
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_datajobs.py" --category Data-Science-Jobs --max-jobs 30 --delay 0.2 --max-pages 10
```

---

### 8) CyberSecJobs / Cyberlist (Next.js SSR)
- **Script**: `scrape_cyberlist.py`
- **How it works**:
  - downloads a page and parses the embedded **Next.js SSR payload** (`__NEXT_DATA__`)
  - supports both:
    - `cyberlist.co`
    - `cybersecjobs.io`
- **Outputs**:
  - `outputs_cyberlist/<domain>/<path>/jobs.json`
  - `outputs_cyberlist/<domain>/<path>/jobs.csv`
  - `outputs_cyberlist/<domain>/<path>/page_*.html`
  - `outputs_cyberlist/<domain>/<path>/scrape.log`
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_cyberlist.py" --domain cybersecjobs.io --path /remote --max-jobs 30 --delay 0.2
```

---

### 9) DevITjobs (UK feed)
- **Script**: `scrape_devitjobs.py`
- **How it works**: uses the public XML feed:
  - `https://devitjobs.uk/job_feed.xml`
- **Outputs**:
  - `outputs_devitjobs/jobs.json`
  - `outputs_devitjobs/jobs.csv`
  - `outputs_devitjobs/raw_feed.xml` (optional, when `--save-raw` is used)
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_devitjobs.py" --max-jobs 30 --save-raw
```

---

### 10) Keejob (via sitemap + JSON-LD)
- **Script**: `scrape_keejob.py`
- **How it works**:
  - reads the public sitemap index `https://www.keejob.com/sitemap.xml`
  - pulls job URLs from `https://www.keejob.com/sitemap-jobs.xml`
  - fetches each job page and extracts `JobPosting` from JSON-LD (`application/ld+json`)
- **Outputs**:
  - `outputs_keejob/jobs.json`
  - `outputs_keejob/jobs.csv`
  - `outputs_keejob/raw_html/*.html` (optional, when `--save-raw` is used)
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_keejob.py" --max-jobs 30 --delay 0.2 --save-raw
```

---

### 11) Emploitic (via sitemap + JSON-LD)
- **Script**: `scrape_emploitic.py`
- **How it works**:
  - reads the public sitemap index `https://emploitic.com/sitemap.xml`
  - pulls job URLs from `https://emploitic.com/sitemap-jobs.xml`
  - fetches each job page and extracts `JobPosting` from JSON-LD (`application/ld+json`)
- **Outputs**:
  - `outputs_emploitic/jobs.json`
  - `outputs_emploitic/jobs.csv`
  - `outputs_emploitic/raw_html/*.html` (optional, when `--save-raw` is used)
- **Run**:

```powershell
python "c:\Users\monta\job scrapping\scrape_emploitic.py" --max-jobs 30 --delay 0.2 --save-raw
```

---

## Indeed / Glassdoor note (not scraped directly)

Direct scraping of Indeed and Glassdoor is high-risk (strong anti-bot + restrictive Terms).

What we added instead:

### Indeed (via SerpApi Google Jobs)
- **Script**: `scrape_indeed_via_serpapi.py`
- **How it works**: calls SerpApi Google Jobs API and optionally filters results to “via Indeed”
- **Outputs**:
  - `outputs_indeed_serpapi/<timestamp>/jobs.json`
  - `outputs_indeed_serpapi/<timestamp>/jobs.csv`
  - `outputs_indeed_serpapi/<timestamp>/raw_pages.json`
  - `outputs_indeed_serpapi/<timestamp>/last_response.json` (debug)
- **Setup**:
  - Set `SERPAPI_API_KEY` in your terminal session
- **Run**:

```powershell
$env:SERPAPI_API_KEY="YOUR_REAL_KEY"
python "c:\Users\monta\job scrapping\scrape_indeed_via_serpapi.py" --q "software engineer" --location "United States" --gl us --hl en --max-jobs 30
```

If you add `--only-indeed`, results can drop (because it filters non-Indeed providers).

---

## SerpApi fallback for blocked sites (403 / Cloudflare / strict robots)

If a site blocks direct scraping (example: Tanitjobs shows a Cloudflare 403 challenge), use SerpApi Google Jobs as a **fallback**.

### Generic SerpApi Google Jobs scraper
- **Script**: `scrape_google_jobs_via_serpapi.py`
- **What it does**: fetches job listings via SerpApi and optionally filters to results whose apply link is on a given domain.
- **Outputs**:
  - `outputs_serpapi_google_jobs/<timestamp>/jobs.json`
  - `outputs_serpapi_google_jobs/<timestamp>/jobs.csv`
  - `outputs_serpapi_google_jobs/<timestamp>/raw_pages.json`
- **Run (example: Tanitjobs)**:

```powershell
$env:SERPAPI_API_KEY="YOUR_REAL_KEY"
python "c:\Users\monta\job scrapping\scrape_google_jobs_via_serpapi.py" --q "site:tanitjobs.com développeur" --gl tn --hl fr --max-jobs 30
```

