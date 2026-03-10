# Detailed change report — XAI, report, skills gap & roadmap

**Branch:** hadil_2  
**Commit:** XAI+report+skillgap+roadmap  
**For:** Quick onboarding so you can understand what was added and which files were touched.

---

## Summary

This update adds **Explainable AI (XAI)** for job scores, **personalized PDF/text reports**, **skills gap** from saved jobs, **in-app roadmap “courses”**, and fixes for the **Matches** tab (sorting + filters). Below is what changed in each file.

---

## 1. New files

### `job-scrapping-subul/xai_explainer.py` (+297 lines) — **NEW**
- **Role:** Explainable AI for job match scores.
- **What it does:** 
  - Takes cosine score, AI match (biencoder), combined score, and skills gap → returns human-readable explanations (why “Good”, “Moderate”, etc.), a short formula line, and optional tips.
  - Used when a job has no `xai` in DB (fallback) and for the “Explain scores (XAI)” panel on job cards.
  - Can call an LLM (Azure OpenAI) for richer explanations when configured.
- **Touched by:** Report explanations, job card XAI panel, backfill when loading jobs without xai.

### `job-scrapping-subul/EXPLAINABLE_METRICS.md` (+111 lines) — **NEW**
- **Role:** Documentation of how scores are explained.
- **What it does:** Describes the metrics (cosine, biencoder, combined, skills gap), how they’re combined, and how the XAI labels (e.g. “Good”, “Worth applying”) are derived. Useful for anyone reading the code or tuning thresholds.

### `job-scrapping-subul/test_roadmap_courses.py` (+98 lines) — **NEW**
- **Role:** Test script for roadmap “course” generation.
- **What it does:** Calls the roadmap XAI/LLM endpoint to check that tips and “small project ideas” per skill are returned correctly. Run manually when you change roadmap generation.

### `job-scrapping-subul/test_xai_judge.py` (+57 lines) — **NEW**
- **Role:** Test for XAI explainer.
- **What it does:** Unit-style tests for the XAI fallback (formula, interpretation, strength) with fixed inputs. Run to ensure explainer logic doesn’t regress.

---

## 2. Backend (Python) — modified files

### `job-scrapping-subul/main.py` (+439 / -176 net)
- **Report (user-specific):**
  - `_user_jobs_to_market_analysis(user_id)` — builds market analysis from **the user’s saved jobs** (not global market).
  - `_user_report_matches(user_id, profile)` — builds “Best Job Matches” from DB using **stored scores** (match_score like the Matches tab), with sanitized company/location/salary so the report doesn’t show garbage (e.g. salary as company).
  - `_sanitize_display_company()` / `_sanitize_display_location()` — avoid showing salary or long text as company/location in report and PDF.
- **API:**
  - `api_report` and `api_report_pdf` use the user’s job list and `_user_report_matches()` when `user_id` is present so report and Matches tab stay in sync.
- **PDF:**
  - “1. Your Profile” — Skills cell uses a `Paragraph` so the full skills list **wraps inside the table** (no truncation).
  - “5. Best Job Matches” — company/location/salary taken from sanitized fields; colors aligned with score bands (e.g. green ≥55%).
- **Verdicts:** Report verdicts aligned with frontend (Strong match, Good, Worth applying, Moderate, Low) and thresholds (75, 55, 40, 25).

### `job-scrapping-subul/job_analyzer_agent.py` (+114 / -1)
- **Skills gap & roadmap:**
  - `compute_gap()` now merges **requirement_counts** from the DB (from `must_have` of user’s jobs) so “missing to learn” in the Skills Gap tab reflects what’s actually demanded in saved jobs.
  - `generate_roadmap()` / roadmap API use **LLM-generated “courses”**: tip + project ideas per skill (no Coursera/Udemy links); `sanitize_learning_tip()` strips platform names from static tips.
- **Report text:**
  - Company line in markdown report uses fallback: `_get_company(j) or j.get('company') or j.get('industry') or 'N/A'` so we never show an empty company when it was sanitized to “—” or similar.

### `job-scrapping-subul/jobs_router.py` (+179 / -1)
- **Matches API:**
  - `_job_to_result()` — builds the unified job object for the frontend (title, company, location, match_score, cosine, gap_missing, etc.). Used by GET/POST matches and by the SSE stream payload when converting to frontend shape (if applicable).
  - `_apply_filters()` — filters by role (title/description), location, and minimum score; sorts by `total` (AI match) and returns top N. Used by POST `/api/matches` when filters are sent.
- **GET `/api/matches/{user_id}`** — returns jobs from DB (order comes from `get_jobs_for_user`).
- **POST `/api/matches`** — when `user_id` is present, loads from DB then applies role/location/min_fit and returns sorted results.
- **XAI backfill:** When loading jobs via `/jobs/{user_id}`, jobs missing `xai` can trigger a background task that calls `xai_explainer` and updates the DB so next load has explanations.

### `job-scrapping-subul/database.py` (+60 / -1)
- **`get_jobs_for_user(user_id)`:**
  - **Order:** Changed from `ORDER BY combined_score DESC` to **`ORDER BY match_score DESC NULLS LAST`** so the Matches tab list order matches the AI Match (biencoder) score.
  - Still returns the same columns; gap_coverage and xai fallback logic unchanged.
- (FIX 1 / FIX 2 docstrings kept for the integer cast and gap_coverage.)

### `job-scrapping-subul/scraping_pipeline.py` (+30)
- **Role:** Scan pipeline (SSE) that fetches jobs, runs matcher, computes gap, and saves to DB.
- **Changes:** Passes through or persists fields needed for XAI and report (e.g. match_score, combined_score, gap_missing, skills_req). May also pass xai-related data to the client in the stream if the frontend expects it. (Exact +30 lines depend on your version; typically ensuring job dicts have match_score, cosine, gap fields for the frontend and report.)

---

## 3. Frontend — modified files

### `job-scrapping-subul/frontend/app/app/page.tsx` (+576 / -many)
- **Matches tab:**
  - **Sorting:** Jobs are now sorted by **AI Match (match_score)**. Backend already returns them in that order; frontend also sorts the list by `normalizeScore(job.match_score)` so the order is consistent.
  - **Filters (working):**
    - **Filter by role:** Keeps jobs where title, company (`industry`), or description contains any word from the input.
    - **Filter by location:** Keeps jobs where location contains the text or job is remote and filter contains “remote”.
    - **Minimum score:** Dropdown (All / ≥40% / ≥55% / ≥75%) filters by `match_score`; only jobs with score ≥ selected threshold are shown.
  - Filtering/sorting done in a `useMemo` so the list updates as you type or change the dropdown (no extra API call). Count shown is the filtered count; empty filter result shows “No jobs match your filters”.
- **Report tab:**
  - Loads report from `/api/report` (with `user_id`), shows markdown, and supports **Download PDF** via `/api/report/pdf`.
- **Job cards:**
  - XAI block: “Explain scores (XAI)” shows formula, interpretation, and optional tip from `job.xai` (or fallback from backend). Score bars and badge (Good/Moderate/Low) use the same thresholds as backend.
- **Misc:** `useMemo` added for filtered/sorted list; `Suspense` kept in imports; types and `normalizeScore` unchanged.

### `job-scrapping-subul/frontend/package-lock.json` (+7)
- **Role:** Lockfile for npm dependencies.
- **Change:** Likely new or updated dependency for the frontend (e.g. a small UI or util). No breaking change expected; run `npm install` if she pulls the branch.

---

## 4. Quick reference — “where is what?”

| Feature | Main files |
|--------|------------|
| Why a job is “Good” or “Worth applying” | `xai_explainer.py`, job card XAI in `page.tsx` |
| Report “Best Job Matches” = same as Matches tab | `main.py` (`_user_report_matches`), `api_report` / `api_report_pdf` |
| Report based on user’s jobs only | `main.py` (`_user_jobs_to_market_analysis`), report APIs |
| Skills gap from saved jobs | `job_analyzer_agent.py` (`compute_gap` + requirement_counts), `jobs_router` / DB |
| Roadmap “courses” (tip + project ideas) | `job_analyzer_agent.py`, `xai_explainer.py` (LLM), roadmap API in `main.py` or `jobs_router` |
| Matches tab sort by AI match | `database.py` (`get_jobs_for_user` ORDER BY), `page.tsx` (sort + filter) |
| Matches tab filters | `page.tsx` (role/location/min score in `useMemo`) |
| PDF profile skills wrap | `main.py` (Paragraph for Skills cell) |
| Sanitized company/location in report | `main.py` (`_sanitize_display_*`), `job_analyzer_agent.py` (company fallback in report text) |

---

## 5. What to run after pulling

- **Backend:** `pip install -r requirements.txt` (if any new deps); `uvicorn main:app --port 8000` (or your usual command).
- **Frontend:** `npm install` (for package-lock changes), then `npm run dev` (or your usual command).
- **Optional:** Run `test_xai_judge.py` and `test_roadmap_courses.py` to confirm XAI and roadmap generation.

If you want, the next step can be a short “how to test” checklist (e.g. run a scan → open Matches → filter/sort → open Report → download PDF).
