# Explainable AI — Metrics in JobScan

This document describes **where and how** matching metrics are calculated (formulas) and how we add **explainability** via **LLM-as-judge only**.

---

## 1. Current metrics (formula-based)

### 1.1 Cosine similarity (title match)

- **Where:** `scraping_pipeline.py` — `cosine_sim(cv_vec, job_vec)`
- **Formula:** `cosine = dot(a, b) / (||a|| * ||b||)` where `a` = embedding of CV title, `b` = embedding of job title.
- **Model:** `paraphrase-multilingual-MiniLM-L12-v2` (SentenceTransformer).
- **Use:** Filter jobs with `cosine >= 0.60` before enrichment; also sent to frontend as `cosine` / `cosine_display` (percentage).
- **Explainable:** Yes — “similarity between your job title and the job’s title.”

### 1.2 AI match score (BiEncoder, fine-tuned MiniLM)

- **Where:** `matcher.py` — `predict(model, tokenizer, cv_structured, job_details)`
- **Model:** Fine-tuned `BiEncoderRegressorFineTuned` (all-MiniLM-L6-v2 backbone). Inputs: resume text + job text (fixed format). Output: single scalar in [0, 1].
- **Internal formula:**  
  `features = [r_emb, j_emb, |r_emb - j_emb|, r_emb * j_emb]` → MLP regressor → Sigmoid → score.  
  No simple human-readable formula; **black box**.
- **Use:** Stored as `match_score`; frontend shows “AI Match (biencoder)”.
- **Explainable:** Not by formula — hence we use **LLM-as-judge** to explain in natural language.

### 1.3 Skills gap

- **Where:** `matcher.py` — `compute_skills_gap(cv_structured, job_details)`
- **Formula:**  
  - Job skills: from `all_skills_list` or `skills_req` (CSV), normalized (canonical map + expansions).  
  - CV skills: from `cv_structured["skills"]`, normalized.  
  - For each job skill: fuzzy match (exact or substring after normalizing).  
  - `matched` = job skills found in CV, `missing` = not found.  
  - `coverage = len(matched) / total` (0–1).
- **Use:** `gap_missing`, `gap_matched`, `gap_coverage`, `gap_total` in job card.
- **Explainable:** Yes — list of matched vs missing skills.

### 1.4 Combined score

- **Where:** `matcher.py` — `compute_combined_score(ai_match, gap)`
- **Formula:**  
  - If `total == 0` (no job skills): `combined = ai_match` (capped [0,1]).  
  - Else: `combined = ai_match * sqrt(coverage)`, then capped to [0,1].
- **Use:** `combined_score` / `combined_score_display` in pipeline and frontend.
- **Explainable:** Yes — “AI match weighted by the square root of skills coverage.”

### 1.5 Job fit score (Career Assistant)

- **Where:** `job_analyzer_agent.py` — `score_job_fit()`
- **Formula:**  
  `total = skill_score * 0.55 + location_score * 0.20 + title_score * 0.25`  
  - `skill_score`: from DB `match_score` (BiEncoder) × 100 if present, else rule-based matched/total.  
  - Location/title: rule-based (preferences, remote, keyword overlap).
- **Use:** Ranking and explanations in analyzer/report; returns `explanation` with `formula`, `skill.reason`, `location.reason`, `title.reason`, `verdict_reason`.
- **Explainable:** Yes — formula and reasons are returned.

---

## 2. Explainability strategy

### 2.1 LLM-as-judge (implemented)

- **Goal:** Explain the **BiEncoder (AI match)** and overall fit in short, natural language, grounded in the actual numbers.
- **Input:** Job title, job required/missing/matched skills, CV role and skills summary, `cosine`, `match_score`, `combined_score`, `gap_coverage`, `gap_total`.
- **Output (JSON):**  
  - `interpretation`: one of `excellent` | `good` | `moderate` | `low`  
  - `score_formula`: one-line description of combined score (e.g. “AI match × √(skills coverage)”)  
  - `explanations`: 2–4 short sentences (why this score, what’s strong, what’s missing)
- **Where:** New module `xai_explainer.py`; called from `scraping_pipeline.enrich()` when `EXPLAINABLE_AI_ENABLED` is set; result stored in `job["xai"]`.
- **Frontend:** Already supports `job.xai` in “Explain scores (XAI)” (see `frontend/app/app/page.tsx`).

### 2.2 SHAP / LIME / block attribution (not used)

- We use **LLM-as-judge only**. SHAP, LIME, and block attribution are not used.

- **Goal:** Attribute the BiEncoder score to input segments (e.g. resume skills vs job requirements).
- **Challenge:** Model takes tokenized resume + job and concatenated embeddings; need to map attributions back to text spans.
- **Possible approach:** Run SHAP or integrated gradients on the encoder inputs; summarize top positive/negative tokens per segment; optionally feed that summary to the LLM for one extra “technical” explanation line.
- **Status:** Deferred; LLM-as-judge gives most of the user-facing benefit with less complexity.

---

## Why LLM-as-judge over SHAP/LIME for the candidate

| Goal | Best choice | Reason |
|------|-------------|--------|
| **Actionable feedback** ("What should I do?") | **LLM-as-judge** | Natural language: "Consider adding Kafka to your CV" or "Highlight your AWS experience in the cover letter." SHAP/LIME give "feature X contributed 35%" — not actionable. |
| **Trust & clarity** ("Why this score?") | **LLM-as-judge** | Short, empathetic sentences the candidate can read in seconds. SHAP/LIME need translation into language. |
| **Encouragement & next step** | **LLM-as-judge** | Can add a tip, a strength to highlight, or a learning suggestion. Model-only methods don't generate advice. |
| **Model transparency** (research / debug) | SHAP / block attribution | Useful for understanding the model; low added value for the candidate in the UI. |

**Recommendation:** **LLM-as-judge only** for the candidate. Richer prompts and outputs (tip, strength) provide actionable value. SHAP/LIME/block attribution are not used.

---

## 3. Files touched

| File | Role |
|------|------|
| `matcher.py` | BiEncoder predict, skills gap, combined score formulas |
| `scraping_pipeline.py` | Cosine sim, pipeline orchestration, calls xai explainer, builds job card with `xai` |
| `job_analyzer_agent.py` | Job fit formula (Skill 55% + Location 20% + Title 25%) and text explanations |
| `xai_explainer.py` | LLM-as-judge: builds prompt, calls Azure OpenAI, returns `xai` dict |
| `frontend/app/app/page.tsx` | Displays `job.xai` (interpretation, explanations, score formula, tip, strength) |

---

## 4. Env var

- `EXPLAINABLE_AI_ENABLED=1` — Enable LLM-as-judge in the scan pipeline (one short LLM call per job). If unset or 0, job cards still get all numeric metrics but no `xai` field (frontend falls back to generic text).
