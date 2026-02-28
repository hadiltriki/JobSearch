#!/usr/bin/env python3
"""
Career Assistant Agent
======================
Candidate-oriented agent that helps job seekers analyse the market,
find matching jobs, identify skill gaps, and build personalised
learning roadmaps.

Usage:
    python job_analyzer_agent.py --setup                 # build your profile
    python job_analyzer_agent.py --match                 # find matching jobs
    python job_analyzer_agent.py --roadmap               # learning roadmap
    python job_analyzer_agent.py --report                # full career report
    python job_analyzer_agent.py --interactive           # guided career session
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

LOG = logging.getLogger("career_agent")

# ═══════════════════════════════════════════════════════════════════════
#  Skill Taxonomy
# ═══════════════════════════════════════════════════════════════════════

SKILL_TAXONOMY: Dict[str, List[str]] = {
    "Programming Languages": [
        "Python", "JavaScript", "TypeScript", "Java", "C#", "C++",
        "Golang", "Rust", "Ruby", "PHP", "Swift", "Kotlin",
        "Scala", "Perl", "MATLAB", "Dart", "Haskell",
        "SQL", "HTML", "CSS", "Bash", "PowerShell",
    ],
    "Web Frameworks & Libraries": [
        "React", "Angular", "Vue.js", "Next.js", "Nuxt",
        "Django", "Flask", "FastAPI", "Spring Boot",
        "Node.js", "Express.js", "ASP.NET", ".NET Core",
        "Rails", "Laravel", "Svelte",
        "Tailwind CSS", "Bootstrap", "jQuery", "Streamlit",
    ],
    "Databases & Data Stores": [
        "PostgreSQL", "MySQL", "MongoDB", "Redis",
        "Elasticsearch", "DynamoDB", "Cassandra",
        "Oracle", "SQL Server", "SQLite",
        "Snowflake", "BigQuery", "Redshift", "Neo4j",
    ],
    "Cloud & DevOps": [
        "AWS", "Azure", "GCP", "Google Cloud",
        "Docker", "Kubernetes", "Terraform", "Ansible",
        "Jenkins", "CI/CD", "GitHub Actions", "GitLab CI",
        "Linux", "Nginx", "OpenShift", "Lambda",
        "Cloudflare", "Vercel", "Heroku",
    ],
    "Data & ML": [
        "Machine Learning", "Deep Learning",
        "NLP", "Natural Language Processing", "Computer Vision",
        "TensorFlow", "PyTorch", "Keras",
        "Scikit-learn", "XGBoost", "LightGBM",
        "Pandas", "NumPy", "SciPy",
        "Spark", "PySpark", "Hadoop",
        "Airflow", "dbt", "ETL",
        "Data Science", "Data Engineering", "Data Pipeline",
        "LLM", "Transformer",
        "MLflow", "Kubeflow",
        "Tableau", "Power BI", "Grafana", "Splunk",
    ],
    "Security": [
        "Cybersecurity", "Information Security",
        "SIEM", "Penetration Testing",
        "Encryption", "OAuth", "SSO",
        "Zero Trust", "NIST", "ISO 27001",
    ],
    "Tools & Platforms": [
        "Git", "Jira", "Confluence",
        "Slack", "Notion", "Figma",
        "Postman", "Swagger", "Datadog", "New Relic",
    ],
    "Methodologies": [
        "Agile", "Scrum", "Kanban", "TDD",
        "Microservices", "RESTful API", "GraphQL", "gRPC", "DevOps",
    ],
}

_SKILL_PATTERNS: List[Tuple[str, re.Pattern, str]] = []

def _build_pattern(skill: str) -> re.Pattern:
    parts = skill.split()
    if len(parts) == 1:
        escaped = re.escape(parts[0])
    else:
        escaped = r"\s+".join(re.escape(p) for p in parts)
    return re.compile(rf"(?<!\w){escaped}(?!\w)", re.IGNORECASE)

for _cat, _skills in SKILL_TAXONOMY.items():
    for _skill in _skills:
        try:
            _SKILL_PATTERNS.append((_skill, _build_pattern(_skill), _cat))
        except re.error:
            pass

# ═══════════════════════════════════════════════════════════════════════
#  Learning-Time Estimates
# ═══════════════════════════════════════════════════════════════════════

LEARNING_META: Dict[str, Dict[str, Any]] = {
    "Python":             {"d": "Beginner",     "w": 8,  "pre": [],                     "tip": "Official tutorial, then build projects on Kaggle / GitHub"},
    "JavaScript":         {"d": "Beginner",     "w": 8,  "pre": ["HTML", "CSS"],         "tip": "MDN Web Docs + freeCodeCamp"},
    "TypeScript":         {"d": "Intermediate", "w": 4,  "pre": ["JavaScript"],          "tip": "Official handbook, migrate a JS project"},
    "Java":               {"d": "Beginner",     "w": 12, "pre": [],                      "tip": "Oracle MOOC or Codecademy Java course"},
    "SQL":                {"d": "Beginner",     "w": 4,  "pre": [],                      "tip": "SQLBolt + LeetCode SQL challenges"},
    "React":              {"d": "Intermediate", "w": 6,  "pre": ["JavaScript"],          "tip": "Official React docs + build a CRUD app"},
    "Angular":            {"d": "Intermediate", "w": 8,  "pre": ["TypeScript"],          "tip": "Angular.io Tour of Heroes tutorial"},
    "Django":             {"d": "Intermediate", "w": 6,  "pre": ["Python"],              "tip": "Django Girls tutorial + DRF for APIs"},
    "Flask":              {"d": "Intermediate", "w": 3,  "pre": ["Python"],              "tip": "Miguel Grinberg's Flask Mega-Tutorial"},
    "FastAPI":            {"d": "Intermediate", "w": 3,  "pre": ["Python"],              "tip": "Official docs tutorial"},
    "Docker":             {"d": "Intermediate", "w": 3,  "pre": ["Linux"],               "tip": "Docker docs Get Started + Dockerfile practice"},
    "Kubernetes":         {"d": "Advanced",     "w": 10, "pre": ["Docker"],              "tip": "Kubernetes the Hard Way + CKA prep"},
    "AWS":                {"d": "Intermediate", "w": 12, "pre": [],                      "tip": "Cloud Practitioner cert, then Solutions Architect"},
    "Azure":              {"d": "Intermediate", "w": 12, "pre": [],                      "tip": "AZ-900 then AZ-104 learning paths"},
    "GCP":                {"d": "Intermediate", "w": 10, "pre": [],                      "tip": "Google Cloud Skills Boost"},
    "Terraform":          {"d": "Intermediate", "w": 4,  "pre": ["AWS"],                 "tip": "HashiCorp Learn tutorials"},
    "Machine Learning":   {"d": "Advanced",     "w": 16, "pre": ["Python", "SQL"],       "tip": "Andrew Ng's ML course + Kaggle competitions"},
    "Deep Learning":      {"d": "Advanced",     "w": 16, "pre": ["Machine Learning"],    "tip": "fast.ai + PyTorch tutorials"},
    "TensorFlow":         {"d": "Advanced",     "w": 8,  "pre": ["Python"],              "tip": "TF official tutorials + Coursera specialisation"},
    "PyTorch":            {"d": "Advanced",     "w": 8,  "pre": ["Python"],              "tip": "PyTorch.org tutorials + fast.ai"},
    "Pandas":             {"d": "Beginner",     "w": 3,  "pre": ["Python"],              "tip": "10 minutes to Pandas + Kaggle datasets"},
    "Spark":              {"d": "Advanced",     "w": 8,  "pre": ["Python", "SQL"],       "tip": "Databricks Community Edition + official docs"},
    "PostgreSQL":         {"d": "Intermediate", "w": 4,  "pre": ["SQL"],                 "tip": "PostgreSQL Tutorial + pgexercises.com"},
    "MongoDB":            {"d": "Intermediate", "w": 3,  "pre": [],                      "tip": "MongoDB University free courses"},
    "Redis":              {"d": "Intermediate", "w": 2,  "pre": [],                      "tip": "Redis University + try.redis.io"},
    "Git":                {"d": "Beginner",     "w": 2,  "pre": [],                      "tip": "Learn Git Branching (interactive)"},
    "Linux":              {"d": "Beginner",     "w": 6,  "pre": [],                      "tip": "Linux Journey + WSL/Ubuntu VM practice"},
    "CI/CD":              {"d": "Intermediate", "w": 4,  "pre": ["Git"],                 "tip": "GitHub Actions docs + build a pipeline"},
    "Agile":              {"d": "Beginner",     "w": 2,  "pre": [],                      "tip": "Scrum Guide + PSM I cert prep"},
    "GraphQL":            {"d": "Intermediate", "w": 3,  "pre": ["JavaScript"],          "tip": "graphql.org/learn + Apollo tutorials"},
    "Elasticsearch":      {"d": "Intermediate", "w": 4,  "pre": [],                      "tip": "Elastic official training"},
    "Airflow":            {"d": "Intermediate", "w": 4,  "pre": ["Python"],              "tip": "Apache Airflow docs + Astronomer guides"},
    "Tableau":            {"d": "Beginner",     "w": 4,  "pre": [],                      "tip": "Tableau Public free + Coursera specialisation"},
    "Power BI":           {"d": "Beginner",     "w": 4,  "pre": [],                      "tip": "Microsoft Learn PL-300 path"},
    "NLP":                {"d": "Advanced",     "w": 12, "pre": ["Python", "Machine Learning"], "tip": "Hugging Face course + spaCy tutorials"},
    "Cybersecurity":      {"d": "Intermediate", "w": 16, "pre": ["Linux"],               "tip": "CompTIA Security+ + TryHackMe"},
    "Data Science":       {"d": "Advanced",     "w": 20, "pre": ["Python", "SQL"],       "tip": "IBM/Coursera DS certificate + Kaggle"},
    "Data Engineering":   {"d": "Advanced",     "w": 16, "pre": ["Python", "SQL"],       "tip": "DataCamp DE track + build an ETL pipeline"},
    "Microservices":      {"d": "Advanced",     "w": 8,  "pre": ["Docker", "RESTful API"], "tip": "Sam Newman's book + build a sample system"},
    "Node.js":            {"d": "Intermediate", "w": 6,  "pre": ["JavaScript"],          "tip": "Official docs + Express.js tutorial"},
    "Scikit-learn":       {"d": "Intermediate", "w": 4,  "pre": ["Python"],              "tip": "Official user guide + Kaggle"},
    "DevOps":             {"d": "Intermediate", "w": 12, "pre": ["Linux", "Git"],        "tip": "DevOps Roadmap + hands-on Docker/K8s/CI"},
    "Golang":             {"d": "Intermediate", "w": 8,  "pre": [],                      "tip": "Go Tour + Effective Go + build a CLI tool"},
    "Rust":               {"d": "Advanced",     "w": 12, "pre": [],                      "tip": "The Rust Book + Rustlings exercises"},
    "Snowflake":          {"d": "Intermediate", "w": 4,  "pre": ["SQL"],                 "tip": "Snowflake University free courses"},
    "Kotlin":             {"d": "Intermediate", "w": 6,  "pre": ["Java"],                "tip": "Kotlin Koans + official docs"},
}

# ═══════════════════════════════════════════════════════════════════════
#  Salary Parser
# ═══════════════════════════════════════════════════════════════════════

def parse_salary(raw: str) -> Optional[Dict[str, Any]]:
    if not raw or not raw.strip():
        return None
    currency = "USD"
    upper = raw.upper()
    if "EUR" in upper or "\u20ac" in raw:
        currency = "EUR"
    elif "GBP" in upper or "\u00a3" in raw:
        currency = "GBP"
    elif "CAD" in upper:
        currency = "CAD"
    elif "AUD" in upper:
        currency = "AUD"

    period = "yearly"
    if re.search(r"per\s*hour|hourly|/hr|/hour", raw, re.IGNORECASE):
        period = "hourly"
    elif re.search(r"per\s*month|monthly|/month", raw, re.IGNORECASE):
        period = "monthly"

    cleaned = re.sub(r"(\d)\s*mil\b", lambda m: m.group(1) + "000", raw, flags=re.IGNORECASE)
    cleaned = re.sub(r"(\d)\s*k\b", lambda m: m.group(1) + "000", cleaned, flags=re.IGNORECASE)

    numbers = re.findall(r"[\d,]+(?:\.\d+)?", cleaned)
    values = []
    for n in numbers:
        stripped = n.replace(",", "").strip()
        if not stripped or stripped == ".":
            continue
        try:
            v = float(stripped)
            if v >= 15:
                values.append(v)
        except ValueError:
            continue

    if len(values) >= 2:
        return {"currency": currency, "min": values[0], "max": values[1], "period": period, "raw": raw}
    if len(values) == 1:
        return {"currency": currency, "min": values[0], "max": values[0], "period": period, "raw": raw}
    return None

def _annualise(sal: Dict) -> float:
    mid = (sal["min"] + sal["max"]) / 2
    if sal["period"] == "hourly":
        mid *= 2080
    elif sal["period"] == "monthly":
        mid *= 12
    return mid

# ═══════════════════════════════════════════════════════════════════════
#  Data Loader
# ═══════════════════════════════════════════════════════════════════════

def load_all_jobs(data_dir: Path) -> List[Dict]:
    jobs: List[Dict] = []
    for entry in sorted(data_dir.iterdir()):
        if not entry.is_dir() or not entry.name.startswith("outputs_"):
            continue
        for jf in entry.rglob("jobs.json"):
            try:
                data = json.loads(jf.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    jobs.extend(data)
                    LOG.info("Loaded %d jobs from %s", len(data), jf)
            except Exception as exc:
                LOG.warning("Skipping %s: %s", jf, exc)
    return jobs

# ═══════════════════════════════════════════════════════════════════════
#  Skill Extraction
# ═══════════════════════════════════════════════════════════════════════

def extract_skills_set(text: str) -> Set[str]:
    """Return flat set of skill names found in text."""
    found: Set[str] = set()
    if not text:
        return found
    for display, pat, _ in _SKILL_PATTERNS:
        if pat.search(text):
            found.add(display)
    return found


def extract_skills_by_cat(text: str) -> Dict[str, Set[str]]:
    found: Dict[str, Set[str]] = defaultdict(set)
    if not text:
        return dict(found)
    for display, pat, cat in _SKILL_PATTERNS:
        if pat.search(text):
            found[cat].add(display)
    return dict(found)

# ═══════════════════════════════════════════════════════════════════════
#  Market Analysis
# ═══════════════════════════════════════════════════════════════════════

class MarketAnalysis:
    def __init__(self, jobs: List[Dict]):
        self.total = len(jobs)
        self.jobs = jobs
        self.sources = Counter(j.get("source", "unknown") for j in jobs)

        self.skill_counts: Counter = Counter()
        self.skill_by_cat: Dict[str, Counter] = defaultdict(Counter)
        self._job_skills: Dict[int, Set[str]] = {}
        for idx, j in enumerate(jobs):
            blob = " ".join([j.get("title", ""), j.get("description", ""), " ".join(j.get("tags", []))])
            skills = extract_skills_set(blob)
            self._job_skills[idx] = skills
            for s in skills:
                self.skill_counts[s] += 1
            for cat, sset in extract_skills_by_cat(blob).items():
                for s in sset:
                    self.skill_by_cat[cat][s] += 1

        self.salaries: List[Dict] = []
        for j in jobs:
            p = parse_salary(j.get("salary", ""))
            if p:
                self.salaries.append(p)

        self.salary_by_currency: Dict[str, List[float]] = defaultdict(list)
        for s in self.salaries:
            self.salary_by_currency[s["currency"]].append(_annualise(s))

        self.locations = Counter(j.get("location", "").strip() for j in jobs if j.get("location", "").strip())
        self.companies = Counter(j.get("company", "").strip() for j in jobs if j.get("company", "").strip())

        remote_kw = re.compile(r"\bremote\b", re.IGNORECASE)
        self.remote_ratio = sum(
            1 for j in jobs
            if remote_kw.search(j.get("title", ""))
            or remote_kw.search(j.get("location", ""))
            or any(remote_kw.search(t) for t in j.get("tags", []))
        ) / max(self.total, 1)

    def job_skills(self, idx: int) -> Set[str]:
        return self._job_skills.get(idx, set())

    def summary_text(self) -> str:
        lines = [
            f"Total jobs analysed: {self.total}",
            f"Sources: {dict(self.sources.most_common())}",
            f"Remote-friendly ratio: {self.remote_ratio:.0%}",
            "", "Top 30 skills overall:",
        ]
        for skill, cnt in self.skill_counts.most_common(30):
            lines.append(f"  {skill}: {cnt} mentions ({cnt/self.total:.0%} of jobs)")
        lines.append("")
        lines.append("Salary statistics (annualised midpoints):")
        for cur, vals in sorted(self.salary_by_currency.items()):
            if len(vals) >= 2:
                lines.append(f"  {cur}: n={len(vals)}, min={min(vals):,.0f}, median={statistics.median(vals):,.0f}, max={max(vals):,.0f}")
            elif vals:
                lines.append(f"  {cur}: n=1, value={vals[0]:,.0f}")
        lines.append("")
        lines.append("Top 15 locations:")
        for loc, cnt in self.locations.most_common(15):
            lines.append(f"  {loc}: {cnt}")
        return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════
#  Candidate Profile
# ═══════════════════════════════════════════════════════════════════════

PROFILE_FILE = "candidate_profile.json"

class CandidateProfile:
    def __init__(
        self,
        name: str = "",
        target_role: str = "",
        experience_years: int = 0,
        skills: List[str] = None,
        preferred_locations: List[str] = None,
        open_to_remote: bool = True,
        salary_expectation: str = "",
    ):
        self.name = name
        self.target_role = target_role
        self.experience_years = experience_years
        self.skills = skills or []
        self.preferred_locations = preferred_locations or []
        self.open_to_remote = open_to_remote
        self.salary_expectation = salary_expectation

    def skills_set(self) -> Set[str]:
        return {s.strip() for s in self.skills if s.strip()}

    def save(self, path: Path):
        path.write_text(json.dumps(self.__dict__, indent=2, ensure_ascii=False), encoding="utf-8")
        LOG.info("Profile saved to %s", path)

    @classmethod
    def load(cls, path: Path) -> "CandidateProfile":
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(**data)

    def display(self) -> str:
        loc = ", ".join(self.preferred_locations) if self.preferred_locations else "Anywhere"
        remote = "Yes" if self.open_to_remote else "No"
        return (
            f"  Name:               {self.name}\n"
            f"  Target role:        {self.target_role}\n"
            f"  Experience:         {self.experience_years} years\n"
            f"  Skills:             {', '.join(self.skills)}\n"
            f"  Preferred location: {loc}\n"
            f"  Open to remote:     {remote}\n"
            f"  Salary expectation: {self.salary_expectation or 'Not specified'}"
        )


def guided_profile_setup(existing: Optional[CandidateProfile] = None) -> CandidateProfile:
    print("\n" + "=" * 60)
    print("  Let's build your candidate profile")
    print("=" * 60)

    def ask(prompt: str, default: str = "") -> str:
        d = f" [{default}]" if default else ""
        val = input(f"  {prompt}{d}: ").strip()
        return val if val else default

    old = existing or CandidateProfile()
    name = ask("Your name", old.name)
    target = ask("Target job role (e.g. Data Engineer, Full-Stack Dev)", old.target_role)
    exp = ask("Years of experience", str(old.experience_years))
    try:
        exp_int = int(exp)
    except ValueError:
        exp_int = 0

    print("\n  Enter your skills (comma-separated).")
    if old.skills:
        print(f"  Current: {', '.join(old.skills)}")
    skills_raw = ask("Skills", ", ".join(old.skills))
    skills = [s.strip() for s in skills_raw.split(",") if s.strip()]

    print("\n  Preferred work locations (comma-separated, or leave blank for anywhere).")
    locs_raw = ask("Locations", ", ".join(old.preferred_locations))
    locs = [l.strip() for l in locs_raw.split(",") if l.strip()]

    remote_raw = ask("Open to remote work? (yes/no)", "yes" if old.open_to_remote else "no")
    remote = remote_raw.lower() in ("yes", "y", "true", "1", "")

    salary = ask("Salary expectation (e.g. 60000 EUR, 90k USD)", old.salary_expectation)

    profile = CandidateProfile(
        name=name, target_role=target, experience_years=exp_int,
        skills=skills, preferred_locations=locs,
        open_to_remote=remote, salary_expectation=salary,
    )

    print("\n" + "-" * 60)
    print("  Your profile:")
    print(profile.display())
    print("-" * 60)
    return profile

# ═══════════════════════════════════════════════════════════════════════
#  Job Matching & Fit Scoring
# ═══════════════════════════════════════════════════════════════════════

def score_job_fit(
    job: Dict, idx: int, profile: CandidateProfile, analysis: MarketAnalysis,
) -> Dict[str, Any]:
    job_skills = analysis.job_skills(idx)
    candidate_lower = {s.lower() for s in profile.skills_set()}
    matched = {s for s in job_skills if s.lower() in candidate_lower}
    missing = job_skills - matched

    skill_score = len(matched) / max(len(job_skills), 1) * 100

    job_loc = job.get("location", "").lower()
    location_score = 50
    loc_reason = "No location preference set — neutral score"
    if profile.preferred_locations:
        if any(pl.lower() in job_loc or job_loc in pl.lower() for pl in profile.preferred_locations):
            location_score = 100
            loc_reason = f"Location '{job.get('location','')}' matches your preference"
        elif "remote" in job_loc and profile.open_to_remote:
            location_score = 90
            loc_reason = "Job is remote and you're open to remote work"
        elif not job_loc:
            location_score = 40
            loc_reason = "Job has no location listed — uncertain match"
        else:
            location_score = 10
            loc_reason = f"Location '{job.get('location','')}' doesn't match your preferences ({', '.join(profile.preferred_locations)})"
    elif "remote" in job_loc and profile.open_to_remote:
        location_score = 90
        loc_reason = "Job is remote and you're open to remote work"
    elif job_loc:
        location_score = 60
        loc_reason = f"Location '{job.get('location','')}' — no preference set, neutral"

    title_score = 0
    title_reason = "No target role specified"
    if profile.target_role:
        target_words = set(re.findall(r"\w+", profile.target_role.lower()))
        title_words = set(re.findall(r"\w+", job.get("title", "").lower()))
        if target_words:
            overlap = target_words & title_words
            title_score = len(overlap) / len(target_words) * 100
            if overlap:
                title_reason = f"Title keywords matched: {', '.join(sorted(overlap))} ({len(overlap)}/{len(target_words)} words)"
            else:
                title_reason = f"No keyword overlap between your target '{profile.target_role}' and job title '{job.get('title','')}'"

    total = skill_score * 0.55 + location_score * 0.20 + title_score * 0.25

    if total >= 70:
        verdict = "Strong match"
    elif total >= 45:
        verdict = "Worth applying"
    elif total >= 25:
        verdict = "Stretch role"
    else:
        verdict = "Low match"

    skill_reason = (
        f"You have {len(matched)}/{len(job_skills)} skills this job requires "
        f"({', '.join(sorted(matched)[:8]) or 'none'})"
        if job_skills else "No specific skills detected for this job"
    )

    explanation = {
        "formula": f"Total = Skill×55% + Location×20% + Title×25% = {skill_score:.0f}×0.55 + {location_score:.0f}×0.20 + {title_score:.0f}×0.25 = {total:.0f}%",
        "skill": {"score": round(skill_score), "weight": "55%", "reason": skill_reason},
        "location": {"score": round(location_score), "weight": "20%", "reason": loc_reason},
        "title": {"score": round(title_score), "weight": "25%", "reason": title_reason},
        "verdict_reason": (
            f"Score {round(total)}% → '{verdict}': "
            + ("≥70% means strong alignment with your profile."
               if total >= 70
               else "45–69% means worth applying — you meet many requirements."
               if total >= 45
               else "25–44% means stretch role — good for growth, expect a learning curve."
               if total >= 25
               else "<25% means low match — significant skill/location gaps.")
        ),
    }

    return {
        "idx": idx,
        "job": job,
        "total": round(total),
        "skill_pct": round(skill_score),
        "loc_pct": round(location_score),
        "title_pct": round(title_score),
        "matched": sorted(matched),
        "missing": sorted(missing),
        "verdict": verdict,
        "explanation": explanation,
    }


def match_jobs(
    analysis: MarketAnalysis, profile: CandidateProfile, top_n: int = 20,
) -> List[Dict]:
    scored = [
        score_job_fit(j, i, profile, analysis)
        for i, j in enumerate(analysis.jobs)
    ]
    scored.sort(key=lambda x: x["total"], reverse=True)
    return scored[:top_n]


def _safe(text: str) -> str:
    """Replace characters that the Windows console can't render."""
    return text.encode("ascii", errors="replace").decode("ascii")


def print_matches(matches: List[Dict], profile: CandidateProfile):
    print(f"\n  Top {len(matches)} job matches for {profile.name or 'you'}:\n")
    for rank, m in enumerate(matches, 1):
        j = m["job"]
        sal = _safe(j.get("salary", "") or "Not disclosed")
        loc = _safe(j.get("location", "") or "Not specified")
        title = _safe(j.get("title", "Untitled"))
        company = _safe(j.get("company", ""))
        print(f"  [{rank:>2}] {m['verdict']} ({m['total']}% fit)")
        print(f"       {title}")
        print(f"       {company}  |  {loc}  |  {sal}")
        print(f"       Skills you have:  {', '.join(m['matched'][:8]) or 'None detected'}")
        if m["missing"]:
            print(f"       Skills to learn:  {', '.join(m['missing'][:6])}")
        print(f"       URL: {j.get('url', '')}")
        print()

# ═══════════════════════════════════════════════════════════════════════
#  Skills Gap
# ═══════════════════════════════════════════════════════════════════════

def compute_gap(analysis: MarketAnalysis, skills: Set[str]) -> Dict[str, Any]:
    my_lower = {s.lower() for s in skills}
    matched, missing = [], []
    for skill, count in analysis.skill_counts.most_common():
        if skill.lower() in my_lower:
            matched.append((skill, count))
        else:
            missing.append((skill, count))
    coverage = len(matched) / max(len(matched) + len(missing), 1)
    return {
        "matched": matched, "missing": missing,
        "coverage": coverage,
        "total_market_skills": len(matched) + len(missing),
    }

# ═══════════════════════════════════════════════════════════════════════
#  Roadmap Generation
# ═══════════════════════════════════════════════════════════════════════

def _build_phases(missing: List[Tuple[str, int]]) -> List[Tuple[str, List[str]]]:
    buckets: Dict[str, List[str]] = {"Beginner": [], "Intermediate": [], "Advanced": []}
    for skill, _ in missing:
        d = LEARNING_META.get(skill, {}).get("d", "Intermediate")
        buckets[d].append(skill)
    phases = []
    if buckets["Beginner"]:
        phases.append(("Phase 1 -- Foundations (weeks 1-8)", buckets["Beginner"]))
    if buckets["Intermediate"]:
        phases.append(("Phase 2 -- Core Skills (weeks 5-16)", buckets["Intermediate"]))
    if buckets["Advanced"]:
        phases.append(("Phase 3 -- Specialisation (weeks 12-24+)", buckets["Advanced"]))
    return phases


def generate_roadmap(
    gap: Dict[str, Any], profile: CandidateProfile, top_n: int = 15,
) -> str:
    name = profile.name or "Candidate"
    lines = [
        f"# Learning Roadmap for {name}",
        f"*Target role: {profile.target_role or 'General'}*",
        "",
        f"Your current market coverage: **{gap['coverage']:.0%}**",
        f"Skills matched: {len(gap['matched'])}  |  Skills to acquire: {len(gap['missing'])}",
        "",
        f"## Top {top_n} Priority Skills",
        "",
    ]
    total_weeks = 0
    for rank, (skill, count) in enumerate(gap["missing"][:top_n], 1):
        meta = LEARNING_META.get(skill, {})
        diff = meta.get("d", "Varies")
        weeks = meta.get("w", 4)
        prereqs = meta.get("pre", [])
        tip = meta.get("tip", "Official docs + hands-on projects")
        total_weeks += weeks
        lines.append(f"### {rank}. {skill}  ({count} jobs require this)")
        lines.append(f"- **Difficulty**: {diff}")
        lines.append(f"- **Time**: ~{weeks} weeks")
        if prereqs:
            have = [p for p in prereqs if p.lower() in {s.lower() for s in profile.skills}]
            need = [p for p in prereqs if p not in have]
            if have:
                lines.append(f"- **Prerequisites you have**: {', '.join(have)}")
            if need:
                lines.append(f"- **Prerequisites to learn first**: {', '.join(need)}")
        lines.append(f"- **How to start**: {tip}")
        lines.append("")

    lines += [
        "---",
        f"**Total estimated investment**: ~{total_weeks} weeks",
        "(many can be learned in parallel, cutting this by 40-60%)",
        "",
    ]

    phases = _build_phases(gap["missing"][:top_n])
    if phases:
        lines.append("## Suggested Learning Phases")
        lines.append("")
        for phase_name, skills_in_phase in phases:
            lines.append(f"### {phase_name}")
            for s in skills_in_phase:
                lines.append(f"- {s}")
            lines.append("")
    return "\n".join(lines)

# ═══════════════════════════════════════════════════════════════════════
#  Report Generation
# ═══════════════════════════════════════════════════════════════════════

def generate_report(
    analysis: MarketAnalysis,
    profile: CandidateProfile,
    gap: Dict,
    matches: List[Dict],
    roadmap_text: str,
) -> str:
    name = profile.name or "Candidate"
    lines = [
        f"# Career Analysis Report for {name}",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "## 1. Your Profile",
        f"- **Name**: {name}",
        f"- **Target role**: {profile.target_role}",
        f"- **Experience**: {profile.experience_years} years",
        f"- **Skills**: {', '.join(profile.skills)}",
        f"- **Preferred locations**: {', '.join(profile.preferred_locations) or 'Anywhere'}",
        f"- **Open to remote**: {'Yes' if profile.open_to_remote else 'No'}",
        f"- **Salary expectation**: {profile.salary_expectation or 'Not specified'}",
        "",
        "## 2. Market Overview",
        f"- **Total jobs scanned**: {analysis.total} across {len(analysis.sources)} sources",
        f"- **Remote-friendly**: {analysis.remote_ratio:.0%} of jobs",
        "",
        "| Source | Jobs |",
        "|--------|------|",
    ]
    for src, cnt in analysis.sources.most_common():
        lines.append(f"| {src} | {cnt} |")

    lines += [
        "",
        "## 3. Your Competitiveness",
        f"- **Market coverage**: {gap['coverage']:.0%}",
        f"- **Skills matched**: {len(gap['matched'])} / {gap['total_market_skills']}",
        "",
    ]

    if gap["coverage"] >= 0.5:
        lines.append("You are a **strong candidate** -- you have more than half the skills employers want.")
    elif gap["coverage"] >= 0.25:
        lines.append("You are a **competitive candidate** with room to grow. Focus on the top missing skills below.")
    else:
        lines.append("You are **building your profile**. The roadmap below will help you reach your target role.")
    lines.append("")

    lines += [
        "### Skills You Have (in demand)",
        "| Skill | Jobs requiring it |",
        "|-------|-------------------|",
    ]
    for s, c in gap["matched"][:20]:
        lines.append(f"| {s} | {c} |")

    lines += [
        "",
        "### Top Missing Skills",
        "| Priority | Skill | Jobs requiring it |",
        "|----------|-------|--------------------|",
    ]
    for i, (s, c) in enumerate(gap["missing"][:20], 1):
        lines.append(f"| {i} | {s} | {c} |")
    lines.append("")

    lines += ["## 4. Best Job Matches", ""]
    for rank, m in enumerate(matches[:15], 1):
        j = m["job"]
        lines.append(f"### [{rank}] {j.get('title', '')} -- {m['verdict']} ({m['total']}%)")
        lines.append(f"- **Company**: {j.get('company', '')}")
        lines.append(f"- **Location**: {j.get('location', '') or 'N/A'}")
        lines.append(f"- **Salary**: {j.get('salary', '') or 'Not disclosed'}")
        lines.append(f"- **Your matching skills**: {', '.join(m['matched'][:10]) or 'N/A'}")
        if m["missing"]:
            lines.append(f"- **Skills to develop**: {', '.join(m['missing'][:8])}")
        lines.append(f"- **Apply**: {j.get('url', '')}")
        lines.append("")

    lines += [
        "## 5. Salary Insights", "",
    ]
    for cur, vals in sorted(analysis.salary_by_currency.items()):
        if len(vals) < 2:
            continue
        lines.append(f"### {cur}")
        lines.append(f"- Range: {min(vals):,.0f} - {max(vals):,.0f}")
        lines.append(f"- Median: {statistics.median(vals):,.0f}")
        lines.append(f"- Average: {statistics.mean(vals):,.0f}")
        lines.append("")

    lines += [
        "## 6. Top Locations Hiring", "",
        "| Location | Jobs |",
        "|----------|------|",
    ]
    for loc, cnt in analysis.locations.most_common(15):
        lines.append(f"| {loc} | {cnt} |")
    lines.append("")

    lines += ["---", "", roadmap_text]
    return "\n".join(lines)


def generate_pdf(md_text: str, output_path: Path):
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.pdfgen import canvas as canvasmod
    except ImportError:
        LOG.warning("reportlab not installed - skipping PDF. pip install reportlab")
        return

    font_name = "Courier"
    try:
        consolas = Path(r"C:\Windows\Fonts\consola.ttf")
        if consolas.exists():
            pdfmetrics.registerFont(TTFont("Consolas", str(consolas)))
            font_name = "Consolas"
    except Exception:
        pass

    c = canvasmod.Canvas(str(output_path), pagesize=letter)
    width, height = letter
    mx, my, fs, lh = 54, 54, 9, 11
    c.setTitle(f"Career Report")
    c.setFont(font_name, fs)
    max_w = width - 2 * mx

    def wrap(line: str) -> list:
        if not line:
            return [""]
        words, out, cur = line.split(" "), [], ""
        for w in words:
            cand = f"{cur} {w}".strip() if cur else w
            if c.stringWidth(cand, font_name, fs) <= max_w:
                cur = cand
            else:
                if cur:
                    out.append(cur)
                cur = w
        out.append(cur)
        return out

    y = height - my
    for raw_line in md_text.splitlines():
        for wl in wrap(raw_line.rstrip()):
            if y < my:
                c.showPage()
                c.setFont(font_name, fs)
                y = height - my
            c.drawString(mx, y, wl)
            y -= lh
    c.save()
    LOG.info("PDF saved to %s", output_path)

# ═══════════════════════════════════════════════════════════════════════
#  LLM Agent (optional)
# ═══════════════════════════════════════════════════════════════════════

class LLMAgent:
    def __init__(self, fast_model: str = "gpt-4o-mini", reasoning_model: str = "gpt-4o"):
        try:
            import openai
            self.client = openai.OpenAI()
        except ImportError:
            raise RuntimeError("pip install openai")
        except Exception as exc:
            raise RuntimeError(f"OpenAI init failed: {exc}")
        self.fast = fast_model
        self.deep = reasoning_model

    def ask(self, question: str, context: str, deep: bool = False) -> str:
        model = self.deep if deep else self.fast
        resp = self.client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": (
                    "You are a career coach helping a job candidate. "
                    "Use the market data below to give specific, actionable advice.\n\n"
                    + context
                )},
                {"role": "user", "content": question},
            ],
            temperature=0.4, max_tokens=2000,
        )
        return resp.choices[0].message.content

    def enhanced_roadmap(self, context: str, profile: CandidateProfile, gap_summary: str) -> str:
        prompt = (
            f"Candidate: {profile.name}\n"
            f"Target role: {profile.target_role}\n"
            f"Experience: {profile.experience_years} years\n"
            f"Current skills: {', '.join(profile.skills)}\n\n"
            f"Top missing skills:\n{gap_summary}\n\n"
            "Create a detailed 6-month career development plan with:\n"
            "1. Month-by-month schedule with specific goals\n"
            "2. Free learning resources for each skill\n"
            "3. Portfolio projects to build that prove these skills\n"
            "4. How each skill connects to the target role\n"
            "5. Tips to stand out in interviews for this role"
        )
        return self.ask(prompt, context, deep=True)

# ═══════════════════════════════════════════════════════════════════════
#  Interactive Mode  (Candidate-Focused)
# ═══════════════════════════════════════════════════════════════════════

_MENU = """
  What would you like to do?

  [1] profile        View or edit your profile
  [2] match          Find jobs that match your profile
  [3] fit <number>   Deep-dive into a specific job match
  [4] gap            See your skills gap vs the market
  [5] roadmap        Get a personalised learning roadmap
  [6] market         Market overview (skills, salaries, locations)
  [7] report         Generate your full career report (MD + PDF)
  [8] ask <question> Ask anything about the job market
  [9] help           Show this menu
  [0] quit           Exit
""".strip()


def interactive_loop(
    analysis: MarketAnalysis,
    profile: CandidateProfile,
    llm: Optional[LLMAgent],
    data_dir: Path,
):
    gap = compute_gap(analysis, profile.skills_set())
    matches = match_jobs(analysis, profile, top_n=30)
    context = analysis.summary_text()
    context += f"\n\nCandidate: {profile.name}, target: {profile.target_role}"
    context += f"\nSkills: {', '.join(profile.skills)}"
    context += f"\nCoverage: {gap['coverage']:.0%}"

    print("\n" + "=" * 60)
    print(f"  Welcome, {profile.name or 'Candidate'}!")
    print(f"  {analysis.total} jobs loaded from {len(analysis.sources)} sources.")
    print(f"  Your market coverage: {gap['coverage']:.0%}")
    print("=" * 60)
    print()
    print(_MENU)
    print()

    while True:
        try:
            cmd = input(f"  {profile.name or 'You'} >> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not cmd:
            continue

        parts = cmd.split(maxsplit=1)
        action = parts[0].lower()

        if action in ("quit", "exit", "0", "q"):
            print(f"\n  Good luck with your job search, {profile.name or 'Candidate'}!\n")
            break

        elif action in ("help", "9", "menu"):
            print(_MENU)

        elif action in ("profile", "1"):
            print(f"\n{profile.display()}\n")
            edit = input("  Edit profile? (yes/no): ").strip().lower()
            if edit in ("yes", "y"):
                profile = guided_profile_setup(profile)
                profile.save(data_dir / PROFILE_FILE)
                gap = compute_gap(analysis, profile.skills_set())
                matches = match_jobs(analysis, profile, top_n=30)
                print(f"\n  Profile updated. New coverage: {gap['coverage']:.0%}\n")

        elif action in ("match", "2"):
            print_matches(matches[:15], profile)

        elif action in ("fit", "3"):
            if len(parts) < 2 or not parts[1].isdigit():
                print("  Usage: fit <number>  (e.g. fit 1)")
                continue
            rank = int(parts[1]) - 1
            if rank < 0 or rank >= len(matches):
                print(f"  Enter a number between 1 and {len(matches)}")
                continue
            m = matches[rank]
            j = m["job"]
            print(f"\n  {'=' * 56}")
            print(f"  Job:      {_safe(j.get('title', ''))}")
            print(f"  Company:  {_safe(j.get('company', ''))}")
            print(f"  Location: {_safe(j.get('location', '') or 'N/A')}")
            print(f"  Salary:   {_safe(j.get('salary', '') or 'Not disclosed')}")
            print(f"  Source:   {j.get('source', '')}")
            print(f"  URL:      {j.get('url', '')}")
            print(f"  {'=' * 56}")
            print(f"  Overall fit:    {m['total']}%  ({m['verdict']})")
            print(f"  Skill match:    {m['skill_pct']}%")
            print(f"  Location match: {m['loc_pct']}%")
            print(f"  Title match:    {m['title_pct']}%")
            print(f"\n  Skills you already have for this role:")
            if m["matched"]:
                for s in m["matched"]:
                    print(f"    + {s}")
            else:
                print("    (none detected)")
            if m["missing"]:
                print(f"\n  Skills you need to develop:")
                for s in m["missing"]:
                    meta = LEARNING_META.get(s, {})
                    w = meta.get("w", "?")
                    print(f"    - {s}  (~{w} weeks to learn)")
            desc = j.get("description", "")
            if desc:
                print(f"\n  Description (first 500 chars):")
                print(f"  {_safe(desc[:500])}...")
            print()

        elif action in ("gap", "4"):
            print(f"\n  Market coverage: {gap['coverage']:.0%}")
            print(f"  You have {len(gap['matched'])} of {gap['total_market_skills']} demanded skills.\n")
            print("  Your strongest skills (by market demand):")
            for s, c in gap["matched"][:10]:
                bar = "#" * min(c, 40)
                print(f"    {s:<25} {c:>4} jobs  {bar}")
            print("\n  Top skills you're missing:")
            for i, (s, c) in enumerate(gap["missing"][:10], 1):
                bar = "#" * min(c, 40)
                print(f"    {i:>2}. {s:<23} {c:>4} jobs  {bar}")
            print()

        elif action in ("roadmap", "5"):
            rm = generate_roadmap(gap, profile)
            print(rm)
            if llm:
                use_llm = input("  Generate enhanced AI roadmap? (yes/no): ").strip().lower()
                if use_llm in ("yes", "y"):
                    print("\n  Generating personalised career plan ...\n")
                    gap_s = "\n".join(f"  {s}: {c} jobs" for s, c in gap["missing"][:15])
                    print(llm.enhanced_roadmap(context, profile, gap_s))

        elif action in ("market", "6"):
            print("\n" + analysis.summary_text())
            print("\n  Top hiring companies:")
            for co, cnt in analysis.companies.most_common(10):
                print(f"    {co:<35} {cnt} positions")
            print()

        elif action in ("report", "7"):
            rm = generate_roadmap(gap, profile)
            md = generate_report(analysis, profile, gap, matches, rm)
            out_dir = data_dir / "analysis_output"
            out_dir.mkdir(exist_ok=True)
            md_path = out_dir / "career_report.md"
            pdf_path = out_dir / "career_report.pdf"
            md_path.write_text(md, encoding="utf-8")
            print(f"\n  Saved: {md_path}")
            generate_pdf(md, pdf_path)
            if pdf_path.exists():
                print(f"  Saved: {pdf_path}")
            print()

        elif action in ("ask", "8"):
            if len(parts) < 2:
                print("  Usage: ask <your question>")
                print('  Example: ask What Python frameworks are most in demand?')
            elif not llm:
                print("  AI not available. Set OPENAI_API_KEY to enable.")
            else:
                print()
                print(llm.ask(parts[1], context))
                print()

        else:
            print(f"  Unknown command. Type 'help' for options.")

# ═══════════════════════════════════════════════════════════════════════
#  CLI Entry Point
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Career Assistant -- find matching jobs, identify skill gaps, build your roadmap.",
    )
    parser.add_argument("--data-dir", default=str(Path(__file__).resolve().parent),
                        help="Root dir with outputs_* folders")
    parser.add_argument("--setup", action="store_true", help="Create/edit your candidate profile")
    parser.add_argument("--match", action="store_true", help="Find jobs matching your profile")
    parser.add_argument("--gap", action="store_true", help="Show your skills gap")
    parser.add_argument("--roadmap", action="store_true", help="Generate learning roadmap")
    parser.add_argument("--report", action="store_true", help="Generate full career report")
    parser.add_argument("--interactive", action="store_true", help="Interactive career session")
    parser.add_argument("--my-skills", default="", help="Quick skills override (comma-separated)")
    parser.add_argument("--target-role", default="", help="Quick target role override")
    parser.add_argument("--ask", default="", help="Ask a question (needs OPENAI_API_KEY)")
    parser.add_argument("--fast-model", default="gpt-4o-mini")
    parser.add_argument("--reasoning-model", default="gpt-4o")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    data_dir = Path(args.data_dir).resolve()
    profile_path = data_dir / PROFILE_FILE

    if args.setup:
        existing = CandidateProfile.load(profile_path) if profile_path.exists() else None
        profile = guided_profile_setup(existing)
        profile.save(profile_path)
        print(f"\n  Profile saved to {profile_path}")
        if not any([args.match, args.gap, args.roadmap, args.report, args.interactive]):
            return

    if profile_path.exists():
        profile = CandidateProfile.load(profile_path)
        LOG.info("Loaded profile for %s", profile.name)
    else:
        profile = CandidateProfile()

    if args.my_skills:
        profile.skills = [s.strip() for s in args.my_skills.split(",") if s.strip()]
    if args.target_role:
        profile.target_role = args.target_role

    if not profile.skills and not args.interactive and not args.setup:
        print("\n  No profile found. Run with --setup to create one,")
        print('  or pass --my-skills "python,sql,docker"\n')
        parser.print_help()
        return

    LOG.info("Loading jobs from %s ...", data_dir)
    jobs = load_all_jobs(data_dir)
    if not jobs:
        LOG.error("No jobs found in %s. Run scrapers first.", data_dir)
        sys.exit(1)

    LOG.info("Analysing %d jobs ...", len(jobs))
    analysis = MarketAnalysis(jobs)
    gap = compute_gap(analysis, profile.skills_set())
    matches = match_jobs(analysis, profile)

    llm: Optional[LLMAgent] = None
    if os.environ.get("OPENAI_API_KEY"):
        try:
            llm = LLMAgent(fast_model=args.fast_model, reasoning_model=args.reasoning_model)
            LOG.info("AI career coach ready.")
        except Exception as exc:
            LOG.warning("LLM init failed: %s", exc)

    if args.match:
        print_matches(matches[:15], profile)

    if args.gap:
        print(f"\n  Market coverage: {gap['coverage']:.0%}")
        print(f"  Matched: {len(gap['matched'])}, Missing: {len(gap['missing'])}")
        print("\n  Top 10 missing skills:")
        for i, (s, c) in enumerate(gap["missing"][:10], 1):
            print(f"    {i:>2}. {s:<25} ({c} jobs)")
        print()

    if args.roadmap:
        print("\n" + generate_roadmap(gap, profile))

    if args.report:
        rm = generate_roadmap(gap, profile)
        md = generate_report(analysis, profile, gap, matches, rm)
        out_dir = data_dir / "analysis_output"
        out_dir.mkdir(exist_ok=True)
        md_path = out_dir / "career_report.md"
        pdf_path = out_dir / "career_report.pdf"
        md_path.write_text(md, encoding="utf-8")
        LOG.info("Report: %s", md_path)
        generate_pdf(md, pdf_path)
        print(f"\n  Career report saved to {out_dir}\n")

    if args.ask:
        if not llm:
            LOG.error("--ask requires OPENAI_API_KEY.")
        else:
            ctx = analysis.summary_text()
            ctx += f"\nCandidate: {profile.name}, skills: {', '.join(profile.skills)}"
            print(llm.ask(args.ask, ctx))

    if args.interactive:
        if not profile.skills:
            profile = guided_profile_setup(profile)
            profile.save(profile_path)
        interactive_loop(analysis, profile, llm, data_dir)

    ran_something = any([args.match, args.gap, args.roadmap, args.report, args.interactive, args.ask, args.setup])
    if not ran_something:
        parser.print_help()


if __name__ == "__main__":
    main()
