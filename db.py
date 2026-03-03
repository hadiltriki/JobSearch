"""
Database layer — PostgreSQL via psycopg2
==========================================
Reads POSTGRES_DSN from environment.
Maps to the existing `users` and `jobs` tables from the Azure pipeline,
and creates `profiles` + `chat_history` for the frontend.
"""

from __future__ import annotations

import json
import os
import logging
from contextlib import contextmanager
from typing import Optional

import psycopg2
import psycopg2.extras

LOG = logging.getLogger(__name__)

DSN = os.environ.get("POSTGRES_DSN", "")

if not DSN:
    LOG.warning("POSTGRES_DSN not set — database features disabled")


def _connect():
    return psycopg2.connect(DSN)


@contextmanager
def get_conn():
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema (only new tables — existing users/jobs are untouched) ─────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS profiles (
    id                  SERIAL PRIMARY KEY,
    user_id             INTEGER NOT NULL,
    name                TEXT DEFAULT '',
    target_role         TEXT DEFAULT '',
    experience_years    INTEGER DEFAULT 0,
    skills              JSONB DEFAULT '[]',
    preferred_locations JSONB DEFAULT '[]',
    open_to_remote      BOOLEAN DEFAULT TRUE,
    salary_expectation  TEXT DEFAULT '',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id)
);

CREATE TABLE IF NOT EXISTS chat_history (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_db():
    if not DSN:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_SCHEMA)
        LOG.info("Database tables initialized (profiles, chat_history)")
    except Exception as e:
        LOG.error("Failed to initialize database: %s", e)


# ── Users (reads from existing pipeline `users` table) ───────────────

def find_user(user_id: str) -> Optional[dict]:
    """Look up a user by integer ID from the existing `users` table."""
    try:
        uid = int(user_id)
    except (ValueError, TypeError):
        return None
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (uid,))
            row = cur.fetchone()
    if not row:
        return None
    skills_raw = row.get("skills", "") or ""
    skills_list = [s.strip() for s in skills_raw.split(",") if s.strip()]
    return {
        "db_id": row["id"],
        "user_id": str(row["id"]),
        "name": " ".join(filter(None, [row.get("first_name"), row.get("last_name")])) or f"User {row['id']}",
        "target_role": row.get("role", ""),
        "experience_years": int(row.get("years_exp", 0) or 0),
        "skills": skills_list,
        "seniority": row.get("seniority", ""),
        "industry": row.get("industry", ""),
        "education": row.get("education", ""),
        "summary": row.get("summary", ""),
        "email": row.get("email", ""),
        "linkedin": row.get("linkedin", ""),
        "preferred_locations": [],
        "open_to_remote": True,
        "salary_expectation": "",
    }


# ── Profiles (extended preferences stored in `profiles` table) ───────

def save_profile(user_id: str, profile: dict) -> dict:
    uid = int(user_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO profiles (user_id, name, target_role, experience_years,
                    skills, preferred_locations, open_to_remote, salary_expectation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    target_role = EXCLUDED.target_role,
                    experience_years = EXCLUDED.experience_years,
                    skills = EXCLUDED.skills,
                    preferred_locations = EXCLUDED.preferred_locations,
                    open_to_remote = EXCLUDED.open_to_remote,
                    salary_expectation = EXCLUDED.salary_expectation,
                    updated_at = NOW()
                """,
                (
                    uid,
                    profile.get("name", ""),
                    profile.get("target_role", ""),
                    profile.get("experience_years", 0),
                    json.dumps(profile.get("skills", [])),
                    json.dumps(profile.get("preferred_locations", [])),
                    profile.get("open_to_remote", True),
                    profile.get("salary_expectation", ""),
                ),
            )
    return {"saved": True, "user_id": user_id}


def load_profile(user_id: str) -> Optional[dict]:
    """Load profile: merge existing `users` row with `profiles` overrides."""
    uid = int(user_id)
    base = find_user(user_id)

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM profiles WHERE user_id = %s", (uid,))
            override = cur.fetchone()

    if not override and not base:
        return None

    if not override:
        return base

    skills = override["skills"]
    if isinstance(skills, str):
        skills = json.loads(skills or "[]")
    locs = override["preferred_locations"]
    if isinstance(locs, str):
        locs = json.loads(locs or "[]")

    return {
        "user_id": user_id,
        "name": override["name"] or (base["name"] if base else ""),
        "target_role": override["target_role"] or (base["target_role"] if base else ""),
        "experience_years": override["experience_years"] or (base["experience_years"] if base else 0),
        "skills": skills if skills else (base["skills"] if base else []),
        "preferred_locations": locs,
        "open_to_remote": override["open_to_remote"],
        "salary_expectation": override["salary_expectation"] or "",
        "seniority": base.get("seniority", "") if base else "",
        "industry": base.get("industry", "") if base else "",
        "education": base.get("education", "") if base else "",
    }


# ── Jobs (reads from existing pipeline `jobs` table) ─────────────────

def load_jobs_from_db(user_id: str | None = None) -> list[dict]:
    """Load jobs from the pipeline `jobs` table, mapped to the format
    expected by job_analyzer_agent.MarketAnalysis."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if user_id:
                cur.execute(
                    "SELECT * FROM jobs WHERE %s = ANY(id_user) ORDER BY created_at DESC",
                    (int(user_id),),
                )
            else:
                cur.execute("SELECT * FROM jobs ORDER BY created_at DESC")
            rows = cur.fetchall()

    jobs = []
    for r in rows:
        loc = r.get("location", "") or ""
        remote_str = r.get("remote", "") or ""
        if "remote" in remote_str.lower() and not loc.strip():
            loc = "Remote"

        jobs.append({
            "title": r.get("title", ""),
            "company": (r.get("industry", "") or "").split("?")[0].strip(),
            "location": loc or remote_str,
            "salary": r.get("salary", "") or "",
            "url": r.get("url", ""),
            "source": r.get("source", ""),
            "description": r.get("description", "") or "",
            "requirements": r.get("requirements", "") or "",
            "seniority": r.get("seniority", "") or "",
            "contract": r.get("contract", "") or "",
            "education": r.get("education", "") or "",
            "remote": remote_str,
            "date_posted": str(r.get("created_at", "")),
            "db_match_score": r.get("match_score"),
            "db_cosine_score": r.get("cosine_score"),
            "db_combined_score": r.get("combined_score"),
        })
    return jobs


# ── Chat History ─────────────────────────────────────────────────────

def save_chat_message(user_id: str, role: str, content: str):
    uid = int(user_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO chat_history (user_id, role, content) VALUES (%s, %s, %s)",
                (uid, role, content),
            )


def load_chat_history(user_id: str, limit: int = 50) -> list[dict]:
    uid = int(user_id)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT role, content, created_at FROM chat_history "
                "WHERE user_id = %s ORDER BY created_at ASC LIMIT %s",
                (uid, limit),
            )
            rows = cur.fetchall()
    return [{"role": r["role"], "content": r["content"]} for r in rows]
