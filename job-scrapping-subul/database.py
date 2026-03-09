"""
database.py — PostgreSQL Azure connection + table creation + trigger

CORRECTIONS APPLIQUÉES (vs version originale) :
  FIX 1 — get_jobs_for_user : WHERE $1::integer = ANY(id_user)
           cast explicite évite le type mismatch asyncpg integer[] vs int4
  FIX 2 — get_jobs_for_user : gap_coverage calculé correctement
           était "1.0 if not gap else 0.0" → toujours 0% si gap non vide!
           maintenant : (total_req - missing) / total_req
  FIX 3 — user_has_jobs : même cast ::integer
"""

import json as _json
import logging
import os
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from pathlib import Path

# Optional: fallback XAI when job was saved without xai (e.g. before column existed)
try:
    from xai_explainer import _fallback_xai as _xai_fallback
except Exception:
    _xai_fallback = None

load_dotenv(Path(__file__).parent / ".env")
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Connection string depuis .env
# ─────────────────────────────────────────────────────────────────────────────

def _get_dsn() -> str:
    dsn = os.getenv("POSTGRES_DSN", "").strip()
    if dsn:
        logger.info("[db] Using POSTGRES_DSN from .env")
        return dsn

    host     = os.getenv("POSTGRES_HOST", "").strip()
    user     = os.getenv("POSTGRES_USER", "").strip()
    password = os.getenv("POSTGRES_PASSWORD", "").strip()
    db       = os.getenv("POSTGRES_DB", "jobscan").strip()
    port     = os.getenv("POSTGRES_PORT", "5432").strip()

    if not host or not user or not password:
        raise ValueError(
            "PostgreSQL credentials missing in .env\n"
            "Add: POSTGRES_DSN=postgresql://user:pass@host/db?sslmode=require\n"
            "  OR: POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB"
        )

    return f"postgresql://{user}:{password}@{host}:{port}/{db}?sslmode=require"


# ─────────────────────────────────────────────────────────────────────────────
#  Pool global (initialisé au startup de FastAPI)
# ─────────────────────────────────────────────────────────────────────────────

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        dsn = _get_dsn()
        logger.info("[db] Creating PostgreSQL pool...")
        _pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=2,
            max_size=10,
            command_timeout=30,
        )
        logger.info("[db] PostgreSQL pool created OK")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("[db] PostgreSQL pool closed")


# ─────────────────────────────────────────────────────────────────────────────
#  Création des tables + trigger (idempotent)
# ─────────────────────────────────────────────────────────────────────────────

CREATE_TABLES_SQL = """
-- ── Table users ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    linkedin      TEXT,
    role          TEXT,
    seniority     TEXT,
    years_exp     TEXT,
    industry      TEXT,
    education     TEXT,
    skills        TEXT,
    summary       TEXT,
    bullets       TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name  TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email      TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS linkedin   TEXT;

-- Add xai column for explainable AI (LLM-as-judge output) if missing
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS xai JSONB;

-- ── Table jobs ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jobs (
    id_job           SERIAL PRIMARY KEY,
    id_user          INTEGER[],
    url              TEXT NOT NULL UNIQUE,
    source           TEXT,
    title            TEXT,
    industry         TEXT,
    location         TEXT,
    seniority        TEXT,
    must_have        TEXT,
    nice_to_have     TEXT,
    description      TEXT,
    responsibilities TEXT,
    requirements     TEXT,
    salary           TEXT,
    match_score      FLOAT,
    cosine_score     FLOAT,
    combined_score   FLOAT,
    contract         TEXT,
    education        TEXT,
    remote           TEXT,
    skills_gap       TEXT,
    xai              JSONB,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_url     ON jobs(url);
CREATE INDEX IF NOT EXISTS idx_jobs_id_user ON jobs USING GIN(id_user);

-- ── Table chat_history ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS chat_history (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    role       TEXT NOT NULL,
    content    TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_chat_history_user_id ON chat_history(user_id);

-- ── Fonction trigger ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_jobs_upsert()
RETURNS TRIGGER AS $$
DECLARE
    existing_id    INTEGER;
    existing_users INTEGER[];
BEGIN
    SELECT id_job, id_user
    INTO existing_id, existing_users
    FROM jobs
    WHERE url = NEW.url
    LIMIT 1;

    IF FOUND THEN
        IF NEW.id_user[1] = ANY(existing_users) THEN
            RAISE NOTICE 'SKIP: url=% already exists for user=%', NEW.url, NEW.id_user[1];
            RETURN NULL;
        ELSE
            UPDATE jobs
            SET
                id_user        = array_append(existing_users, NEW.id_user[1]),
                match_score    = GREATEST(match_score,    COALESCE(NEW.match_score, match_score)),
                cosine_score   = GREATEST(cosine_score,   COALESCE(NEW.cosine_score, cosine_score)),
                combined_score = GREATEST(combined_score, COALESCE(NEW.combined_score, combined_score))
            WHERE id_job = existing_id;
            RAISE NOTICE 'APPEND: user=% added to job url=%', NEW.id_user[1], NEW.url;
            RETURN NULL;
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_jobs_upsert ON jobs;
CREATE TRIGGER trg_jobs_upsert
    BEFORE INSERT ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION fn_jobs_upsert();
"""


async def init_db():
    """Crée les tables et le trigger si pas encore existants."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(CREATE_TABLES_SQL)
        logger.info("[db] Tables + trigger initialized OK")
    except Exception as e:
        logger.error(f"[db] init_db FAILED: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
#  CRUD users
# ─────────────────────────────────────────────────────────────────────────────

async def upsert_user(user_id: int, cv_structured: dict) -> bool:
    logger.info(f"[db] upsert_user called — user_id={user_id}")

    if not user_id or user_id <= 0:
        logger.warning("[db] upsert_user skipped: user_id invalide (0 ou négatif)")
        return False

    pool = await get_pool()
    sql = """
        INSERT INTO users (
            id, first_name, last_name, email, linkedin,
            role, seniority, years_exp, industry, education,
            skills, summary, bullets
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (id) DO UPDATE SET
            first_name = COALESCE(EXCLUDED.first_name, users.first_name),
            last_name  = COALESCE(EXCLUDED.last_name,  users.last_name),
            email      = COALESCE(EXCLUDED.email,      users.email),
            linkedin   = COALESCE(EXCLUDED.linkedin,   users.linkedin),
            role       = EXCLUDED.role,
            seniority  = EXCLUDED.seniority,
            years_exp  = EXCLUDED.years_exp,
            industry   = EXCLUDED.industry,
            education  = EXCLUDED.education,
            skills     = EXCLUDED.skills,
            summary    = EXCLUDED.summary,
            bullets    = EXCLUDED.bullets
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                user_id,
                cv_structured.get("first_name") or None,
                cv_structured.get("last_name")  or None,
                cv_structured.get("email")      or None,
                cv_structured.get("linkedin")   or None,
                cv_structured.get("role", ""),
                cv_structured.get("seniority", ""),
                cv_structured.get("years_experience", ""),
                cv_structured.get("industry", ""),
                cv_structured.get("education", ""),
                cv_structured.get("skills", ""),
                cv_structured.get("summary", ""),
                cv_structured.get("bullets", ""),
            )
        logger.info(f"[db] ✅ User {user_id} upserted OK")
        return True
    except Exception as e:
        logger.error(f"[db] ❌ upsert_user failed for user_id={user_id}: {e}")
        return False


async def get_user(user_id: int) -> Optional[dict]:
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE id = $1", user_id
            )
        if row is None:
            logger.info(f"[db] get_user: no user found for id={user_id}")
            return None
        return {
            "first_name":       row["first_name"],
            "last_name":        row["last_name"],
            "email":            row["email"],
            "linkedin":         row["linkedin"],
            "role":             row["role"],
            "seniority":        row["seniority"],
            "years_experience": row["years_exp"],
            "industry":         row["industry"],
            "education":        row["education"],
            "skills":           row["skills"],
            "summary":          row["summary"],
            "bullets":          row["bullets"],
        }
    except Exception as e:
        logger.error(f"[db] get_user failed for id={user_id}: {e}")
        return None


async def update_user_profile(
    user_id: int,
    first_name: str = None,
    last_name:  str = None,
    email:      str = None,
    linkedin:   str = None,
) -> bool:
    logger.info(f"[db] update_user_profile — user_id={user_id}")

    if not user_id or user_id <= 0:
        logger.warning("[db] update_user_profile skipped: user_id invalide")
        return False

    pool = await get_pool()
    sql = """
        UPDATE users
        SET
            first_name = COALESCE($2, first_name),
            last_name  = COALESCE($3, last_name),
            email      = COALESCE($4, email),
            linkedin   = COALESCE($5, linkedin)
        WHERE id = $1
    """
    try:
        async with pool.acquire() as conn:
            result = await conn.execute(
                sql,
                user_id,
                first_name or None,
                last_name  or None,
                email      or None,
                linkedin   or None,
            )
        updated = int(result.split()[-1]) if result else 0
        if updated == 0:
            logger.warning(f"[db] update_user_profile: user_id={user_id} not found in DB")
            return False
        logger.info(f"[db] ✅ Profile updated for user_id={user_id}")
        return True
    except Exception as e:
        logger.error(f"[db] ❌ update_user_profile failed for user_id={user_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  CRUD jobs
# ─────────────────────────────────────────────────────────────────────────────

async def insert_job(user_id: int, card: dict) -> bool:
    logger.info(f"[db] insert_job called — user_id={user_id} url={card.get('url','')[:60]}")

    if not user_id or user_id <= 0:
        logger.warning("[db] insert_job skipped: user_id invalide")
        return False

    if not card.get("url"):
        logger.warning("[db] insert_job skipped: url vide")
        return False

    gap_missing = card.get("gap_missing", [])
    skills_gap  = _json.dumps(gap_missing, ensure_ascii=False)

    xai_obj = card.get("xai")
    xai_db  = _json.dumps(xai_obj, ensure_ascii=False) if xai_obj else None

    raw_match = card.get("match_score", -1)
    match_score_db: Optional[float] = None
    if raw_match is not None and raw_match >= 0:
        match_score_db = float(raw_match)

    raw_cosine = card.get("cosine", card.get("cosine_score", 0))
    cosine_score_db = float(raw_cosine or 0)

    raw_combined = card.get("combined_score", 0)
    combined_score_db = float(raw_combined or 0)

    pool = await get_pool()
    sql = """
        INSERT INTO jobs (
            id_user, url, source, title, industry, location,
            seniority, must_have, nice_to_have,
            description, responsibilities, requirements,
            salary, match_score, cosine_score, combined_score,
            contract, education, remote, skills_gap, xai
        ) VALUES (
            $1,  $2,  $3,  $4,  $5,  $6,
            $7,  $8,  $9,
            $10, $11, $12,
            $13, $14, $15, $16,
            $17, $18, $19, $20, $21::jsonb
        )
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                [user_id],
                card.get("url", ""),
                card.get("source", ""),
                card.get("title", ""),
                card.get("industry") or card.get("company", ""),
                card.get("location", ""),
                card.get("experience", ""),
                card.get("skills_req", ""),
                card.get("skills_bon", ""),
                card.get("description", ""),
                card.get("description", ""),
                card.get("skills_req", ""),
                card.get("salary", ""),
                match_score_db,
                cosine_score_db,
                combined_score_db,
                card.get("contract", ""),
                card.get("education", ""),
                card.get("remote", ""),
                skills_gap,
                xai_db,
            )
        logger.info(f"[db] ✅ Job inserted/updated — user={user_id} url={card.get('url','')[:60]}")
        return True
    except Exception as e:
        logger.error(f"[db] ❌ insert_job failed — user={user_id} url={card.get('url','')[:60]}: {e}")
        return False


async def get_jobs_for_user(user_id: int) -> list[dict]:
    """
    Retourne tous les jobs où user_id est dans id_user[].
    Triés par match_score DESC (AI Match) pour alignement avec l’onglet Matches.

    FIX 1 : $1::integer — cast explicite pour éviter type mismatch asyncpg
             avec les colonnes INTEGER[] sur Azure PostgreSQL.
    FIX 2 : gap_coverage calculé correctement depuis must_have vs gap_missing.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM jobs
                WHERE $1::integer = ANY(id_user)
                ORDER BY match_score DESC NULLS LAST
                """,
                user_id,
            )
        result = []
        for row in rows:
            gap = []
            try:
                gap = _json.loads(row["skills_gap"] or "[]")
            except Exception:
                pass

            match_raw    = row["match_score"]
            cosine_raw   = row["cosine_score"]  or 0.0
            combined_raw = row["combined_score"] or 0.0

            # ── FIX 2 : gap_coverage correct ──────────────────────────────
            must_have_str  = row["must_have"] or ""
            must_have_list = [s.strip() for s in must_have_str.split(",") if s.strip()]
            total_skills   = len(must_have_list)
            missing_count  = len(gap)
            if total_skills > 0:
                gap_coverage = max(0.0, (total_skills - missing_count) / total_skills)
            else:
                # Pas de skills requis connus → couverture complète si pas de gap
                gap_coverage = 1.0 if missing_count == 0 else 0.5

            # xai from DB; if missing (old jobs saved before xai column), use fallback so UI still shows formula + interpretation
            xai_val = row.get("xai")
            needs_xai_backfill = False
            if xai_val is None and _xai_fallback is not None:
                needs_xai_backfill = True
                try:
                    xai_val = _xai_fallback(
                        float(cosine_raw or 0),
                        float(match_raw if match_raw is not None else 0),
                        float(combined_raw or 0),
                        gap_coverage,
                        total_skills,
                    )
                except Exception:
                    pass

            result.append({
                "id_job":         row["id_job"],
                "url":            row["url"],
                "source":         row["source"],
                "title":          row["title"],
                "industry":       row["industry"] or "",
                "location":       row["location"],
                "remote":         row["remote"],
                "salary":         row["salary"],
                "contract":       row["contract"],
                "education":      row["education"],
                "experience":     row["seniority"],
                "match_score":         match_raw if match_raw is not None else -1,
                "cosine":              cosine_raw,
                "combined_score":      combined_raw,
                "match_score_display":    f"{(match_raw or 0) * 100:.2f}" if match_raw is not None and match_raw >= 0 else "—",
                "cosine_display":         f"{cosine_raw   * 100:.2f}",
                "combined_score_display": f"{combined_raw * 100:.2f}",
                "gap_missing":    gap,
                "gap_matched":    [],
                "gap_coverage":   gap_coverage,   # ← FIX 2
                "gap_total":      total_skills,
                "description":    row["description"],
                "skills_req":     row["must_have"],
                "skills_bon":     row["nice_to_have"],
                "tags":           row["requirements"],
                "event":          "job",
                "xai":            xai_val,
                "_needs_xai_backfill": needs_xai_backfill,
            })
        logger.info(f"[db] get_jobs_for_user: {len(result)} jobs for user_id={user_id}")
        return result
    except Exception as e:
        logger.error(f"[db] get_jobs_for_user failed for user_id={user_id}: {e}")
        return []


async def update_job_xai(id_job: int, xai_dict: dict) -> bool:
    """Update the xai JSONB column for a job (e.g. after LLM backfill)."""
    if not xai_dict:
        return False
    pool = await get_pool()
    try:
        json_str = _json.dumps(xai_dict, ensure_ascii=False)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET xai = $1::jsonb WHERE id_job = $2",
                json_str,
                id_job,
            )
        logger.debug("[db] update_job_xai OK — id_job=%s", id_job)
        return True
    except Exception as e:
        logger.error(f"[db] update_job_xai failed id_job={id_job}: {e}")
        return False


async def user_has_jobs(user_id: int) -> bool:
    """
    Vérifie si l'utilisateur a déjà des jobs en base.
    FIX : cast ::integer comme get_jobs_for_user.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE $1::integer = ANY(id_user)",
                user_id,
            )
        has = (count or 0) > 0
        logger.info(f"[db] user_has_jobs: user_id={user_id} → {count} jobs")
        return has
    except Exception as e:
        logger.error(f"[db] user_has_jobs failed for user_id={user_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  Chat History
# ─────────────────────────────────────────────────────────────────────────────

async def save_chat_message(user_id: int, role: str, content: str) -> bool:
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO chat_history (user_id, role, content) VALUES ($1, $2, $3)",
                user_id, role, content,
            )
        logger.info(f"[db] save_chat_message OK — user_id={user_id} role={role}")
        return True
    except Exception as e:
        logger.error(f"[db] save_chat_message failed for user_id={user_id}: {e}")
        return False


async def load_chat_history(user_id: int, limit: int = 50) -> list[dict]:
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT role, content, created_at
                FROM chat_history
                WHERE user_id = $1
                ORDER BY created_at ASC
                LIMIT $2
                """,
                user_id, limit,
            )
        result = [{"role": r["role"], "content": r["content"]} for r in rows]
        logger.info(f"[db] load_chat_history: {len(result)} messages for user_id={user_id}")
        return result
    except Exception as e:
        logger.error(f"[db] load_chat_history failed for user_id={user_id}: {e}")
        return []