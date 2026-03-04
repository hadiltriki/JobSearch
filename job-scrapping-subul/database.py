"""
database.py — PostgreSQL Azure connection + table creation + trigger

SCHEMA :
  TABLE users :
    id            INTEGER PRIMARY KEY
    first_name    TEXT                  ← nouveau
    last_name     TEXT                  ← nouveau
    email         TEXT                  ← nouveau
    linkedin      TEXT                  ← nouveau
    role          TEXT
    seniority     TEXT
    years_exp     TEXT
    industry      TEXT
    education     TEXT
    skills        TEXT
    summary       TEXT
    bullets       TEXT
    created_at    TIMESTAMP DEFAULT NOW()

  TABLE jobs :
    id_job           SERIAL PRIMARY KEY
    id_user          INTEGER[]
    url              TEXT UNIQUE (contrainte UNIQUE pour le trigger)
    source           TEXT
    title            TEXT
    company          TEXT
    location         TEXT
    seniority        TEXT
    industry         TEXT
    must_have        TEXT
    nice_to_have     TEXT
    description      TEXT
    responsibilities TEXT
    requirements     TEXT
    salary           TEXT
    match_score      FLOAT
    cosine_score     FLOAT
    combined_score   FLOAT
    contract         TEXT
    education        TEXT
    remote           TEXT
    skills_gap       TEXT   ← JSON string des skills manquants
    created_at       TIMESTAMP DEFAULT NOW()

TRIGGER :
  Avant INSERT dans jobs :
    - Si url déjà présent ET id_user déjà dans id_user[] → SKIP
    - Si url déjà présent ET id_user pas dans id_user[] → UPDATE (append id_user)
    - Si url absent → INSERT normal
"""

import json as _json
import logging
import os
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent / ".env")
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Connection string depuis .env
# ─────────────────────────────────────────────────────────────────────────────

def _get_dsn() -> str:
    """
    Lit la connection string PostgreSQL Azure depuis .env.
    Format attendu dans .env :
      POSTGRES_DSN=postgresql://user:password@host.postgres.database.azure.com:5432/dbname?sslmode=require
    OU les variables séparées :
      POSTGRES_HOST=...
      POSTGRES_USER=...
      POSTGRES_PASSWORD=...
      POSTGRES_DB=...
      POSTGRES_PORT=5432
    """
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

-- ── Migration : ajouter les colonnes si elles n'existent pas encore ──────────
-- Idempotent : ALTER TABLE … ADD COLUMN IF NOT EXISTS (PostgreSQL 9.6+)
ALTER TABLE users ADD COLUMN IF NOT EXISTS first_name TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_name  TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS email      TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS linkedin   TEXT;

-- ── Table jobs ───────────────────────────────────────────────────────────────
-- IMPORTANT : url a une contrainte UNIQUE pour que le trigger fonctionne
-- correctement et pour que SELECT … WHERE url = $1 soit performant.
CREATE TABLE IF NOT EXISTS jobs (
    id_job           SERIAL PRIMARY KEY,
    id_user          INTEGER[],
    url              TEXT NOT NULL UNIQUE,
    source           TEXT,
    title            TEXT,
    industry         TEXT,            -- nom de la société (ex: "Anthropic", "Nearform")
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
    created_at       TIMESTAMP DEFAULT NOW()
);

-- Index sur url pour le trigger (performance)
CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(url);
-- Index GIN sur id_user pour les recherches par user
CREATE INDEX IF NOT EXISTS idx_jobs_id_user ON jobs USING GIN(id_user);

-- ── Table chat_history ────────────────────────────────────────────────────────
-- Stocke l'historique des conversations Career Assistant
CREATE TABLE IF NOT EXISTS chat_history (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    role       TEXT NOT NULL,    -- "user" | "assistant"
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
    -- Cherche si cette URL existe déjà
    SELECT id_job, id_user
    INTO existing_id, existing_users
    FROM jobs
    WHERE url = NEW.url
    LIMIT 1;

    IF FOUND THEN
        -- URL déjà présente
        IF NEW.id_user[1] = ANY(existing_users) THEN
            -- Ce user a déjà ce job → SKIP (annuler l'INSERT)
            RAISE NOTICE 'SKIP: url=% already exists for user=%', NEW.url, NEW.id_user[1];
            RETURN NULL;
        ELSE
            -- Nouveau user pour ce job → append id_user et UPDATE scores
            UPDATE jobs
            SET
                id_user        = array_append(existing_users, NEW.id_user[1]),
                match_score    = GREATEST(match_score,    COALESCE(NEW.match_score, match_score)),
                cosine_score   = GREATEST(cosine_score,   COALESCE(NEW.cosine_score, cosine_score)),
                combined_score = GREATEST(combined_score, COALESCE(NEW.combined_score, combined_score))
            WHERE id_job = existing_id;
            RAISE NOTICE 'APPEND: user=% added to job url=%', NEW.id_user[1], NEW.url;
            RETURN NULL;  -- annuler l'INSERT original (on a fait UPDATE)
        END IF;
    END IF;

    -- URL absente → INSERT normal
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Trigger BEFORE INSERT ────────────────────────────────────────────────────
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
    """
    Insère ou met à jour l'utilisateur avec les données structurées du CV.
    Retourne True si succès.

    Champs acceptés dans cv_structured :
      first_name, last_name, email, linkedin  (optionnels — profil utilisateur)
      role, seniority, years_experience, industry, education,
      skills, summary, bullets               (extraits du CV)
    """
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
    # COALESCE pour first_name/last_name/email/linkedin :
    # si la valeur entrante est NULL (non fournie), on conserve l'ancienne.
    # Ainsi un scan CV ne réinitialise pas le profil saisi manuellement.
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                user_id,                                          # $1  id
                cv_structured.get("first_name") or None,          # $2  first_name
                cv_structured.get("last_name")  or None,          # $3  last_name
                cv_structured.get("email")      or None,          # $4  email
                cv_structured.get("linkedin")   or None,          # $5  linkedin
                cv_structured.get("role", ""),                    # $6  role
                cv_structured.get("seniority", ""),               # $7  seniority
                cv_structured.get("years_experience", ""),        # $8  years_exp
                cv_structured.get("industry", ""),                # $9  industry
                cv_structured.get("education", ""),               # $10 education
                cv_structured.get("skills", ""),                  # $11 skills
                cv_structured.get("summary", ""),                 # $12 summary
                cv_structured.get("bullets", ""),                 # $13 bullets
            )
        logger.info(f"[db] ✅ User {user_id} upserted OK")
        return True
    except Exception as e:
        logger.error(f"[db] ❌ upsert_user failed for user_id={user_id}: {e}")
        return False


async def get_user(user_id: int) -> Optional[dict]:
    """
    Retourne le CV structuré de l'utilisateur ou None s'il n'existe pas.
    """
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
            # ── Profil utilisateur ────────────────────────────────────────
            "first_name":       row["first_name"],
            "last_name":        row["last_name"],
            "email":            row["email"],
            "linkedin":         row["linkedin"],
            # ── Données CV ────────────────────────────────────────────────
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
    """
    Met à jour uniquement les champs de profil (first_name, last_name, email, linkedin).
    N'écrase pas les données CV (role, skills, etc.).
    Utilisé depuis l'API /profile ou la page de paramètres.

    Exemple d'appel depuis main.py :
        await update_user_profile(1001, first_name="Alice", last_name="Dupont",
                                  email="alice@example.com", linkedin="linkedin.com/in/alice")
    """
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
        # asyncpg retourne "UPDATE N" — vérifier qu'au moins 1 ligne a été modifiée
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

async def insert_job(user_id: int, card: dict) -> bool:
    """
    Insère un job pour un utilisateur.
    Le trigger gère automatiquement :
      - SKIP si url + user_id déjà présents
      - APPEND si url présente mais user_id nouveau
      - INSERT si url absente

    FIX 1 : Vérification user_id valide avant d'appeler
    FIX 2 : Mapping correct des clés du card SSE vers les colonnes DB
             card SSE      → colonne DB
             "cosine"      → cosine_score   (et non "cosine_score")
             "experience"  → seniority      (niveau d'expérience requis)
             "tags"        → industry
             "skills_req"  → must_have + requirements
             "skills_bon"  → nice_to_have
             "gap_missing" → skills_gap (JSON)
    FIX 3 : match_score peut être -1.0 si pas de modèle → stocker NULL plutôt
    """
    logger.info(f"[db] insert_job called — user_id={user_id} url={card.get('url','')[:60]}")

    if not user_id or user_id <= 0:
        logger.warning("[db] insert_job skipped: user_id invalide")
        return False

    if not card.get("url"):
        logger.warning("[db] insert_job skipped: url vide")
        return False

    # Skills gap → JSON string
    gap_missing = card.get("gap_missing", [])
    skills_gap  = _json.dumps(gap_missing, ensure_ascii=False)

    # match_score = -1 si pas de modèle fine-tuné → stocker NULL en DB
    raw_match = card.get("match_score", -1)
    match_score_db: Optional[float] = None
    if raw_match is not None and raw_match >= 0:
        match_score_db = float(raw_match)

    # cosine → le card SSE utilise la clé "cosine" (et non "cosine_score")
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
            contract, education, remote, skills_gap
        ) VALUES (
            $1,  $2,  $3,  $4,  $5,  $6,
            $7,  $8,  $9,
            $10, $11, $12,
            $13, $14, $15, $16,
            $17, $18, $19, $20
        )
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                sql,
                [user_id],                                       # $1  id_user
                card.get("url", ""),                             # $2  url
                card.get("source", ""),                          # $3  source
                card.get("title", ""),                           # $4  title
                card.get("industry") or card.get("company", ""),# $5  industry = nom société
                card.get("location", ""),                        # $6  location
                card.get("experience", ""),                      # $7  seniority
                card.get("skills_req", ""),                      # $8  must_have
                card.get("skills_bon", ""),                      # $9  nice_to_have
                card.get("description", ""),                     # $10 description
                card.get("description", ""),                     # $11 responsibilities
                card.get("skills_req", ""),                      # $12 requirements
                card.get("salary", ""),                          # $13 salary
                match_score_db,                                  # $14 match_score
                cosine_score_db,                                 # $15 cosine_score
                combined_score_db,                               # $16 combined_score
                card.get("contract", ""),                        # $17 contract
                card.get("education", ""),                       # $18 education
                card.get("remote", ""),                          # $19 remote
                skills_gap,                                      # $20 skills_gap (JSON)
            )
        logger.info(f"[db] ✅ Job inserted/updated — user={user_id} url={card.get('url','')[:60]}")
        return True
    except Exception as e:
        logger.error(f"[db] ❌ insert_job failed — user={user_id} url={card.get('url','')[:60]}: {e}")
        return False


async def get_jobs_for_user(user_id: int) -> list[dict]:
    """
    Retourne tous les jobs où user_id est dans id_user[].
    Triés par combined_score DESC.
    """
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM jobs
                WHERE $1 = ANY(id_user)
                ORDER BY combined_score DESC NULLS LAST
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

            result.append({
                # ── Identifiants ──────────────────────────────────────────
                "id_job":         row["id_job"],
                "url":            row["url"],
                "source":         row["source"],
                # ── Infos job ─────────────────────────────────────────────
                "title":          row["title"],
                "industry":       row["industry"] or "",   # nom société → affiché "at Nearform"
                "location":       row["location"],
                "remote":         row["remote"],
                "salary":         row["salary"],
                "contract":       row["contract"],
                "education":      row["education"],
                "experience":     row["seniority"],
                # ── Scores ────────────────────────────────────────────────
                "match_score":         match_raw if match_raw is not None else -1,
                "cosine":              cosine_raw,
                "combined_score":      combined_raw,
                "match_score_display":    f"{(match_raw or 0) * 100:.2f}" if match_raw is not None and match_raw >= 0 else "—",
                "cosine_display":         f"{cosine_raw   * 100:.2f}",
                "combined_score_display": f"{combined_raw * 100:.2f}",
                # ── Skills gap ────────────────────────────────────────────
                "gap_missing":    gap,
                "gap_matched":    [],
                "gap_coverage":   1.0 if not gap else 0.0,
                "gap_total":      len(gap),
                # ── Détails ───────────────────────────────────────────────
                "description":    row["description"],
                "skills_req":     row["must_have"],
                "skills_bon":     row["nice_to_have"],
                "tags":           row["requirements"],      # tags frontend = colonne requirements DB
                # ── Compatibilité SSE frontend ────────────────────────────
                "event":          "job",
            })
        logger.info(f"[db] get_jobs_for_user: {len(result)} jobs for user_id={user_id}")
        return result
    except Exception as e:
        logger.error(f"[db] get_jobs_for_user failed for user_id={user_id}: {e}")
        return []


async def user_has_jobs(user_id: int) -> bool:
    """Vérifie si l'utilisateur a déjà des jobs en base."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE $1 = ANY(id_user)", user_id
            )
        has = (count or 0) > 0
        logger.info(f"[db] user_has_jobs: user_id={user_id} → {count} jobs")
        return has
    except Exception as e:
        logger.error(f"[db] user_has_jobs failed for user_id={user_id}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
#  Chat History (Career Assistant)
# ─────────────────────────────────────────────────────────────────────────────

async def save_chat_message(user_id: int, role: str, content: str) -> bool:
    """
    Sauvegarde un message dans l'historique de chat.
    role = "user" | "assistant"
    """
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
    """
    Retourne l'historique de chat d'un utilisateur (ordre chronologique).
    """
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
    