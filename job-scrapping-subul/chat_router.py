"""
chat_router.py — Module Chatbot IT JobScan
==========================================
Responsabilités :
  - System prompt expert IT avec profil + jobs réels (PostgreSQL)
  - POST /api/chat         → répondre via Azure OpenAI GPT-4o-mini
  - GET  /api/chat/history → retourner l'historique de conversation

Usage dans main.py :
    from chat_router import chat_router
    app.include_router(chat_router)
"""

import asyncio
import logging
import os
import time
from datetime import datetime

from fastapi import APIRouter
from openai import AsyncAzureOpenAI
from pydantic import BaseModel

# ── Détection de langue ───────────────────────────────────────────────────────
try:
    from langdetect import detect as _detect_lang
    _LANGDETECT_AVAILABLE = True
except ImportError:
    _LANGDETECT_AVAILABLE = False

# Short phrases: treat as this language when message is only this (or very short)
_COMMON_EN = frozenset((
    "hi", "hello", "hey", "good morning", "good afternoon", "good evening",
    "goodmorning", "goodnight", "thanks", "thank you", "ok", "okay", "yes", "no",
    "in english", "english please", "in english please", "answer in english",
    "respond in english", "reply in english", "i want answers in english",
))
_COMMON_FR = frozenset((
    "bonjour", "bonsoir", "salut", "coucou", "merci", "oui", "non", "ok", "d'accord",
    "bonne nuit", "bonne soirée", "bonne journée", "à bientôt", "s'il te plaît",
    "en français", "en francais", "en français s'il te plaît", "réponds en français",
))
# French sentence starters: if message begins with these, respond in French
_FR_STARTERS = (
    "je veux", "je voudrais", "comment ", "pourquoi ", "qu'est-ce", "c'est quoi",
    "quel est", "combien ", "est-ce que", "peux-tu", "pouvez-vous", "aide-moi",
    "aide moi", "dis-moi", "explique", "donne-moi", "je cherche", "j'aimerais",
)


def _detect_response_language(message: str) -> str:
    """
    Decide response language: "en" or "fr".
    - Explicit user request ("in english", "english please", "en français") wins.
    - Short messages use common phrase list, then langdetect.
    """
    msg = (message or "").strip()
    if not msg:
        return "fr"
    low = msg.lower().strip()
    # Explicit request for English
    if any(phrase in low for phrase in (
        "in english", "english please", "answer in english", "respond in english",
        "reply in english", "i want answers in english", "in english please",
    )):
        return "en"
    # Explicit request for French (e.g. "pourquoi tu ne réponds pas en français")
    if any(phrase in low for phrase in (
        "en français", "en francais", "réponds en français", "in french",
        "reponds en francais", "pas en français", "pas en francais",
    )):
        return "fr"
    # French sentence starters (e.g. "je veux savoir...", "comment apprendre...")
    if any(low.startswith(s) for s in _FR_STARTERS):
        return "fr"
    # Short message: common greetings so langdetect doesn't mis-detect
    if len(msg) <= 30:
        if low in _COMMON_EN:
            return "en"
        if any(low.startswith(p) for p in ("hi ", "hello ", "hey ", "good morning", "good afternoon", "good evening", "goodmorning ")):
            return "en"
        if low in _COMMON_FR or any(low.startswith(p) for p in ("bonjour", "bonsoir", "salut", "merci ")):
            return "fr"
    # Use langdetect
    if _LANGDETECT_AVAILABLE:
        try:
            code = _detect_lang(msg)
            return "fr" if code == "fr" else "en"
        except Exception:
            pass
    return "en"

# ── Imports DB ────────────────────────────────────────────────────────────────
from database import (
    get_user,
    get_jobs_for_user,
    save_chat_message as _save_chat_msg,
    load_chat_history as _load_chat_history,
)

logger      = logging.getLogger(__name__)
chat_router = APIRouter(tags=["Chatbot"])


# ═══════════════════════════════════════════════════════════════════════════════
#  Pydantic models
# ═══════════════════════════════════════════════════════════════════════════════

class ProfileIn(BaseModel):
    name:                str       = ""
    target_role:         str       = ""
    experience_years:    int       = 0
    skills:              list[str] = []
    preferred_locations: list[str] = []
    open_to_remote:      bool      = True
    salary_expectation:  str       = ""
    user_id:             str       = ""


class ChatIn(BaseModel):
    message: str
    profile: ProfileIn | None = None
    user_id: str = ""
    jobs_context: list[dict] = []  # jobs envoyés depuis le frontend (fallback si DB vide)


# ═══════════════════════════════════════════════════════════════════════════════
#  Azure OpenAI client factory
# ═══════════════════════════════════════════════════════════════════════════════

def _azure_client() -> AsyncAzureOpenAI:
    return AsyncAzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", ""),
        api_key        = os.getenv("AZURE_OPENAI_API_KEY", ""),
        api_version    = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  System Prompt — Expert IT
# ═══════════════════════════════════════════════════════════════════════════════

def _build_source_index(jobs: list[dict]) -> str:
    """Construit un index source → nb de jobs + titres pour que le LLM réponde
    exactement aux questions comme 'de quelle source viennent mes jobs ?'"""
    from collections import defaultdict
    src_map: dict = defaultdict(list)
    for j in jobs:
        src = (j.get("source") or "unknown").strip()
        src_map[src].append(j.get("title", "?"))
    lines = []
    for src, titles in sorted(src_map.items(), key=lambda x: -len(x[1])):
        sample = ", ".join(titles[:4])
        suffix = f"… +{len(titles)-4} autres" if len(titles) > 4 else ""
        lines.append(f"- **{src}** : {len(titles)} job(s) — ex: {sample}{suffix}")
    return "".join(lines) if lines else "Aucune source disponible."


def _build_location_index(jobs: list[dict]) -> str:
    """Construit un index location → liste de jobs pour aide le LLM à filtrer par ville."""
    from collections import defaultdict
    loc_map: dict = defaultdict(list)
    for j in jobs:
        loc = (j.get("location") or "Unknown").strip()
        loc_map[loc].append(j.get("title", "?"))
    lines = []
    for loc, titles in sorted(loc_map.items(), key=lambda x: -len(x[1])):
        lines.append(f"- {loc} ({len(titles)} jobs): {', '.join(titles[:5])}{'…' if len(titles)>5 else ''}")
    return "\n".join(lines) if lines else "Aucune localisation disponible."


def _build_chat_system_prompt(db_user: dict | None, db_jobs: list[dict]) -> str:
    """Construit le system prompt avec données réelles profil + jobs depuis PostgreSQL."""

    # ── Bloc profil ───────────────────────────────────────────────────────────
    if db_user:
        skills_str = db_user.get("skills", "") or "Non renseigné"
        user_block = f"""## PROFIL DE L'UTILISATEUR (PostgreSQL · table `users`)
- Nom        : {db_user.get('first_name', '')} {db_user.get('last_name', '')}
- Rôle cible : {db_user.get('role', 'N/A')}
- Séniorité  : {db_user.get('seniority', 'N/A')}
- Expérience : {db_user.get('years_experience', 'N/A')} ans
- Compétences: {skills_str}
- Résumé     : {db_user.get('summary', 'N/A')}"""
    else:
        user_block = "## PROFIL\nAucun profil trouvé en base."

    # ── Bloc jobs : TOUS les jobs, triés par AI match_score (score matching) ────
    # "score" affiché à l'utilisateur = match_score (AI BiEncoder), pas combined_score
    # combined_score n'est jamais affiché ni mentionné au LLM
    if db_jobs:
        def _norm_score(j: dict) -> float:
            """Normalise le score en 0.0-1.0.
            - match_score DB : FLOAT 0.0-1.0 (ou -1 si non calculé → traité comme 0)
            - total frontend : FLOAT 0.0-100.0
            """
            v = float(j.get("match_score") or j.get("total") or 0)
            if v < 0:    # match_score = -1 (non calculé en DB)
                v = 0.0
            return v / 100.0 if v > 1 else v

        # Trier par score DESC — les jobs avec match_score=-1 (non calculé)
        # sont normalisés à 0.0 et apparaissent EN FIN DE LISTE mais TOUJOURS présents.
        # IMPORTANT : tous les jobs sont inclus dans le prompt, aucun n'est écrêté.
        sorted_jobs = sorted(db_jobs, key=_norm_score, reverse=True)
        total  = len(sorted_jobs)
        scores = [_norm_score(j) for j in sorted_jobs]
        avg    = sum(scores) / total if total else 0

        best        = sorted_jobs[0]
        worst       = sorted_jobs[-1]
        best_score  = scores[0]
        worst_score = scores[-1]

        excellent = sum(1 for s in scores if s >= 0.70)
        good      = sum(1 for s in scores if 0.50 <= s < 0.70)
        moderate  = sum(1 for s in scores if 0.30 <= s < 0.50)
        low_score = sum(1 for s in scores if s < 0.30)

        def _fmt_job(idx: int, j: dict) -> str:
            # gap_missing : issu de skills_gap JSON en DB
            gap = j.get("gap_missing", j.get("missing", []))

            # gap_matched : N'EXISTE PAS en DB — calculé ici depuis must_have (skills_req) - gap_missing
            # get_jobs_for_user retourne gap_matched=[] toujours → on recalcule
            must_have_str  = j.get("skills_req") or j.get("must_have") or ""
            all_req        = [s.strip() for s in must_have_str.split(",") if s.strip()]
            gap_lower      = {g.lower() for g in gap}
            matched        = [s for s in all_req if s.lower() not in gap_lower]

            # Score : match_score (0.0-1.0 en DB, -1 si non calculé)
            raw_score = j.get("match_score") or j.get("total") or 0
            if raw_score is None or raw_score < 0:
                raw_score = 0.0
            ai_score = float(raw_score)
            if ai_score > 1:  # frontend jobs_context envoie déjà en %
                ai_score = ai_score / 100

            # cosine_score DB → retourné sous clé "cosine" par get_jobs_for_user (0.0-1.0)
            cosine = float(j.get("cosine", 0) or 0)
            if cosine > 1:  # sécurité si frontend envoie en %
                cosine = cosine / 100

            gt          = int(j.get("gap_total", len(all_req)) or 0)
            gc          = len(matched)
            miss        = ", ".join(gap) if gap else "aucun"
            matched_str = ", ".join(matched[:5]) if matched else "—"

            # industry DB = entreprise/secteur (pas de champ "company" dans la table jobs)
            company = j.get("industry") or j.get("company", "?")

            return (
                f"{idx}. [score:{ai_score*100:.1f}%] **{j.get('title','?')}** @ {company} | "
                f"loc:{j.get('location','?')} | remote:{j.get('remote','?')} | "
                f"cosine:{cosine*100:.1f}% | "
                f"skills:{gc}/{gt} matched:[{matched_str}] missing:[{miss}] | "
                f"salary:{j.get('salary','Non spécifié')} | "
                f"contract:{j.get('contract','?')} | seniority:{j.get('experience','?')} | "
                f"source:{j.get('source','?')} | url:{j.get('url','')}"
            )

        all_lines = [_fmt_job(i + 1, j) for i, j in enumerate(sorted_jobs)]

        jobs_block = (
            f"## JOBS DÉTECTÉS — {total} jobs au total\n"
            f"(triés par SCORE MATCHING = AI BiEncoder match_score, du meilleur au moins bon)\n\n"
            f"### 📊 STATISTIQUES (basées sur le score matching AI)\n"
            f"- 🏆 MEILLEUR SCORE : [{best_score*100:.1f}%] **{best.get('title','?')}** @ {best.get('industry','?')} | {best.get('location','?')} | url:{best.get('url','')}\n"
            f"- 📉 SCORE LE PLUS BAS : [{worst_score*100:.1f}%] **{worst.get('title','?')}** @ {worst.get('industry','?')} | {worst.get('location','?')} | url:{worst.get('url','')}\n"
            f"- 📈 MOYENNE  : {avg*100:.1f}%\n"
            f"- Distribution : ≥70%→{excellent} | 50-70%→{good} | 30-50%→{moderate} | <30%→{low_score}\n\n"
            f"### 🌐 INDEX PAR SOURCE DE SCRAPING\n"
            + _build_source_index(sorted_jobs)
            + f"\n\n### 📍 INDEX PAR LOCALISATION\n"
            + _build_location_index(sorted_jobs)
            + f"\n\n### 📋 LISTE COMPLÈTE ({total} jobs)\n"
            + "\n".join(all_lines)
        )
    else:
        jobs_block = "## JOBS DÉTECTÉS\nAucun job trouvé pour cet utilisateur."

    return f"""Tu es **JobScan AI**, un assistant expert en IT et carrière tech, intégré dans la plateforme JobScan.

{user_block}

{jobs_block}

---

## COMPORTEMENT GÉNÉRAL

### ✅ SALUTATIONS & CONVERSATION
Si l'utilisateur dit "hi", "hello", "bonjour", "salut", "hey" ou toute salutation →
**Réponds chaleureusement** : salue-le, présente-toi brièvement, propose ton aide.
Exemple : "Bonjour ! Je suis JobScan AI, ton assistant carrière IT. Comment puis-je t'aider aujourd'hui ?"
**NE JAMAIS refuser une salutation.**

### ✅ QUESTIONS SUR LES JOBS
Si l'utilisateur pose une question sur ses jobs →
**Utilise UNIQUEMENT les données exactes fournies ci-dessus.**

⚠️ SOURCES DE SCRAPING :
- Les sources sont listées dans l'**INDEX PAR SOURCE DE SCRAPING** ci-dessus
- Pour toute question sur les sources ("d'où viennent mes jobs ?", "quelles sources ?") →
  recopie EXACTEMENT les sources listées dans l'index (ex: emploitic, greenhouse, aijobs, remoteok, tanitjobs…)
- NE JAMAIS inventer ou deviner des sources non présentes dans l'index
- Si une source est présente dans l'index → elle EXISTE, même si l'utilisateur conteste

⚠️ DÉFINITION DU SCORE :
- "score", "score matching", "match score" = TOUJOURS le champ `score:XX%` de la liste
- Ce score = AI BiEncoder (match_score) UNIQUEMENT
- Ne jamais mentionner "combined score" comme étant "le score"
- Le cosine (`cosine:XX%`) est différent — ne pas le confondre avec le score matching
- MEILLEUR score = job #1 en haut de la liste
- PLUS BAS score = dernier job en bas de la liste
- Recopier le score EXACTEMENT tel qu'écrit (ex: 9.6%, 3.2%, 50.0%) sans arrondir

### ✅ QUESTIONS IT GÉNÉRALES
Réponds à TOUTES les questions liées à l'informatique, la technologie et la carrière tech :
Hardware, Software, Réseaux, Développement, Cloud, DevOps, IA, Cybersécurité, Carrière IT...

---

## TON DOMAINE — L'INFORMATIQUE AU SENS LARGE

**Hardware & Matériel :** PC, ordinateur, laptop, disque dur, SSD, RAM, processeur, CPU, GPU,
carte graphique, carte mère, écran, clavier, souris, serveur, datacenter, smartphone, tablette,
routeur, switch, modem...

**Systèmes & Software :** Windows, Linux, macOS, Ubuntu, Android, iOS, OS, logiciel, application,
driver, firmware, antivirus, mise à jour, virtualisation, VM...

**Réseaux & Internet :** HTTP, HTTPS, DNS, TCP/IP, VPN, WiFi, Ethernet, protocole, pare-feu,
SSL, TLS, SSH, FTP, proxy, navigateur...

**Développement & Code :** Python, JavaScript, TypeScript, Java, C, C++, C#, Go, Rust, PHP,
SQL, Bash, API, REST, GraphQL, Git, GitHub, algorithme, debug, IDE...

**Cloud & DevOps :** Azure, AWS, GCP, Docker, Kubernetes, Terraform, CI/CD, pipeline,
microservices, serverless, Nginx, monitoring...

**IA & Data Science :** LLM, GPT, machine learning, deep learning, NLP, TensorFlow, PyTorch,
Pandas, NumPy, Spark, ETL, dataset, modèle, inférence...

**Cybersécurité :** virus, malware, phishing, chiffrement, OAuth, JWT, pentest, OWASP, firewall...

**Carrière IT :** développeur, ingénieur, DevOps, data scientist, salaire, entretien technique, CV...

---

### ❌ REFUSE SEULEMENT (clairement hors IT) :
Recettes de cuisine · Sport · Médecine · Politique · Météo · Tourisme · Animaux · Physique non-informatique

### ⚠️ RÈGLE D'OR : EN CAS DE DOUTE → RÉPONDS

---

## RÈGLES DE RÉPONSE
1. **Réponds dans la langue de l'utilisateur** (français, anglais, arabe...)
2. **Utilise le markdown** : titres, listes, blocs de code, gras
3. **Jobs : cite les données EXACTES** — recopie le score, la source, la localisation tels quels depuis les index et la liste (ex: 9.6%, 3.2%, 18.5%), NE JAMAIS arrondir à 0% ou 10%
4. **Score minimum** = le dernier job de la liste (index le plus élevé) — c'est lui le plus bas, pas le premier
5. **Ne jamais dire qu'un job n'existe pas** si tu ne le trouves pas — dis plutôt "je ne vois pas ce job dans les données actuelles"
6. **Réponds en expert** : précis, concret, exemples de code si utile
7. **Jamais d'inventions** : si une info manque, dis-le clairement

Date : {datetime.now().strftime('%Y-%m-%d')}"""


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers internes
# ═══════════════════════════════════════════════════════════════════════════════

async def _safe_save_msg(uid: int, role: str, content: str):
    """Sauvegarde non-bloquante d'un message chat en DB."""
    try:
        await _save_chat_msg(uid, role, content)
    except Exception as e:
        logger.warning(f"[chat] save_msg failed: {e}")


async def _auto_summarize(uid: int, history: list, deployment: str):
    """
    Résumé automatique si historique > 8 messages.
    Compresse les vieux échanges en 3 lignes pour économiser les tokens.
    """
    try:
        convo = "\n".join(
            [f"{m['role'].upper()}: {m['content'][:200]}" for m in history[-8:]]
        )
        prompt = (
            "Résume en 3 lignes maximum les sujets IT abordés dans "
            "cette conversation JobScan :\n\n"
            f"{convo}\n\nRéponds uniquement avec le résumé, sans introduction."
        )
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model       = deployment,
                messages    = [{"role": "user", "content": prompt}],
                max_tokens  = 150,
                temperature = 0.1,
            )
        summary = resp.choices[0].message.content or ""
        if summary:
            await _save_chat_msg(uid, "assistant", f"[RÉSUMÉ SESSION] {summary}")
            logger.info(f"[chat] auto-summary saved for user={uid}")
    except Exception as e:
        logger.warning(f"[chat] auto_summarize failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
#  Routes FastAPI
# ═══════════════════════════════════════════════════════════════════════════════

@chat_router.post("/api/chat")
async def api_chat(data: ChatIn):
    """
    Chatbot IT JobScan :
    ✅ asyncio.gather()  — user + jobs + historique chargés en parallèle
    ✅ langdetect        — détection langue automatique
    ✅ auto-summarize    — résumé si historique > 8 messages
    ✅ timeout 8s        — évite les blocages DB
    ✅ save non-bloquant — create_task pour ne pas ralentir la réponse
    """
    t0 = time.time()

    # ── Résoudre user_id ──────────────────────────────────────────────────────
    uid_str = data.user_id or (data.profile.user_id if data.profile else "")
    uid = 0
    if uid_str:
        try:
            uid = int(uid_str)
        except Exception:
            pass

    # ── Détection langue (texte) : répondre en français ou en anglais selon la question
    detected_lang = _detect_response_language(data.message or "")

    # ── Chargement parallèle depuis DB ────────────────────────────────────────
    async def _get_user():
        return await get_user(uid) if uid > 0 else None

    async def _get_jobs():
        return await get_jobs_for_user(uid) if uid > 0 else []

    async def _get_history():
        if uid > 0:
            try:
                return await _load_chat_history(uid, limit=20)
            except Exception:
                return []
        return []

    try:
        db_user, db_jobs, history = await asyncio.wait_for(
            asyncio.gather(_get_user(), _get_jobs(), _get_history()),
            timeout=8.0
        )
    except asyncio.TimeoutError:
        logger.warning("[chat] DB timeout — réponse sans contexte")
        db_user, db_jobs, history = None, [], []
    except Exception as e:
        logger.error(f"[chat] gather error: {e}")
        db_user, db_jobs, history = None, [], []

    # ── Fallback : si DB vide, utiliser les jobs envoyés par le frontend ─────
    if not db_jobs and data.jobs_context:
        logger.info(f"[chat] DB empty — using {len(data.jobs_context)} jobs from frontend context")
        db_jobs = data.jobs_context

    logger.info(
        f"[chat] user={uid} lang={detected_lang} "
        f"jobs={len(db_jobs)} history={len(history)} "
        f"prep={time.time()-t0:.2f}s"
    )

    # ── Sauvegarder message user (non bloquant) ───────────────────────────────
    if uid > 0:
        asyncio.create_task(_safe_save_msg(uid, "user", data.message))

    # ── Construire les messages pour le LLM ───────────────────────────────────
    # Strong language rule so the model follows it even when history is in the other language
    if detected_lang == "fr":
        lang_rule = (
            "\n\n[RÈGLE OBLIGATOIRE : Réponds UNIQUEMENT en français. "
            "L'utilisateur a écrit en français. N'utilise pas l'anglais.]"
        )
    else:
        lang_rule = (
            "\n\n[MANDATORY RULE: Respond ONLY in English. "
            "The user wrote in English. Do not use French.]"
        )
    system_prompt = _build_chat_system_prompt(db_user, db_jobs)
    # CRITICAL: Put language instruction FIRST so the model follows it (system prompt is mostly FR)
    if detected_lang == "fr":
        lang_block = (
            "CRITIQUE — LANGUE : Tu DOIS répondre UNIQUEMENT en français. "
            "L'utilisateur a écrit en français. N'utilise pas l'anglais dans ta réponse.\n\n"
        )
    else:
        lang_block = (
            "CRITICAL — LANGUAGE: You MUST respond ONLY in English. "
            "The user wrote in English. Do not use French in your response.\n\n"
        )
    system_prompt = lang_block + system_prompt.rstrip()
    messages_payload = [{"role": "system", "content": system_prompt}]
    for h in history[-16:]:
        messages_payload.append({"role": h["role"], "content": h["content"]})
    messages_payload.append({
        "role": "user",
        "content": (data.message or "").strip() + lang_rule
    })

    # ── Appel Azure OpenAI ────────────────────────────────────────────────────
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o-mini")
    response   = ""
    try:
        async with _azure_client() as az:
            resp = await az.chat.completions.create(
                model       = deployment,
                messages    = messages_payload,
                max_tokens  = 1500,
                temperature = 0.3,
            )
        response = resp.choices[0].message.content or ""
    except Exception as e:
        logger.error(f"[chat] LLM error: {e}")
        response = f"⚠️ Erreur LLM : {str(e)}"

    # ── Sauvegarder réponse + résumé auto (non bloquant) ─────────────────────
    if uid > 0 and response:
        asyncio.create_task(_safe_save_msg(uid, "assistant", response))
        if len(history) >= 8:
            asyncio.create_task(_auto_summarize(uid, history, deployment))

    return {"response": response, "intent": "llm", "jobs_count": len(db_jobs)}


@chat_router.get("/api/chat/history")
async def api_chat_history(user_id: str = ""):
    """Retourne l'historique complet de chat d'un utilisateur depuis PostgreSQL."""
    if not user_id:
        return {"messages": []}
    try:
        msgs = await _load_chat_history(int(user_id))
    except Exception:
        msgs = []
    return {"messages": msgs}