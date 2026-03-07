"""
voice_router.py — Module Voice (STT + TTS) pour JobScan
========================================================
Responsabilités :
  - POST /api/voice/stt  → Audio blob → Deepgram Nova-2 STT → { text }
  - POST /api/voice/tts  → { text, lang } → Deepgram Aura TTS → audio/mp3

Deepgram SDK : pip install deepgram-sdk
Variable d'environnement requise : DEEPGRAM_API_KEY

Usage dans main.py :
    from voice_router import voice_router
    app.include_router(voice_router)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

logger       = logging.getLogger(__name__)
voice_router = APIRouter(tags=["Voice"])

# ═══════════════════════════════════════════════════════════════════════════════
#  Constantes Deepgram
# ═══════════════════════════════════════════════════════════════════════════════

DEEPGRAM_API_URL_STT = "https://api.deepgram.com/v1/listen"
DEEPGRAM_API_URL_TTS = "https://api.deepgram.com/v1/speak"

# ──────────────────────────────────────────────────────────────────────
# 🗣️  MODÈLES DEEPGRAM AURA-2 PAR LANGUE
#
# ⚠️  RÈGLE CRITIQUE : le paramètre &language= dans l'URL est OBLIGATOIRE
#     Sans lui → Deepgram utilise une voix anglaise qui "lit" le français
#     → accent anglais même si le texte est en français !
#
# Format URL correct :
#   ?model=aura-2-andromeda-en&language=fr   ← accent français natif
#   ?model=aura-2-thalia-en&language=en      ← accent anglais natif
# ──────────────────────────────────────────────────────────────────────
VOICE_BY_LANG: dict[str, dict] = {
    "fr": {
        "model":    "aura-2-andromeda-en",  # voix féminine claire
        "language": "fr",                    # ← CLEF : accent français natif
    },
    "en": {
        "model":    "aura-2-thalia-en",     # voix féminine professionnelle
        "language": "en",
    },
    "ar": {
        "model":    "aura-2-thalia-en",     # fallback anglais pour l'arabe
        "language": "en",
    },
    "default": {
        "model":    "aura-2-andromeda-en",
        "language": "fr",
    },
}

# Modèle STT Deepgram le plus précis pour le multilingual
STT_MODEL = "nova-2"

# Taille max audio acceptée : 25 Mo
MAX_AUDIO_BYTES = 25 * 1024 * 1024


# ═══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _deepgram_key() -> str:
    """Récupère la clé Deepgram depuis les variables d'environnement."""
    key = os.getenv("DEEPGRAM_API_KEY", "").strip()
    if not key:
        raise HTTPException(
            status_code=500,
            detail="DEEPGRAM_API_KEY manquante — ajoutez-la dans votre fichier .env"
        )
    return key


def _voice_for_lang(lang: str) -> dict:
    """Retourne la config voix Deepgram (model + language) adaptée à la langue."""
    return VOICE_BY_LANG.get(lang.lower(), VOICE_BY_LANG["default"])


# ═══════════════════════════════════════════════════════════════════════════════
#  Pydantic models
# ═══════════════════════════════════════════════════════════════════════════════

class TTSRequest(BaseModel):
    text: str
    lang: str = "fr"   # langue détectée côté frontend (fr | en | ar)


# ═══════════════════════════════════════════════════════════════════════════════
#  Route STT — Speech-to-Text
# ═══════════════════════════════════════════════════════════════════════════════

@voice_router.post("/api/voice/stt")
async def api_voice_stt(
    audio: UploadFile = File(...),
    lang:  str        = Form(default="fr"),
):
    """
    Convertit un enregistrement audio en texte via Deepgram Nova-2.

    - Accepte : audio/webm, audio/wav, audio/mp4, audio/ogg, audio/mpeg
    - Retourne : { "text": "..." , "confidence": 0.99 }
    - Modèle   : nova-2 (meilleure précision multilingue)
    - Si Deepgram ne peut pas transcrire → { "text": "" }
    """
    api_key = _deepgram_key()

    # ── Lire le contenu audio ─────────────────────────────────────────────────
    audio_bytes = await audio.read()
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="Fichier audio vide.")
    if len(audio_bytes) > MAX_AUDIO_BYTES:
        raise HTTPException(status_code=413, detail="Fichier audio trop grand (max 25 Mo).")

    content_type = audio.content_type or "audio/webm"
    logger.info(
        f"[voice/stt] user audio: {len(audio_bytes)//1024}KB "
        f"type={content_type} lang={lang}"
    )

    # ── Paramètres Deepgram STT ───────────────────────────────────────────────
    params: dict[str, str] = {
        "model":            STT_MODEL,
        "smart_format":     "true",   # ponctuation + majuscules auto
        "punctuate":        "true",
        "utterances":       "false",
        "filler_words":     "false",  # supprimer "euh", "hmm"
        "detect_language":  "true",   # ← TOUJOURS actif : Deepgram détecte fr/en
    }

    # On donne un hint de langue mais detect_language reste actif
    # Deepgram choisit la meilleure langue entre le hint et ce qu'il entend
    if lang in ("fr", "en"):
        params["language"] = lang

    # ── Appel API Deepgram ────────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                DEEPGRAM_API_URL_STT,
                params  = params,
                headers = {
                    "Authorization": f"Token {api_key}",
                    "Content-Type":  content_type,
                },
                content = audio_bytes,
            )
    except httpx.TimeoutException:
        logger.error("[voice/stt] Deepgram timeout")
        raise HTTPException(status_code=504, detail="Timeout Deepgram STT — réessayez.")
    except Exception as e:
        logger.error(f"[voice/stt] httpx error: {e}")
        raise HTTPException(status_code=502, detail=f"Erreur réseau Deepgram: {e}")

    if resp.status_code != 200:
        logger.error(f"[voice/stt] Deepgram error {resp.status_code}: {resp.text[:300]}")
        raise HTTPException(
            status_code=502,
            detail=f"Deepgram STT a retourné {resp.status_code}: {resp.text[:200]}"
        )

    # ── Parser la réponse JSON ────────────────────────────────────────────────
    try:
        data       = resp.json()
        channel    = data["results"]["channels"][0]
        alt        = channel["alternatives"][0]
        transcript = alt.get("transcript", "").strip()
        confidence = alt.get("confidence", 0.0)
        # Langue détectée par Deepgram (ex: "fr", "en") — renvoyée au frontend
        # Le frontend l'utilise pour choisir la voix TTS avec le bon accent
        detected_language = channel.get("detected_language", lang or "fr")
    except (KeyError, IndexError, ValueError) as e:
        logger.warning(f"[voice/stt] Parse error: {e} | raw: {resp.text[:300]}")
        transcript, confidence, detected_language = "", 0.0, lang or "fr"

    logger.info(
        f"[voice/stt] transcribed={repr(transcript[:80])} "
        f"confidence={confidence:.2f} lang={detected_language}"
    )

    return JSONResponse({
        "text":               transcript,
        "confidence":         round(confidence, 3),
        "detected_language":  detected_language,   # ← "fr" ou "en"
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  Route TTS — Text-to-Speech
# ═══════════════════════════════════════════════════════════════════════════════

@voice_router.post("/api/voice/tts")
async def api_voice_tts(req: TTSRequest):
    """
    Convertit du texte en audio via Deepgram Aura TTS.

    - Entrée  : { text, lang }
    - Retourne: audio/mp3 (bytes)
    - Voix    : aura-luna-en (fr) | aura-asteria-en (en/default)
    - Le texte est nettoyé (markdown retiré) avant envoi à Deepgram.
    """
    api_key = _deepgram_key()

    # ── Nettoyer le markdown du texte LLM ────────────────────────────────────
    # Deepgram TTS lit mieux un texte plain — on enlève **, ##, bullets, etc.
    import re
    clean_text = req.text
    clean_text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", clean_text)     # **bold** → bold
    clean_text = re.sub(r"#{1,6}\s*(.+)",        r"\1", clean_text)     # ## Titre → Titre
    clean_text = re.sub(r"```[\s\S]*?```",        "",    clean_text)     # blocs code → vide
    clean_text = re.sub(r"`(.+?)`",              r"\1", clean_text)     # `code` → code
    clean_text = re.sub(r"^\s*[-*•]\s+",         "",    clean_text, flags=re.MULTILINE)  # bullets
    clean_text = re.sub(r"\n{3,}",              "\n\n", clean_text)     # triple saut → double
    clean_text = clean_text.strip()

    # Limiter la longueur (Deepgram TTS max ~2000 chars recommandé)
    if len(clean_text) > 2000:
        clean_text = clean_text[:1950] + "…"

    if not clean_text:
        raise HTTPException(status_code=400, detail="Texte vide après nettoyage.")

    voice_cfg      = _voice_for_lang(req.lang)
    voice_model    = voice_cfg["model"]
    voice_language = voice_cfg["language"]

    logger.info(
        f"[voice/tts] text_len={len(clean_text)} "
        f"lang={req.lang} model={voice_model} language={voice_language}"
    )

    # ── Appel API Deepgram TTS ────────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                DEEPGRAM_API_URL_TTS,
                # ✅ model= ET language= sont TOUS LES DEUX obligatoires
                # Sans language=fr → voix anglaise qui lit du français (mauvais accent)
                params  = {"model": voice_model, "language": voice_language},
                headers = {
                    "Authorization": f"Token {api_key}",
                    "Content-Type":  "application/json",
                },
                json    = {"text": clean_text},
            )
    except httpx.TimeoutException:
        logger.error("[voice/tts] Deepgram timeout")
        raise HTTPException(status_code=504, detail="Timeout Deepgram TTS — réessayez.")
    except Exception as e:
        logger.error(f"[voice/tts] httpx error: {e}")
        raise HTTPException(status_code=502, detail=f"Erreur réseau Deepgram TTS: {e}")

    if resp.status_code != 200:
        logger.error(f"[voice/tts] Deepgram error {resp.status_code}: {resp.text[:300]}")
        raise HTTPException(
            status_code=502,
            detail=f"Deepgram TTS a retourné {resp.status_code}: {resp.text[:200]}"
        )

    audio_bytes = resp.content
    logger.info(f"[voice/tts] audio generated: {len(audio_bytes)//1024}KB")

    return Response(
        content      = audio_bytes,
        media_type   = "audio/mp3",
        headers      = {
            "Content-Disposition": "inline; filename=response.mp3",
            "Cache-Control":       "no-cache",
        },
    )