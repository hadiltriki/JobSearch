/**
 * useVoice.ts — Hook React pour Voice Input/Output (STT + TTS)
 * =============================================================
 *
 * Fonctionnalités :
 *   - Capture micro via MediaRecorder (Web Audio API)
 *   - VAD (Voice Activity Detection) : si silence ≥ 3s → arrêt auto
 *   - Envoi audio → /api/voice/stt → transcription Deepgram Nova-2
 *   - Lecture réponse LLM → /api/voice/tts → audio Deepgram Aura
 *
 * États voice :
 *   idle       → pas d'activité voice
 *   listening  → micro ouvert, en attente de parole
 *   processing → audio envoyé à Deepgram STT, en attente transcription
 *   speaking   → lecture TTS de la réponse LLM
 *   error      → erreur micro ou réseau
 *
 * Usage dans ChatSidebar :
 *   const { voiceState, startListening, stopListening, speakText, cancelSpeech } = useVoice({
 *     onTranscript: (text) => { setInput(text); sendMessage(text); },
 *     lang: "fr",
 *   });
 */

"use client";

/// <reference types="react" />
import { useRef, useState, useCallback, useEffect } from "react";

// ─────────────────────────────────────────────────────────────────────────────
//  Types
// ─────────────────────────────────────────────────────────────────────────────

export type VoiceState = "idle" | "listening" | "processing" | "speaking" | "error";

export interface UseVoiceOptions {
  /**
   * Appelé avec (text, detectedLang) après transcription Deepgram STT.
   * detectedLang = "fr" | "en" selon la langue que Deepgram a reconnue.
   */
  onTranscript: (text: string, detectedLang: string) => void;
  /** Langue par défaut si Deepgram ne détecte rien — défaut : "fr" */
  defaultLang?: string;
  /** Durée silence (ms) avant arrêt auto — défaut : 2000ms */
  silenceThresholdMs?: number;
  /** Seuil RMS en dessous duquel on considère le silence — défaut : 0.012 */
  silenceRmsThreshold?: number;
}

export interface UseVoiceReturn {
  voiceState:     VoiceState;
  voiceError:     string;
  detectedLang:   string;               // langue détectée lors du dernier STT
  startListening: () => Promise<void>;
  stopListening:  () => void;
  /** speakText(text, lang) — lang = "fr"|"en", accent Deepgram adapté */
  speakText:      (text: string, lang?: string) => Promise<void>;
  cancelSpeech:   () => void;
}

// ─────────────────────────────────────────────────────────────────────────────
//  Hook principal
// ─────────────────────────────────────────────────────────────────────────────

export function useVoice({
  onTranscript,
  defaultLang         = "fr",
  silenceThresholdMs  = 2000,
  silenceRmsThreshold = 0.012,
}: UseVoiceOptions): UseVoiceReturn {

  const [voiceState,    setVoiceState]    = useState<VoiceState>("idle");
  const [voiceError,    setVoiceError]    = useState<string>("");
  // Langue détectée par Deepgram lors du dernier STT — utilisée pour le TTS
  const [detectedLang,  setDetectedLang]  = useState<string>(defaultLang);

  // Refs audio (pas de re-render nécessaire)
  const mediaRecorderRef  = useRef<MediaRecorder | null>(null);
  const audioChunksRef    = useRef<Blob[]>([]);
  const streamRef         = useRef<MediaStream | null>(null);
  const audioCtxRef       = useRef<AudioContext | null>(null);
  const analyserRef       = useRef<AnalyserNode | null>(null);
  const silenceTimerRef   = useRef<ReturnType<typeof setTimeout> | null>(null);
  const vadRafRef         = useRef<number | null>(null);
  const isRecordingRef    = useRef(false);
  const ttsAudioRef       = useRef<HTMLAudioElement | null>(null);
  const ttsObjectUrlRef   = useRef<string | null>(null);

  // Nettoyer les ressources audio au démontage
  useEffect(() => {
    return () => {
      _cleanupRecording();
      cancelSpeech();
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Nettoyage enregistrement ─────────────────────────────────────────────

  const _cleanupRecording = useCallback(() => {
    // Arrêter le VAD loop
    if (vadRafRef.current) {
      cancelAnimationFrame(vadRafRef.current);
      vadRafRef.current = null;
    }
    // Annuler le timer silence
    if (silenceTimerRef.current) {
      clearTimeout(silenceTimerRef.current);
      silenceTimerRef.current = null;
    }
    // Fermer AudioContext
    if (audioCtxRef.current && audioCtxRef.current.state !== "closed") {
      audioCtxRef.current.close().catch(() => {});
      audioCtxRef.current = null;
      analyserRef.current = null;
    }
    // Stopper le stream micro
    if (streamRef.current) {
      streamRef.current.getTracks().forEach((t: MediaStreamTrack) => t.stop());
      streamRef.current = null;
    }
    isRecordingRef.current = false;
  }, []);

  // ── VAD : Voice Activity Detection via Web Audio API ────────────────────

  const _startVAD = useCallback((stream: MediaStream) => {
    try {
      const audioCtx  = new AudioContext();
      const analyser  = audioCtx.createAnalyser();
      analyser.fftSize = 256;

      const source = audioCtx.createMediaStreamSource(stream);
      source.connect(analyser);

      audioCtxRef.current = audioCtx;
      analyserRef.current = analyser;

      const dataArray = new Float32Array(analyser.fftSize);
      let lastSoundAt = Date.now();

      const tick = () => {
        if (!isRecordingRef.current) return;

        analyser.getFloatTimeDomainData(dataArray);

        // Calcul RMS (Root Mean Square) = volume
        let sumSq = 0;
        for (let i = 0; i < dataArray.length; i++) {
          sumSq += dataArray[i] * dataArray[i];
        }
        const rms = Math.sqrt(sumSq / dataArray.length);

        if (rms > silenceRmsThreshold) {
          // Son détecté → reset timer silence
          lastSoundAt = Date.now();
          if (silenceTimerRef.current) {
            clearTimeout(silenceTimerRef.current);
            silenceTimerRef.current = null;
          }
        } else {
          // Silence → vérifier durée
          const silenceDuration = Date.now() - lastSoundAt;
          if (silenceDuration >= silenceThresholdMs && !silenceTimerRef.current) {
            // 3 secondes de silence → arrêt automatique
            silenceTimerRef.current = setTimeout(() => {
              if (isRecordingRef.current) {
                stopListening();
              }
            }, 50); // petit délai pour éviter les faux positifs
          }
        }

        vadRafRef.current = requestAnimationFrame(tick);
      };

      vadRafRef.current = requestAnimationFrame(tick);
    } catch (err) {
      console.warn("[useVoice] VAD init failed (non-fatal):", err);
      // VAD optionnel — si échec, l'utilisateur arrête manuellement
    }
  }, [silenceThresholdMs, silenceRmsThreshold]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Envoi audio vers Deepgram STT ────────────────────────────────────────

  const _transcribeAudio = useCallback(async (chunks: Blob[], mimeType: string) => {
    setVoiceState("processing");

    const audioBlob = new Blob(chunks, { type: mimeType });
    if (audioBlob.size < 1000) {
      // Audio trop court → ignorer (probablement pas de vraie parole)
      setVoiceState("idle");
      return;
    }

    const formData = new FormData();
    const ext = mimeType.includes("mp4") ? "mp4"
              : mimeType.includes("ogg")  ? "ogg"
              : mimeType.includes("wav")  ? "wav"
              : "webm";
    formData.append("audio", audioBlob, `recording.${ext}`);
    // On envoie defaultLang comme hint mais Deepgram auto-détecte aussi
    formData.append("lang", defaultLang);

    try {
      const resp = await fetch("/api/voice/stt", {
        method: "POST",
        body:   formData,
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
        throw new Error(err.detail || `STT error ${resp.status}`);
      }

      const data = await resp.json() as { text: string; confidence: number; detected_language?: string };

      if (data.text && data.text.trim()) {
        // Langue détectée par Deepgram STT (ex: "fr", "en")
        // Si Deepgram ne renvoie pas detected_language → garder defaultLang
        const lang = data.detected_language || defaultLang;
        setDetectedLang(lang);
        setVoiceState("idle");
        onTranscript(data.text.trim(), lang);  // ← on passe aussi la langue
      } else {
        setVoiceState("idle");
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.error("[useVoice] STT error:", message);
      setVoiceError(message);
      setVoiceState("error");
      // Reset erreur après 4 secondes
      setTimeout(() => { setVoiceState("idle"); setVoiceError(""); }, 4000);
    }
  }, [defaultLang, onTranscript]);

  // ── startListening ───────────────────────────────────────────────────────

  const startListening = useCallback(async () => {
    if (voiceState !== "idle" && voiceState !== "error") return;

    // Annuler TTS en cours si l'utilisateur reprend la parole
    cancelSpeech();

    setVoiceError("");

    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          sampleRate:       16000,  // optimal pour Deepgram Nova-2
        }
      });

      streamRef.current     = stream;
      audioChunksRef.current = [];
      isRecordingRef.current = true;

      // Choisir le meilleur format supporté par le navigateur
      const mimeType = [
        "audio/webm;codecs=opus",
        "audio/webm",
        "audio/ogg;codecs=opus",
        "audio/mp4",
      ].find(m => MediaRecorder.isTypeSupported(m)) || "";

      const recorder = new MediaRecorder(stream, mimeType ? { mimeType } : undefined);
      mediaRecorderRef.current = recorder;

      recorder.ondataavailable = (e) => {
        if (e.data && e.data.size > 0) {
          audioChunksRef.current.push(e.data);
        }
      };

      recorder.onstop = async () => {
        _cleanupRecording();
        const chunks   = audioChunksRef.current;
        const finalMime = mimeType || recorder.mimeType || "audio/webm";
        await _transcribeAudio(chunks, finalMime);
      };

      recorder.onerror = (e) => {
        console.error("[useVoice] MediaRecorder error:", e);
        setVoiceError("Erreur enregistrement audio");
        setVoiceState("error");
        _cleanupRecording();
        setTimeout(() => { setVoiceState("idle"); setVoiceError(""); }, 3000);
      };

      // Collecte les données toutes les 250ms pour un meilleur streaming
      recorder.start(250);
      setVoiceState("listening");

      // Démarrer le VAD
      _startVAD(stream);

    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      let userMessage = "Impossible d'accéder au microphone.";

      if (message.includes("NotAllowedError") || message.includes("Permission")) {
        userMessage = "Permission microphone refusée. Autorisez l'accès dans votre navigateur.";
      } else if (message.includes("NotFoundError")) {
        userMessage = "Aucun microphone détecté.";
      }

      setVoiceError(userMessage);
      setVoiceState("error");
      setTimeout(() => { setVoiceState("idle"); setVoiceError(""); }, 4000);
    }
  }, [voiceState, _cleanupRecording, _startVAD, _transcribeAudio]);

  // ── stopListening ────────────────────────────────────────────────────────

  const stopListening = useCallback(() => {
    if (!isRecordingRef.current) return;

    // Stopper le MediaRecorder → déclenche recorder.onstop → _transcribeAudio
    if (mediaRecorderRef.current && mediaRecorderRef.current.state !== "inactive") {
      mediaRecorderRef.current.stop();
    }

    // Le cleanup complet se fait dans recorder.onstop → _cleanupRecording
    // On met à jour l'état visuel immédiatement
    if (vadRafRef.current) {
      cancelAnimationFrame(vadRafRef.current);
      vadRafRef.current = null;
    }
    if (silenceTimerRef.current) {
      clearTimeout(silenceTimerRef.current);
      silenceTimerRef.current = null;
    }
  }, []);

  // ── cancelSpeech ─────────────────────────────────────────────────────────

  const cancelSpeech = useCallback(() => {
    if (ttsAudioRef.current) {
      ttsAudioRef.current.pause();
      ttsAudioRef.current.src = "";
      ttsAudioRef.current     = null;
    }
    if (ttsObjectUrlRef.current) {
      URL.revokeObjectURL(ttsObjectUrlRef.current);
      ttsObjectUrlRef.current = null;
    }
    if (voiceState === "speaking") {
      setVoiceState("idle");
    }
  }, [voiceState]);

  // ── speakText : TTS via Deepgram ─────────────────────────────────────────
  // lang est dynamique : "fr" si l'utilisateur a parlé français, "en" si anglais
  // → la VOIX Deepgram change automatiquement selon la langue détectée

  const speakText = useCallback(async (text: string, lang?: string) => {
    if (!text.trim()) return;

    // Langue pour le TTS : paramètre explicite > langue détectée > défaut
    const ttsLang = lang || detectedLang || defaultLang;

    // Annuler toute lecture TTS précédente (inline pour éviter dépendance circulaire)
    if (ttsAudioRef.current) {
      ttsAudioRef.current.pause();
      ttsAudioRef.current.src = "";
      ttsAudioRef.current = null;
    }
    if (ttsObjectUrlRef.current) {
      URL.revokeObjectURL(ttsObjectUrlRef.current);
      ttsObjectUrlRef.current = null;
    }

    setVoiceState("speaking");

    try {
      const resp = await fetch("/api/voice/tts", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ text, lang: ttsLang }),  // ← langue dynamique
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: `HTTP ${resp.status}` }));
        throw new Error(err.detail || `TTS error ${resp.status}`);
      }

      const audioBlob = await resp.blob();

      // ── Lecture audio avec AudioContext (plus compatible que new Audio()) ──
      // new Audio() avec blob WAV crashe sur certains navigateurs (Firefox/Safari)
      // AudioContext.decodeAudioData() supporte WAV + MP3 + OGG partout
      const arrayBuffer = await audioBlob.arrayBuffer();

      const audioCtxTts = new AudioContext();
      let decodedBuffer: AudioBuffer;

      try {
        decodedBuffer = await audioCtxTts.decodeAudioData(arrayBuffer);
      } catch {
        // Si le décodage échoue → fallback sur new Audio() avec objectURL
        const objectUrl = URL.createObjectURL(audioBlob);
        ttsObjectUrlRef.current = objectUrl;
        const audio = new Audio(objectUrl);
        ttsAudioRef.current = audio;
        audio.onended = () => {
          setVoiceState("idle");
          URL.revokeObjectURL(objectUrl);
          ttsObjectUrlRef.current = null;
          ttsAudioRef.current = null;
          audioCtxTts.close();
        };
        audio.onerror = () => {
          setVoiceState("idle");
          URL.revokeObjectURL(objectUrl);
          ttsObjectUrlRef.current = null;
          ttsAudioRef.current = null;
          audioCtxTts.close();
        };
        await audio.play().catch(() => setVoiceState("idle"));
        return;
      }

      const source = audioCtxTts.createBufferSource();
      source.buffer = decodedBuffer;
      source.connect(audioCtxTts.destination);

      source.onended = () => {
        setVoiceState("idle");
        audioCtxTts.close().catch(() => {});
      };

      source.start(0);

    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn("[useVoice] TTS error (non-fatal):", message);
      setVoiceState("idle");
    }
  }, [detectedLang, defaultLang]); // eslint-disable-line react-hooks/exhaustive-deps

  return {
    voiceState,
    voiceError,
    detectedLang,      // langue détectée lors du dernier STT
    startListening,
    stopListening,
    speakText,
    cancelSpeech,
  };
}