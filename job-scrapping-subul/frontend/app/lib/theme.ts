// ─────────────────────────────────────────────────────────────────────────────
//  lib/theme.ts
//  Shared design tokens, types, and style helpers used across all pages.
// ─────────────────────────────────────────────────────────────────────────────

import React from "react";

// ── Design tokens ─────────────────────────────────────────────────────────────
export const GRAD = "linear-gradient(135deg,#FF2D7A 0%,#C3379B 50%,#7A3FB0 100%)";
export const FONT = "'Sora','Segoe UI',system-ui,sans-serif";
export const MONO = "'DM Mono','Courier New',monospace";

export const C = {
  p0    : "#FF2D7A",
  p1    : "#C3379B",
  p2    : "#7A3FB0",
  bg    : "#f7f6fc",
  white : "#ffffff",
  border: "#ede8f5",
  text  : "#1a1025",
  muted : "#6b5c80",
  light : "#f0ecfa",
  green : "#16a34a",
  amber : "#d97706",
  red   : "#dc2626",
} as const;

// ── Pipeline ──────────────────────────────────────────────────────────────────
export type PipeState = "waiting" | "active" | "done";

export const PIPE_STEPS = [
{ id: "lang",   icon: "🌍", label: "Reading your profile"     },

{ id: "scrape", icon: "🔍", label: "Searching job offers"     },
{ id: "enrich", icon: "🧠", label: "Analyzing matches"        }

] as const;

export const SOURCES = [
  "aijobs", "remoteok", "tanitjobs", "greenhouse", "eluta", "linkedin", "indeed", "lever","whatjobs", "Welcome to the Jungle"
] as const;

export function initPipeSteps(): Record<string, PipeState> {
  return Object.fromEntries(
    [...PIPE_STEPS.map(p => p.id), ...SOURCES].map(k => [k, "waiting" as PipeState])
  );
}

// Pipe step colour helpers
export const pipeColor  = (s: PipeState) => s === "done" ? C.green : s === "active" ? C.p0 : "#d4cce8";
export const pipeBg     = (s: PipeState) => s === "done" ? "rgba(22,163,74,.06)"  : s === "active" ? "rgba(255,45,122,.06)"  : C.white;
export const pipeBorder = (s: PipeState) => s === "done" ? "rgba(22,163,74,.3)"   : s === "active" ? "rgba(255,45,122,.35)"  : C.border;

// ── Score helpers ──────────────────────────────────────────────────────────────
export function scoreColor(v: number): string {
  const p = v * 100;
  if (p >= 75) return C.p2;
  if (p >= 55) return C.p1;
  if (p >= 40) return C.p0;
  return C.red;
}

export function interpBadge(interp: string) {
  const map: Record<string, { bg: string; color: string; label: string }> = {
    excellent: { bg: "rgba(122,63,176,.12)", color: C.p2, label: "Excellent" },
    good:      { bg: "rgba(195,55,155,.12)", color: C.p1, label: "Good"      },
    moderate:  { bg: "rgba(255,45,122,.12)", color: C.p0, label: "Moderate"  },
    low:       { bg: "rgba(220,38,38,.12)",  color: C.red, label: "Low"      },
  };
  return map[interp] ?? map["moderate"];
}

// ── Shared style presets ───────────────────────────────────────────────────────
export const S = {
  page  : { minHeight: "100vh", background: C.bg, color: C.text, fontFamily: FONT } as React.CSSProperties,
  card  : { background: C.white, border: `1px solid ${C.border}`, borderRadius: 16, padding: "24px 28px", boxShadow: "0 2px 16px rgba(122,63,176,.08)" } as React.CSSProperties,
  input : { width: "100%", background: C.bg, border: `1.5px solid ${C.border}`, borderRadius: 10, padding: "11px 14px", color: C.text, fontSize: 13, outline: "none", fontFamily: FONT } as React.CSSProperties,
  btn   : { padding: "11px 22px", background: GRAD, border: "none", borderRadius: 10, color: "#fff", fontWeight: 700, fontSize: 14, cursor: "pointer", boxShadow: "0 4px 14px rgba(233,47,138,.25)", fontFamily: FONT } as React.CSSProperties,
  btnOut: { padding: "8px 16px", background: "transparent", border: `1.5px solid ${C.border}`, borderRadius: 8, color: C.muted, fontSize: 12, cursor: "pointer", fontFamily: FONT } as React.CSSProperties,
  tab   : (active: boolean): React.CSSProperties => ({
    padding: "7px 16px",
    background: active ? "rgba(255,45,122,.07)" : "transparent",
    border: `1px solid ${active ? "rgba(255,45,122,.35)" : C.border}`,
    borderRadius: 8,
    color: active ? C.p0 : C.muted,
    fontSize: 13, fontWeight: active ? 700 : 400,
    cursor: "pointer", fontFamily: FONT,
  }),
  sec   : { background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 18px", marginBottom: 12 } as React.CSSProperties,
};

// ── Global CSS string (inject once with <style>) ──────────────────────────────
export const GLOBAL_CSS = `
  @import url('https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700;800&family=DM+Mono:wght@400;500&display=swap');
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: ${C.bg}; }
  input:focus, textarea:focus, select:focus {
    border-color: ${C.p2} !important;
    box-shadow: 0 0 0 3px rgba(122,63,176,.1);
    outline: none;
  }
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-thumb { background: ${C.border}; border-radius: 10px; }
  @keyframes pulse  { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.3;transform:scale(1.8)} }
  @keyframes bounce { 0%,100%{transform:translateY(0);opacity:.4} 50%{transform:translateY(-8px);opacity:1} }
  @keyframes cardIn { from{opacity:0;transform:translateY(14px)} to{opacity:1;transform:translateY(0)} }
  @keyframes fadeUp { from{opacity:0;transform:translateY(16px)} to{opacity:1;transform:translateY(0)} }
`;