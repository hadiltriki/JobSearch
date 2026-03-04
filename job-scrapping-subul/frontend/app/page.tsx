"use client";
// ─────────────────────────────────────────────────────────────────────────────
//  app/app/page.tsx  —  DASHBOARD
//
//  URL patterns:
//    /app?user_id=7          → load existing user's jobs directly
//    /app?user_id=7&scan=1   → run SSE pipeline then show results
//
//  Layout (always the same):
//    ┌─────────────────────────────────────────────────────────────┐
//    │  Header (sticky)                                            │
//    ├──────────────┬──────────────────────────────────────────────┤
//    │  Chat        │  [Scanning banner — visible only while scan] │
//    │  sidebar     │  Tabs: Matches · Gap · Roadmap · Market …    │
//    │  (sticky)    │  Tab content                                  │
//    └──────────────┴──────────────────────────────────────────────┘
//
//  During scan the Matches tab shows live job cards (cosine ≥ 0.60)
//  appearing one by one in the centre grid.
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect, useRef, Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import {
  GRAD, FONT, MONO, C, S, GLOBAL_CSS,
  PIPE_STEPS, SOURCES, initPipeSteps,
  scoreColor, interpBadge, pipeColor, pipeBg, pipeBorder,
  type PipeState,
} from "@/app/lib/theme";

// ─────────────────────────────────────────────────────────────────────────────
//  Types
// ─────────────────────────────────────────────────────────────────────────────

interface Job {
  url: string; source: string; title: string; industry: string;
  location: string; remote: string; salary: string; contract: string;
  education: string; experience: string; description: string;
  skills_req: string; skills_bon: string;
  cosine: number;        // clé réelle backend (0–1 DB, ou ×100 SSE)
  cosine_score?: number; // alias legacy
  match_score: number;
  gap_missing: string[]; gap_matched?: string[];
  gap_coverage?: number; gap_total: number;
  xai?: {
    cosine_score: number; match_score: number;
    explanations: string[]; score_formula: string; interpretation: string;
  };
}

// Normalise score 0–1 (DB) ou 0–100 (SSE) → toujours 0–1
function normalizeScore(v: number | undefined | null): number {
  if (!v) return 0;
  return v > 1 ? v / 100 : v;
}

interface RoadmapItem {
  skill: string; week_start: number; week_end: number;
  duration: string; difficulty: string; resources: string[]; priority: string;
}

interface Message { role: "user" | "assistant"; content: string; }

type Tab = "matches" | "gap" | "roadmap" | "market" | "report";

// ─────────────────────────────────────────────────────────────────────────────
//  ScoreBars — two horizontal bars (Cosine + AI Match)
// ─────────────────────────────────────────────────────────────────────────────

function ScoreBars({ job }: { job: Job }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, margin: "8px 0" }}>
      {[
        { label: "Title Match", sub: "cosine",   value: normalizeScore(job.cosine ?? job.cosine_score) },
        { label: "AI Match",    sub: "biencoder", value: normalizeScore(job.match_score) },
      ].map(({ label, sub, value }) =>
        value > 0 ? (
          <div key={label} style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {/* Label + sub */}
            <div style={{ width: 68, flexShrink: 0 }}>
              <div style={{ fontSize: 9, color: C.muted, fontFamily: MONO, textTransform: "uppercase", fontWeight: 700, lineHeight: 1.2 }}>{label}</div>
              <div style={{ fontSize: 8, color: "#b8aece", fontFamily: MONO, lineHeight: 1.2 }}>{sub}</div>
            </div>
            <div style={{ flex: 1, height: 4, background: C.border, borderRadius: 2, overflow: "hidden" }}>
              <div style={{ width: `${value * 100}%`, height: "100%", background: scoreColor(value), borderRadius: 2, transition: "width .5s" }} />
            </div>
            <span style={{ fontSize: 10, fontWeight: 700, color: scoreColor(value), minWidth: 42, textAlign: "right", fontFamily: MONO }}>
              {(value * 100).toFixed(1)}%
            </span>
          </div>
        ) : null
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  JobCard — full or mini variant
// ─────────────────────────────────────────────────────────────────────────────

// ── Dérive l'interprétation depuis le score si xai absent ────────────────────
function scoreToInterp(score: number): string {
  if (score >= 0.75) return "excellent";
  if (score >= 0.55) return "good";
  if (score >= 0.40) return "moderate";
  return "low";
}

// ── Commentaire sous le badge — toujours affiché ─────────────────────────────
function InterpComment({ interp }: { interp: string }) {
  const map: Record<string, { icon: string; text: string; color: string }> = {
    excellent: { icon: "🔥", text: "Top pick — strongly recommended", color: C.p2   },
    good:      { icon: "👍", text: "Strong fit for your profile",      color: C.p1   },
    moderate:  { icon: "⚠️", text: "Partial fit — some gaps",          color: C.amber },
    low:       { icon: "❌", text: "Weak fit — significant gaps",       color: C.red   },
  };
  const m = map[interp] ?? map["moderate"];
  return (
    <div style={{ fontSize: 9, color: m.color, marginTop: 3, fontFamily: MONO, fontWeight: 600, textAlign: "right", lineHeight: 1.3 }}>
      {m.icon} {m.text}
    </div>
  );
}

function JobCard({ job }: { job: Job }) {
  const [expanded,    setExpanded]    = useState(false);
  const [showXAI,     setShowXAI]     = useState(false);
  const [showAllGap,  setShowAllGap]  = useState(false);  // ← +N cliquable

  const score = normalizeScore(job.match_score) || normalizeScore(job.cosine ?? job.cosine_score) || 0;
  const col   = scoreColor(score);
  const interp = job.xai?.interpretation ?? scoreToInterp(score);
  const b      = interpBadge(interp);

  // Skills gap helpers
  const missingAll    = job.gap_missing || [];
  const matchedAll    = job.gap_matched || [];
  const PREVIEW_MISS  = 3;
  const PREVIEW_MATCH = 2;
  const extraMissing  = missingAll.length - PREVIEW_MISS;
  const visibleMissing = showAllGap ? missingAll : missingAll.slice(0, PREVIEW_MISS);
  const visibleMatched = showAllGap ? matchedAll : matchedAll.slice(0, PREVIEW_MATCH);

  return (
    <div style={{
      background: C.white,
      border: `1px solid ${col}33`, borderTop: `3px solid ${col}`,
      borderRadius: 12, padding: "14px 16px",
      display: "flex", flexDirection: "column", gap: 7,
    }}>
      {/* ── Header row ── */}
      <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
        {/* Company avatar */}
        <div style={{ width: 34, height: 34, borderRadius: 8, flexShrink: 0, background: GRAD, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 800, fontSize: 13, color: "#fff" }}>
          {(job.industry || job.title || "?").charAt(0).toUpperCase()}
        </div>
        {/* Title + company */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: C.text, lineHeight: 1.3 }}>{job.title}</div>
          <div style={{ fontSize: 10, color: C.muted, marginTop: 2 }}>{job.industry || "—"}</div>
        </div>
        {/* Score + badge + commentaire */}
        <div style={{ textAlign: "right", flexShrink: 0 }}>
          <div style={{ fontSize: 16, fontWeight: 800, color: col, fontFamily: MONO, lineHeight: 1 }}>
            {(score * 100).toFixed(1)}%
          </div>
          {b && (
            <span style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: b.bg, color: b.color, fontWeight: 700 }}>
              {b.label}
            </span>
          )}
          {/* Commentaire sous badge — toujours visible, dérive de xai ou du score */}
          <InterpComment interp={interp} />
          <div style={{ fontSize: 9, color: "#9f8fb0", fontFamily: MONO, marginTop: 2 }}>{job.source}</div>
        </div>
      </div>

      {/* ── Location / remote / salary ── */}
      <div style={{ fontSize: 10, color: C.muted, display: "flex", flexWrap: "wrap", gap: 5 }}>
        {job.location && <span>📍 {job.location}</span>}
        {job.remote   && <span style={{ color: C.p2, fontWeight: 600 }}>{job.remote}</span>}
        {job.salary && job.salary !== "Not specified" && (
          <span style={{ color: C.amber }}>💰 {job.salary}</span>
        )}
      </div>

      {/* ── Score bars (Cosine + AI Match) ── */}
      <ScoreBars job={job} />

      {/* ── Skills gap avec +N cliquable ── */}
      {job.gap_total > 0 && (
        <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 7 }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
            <span style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>
              Skills Gap
            </span>
            <span style={{ fontSize: 9, fontWeight: 700, color: missingAll.length === 0 ? C.green : C.amber }}>
              {job.gap_total - missingAll.length}/{job.gap_total} covered
            </span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
            {/* Missing skills (rouges) */}
            {visibleMissing.map(s => (
              <span key={s} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(220,38,38,.07)", color: C.red, border: "1px solid rgba(220,38,38,.2)" }}>{s}</span>
            ))}
            {/* Matched skills (verts) */}
            {visibleMatched.map(s => (
              <span key={s} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(22,163,74,.07)", color: C.green, border: "1px solid rgba(22,163,74,.2)" }}>✓ {s}</span>
            ))}
            {/* ← CORRIGÉ : +N cliquable → expand toutes les skills */}
            {!showAllGap && extraMissing > 0 && (
              <button
                onClick={() => setShowAllGap(true)}
                style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(217,119,6,.10)", color: C.amber, border: "1px solid rgba(217,119,6,.3)", fontFamily: MONO, fontWeight: 700, cursor: "pointer" }}
              >
                +{extraMissing}
              </button>
            )}
            {/* Bouton collapse */}
            {showAllGap && extraMissing > 0 && (
              <button
                onClick={() => setShowAllGap(false)}
                style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: C.light, color: C.muted, border: `1px solid ${C.border}`, fontFamily: MONO, fontWeight: 700, cursor: "pointer" }}
              >
                ▲ less
              </button>
            )}
          </div>
        </div>
      )}

      {/* ── Explain scores — toujours visible, juste sous skills gap ── */}
      <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 8 }}>
        <button
          onClick={() => setShowXAI(!showXAI)}
          style={{
            background: showXAI ? `${col}0d` : "transparent",
            border: `1px solid ${col}44`, borderRadius: 6, color: col,
            fontSize: 10, padding: "4px 10px", cursor: "pointer",
            fontFamily: MONO, textAlign: "left", width: "100%",
            transition: "background .2s",
          }}
        >
          {showXAI ? "▲ Hide explanation" : "🔍 Explain scores (XAI)"}
        </button>

        {showXAI && (
          <div style={{ marginTop: 8, background: C.light, border: `1px solid ${C.border}`, borderRadius: 10, padding: "12px 14px" }}>
            {/* Header */}
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
              <span style={{ fontSize: 12, fontWeight: 700, color: C.text }}>Score Explanation</span>
              <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: b.bg, color: b.color, fontWeight: 700 }}>
                {b.label}
              </span>
            </div>

            {/* ── Ligne 1 : Cosine Similarity = Title Match ── */}
            {(() => {
              const cosP  = normalizeScore(job.xai?.cosine_score ?? job.cosine ?? job.cosine_score);
              const cosCol = scoreColor(cosP);
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 6, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>
                      🎯 Cosine Similarity
                    </div>
                    {/* Mini bar */}
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${cosP * 100}%`, background: cosCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: cosCol, fontFamily: MONO }}>
                      {(cosP * 100).toFixed(1)}%
                    </span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>Title Match</strong> —{" "}
                    {cosP >= 0.75
                      ? "Your job title strongly aligns with this role. The semantic similarity between your profile and the job title is excellent."
                      : cosP >= 0.55
                      ? "Your profile partially matches the job title. Some overlap exists but your title may differ slightly."
                      : "Limited title overlap. Your current title differs from this role — consider tailoring your headline."}
                  </div>
                </div>
              );
            })()}

            {/* ── Ligne 2 : AI Match = BiEncoder score ── */}
            {(() => {
              const aiP   = normalizeScore(job.xai?.match_score ?? job.match_score);
              const aiCol = scoreColor(aiP);
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 6, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>
                      🤖 AI Match
                    </div>
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${aiP * 100}%`, background: aiCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: aiCol, fontFamily: MONO }}>
                      {(aiP * 100).toFixed(1)}%
                    </span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>BiEncoder Score</strong> —{" "}
                    {job.xai?.explanations?.[0]
                      ? job.xai.explanations[0]
                      : aiP >= 0.75
                      ? "Excellent overall fit. Your experience, skills and profile strongly match the job requirements."
                      : aiP >= 0.55
                      ? "Good fit. Your profile covers most requirements — a few gaps exist."
                      : "Moderate fit. Your profile meets some criteria but key requirements may be missing."}
                  </div>
                </div>
              );
            })()}

            {/* ── Ligne 3 : Skills Coverage ── */}
            {job.gap_total > 0 && (() => {
              const covered = job.gap_total - missingAll.length;
              const pct     = Math.round(covered / job.gap_total * 100);
              const covCol  = pct >= 70 ? C.green : pct >= 40 ? C.amber : C.red;
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 4, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>
                      📊 Skills Coverage
                    </div>
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${pct}%`, background: covCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: covCol, fontFamily: MONO }}>
                      {covered}/{job.gap_total}
                    </span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>{pct}% covered</strong> —{" "}
                    {missingAll.length === 0
                      ? "You meet all required skills for this role. 🎉"
                      : `Missing: ${missingAll.slice(0, 4).join(", ")}${missingAll.length > 4 ? ` +${missingAll.length - 4} more` : ""}.`}
                  </div>
                </div>
              );
            })()}

            {/* ── Textes XAI supplémentaires du backend (si présents) ── */}
            {job.xai?.explanations?.slice(1).map((e, i) => (
              <div key={i} style={{ fontSize: 10, color: C.muted, lineHeight: 1.5, padding: "4px 8px", background: C.bg, borderRadius: 6, marginTop: 4, fontFamily: MONO, fontStyle: "italic" }}>
                {e}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── Expanded details (More details) ── */}
      {expanded && (
        <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 8, display: "flex", flexDirection: "column", gap: 6 }}>
          {job.skills_req && (
            <div>
              <div style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", marginBottom: 4 }}>Required Skills</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
                {job.skills_req.split(",").slice(0, 6).map(s => s.trim()).filter(Boolean).map(s => (
                  <span key={s} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(122,63,176,.07)", color: C.p2, border: "1px solid rgba(122,63,176,.2)" }}>{s}</span>
                ))}
              </div>
            </div>
          )}
          {job.description && (
            <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.6, maxHeight: 100, overflowY: "auto", background: C.bg, borderRadius: 6, padding: 8, fontFamily: MONO }}>
              {job.description.slice(0, 300)}…
            </div>
          )}
          <a href={job.url} target="_blank" rel="noopener" style={{ display: "block", textAlign: "center", padding: "8px", background: GRAD, borderRadius: 8, fontSize: 12, fontWeight: 700, color: "#fff", textDecoration: "none" }}>
            Apply →
          </a>
        </div>
      )}

      <button onClick={() => setExpanded(!expanded)} style={{ background: "transparent", border: `1px solid ${C.border}`, borderRadius: 6, color: C.muted, fontSize: 10, padding: "4px 8px", cursor: "pointer", fontFamily: MONO, width: "100%" }}>
        {expanded ? "▲ Show less" : "▼ More details"}
      </button>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Chart components
// ─────────────────────────────────────────────────────────────────────────────

function VerticalChart({ data, title, valueKey, labelKey, barColor = C.p2, height = 220 }: {
  data: any[]; title: string; valueKey: string; labelKey: string; barColor?: string; height?: number;
}) {
  if (!data?.length) return null;
  const maxVal = Math.max(...data.map(d => d[valueKey]));
  const BW = 44, GAP = 10, M = { top: 20, right: 16, bottom: 64, left: 36 };
  const cH = height - M.top - M.bottom;
  const cW = data.length * (BW + GAP) - GAP;

  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px 18px" }}>
      <div style={{ fontSize: 13, fontWeight: 700, color: C.text, marginBottom: 14 }}>{title}</div>
      <div style={{ overflowX: "auto" }}>
        <svg width={cW + M.left + M.right} height={height} style={{ display: "block" }}>
          {[0, Math.round(maxVal / 2), maxVal].map(t => {
            const y = M.top + cH - (t / maxVal) * cH;
            return (
              <g key={t}>
                <line x1={M.left} x2={M.left + cW} y1={y} y2={y} stroke={C.border} strokeWidth={1} strokeDasharray={t === 0 ? "0" : "4 3"} />
                <text x={M.left - 6} y={y + 4} textAnchor="end" fontSize={9} fill={C.muted}>{t}</text>
              </g>
            );
          })}
          {data.map((d, i) => {
            const x   = M.left + i * (BW + GAP);
            const bH  = Math.max(2, (d[valueKey] / maxVal) * cH);
            const y   = M.top + cH - bH;
            const lbl : string = d[labelKey] || "";
            const tr  = lbl.length > 9 ? lbl.slice(0, 8) + "…" : lbl;
            return (
              <g key={lbl}>
                <rect x={x} y={M.top} width={BW} height={cH} fill={C.light} rx={4} />
                <rect x={x} y={y} width={BW} height={bH} fill={barColor} rx={4} opacity={0.9}>
                  <title>{lbl}: {d[valueKey]}</title>
                </rect>
                <text x={x + BW / 2} y={y - 5} textAnchor="middle" fontSize={10} fontWeight="700" fill={barColor}>{d[valueKey]}</text>
                <text x={x + BW / 2} y={M.top + cH + 14} textAnchor="end" fontSize={9} fill={C.muted} transform={`rotate(-35,${x + BW / 2},${M.top + cH + 14})`}>{tr}</text>
              </g>
            );
          })}
          <line x1={M.left} x2={M.left} y1={M.top} y2={M.top + cH} stroke={C.border} strokeWidth={1} />
        </svg>
      </div>
    </div>
  );
}

function HorizontalChart({ data, title, valueKey, labelKey, barColor = C.p1 }: {
  data: any[]; title: string; valueKey: string; labelKey: string; barColor?: string;
}) {
  if (!data?.length) return null;
  const maxVal = Math.max(...data.map(d => d[valueKey]));
  const RH = 30, GAP = 6, LW = 130, BA = 260;

  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 12, padding: "16px 18px" }}>
      <div style={{ fontSize: 13, fontWeight: 700, color: C.text, marginBottom: 14 }}>{title}</div>
      <svg width={LW + BA + 50} height={data.length * (RH + GAP) + 10} style={{ display: "block", width: "100%" }}>
        {data.map((d, i) => {
          const y   = i * (RH + GAP);
          const bW  = Math.max(4, (d[valueKey] / maxVal) * BA);
          const lbl : string = d[labelKey] || "";
          const tr  = lbl.length > 18 ? lbl.slice(0, 17) + "…" : lbl;
          return (
            <g key={lbl}>
              <text x={LW - 8} y={y + RH / 2 + 4} textAnchor="end" fontSize={10} fill={C.muted}>{tr}</text>
              <rect x={LW} y={y + 4} width={BA} height={RH - 8} fill={C.light} rx={4} />
              <rect x={LW} y={y + 4} width={bW} height={RH - 8} fill={barColor} rx={4} opacity={0.85}>
                <title>{lbl}: {d[valueKey]}</title>
              </rect>
              <text x={LW + bW + 6} y={y + RH / 2 + 4} fontSize={10} fontWeight="700" fill={barColor}>{d[valueKey]}</text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  ScanningBanner — shown at top of main area while SSE pipeline is running
// ─────────────────────────────────────────────────────────────────────────────

function ScanningBanner({ pipeSteps, pipeRole, enrichN }: {
  pipeSteps: Record<string, PipeState>; pipeRole: string; enrichN: number;
}) {
  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 16, padding: "16px 22px", marginBottom: 20, boxShadow: "0 2px 12px rgba(122,63,176,.08)", position: "relative", overflow: "hidden" }}>
      {/* Gradient top bar */}
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: GRAD }} />

      {/* Title row */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          {/* Animated dots */}
          <div style={{ display: "flex", gap: 4 }}>
            {[0, 120, 240].map(d => (
              <div key={d} style={{ width: 6, height: 6, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />
            ))}
          </div>
          <span style={{ fontSize: 14, fontWeight: 700, color: C.text }}>
            Analyzing CV{pipeRole ? ` — ${pipeRole}` : "…"}
          </span>
        </div>
        {enrichN > 0 && (
          <span style={{ fontSize: 12, color: C.muted, fontFamily: MONO }}>
            Enriched: <b style={{ color: C.p1 }}>{enrichN}</b>
          </span>
        )}
      </div>

      {/* Pipeline step chips */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 10 }}>
        {PIPE_STEPS.map(step => (
          <div key={step.id} style={{
            display: "flex", alignItems: "center", gap: 5,
            padding: "4px 10px", borderRadius: 7,
            fontSize: 10, fontWeight: 600,
            border: `1px solid ${pipeBorder(pipeSteps[step.id])}`,
            background: pipeBg(pipeSteps[step.id]),
            color: pipeColor(pipeSteps[step.id]),
            transition: "all .3s", fontFamily: MONO,
          }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: "currentColor", display: "inline-block", flexShrink: 0, animation: pipeSteps[step.id] === "active" ? "pulse 1.1s infinite" : "none" }} />
            {step.icon} {step.label}
          </div>
        ))}
      </div>

      {/* Source chips */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
        {SOURCES.map(src => (
          <div key={src} style={{
            display: "flex", alignItems: "center", gap: 3,
            padding: "2px 8px", borderRadius: 5,
            fontSize: 9, fontWeight: 600,
            border: `1px solid ${pipeBorder(pipeSteps[src])}`,
            background: pipeBg(pipeSteps[src]),
            color: pipeColor(pipeSteps[src]),
            transition: "all .3s", fontFamily: MONO,
          }}>
            {pipeSteps[src] === "done"
              ? "✓"
              : <span style={{ width: 4, height: 4, borderRadius: "50%", background: "currentColor", display: "inline-block", animation: "pulse 1.1s infinite" }} />
            }
            {" "}{src}
          </div>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  ChatSidebar — left panel, sticky
// ─────────────────────────────────────────────────────────────────────────────

function ChatSidebar({ userId }: { userId: number }) {
  const [msgs,    setMsgs]    = useState<Message[]>([]);
  const [input,   setInput]   = useState("");
  const [loading, setLoading] = useState(false);
  const [chatErr, setChatErr] = useState("");
  const endRef = useRef<HTMLDivElement>(null);

  // Load chat history on mount
  useEffect(() => {
    if (!userId) return;
    fetch(`/api/chat/history?user_id=${userId}`)
      .then(r => r.json())
      .then(d => { if (d.messages?.length) setMsgs(d.messages); })
      .catch(() => {});
  }, [userId]);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [msgs]);

  async function send() {
    const msg = input.trim();
    if (!msg) return;
    setInput(""); setChatErr("");
    setMsgs(p => [...p, { role: "user", content: msg }]);
    setLoading(true);
    try {
      const r = await fetch("/api/chat", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: String(userId), message: msg }),
      });
      if (!r.ok) { setChatErr(`Server error ${r.status}`); return; }
      const d = await r.json();
      if (d.response) {
        setMsgs(p => [...p, { role: "assistant", content: d.response }]);
      } else {
        setChatErr("Empty response from server.");
      }
    } catch (e) {
      setChatErr("Connection error — is the backend running?");
    } finally { setLoading(false); }
  }

  return (
    <div style={{
      width: 272, flexShrink: 0,
      background: C.white, border: `1px solid ${C.border}`, borderRadius: 16,
      padding: "14px 16px", display: "flex", flexDirection: "column",
      height: "calc(100vh - 112px)", position: "sticky", top: 72,
      boxShadow: "0 2px 12px rgba(122,63,176,.07)",
    }}>
      <div style={{ fontWeight: 700, fontSize: 13, color: C.p1, marginBottom: 12 }}>
        💬 Career Assistant Chat
      </div>

      {/* Messages */}
      <div style={{ flex: 1, overflowY: "auto", display: "flex", flexDirection: "column", gap: 8, paddingRight: 2 }}>
        {msgs.length === 0 && (
          <div style={{ fontSize: 11, color: C.muted, textAlign: "center", marginTop: 20, lineHeight: 1.7 }}>
            Ask me about your job matches, skills gap, roadmap or score explanations.
          </div>
        )}
        {msgs.map((m, i) => (
          <div key={i} style={{
            padding: "8px 12px", borderRadius: 8, fontSize: 12, lineHeight: 1.6,
            ...(m.role === "user"
              ? { background: "rgba(122,63,176,.07)", borderRight: `3px solid ${C.p1}`, alignSelf: "flex-end",   maxWidth: "88%" }
              : { background: C.bg,                  borderLeft:  `3px solid ${C.p2}`, alignSelf: "flex-start", maxWidth: "95%" }
            ),
          }}>
            {m.content.split("\n").map((l, j) => <div key={j}>{l || " "}</div>)}
          </div>
        ))}
        {loading && <div style={{ fontSize: 11, color: "#9f8fb0", padding: "6px 12px" }}>Thinking…</div>}
        {chatErr && <div style={{ fontSize: 11, color: C.red, padding: "4px 12px" }}>⚠ {chatErr}</div>}
        <div ref={endRef} />
      </div>

      {/* Input */}
      <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
        <input
          style={{ ...S.input, fontSize: 12, flex: 1 }}
          placeholder="Ask anything…"
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === "Enter" && !loading && send()}
        />
        <button style={{ ...S.btn, padding: "8px 14px", fontSize: 12 }} onClick={send} disabled={loading}>→</button>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  MatchesTab — shows live cards during scan, loaded cards after
// ─────────────────────────────────────────────────────────────────────────────

function MatchesTab({
  isScanning, scanJobs, jobs, jobsLoad,
  roleFilter, setRoleFilter,
  locFilter,  setLocFilter,
  minFit,     setMinFit,
  onSearch,
}: {
  isScanning: boolean; scanJobs: Job[]; jobs: Job[]; jobsLoad: boolean;
  roleFilter: string; setRoleFilter: (v: string) => void;
  locFilter:  string; setLocFilter:  (v: string) => void;
  minFit: number;     setMinFit:     (v: number) => void;
  onSearch: () => void;
}) {
  return (
    <div>
      {/* Filter bar */}
      <div style={{ ...S.sec, display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap", marginBottom: 16 }}>
        <input style={{ ...S.input, width: 175, fontSize: 12 }} placeholder="Filter by role"     value={roleFilter} onChange={e => setRoleFilter(e.target.value)} />
        <input style={{ ...S.input, width: 155, fontSize: 12 }} placeholder="Filter by location" value={locFilter}  onChange={e => setLocFilter(e.target.value)} />
        <select style={{ ...S.input, width: 155, fontSize: 12 }} value={minFit} onChange={e => setMinFit(parseFloat(e.target.value))}>
          <option value={0}>All scores</option>
          <option value={0.4}>≥ 40% AI Match</option>
          <option value={0.55}>≥ 55% AI Match</option>
          <option value={0.75}>≥ 75% AI Match</option>
        </select>
        <button style={S.btn} onClick={onSearch} disabled={isScanning}>Search</button>
        <span style={{ fontSize: 11, color: C.muted }}>
          {isScanning
            ? `${scanJobs.length} matched so far…`
            : `${jobs.length} jobs · Cosine + AI Match`}
        </span>
      </div>

      {/* ── Loading (after scan, fetching from DB) ── */}
      {jobsLoad && !isScanning && (
        <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Loading matches…</div>
      )}

      {/* ── SCAN IN PROGRESS — no jobs yet ── */}
      {isScanning && scanJobs.length === 0 && (
        <div style={{ textAlign: "center", padding: "80px 20px" }}>
          <div style={{ display: "flex", justifyContent: "center", gap: 7, marginBottom: 20 }}>
            {[0, 150, 300].map(d => (
              <div key={d} style={{ width: 11, height: 11, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />
            ))}
          </div>
          <div style={{ fontSize: 17, fontWeight: 700, color: C.text, marginBottom: 10 }}>
            Scraping in progress…
          </div>
          <div style={{ fontSize: 13, color: C.muted, marginBottom: 8 }}>
            Scraping boards · AI scoring · Skills gap computation
          </div>
          
        </div>
      )}

      {/* ── SCAN IN PROGRESS — live job cards ── */}
      {isScanning && scanJobs.length > 0 && (
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
            <div style={{ display: "flex", gap: 4 }}>
              {[0, 100, 200].map(d => (
                <div key={d} style={{ width: 6, height: 6, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />
              ))}
            </div>
            <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>
              {scanJobs.length} job{scanJobs.length > 1 ? "s" : ""} matched · still scanning…
            </span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(310px,1fr))", gap: 14 }}>
            {scanJobs.map((job, i) => (
              <div key={job.url + i} style={{ animation: "cardIn .4s ease both" }}>
                <JobCard job={job} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── NO JOBS (scan done, nothing in DB) ── */}
      {!isScanning && !jobsLoad && jobs.length === 0 && (
        <div style={{ textAlign: "center", padding: "70px 20px", color: C.muted }}>
          <div style={{ fontSize: 36, marginBottom: 14 }}>🔍</div>
          <div style={{ fontSize: 15, fontWeight: 600, color: C.text, marginBottom: 8 }}>No jobs yet</div>
          <div style={{ fontSize: 12 }}>Go back to the home page and run a scan to populate your matches.</div>
        </div>
      )}

      {/* ── LOADED JOBS ── */}
      {!isScanning && jobs.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(310px,1fr))", gap: 14 }}>
          {jobs.map(job => <JobCard key={job.url} job={job} />)}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Dashboard — main component (inside Suspense for useSearchParams)
// ─────────────────────────────────────────────────────────────────────────────

function Dashboard() {
  const router       = useRouter();
  const searchParams = useSearchParams();
  const userId       = parseInt(searchParams.get("user_id") || "0", 10);
  const shouldScan   = searchParams.get("scan") === "1";

  // ── User ──────────────────────────────────────────────────────────────────
  const [userName, setUserName] = useState(`User #${userId}`);

  // ── Pipeline ──────────────────────────────────────────────────────────────
  const [isScanning, setIsScanning] = useState(false);
  const [pipeSteps,  setPipeSteps]  = useState<Record<string, PipeState>>(initPipeSteps());
  const [pipeRole,   setPipeRole]   = useState("");
  const [scanJobs,   setScanJobs]   = useState<Job[]>([]);
  const [enrichN,    setEnrichN]    = useState(0);

  // ── Matches tab ───────────────────────────────────────────────────────────
  const [activeTab,  setActiveTab]  = useState<Tab>("matches");
  const [jobs,       setJobs]       = useState<Job[]>([]);
  const [jobsLoad,   setJobsLoad]   = useState(false);
  const [roleFilter, setRoleFilter] = useState("");
  const [locFilter,  setLocFilter]  = useState("");
  const [minFit,     setMinFit]     = useState(0);

  // ── Other tabs ────────────────────────────────────────────────────────────
  const [gapData,  setGapData]  = useState<any>(null);
  const [gapLoad,  setGapLoad]  = useState(false);
  const [roadData, setRoadData] = useState<any>(null);
  const [roadLoad, setRoadLoad] = useState(false);
  const [mktData,  setMktData]  = useState<any>(null);
  const [mktLoad,  setMktLoad]  = useState(false);
  const [repData,  setRepData]  = useState("");
  const [repLoad,  setRepLoad]  = useState(false);

  // ── Boot ──────────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!userId) { router.replace("/"); return; }
    if (shouldScan) {
      const cv = sessionStorage.getItem("jobscan_cv_text") || "";
      runScan(cv);
    } else {
      fetchUserAndJobs();
    }
  }, []);

  // Load tab data lazily
  useEffect(() => {
    if (activeTab === "gap"     && !gapData  && userId) loadGap();
    if (activeTab === "roadmap" && !roadData  && userId) loadRoadmap();
    if (activeTab === "market"  && !mktData   && userId) loadMarket();
    if (activeTab === "report"  && !repData   && userId) loadReport();
  }, [activeTab]);

  // ── API helpers ───────────────────────────────────────────────────────────

  async function fetchUserAndJobs() {
    try {
      const r = await fetch("/api/login", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) });
      if (r.ok) { const d = await r.json(); setUserName(d.name || `User #${userId}`); }
    } catch {}
    loadJobs();
  }

async function loadJobs() {
  setJobsLoad(true);
  try {
    const resp = await fetch(`/jobs/${userId}`);
    if (!resp.ok || !resp.body) return;

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    const loaded: Job[] = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      const chunks = buf.split("\n\n");
      buf = chunks.pop() || "";

      for (const chunk of chunks) {
        if (!chunk.startsWith("data: ")) continue;
        let d: any;
        try { d = JSON.parse(chunk.slice(6)); } catch { continue; }

        if (d.event === "no_cache") {
          // Aucun job en DB pour cet utilisateur
          break;
        }
        if (d.event === "job") {
          loaded.push(d as Job);
        }
        if (d.event === "done") {
          setJobs(loaded);
          break;
        }
      }
    }
    setJobs(loaded);  // sécurité si "done" pas reçu
  } finally {
    setJobsLoad(false);
  }
}

  async function loadGap() {
    setGapLoad(true);
    try { const r = await fetch("/api/gap", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) }); setGapData(await r.json()); }
    finally { setGapLoad(false); }
  }

  async function loadRoadmap() {
    setRoadLoad(true);
    try { const r = await fetch("/api/roadmap", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) }); setRoadData(await r.json()); }
    finally { setRoadLoad(false); }
  }

  async function loadMarket() {
    setMktLoad(true);
    try { const r = await fetch(`/api/market?user_id=${userId}`); setMktData(await r.json()); }
    finally { setMktLoad(false); }
  }

  async function loadReport() {
    setRepLoad(true);
    try { const r = await fetch("/api/report", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) }); const d = await r.json(); setRepData(d.report || ""); }
    finally { setRepLoad(false); }
  }

  async function downloadPDF() {
    const r    = await fetch("/api/report/pdf", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) });
    const blob = await r.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement("a"); a.href = url; a.download = `career_report_${userId}.pdf`; a.click(); URL.revokeObjectURL(url);
  }

  // ── SSE pipeline ──────────────────────────────────────────────────────────

  async function runScan(cvText: string) {
    setIsScanning(true);
    setPipeSteps({ ...initPipeSteps(), lang: "active" });
    setPipeRole(""); setScanJobs([]); setEnrichN(0);

    try {
      const resp = await fetch("http://localhost:8000/scan", {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cv_text: cvText, user_id: userId }),
      });
      if (!resp.ok || !resp.body) throw new Error("Scan request failed");

      const reader   = resp.body.getReader();
      const decoder  = new TextDecoder();
      let   buf      = "";
      let   enriched = 0;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        // Process complete SSE messages separated by double newline
        const chunks = buf.split("\n\n");
        buf = chunks.pop() || "";

        for (const chunk of chunks) {
          const line = chunk.trim();
          if (!line.startsWith("data: ")) continue;
          let d: any;
          try { d = JSON.parse(line.slice(6)); } catch { continue; }

          switch (d.event) {
            case "lang_ready":
              setPipeSteps(p => ({ ...p, lang: "done", title: "active" }));
              break;
            case "cv_title":
              setPipeSteps(p => ({ ...p, title: "done", struct: "active" }));
              if (d.title) setPipeRole(d.title);
              break;
            case "cv_ready":
              setPipeSteps(p => ({ ...p, struct: "done", dbuser: "active", scrape: "active", enrich: "active" }));
              break;
            case "user_saved":
              setPipeSteps(p => ({ ...p, dbuser: "done" }));
              break;
            case "source_done":
              setPipeSteps(p => ({ ...p, [d.source]: "done" }));
              break;
            case "job":
              enriched++;
              setEnrichN(enriched);
              setScanJobs(prev => [...prev, d as Job]);
              if (enriched === 1) setPipeSteps(p => ({ ...p, dbjobs: "active" }));
              break;
            case "done":
              setPipeSteps(p => ({ ...p, scrape: "done", enrich: "done", dbjobs: "done" }));
              reader.cancel();
              break;
          }
        }
      }

      // Fetch user info then reload jobs from DB
      const r = await fetch("/api/login", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) });
      if (r.ok) { const d = await r.json(); setUserName(d.name || `User #${userId}`); }
      await loadJobs();

      // ── Nettoyer l'URL : retirer ?scan=1 sans recharger la page ──
      router.replace(`/app?user_id=${userId}`);
    } catch (err) {
      console.error("Scan error:", err);
    } finally {
      setIsScanning(false);
    }
  }

  function logout() { sessionStorage.clear(); router.push("/"); }

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div style={S.page}>
      <style>{GLOBAL_CSS}</style>

      {/* ════════════════════════════════ HEADER ════════════════════════════ */}
      <header style={{
        background: C.white, borderBottom: `1px solid ${C.border}`,
        padding: "0 32px", display: "flex", alignItems: "center", justifyContent: "space-between",
        height: 64, position: "sticky", top: 0, zIndex: 100,
        boxShadow: "0 1px 8px rgba(122,63,176,.06)",
      }}>
        <div>
          <span style={{ fontWeight: 800, fontSize: 20, background: GRAD, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", backgroundClip: "text" }}>
            CareerAssistant
          </span>
          <span style={{ fontSize: 10, color: "#9f8fb0", marginLeft: 10, fontFamily: MONO }}>
            JobScan AI · Cosine · BiEncoder · XAI
          </span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 12, color: C.muted }}>👤 {userName}</span>
          <button style={{ ...S.btnOut, fontSize: 11 }} onClick={logout}>Logout</button>
        </div>
      </header>

      {/* ════════════════════════════════ BODY ══════════════════════════════ */}
      <div style={{ maxWidth: 1320, margin: "0 auto", padding: "24px 24px", display: "flex", gap: 20 }}>

        {/* ── LEFT: Chat sidebar ── */}
        <ChatSidebar userId={userId} />

        {/* ── RIGHT: Main content ── */}
        <div style={{ flex: 1, minWidth: 0 }}>

          {/* Scanning banner (only while SSE is running) */}
          {isScanning && (
            <ScanningBanner pipeSteps={pipeSteps} pipeRole={pipeRole} enrichN={enrichN} />
          )}

          {/* Tab navigation */}
          <div style={{ display: "flex", gap: 6, marginBottom: 18, flexWrap: "wrap" }}>
            {(["matches", "gap", "roadmap", "market", "report"] as Tab[]).map(tab => (
              <button key={tab} style={S.tab(activeTab === tab)} onClick={() => setActiveTab(tab)}>
                {{ matches: "🏆 Matches", gap: "📊 Skills Gap", roadmap: "🗺️ Roadmap", market: "📈 Market", report: "📄 Report" }[tab]}
              </button>
            ))}
          </div>

          {/* ═══════════ MATCHES ═══════════ */}
          {activeTab === "matches" && (
            <MatchesTab
              isScanning={isScanning} scanJobs={scanJobs} jobs={jobs} jobsLoad={jobsLoad}
              roleFilter={roleFilter} setRoleFilter={setRoleFilter}
              locFilter={locFilter}   setLocFilter={setLocFilter}
              minFit={minFit}         setMinFit={setMinFit}
              onSearch={loadJobs}
            />
          )}

          {/* ═══════════ SKILLS GAP ═══════════ */}
          {activeTab === "gap" && (
            <div>
              {gapLoad
                ? <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Analyzing skills gap…</div>
                : !gapData
                ? <button style={S.btn} onClick={loadGap}>Analyze Skills Gap</button>
                : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                    <div style={{ fontSize: 13, fontWeight: 700, color: C.text }}>
                      📊 Top {gapData.top_missing_skills?.length || 0} missing skills across {gapData.total_jobs_analyzed} jobs
                    </div>
                    <VerticalChart data={gapData.top_missing_skills || []} title="Missing Skills Frequency" valueKey="frequency" labelKey="skill" barColor={C.p0} height={260} />
                    {gapData.cv_skills && (
                      <div style={S.sec}>
                        <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase" }}>Your Current Skills</div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                          {gapData.cv_skills.split(",").slice(0, 20).map((s: string) => s.trim()).filter(Boolean).map((s: string) => (
                            <span key={s} style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: "rgba(22,163,74,.07)", color: C.green, border: "1px solid rgba(22,163,74,.2)" }}>✓ {s}</span>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                )
              }
            </div>
          )}

          {/* ═══════════ ROADMAP ═══════════ */}
          {activeTab === "roadmap" && (
            <div>
              {roadLoad
                ? <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Generating roadmap…</div>
                : !roadData
                ? <button style={S.btn} onClick={loadRoadmap}>Generate Learning Roadmap</button>
                : (
                  <div>
                    <div style={{ ...S.sec, marginBottom: 16, display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <div>
                        <div style={{ fontSize: 14, fontWeight: 700, color: C.text }}>🗺️ Your Personalized Learning Roadmap</div>
                        <div style={{ fontSize: 11, color: C.muted }}>{roadData.message}</div>
                      </div>
                      <div style={{ textAlign: "right" }}>
                        <div style={{ fontSize: 22, fontWeight: 800, color: C.p1 }}>{roadData.total_weeks}w</div>
                        <div style={{ fontSize: 10, color: "#9f8fb0" }}>total plan</div>
                      </div>
                    </div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                      {(roadData.roadmap || []).map((item: RoadmapItem) => {
                        const dc = item.difficulty === "beginner" ? "#22c55e" : item.difficulty === "intermediate" ? "#f59e0b" : "#ef4444";
                        return (
                          <div key={item.skill} style={{ ...S.sec, display: "flex", gap: 14, alignItems: "flex-start" }}>
                            <div style={{ textAlign: "center", minWidth: 56 }}>
                              <div style={{ fontSize: 18, fontWeight: 800, color: C.p1 }}>W{item.week_start}</div>
                              <div style={{ fontSize: 9, color: "#9f8fb0" }}>–W{item.week_end}</div>
                            </div>
                            <div style={{ flex: 1 }}>
                              <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 6 }}>
                                <span style={{ fontWeight: 700, fontSize: 13, color: C.text }}>{item.skill}</span>
                                <span style={{ fontSize: 9, padding: "1px 7px", borderRadius: 4, background: `${dc}22`, color: dc, border: `1px solid ${dc}44` }}>{item.difficulty}</span>
                                <span style={{ fontSize: 9, padding: "1px 7px", borderRadius: 4, background: "rgba(122,63,176,.07)", color: C.p2, border: "1px solid rgba(122,63,176,.2)" }}>{item.priority}</span>
                              </div>
                              <div style={{ fontSize: 11, color: C.muted }}>📚 {item.resources.join(" · ")}</div>
                              <div style={{ fontSize: 10, color: "#9f8fb0", marginTop: 3 }}>{item.duration}</div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )
              }
            </div>
          )}

          {/* ═══════════ MARKET ═══════════ */}
          {activeTab === "market" && (
            <div>
              {mktLoad
                ? <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Loading market data…</div>
                : !mktData
                ? <button style={S.btn} onClick={loadMarket}>Load Market Insights</button>
                : (
                  <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                    <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                      {[
                        { label: "Total Jobs",   value: mktData.total_jobs },
                        { label: "Avg AI Score", value: `${mktData.avg_ai_score}%` },
                        { label: "Excellent",    value: mktData.score_breakdown?.excellent || 0 },
                        { label: "Good",         value: mktData.score_breakdown?.good || 0 },
                      ].map(({ label, value }) => (
                        <div key={label} style={{ ...S.sec, flex: 1, minWidth: 120, marginBottom: 0, textAlign: "center" }}>
                          <div style={{ fontSize: 22, fontWeight: 800, color: C.p1 }}>{value}</div>
                          <div style={{ fontSize: 10, color: C.muted }}>{label}</div>
                        </div>
                      ))}
                    </div>
                    <VerticalChart   data={mktData.top_skills    || []} title="📊 Top Skills Demanded" valueKey="count" labelKey="skill"   barColor={C.p2} height={260} />
                    <HorizontalChart data={mktData.top_companies || []} title="🏢 Top Companies"       valueKey="count" labelKey="company" barColor={C.p1} />
                    <div style={S.sec}>
                      <div style={{ fontWeight: 700, marginBottom: 12, fontSize: 13, color: C.text }}>📍 Top Locations</div>
                      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                        {(mktData.top_locations || []).map((item: any) => (
                          <span key={item.location} style={{ fontSize: 11, padding: "4px 12px", borderRadius: 20, background: "rgba(122,63,176,.07)", border: "1px solid rgba(122,63,176,.2)", color: C.p2 }}>
                            {item.location} ({item.count})
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                )
              }
            </div>
          )}

          {/* ═══════════ REPORT ═══════════ */}
          {activeTab === "report" && (
            <div>
              <div style={{ display: "flex", gap: 10, marginBottom: 16 }}>
                <button style={S.btn} onClick={loadReport} disabled={repLoad}>
                  {repLoad ? "Generating…" : "🔄 Regenerate Report"}
                </button>
                <button style={{ ...S.btn, background: "linear-gradient(135deg,#22c55e,#16a34a)" }} onClick={downloadPDF}>
                  📄 Download PDF
                </button>
              </div>
              {repData
                ? (
                  <div style={{ ...S.sec, maxHeight: 600, overflowY: "auto" }}>
                    <pre style={{ fontSize: 11, lineHeight: 1.7, color: "#4a3f60", whiteSpace: "pre-wrap", fontFamily: MONO }}>{repData}</pre>
                  </div>
                )
                : <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Click "Regenerate Report" to generate your career analysis.</div>
              }
            </div>
          )}

        </div>{/* end main content */}
      </div>{/* end body */}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Export — wrapped in Suspense (required for useSearchParams in Next.js)
// ─────────────────────────────────────────────────────────────────────────────

export default function AppPage() {
  return (
    <Suspense fallback={
      <div style={{ minHeight: "100vh", background: C.bg, display: "flex", alignItems: "center", justifyContent: "center", fontFamily: FONT, color: C.muted }}>
        Loading…
      </div>
    }>
      <Dashboard />
    </Suspense>
  );
}