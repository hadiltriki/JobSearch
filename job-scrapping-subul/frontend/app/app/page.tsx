"use client";
// ─────────────────────────────────────────────────────────────────────────────
//  app/app/page.tsx  —  DASHBOARD
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect, useRef, Suspense, useCallback, useMemo } from "react";
import { useVoice, type VoiceState } from "@/app/lib/useVoice";
import { useRouter, useSearchParams } from "next/navigation";
import {
  GRAD, FONT, MONO, C, S, GLOBAL_CSS,
  PIPE_STEPS, SOURCES, initPipeSteps,
  scoreColor, interpBadge, pipeColor, pipeBg, pipeBorder,
  type PipeState,
} from "@/app/lib/theme";

interface Job {
  url: string; source: string; title: string; industry: string;
  location: string; remote: string; salary: string; contract: string;
  education: string; experience: string; description: string;
  skills_req: string; skills_bon: string;
  cosine: number;
  cosine_score?: number;
  match_score: number;
  gap_missing: string[]; gap_matched?: string[];
  gap_coverage?: number; gap_total: number;
  xai?: {
    cosine_score: number; match_score: number;
    explanations: string[]; score_formula: string; interpretation: string;
    tip?: string;
    strength?: string;
  };
}

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
//  ScoreBars
// ─────────────────────────────────────────────────────────────────────────────

function ScoreBars({ job }: { job: Job }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6, margin: "8px 0" }}>
      {[
        { label: "Title Match", sub: "cosine",    value: normalizeScore(job.cosine ?? job.cosine_score) },
        { label: "AI Match",    sub: "biencoder",  value: normalizeScore(job.match_score) },
      ].map(({ label, sub, value }) =>
        value > 0 ? (
          <div key={label} style={{ display: "flex", alignItems: "center", gap: 8 }}>
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
//  JobCard
// ─────────────────────────────────────────────────────────────────────────────

function scoreToInterp(score: number): string {
  if (score >= 0.75) return "excellent";
  if (score >= 0.55) return "good";
  if (score >= 0.40) return "moderate";
  return "low";
}

function JobCard({ job }: { job: Job }) {
  const [expanded,   setExpanded]   = useState(false);
  const [showXAI,    setShowXAI]    = useState(!!(job.xai?.score_formula || job.xai?.interpretation));
  const [showAllGap, setShowAllGap] = useState(false);

  const score  = normalizeScore(job.match_score) || normalizeScore(job.cosine ?? job.cosine_score) || 0;
  const col    = scoreColor(score);
  const interp = job.xai?.interpretation ?? scoreToInterp(score);
  const b      = interpBadge(interp);

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
      <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
        <div style={{ width: 34, height: 34, borderRadius: 8, flexShrink: 0, background: GRAD, display: "flex", alignItems: "center", justifyContent: "center", fontWeight: 800, fontSize: 13, color: "#fff" }}>
          {(job.industry || job.title || "?").charAt(0).toUpperCase()}
        </div>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: C.text, lineHeight: 1.3 }}>{job.title}</div>
          <div style={{ fontSize: 10, color: C.muted, marginTop: 2 }}>{job.industry || "—"}</div>
        </div>
        <div style={{ textAlign: "right", flexShrink: 0 }}>
          <div style={{ fontSize: 16, fontWeight: 800, color: col, fontFamily: MONO, lineHeight: 1 }}>
            {(score * 100).toFixed(1)}%
          </div>
          {b && (
            <span style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: b.bg, color: b.color, fontWeight: 700 }}>
              {b.label}
            </span>
          )}
          <div style={{ fontSize: 9, color: "#9f8fb0", fontFamily: MONO, marginTop: 2 }}>{job.source}</div>
        </div>
      </div>

      <div style={{ fontSize: 10, color: C.muted, display: "flex", flexWrap: "wrap", gap: 5 }}>
        {job.location && <span>📍 {job.location}</span>}
        {job.remote   && <span style={{ color: C.p2, fontWeight: 600 }}>{job.remote}</span>}
        {job.salary && job.salary !== "Not specified" && (
          <span style={{ color: C.amber }}>💰 {job.salary}</span>
        )}
      </div>

      <ScoreBars job={job} />

      {job.gap_total > 0 && (
        <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 7 }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
            <span style={{ fontSize: 9, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em" }}>Skills Gap</span>
            <span style={{ fontSize: 9, fontWeight: 700, color: missingAll.length === 0 ? C.green : C.amber }}>
              {job.gap_total - missingAll.length}/{job.gap_total} covered
            </span>
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 3 }}>
            {visibleMissing.map(s => (
              <span key={s} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(220,38,38,.07)", color: C.red, border: "1px solid rgba(220,38,38,.2)" }}>{s}</span>
            ))}
            {visibleMatched.map(s => (
              <span key={s} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(22,163,74,.07)", color: C.green, border: "1px solid rgba(22,163,74,.2)" }}>✓ {s}</span>
            ))}
            {!showAllGap && extraMissing > 0 && (
              <button onClick={() => setShowAllGap(true)} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: "rgba(217,119,6,.10)", color: C.amber, border: "1px solid rgba(217,119,6,.3)", fontFamily: MONO, fontWeight: 700, cursor: "pointer" }}>
                +{extraMissing}
              </button>
            )}
            {showAllGap && extraMissing > 0 && (
              <button onClick={() => setShowAllGap(false)} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: C.light, color: C.muted, border: `1px solid ${C.border}`, fontFamily: MONO, fontWeight: 700, cursor: "pointer" }}>
                ▲ less
              </button>
            )}
          </div>
        </div>
      )}

      <div style={{ borderTop: `1px solid ${C.border}`, paddingTop: 8 }}>
        <button onClick={() => setShowXAI(!showXAI)} style={{ background: showXAI ? `${col}0d` : "transparent", border: `1px solid ${col}44`, borderRadius: 6, color: col, fontSize: 10, padding: "4px 10px", cursor: "pointer", fontFamily: MONO, textAlign: "left", width: "100%", transition: "background .2s" }}>
          {showXAI ? "▲ Hide explanation" : "🔍 Explain scores (XAI)"}
        </button>
        {showXAI && (
          <div style={{ marginTop: 8, background: C.light, border: `1px solid ${C.border}`, borderRadius: 10, padding: "12px 14px" }}>
            <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
              <span style={{ fontSize: 12, fontWeight: 700, color: C.text }}>Score Explanation</span>
              <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: b.bg, color: b.color, fontWeight: 700 }}>{b.label}</span>
            </div>
            {job.xai?.score_formula && (
              <div style={{ fontSize: 10, color: C.muted, fontFamily: MONO, marginBottom: 8, padding: "6px 8px", background: C.bg, borderRadius: 6 }}>📐 {job.xai.score_formula}</div>
            )}
            {(() => {
              const cosP   = normalizeScore(job.xai?.cosine_score ?? job.cosine ?? job.cosine_score);
              const cosCol = scoreColor(cosP);
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 6, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>🎯 Cosine Similarity</div>
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${cosP * 100}%`, background: cosCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: cosCol, fontFamily: MONO }}>{(cosP * 100).toFixed(1)}%</span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>Title Match</strong> —{" "}
                    {cosP >= 0.75 ? "Your job title strongly aligns with this role." : cosP >= 0.55 ? "Your profile partially matches the job title." : "Limited title overlap — consider tailoring your headline."}
                  </div>
                </div>
              );
            })()}
            {(() => {
              const aiP   = normalizeScore(job.xai?.match_score ?? job.match_score);
              const aiCol = scoreColor(aiP);
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 6, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>🤖 AI Match</div>
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${aiP * 100}%`, background: aiCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: aiCol, fontFamily: MONO }}>{(aiP * 100).toFixed(1)}%</span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>BiEncoder Score</strong> —{" "}
                    {job.xai?.explanations?.[0] ? job.xai.explanations[0] : aiP >= 0.75 ? "Excellent overall fit." : aiP >= 0.55 ? "Good fit — a few gaps exist." : "Moderate fit — key requirements may be missing."}
                  </div>
                </div>
              );
            })()}
            {job.gap_total > 0 && (() => {
              const covered = job.gap_total - missingAll.length;
              const pct     = Math.round(covered / job.gap_total * 100);
              const covCol  = pct >= 70 ? C.green : pct >= 40 ? C.amber : C.red;
              return (
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "7px 10px", background: C.white, borderRadius: 8, marginBottom: 4, border: `1px solid ${C.border}` }}>
                  <div style={{ minWidth: 130 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.06em", fontFamily: MONO, marginBottom: 4 }}>📊 Skills Coverage</div>
                    <div style={{ height: 4, borderRadius: 99, background: C.border, overflow: "hidden", marginBottom: 3 }}>
                      <div style={{ height: "100%", width: `${pct}%`, background: covCol, borderRadius: 99 }} />
                    </div>
                    <span style={{ fontSize: 12, fontWeight: 800, color: covCol, fontFamily: MONO }}>{covered}/{job.gap_total}</span>
                  </div>
                  <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.55, paddingTop: 2 }}>
                    <strong style={{ color: C.text }}>{pct}% covered</strong> —{" "}
                    {missingAll.length === 0 ? "You meet all required skills! 🎉" : `Missing: ${missingAll.slice(0, 4).join(", ")}${missingAll.length > 4 ? ` +${missingAll.length - 4} more` : ""}.`}
                  </div>
                </div>
              );
            })()}
            {job.xai?.explanations?.slice(1).map((e, i) => (
              <div key={i} style={{ fontSize: 10, color: C.muted, lineHeight: 1.5, padding: "4px 8px", background: C.bg, borderRadius: 6, marginTop: 4, fontFamily: MONO, fontStyle: "italic" }}>{e}</div>
            ))}
            {job.xai?.tip && job.xai.tip.trim() && (
              <div style={{ marginTop: 8, padding: "10px 12px", background: "linear-gradient(135deg, rgba(250,204,21,.12) 0%, rgba(250,204,21,.04) 100%)", borderRadius: 8, border: "1px solid rgba(250,204,21,.25)" }}>
                <div style={{ fontSize: 9, fontWeight: 800, color: C.amber, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4 }}>💡 Tip for this role</div>
                <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.5 }}>{job.xai.tip}</div>
              </div>
            )}
            {job.xai?.strength && job.xai.strength.trim() && (
              <div style={{ marginTop: 6, padding: "10px 12px", background: "linear-gradient(135deg, rgba(22,163,74,.08) 0%, rgba(22,163,74,.03) 100%)", borderRadius: 8, border: "1px solid rgba(22,163,74,.2)" }}>
                <div style={{ fontSize: 9, fontWeight: 800, color: C.green, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 4 }}>✓ Your strength to highlight</div>
                <div style={{ fontSize: 11, color: "#4a3f60", lineHeight: 1.5 }}>{job.xai.strength}</div>
              </div>
            )}
          </div>
        )}
      </div>

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
//  Charts
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
            const x  = M.left + i * (BW + GAP);
            const bH = Math.max(2, (d[valueKey] / maxVal) * cH);
            const y  = M.top + cH - bH;
            const lbl: string = d[labelKey] || "";
            const tr = lbl.length > 9 ? lbl.slice(0, 8) + "…" : lbl;
            return (
              <g key={lbl}>
                <rect x={x} y={M.top} width={BW} height={cH} fill={C.light} rx={4} />
                <rect x={x} y={y} width={BW} height={bH} fill={barColor} rx={4} opacity={0.9}><title>{lbl}: {d[valueKey]}</title></rect>
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
          const lbl: string = d[labelKey] || "";
          const tr  = lbl.length > 18 ? lbl.slice(0, 17) + "…" : lbl;
          return (
            <g key={lbl}>
              <text x={LW - 8} y={y + RH / 2 + 4} textAnchor="end" fontSize={10} fill={C.muted}>{tr}</text>
              <rect x={LW} y={y + 4} width={BA} height={RH - 8} fill={C.light} rx={4} />
              <rect x={LW} y={y + 4} width={bW} height={RH - 8} fill={barColor} rx={4} opacity={0.85}><title>{lbl}: {d[valueKey]}</title></rect>
              <text x={LW + bW + 6} y={y + RH / 2 + 4} fontSize={10} fontWeight="700" fill={barColor}>{d[valueKey]}</text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  ScanningBanner
// ─────────────────────────────────────────────────────────────────────────────

function ScanningBanner({ pipeSteps, pipeRole, enrichN }: {
  pipeSteps: Record<string, PipeState>; pipeRole: string; enrichN: number;
}) {
  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 16, padding: "16px 22px", marginBottom: 20, boxShadow: "0 2px 12px rgba(122,63,176,.08)", position: "relative", overflow: "hidden" }}>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: GRAD }} />
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <div style={{ display: "flex", gap: 4 }}>
            {[0, 120, 240].map(d => (
              <div key={d} style={{ width: 6, height: 6, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />
            ))}
          </div>
          <span style={{ fontSize: 14, fontWeight: 700, color: C.text }}>Analyzing CV{pipeRole ? ` — ${pipeRole}` : "…"}</span>
        </div>
        {enrichN > 0 && <span style={{ fontSize: 12, color: C.muted, fontFamily: MONO }}>Enriched: <b style={{ color: C.p1 }}>{enrichN}</b></span>}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 10 }}>
        {PIPE_STEPS.map(step => (
          <div key={step.id} style={{ display: "flex", alignItems: "center", gap: 5, padding: "4px 10px", borderRadius: 7, fontSize: 10, fontWeight: 600, border: `1px solid ${pipeBorder(pipeSteps[step.id])}`, background: pipeBg(pipeSteps[step.id]), color: pipeColor(pipeSteps[step.id]), transition: "all .3s", fontFamily: MONO }}>
            <span style={{ width: 5, height: 5, borderRadius: "50%", background: "currentColor", display: "inline-block", flexShrink: 0, animation: pipeSteps[step.id] === "active" ? "pulse 1.1s infinite" : "none" }} />
            {step.icon} {step.label}
          </div>
        ))}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
        {SOURCES.map(src => (
          <div key={src} style={{ display: "flex", alignItems: "center", gap: 3, padding: "2px 8px", borderRadius: 5, fontSize: 9, fontWeight: 600, border: `1px solid ${pipeBorder(pipeSteps[src])}`, background: pipeBg(pipeSteps[src]), color: pipeColor(pipeSteps[src]), transition: "all .3s", fontFamily: MONO }}>
            {pipeSteps[src] === "done" ? "✓" : <span style={{ width: 4, height: 4, borderRadius: "50%", background: "currentColor", display: "inline-block", animation: "pulse 1.1s infinite" }} />}
            {" "}{src}
          </div>
        ))}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  ChatSidebar
// ─────────────────────────────────────────────────────────────────────────────

// ── Couleurs état voice ───────────────────────────────────────────────────────
const VOICE_COLORS: Record<string, string> = {
  idle:       C.p1,
  listening:  "#e53e3e",   // rouge pulsant = en écoute
  processing: "#d69e2e",   // jaune = traitement STT
  speaking:   "#38a169",   // vert = lecture TTS
  error:      "#e53e3e",
};

// ── Icône micro SVG inline ─────────────────────────────────────────────────
function MicIcon({ size = 16, color = "currentColor" }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color}
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z"/>
      <path d="M19 10v2a7 7 0 0 1-14 0v-2"/>
      <line x1="12" y1="19" x2="12" y2="23"/>
      <line x1="8"  y1="23" x2="16" y2="23"/>
    </svg>
  );
}

// ── Icône stop ─────────────────────────────────────────────────────────────
function StopIcon({ size = 14, color = "currentColor" }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill={color}>
      <rect x="4" y="4" width="16" height="16" rx="2"/>
    </svg>
  );
}

// ── Icône speaker ──────────────────────────────────────────────────────────
function SpeakerIcon({ size = 14, color = "currentColor" }: { size?: number; color?: string }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke={color}
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="11 5 6 9 2 9 2 15 6 15 11 19 11 5"/>
      <path d="M15.54 8.46a5 5 0 0 1 0 7.07"/>
      <path d="M19.07 4.93a10 10 0 0 1 0 14.14"/>
    </svg>
  );
}

function ChatSidebar({ userId, jobs = [] }: { userId: number; jobs?: Job[] }) {
  const [msgs,    setMsgs]    = useState<Message[]>([]);
  const [input,   setInput]   = useState("");
  const [loading, setLoading] = useState(false);
  const [chatErr, setChatErr] = useState("");
  const [ttsEnabled, setTtsEnabled] = useState(true);  // TTS activé par défaut
  const endRef = useRef<HTMLDivElement>(null);

  // ── Détection langue active (fr/en) pour STT + TTS ───────────────────────
  const [activeLang, setActiveLang] = useState<"fr" | "en">("fr");

  // ── Hook Voice ────────────────────────────────────────────────────────────
  const {
    voiceState, voiceError, detectedLang,
    startListening, stopListening, speakText, cancelSpeech,
  } = useVoice({
    defaultLang:        activeLang,       // langue par défaut (toggle FR/EN)
    silenceThresholdMs: 2000,             // 2s silence → arrêt auto
    silenceRmsThreshold: 0.012,
    onTranscript: (text: string, lang: string) => {
      // lang = langue détectée par Deepgram ("fr" ou "en")
      // On met à jour activeLang pour que le TTS suive automatiquement
      setActiveLang(lang as "fr" | "en");
      setInput(text);
      setTimeout(() => sendVoice(text, lang), 100);
    },
  });

  const isListening  = voiceState === "listening";
  const isProcessing = voiceState === "processing";
  const isSpeaking   = voiceState === "speaking";
  const micBusy      = voiceState !== "idle" && voiceState !== "error";

  // ── Charger l'historique depuis la DB au montage ──────────────────────────
  useEffect(() => {
    if (!userId) return;
    fetch(`/api/chat/history?user_id=${userId}`)
      .then(r => r.json())
      .then(d => { if (d.messages?.length) setMsgs(d.messages); })
      .catch(() => {});
  }, [userId]);

  // ── Écouter l'event logout → vider les msgs locaux ────────────────────────
  useEffect(() => {
    const handleLogout = () => { setMsgs([]); cancelSpeech(); };
    window.addEventListener("jobscan:logout", handleLogout);
    return () => window.removeEventListener("jobscan:logout", handleLogout);
  }, [cancelSpeech]);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: "smooth" }); }, [msgs]);

  // ── Fonction d'envoi commune (texte + voice) ─────────────────────────────
  async function _sendMessage(msg: string, voiceLang?: string) {
    if (!msg.trim() || loading) return;
    setInput("");
    setChatErr("");
    setMsgs(p => [...p, { role: "user", content: msg }]);
    setLoading(true);

    // Construire jobs_context pour le contexte LLM
    const jobsContext = jobs.slice(0, 30).map(j => ({
      title:      j.title,
      industry:   j.industry,
      location:   j.location,
      salary:     j.salary,
      remote:     j.remote,
      contract:   j.contract,
      experience: j.experience,
      match_score: normalizeScore(j.match_score),
      cosine:      normalizeScore(j.cosine ?? j.cosine_score),
      missing:     j.gap_missing || [],
      url:         j.url,
      source:      j.source,
    }));

    try {
      const r = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user_id:      String(userId),
          message:      msg,
          jobs_context: jobsContext,
        }),
      });
      if (!r.ok) { setChatErr(`Server error ${r.status}`); return; }
      const d = await r.json();
      if (d.response) {
        setMsgs(p => [...p, { role: "assistant", content: d.response }]);
        // Lire la réponse si TTS activé
        if (ttsEnabled) {
          // voiceLang = langue détectée par Deepgram STT
          // → Deepgram TTS répondra avec le même accent (FR→FR, EN→EN)
          const ttsLang = voiceLang || detectedLang || activeLang;
          await speakText(d.response, ttsLang);
        }
      } else {
        setChatErr("Empty response from server.");
      }
    } catch {
      setChatErr("Connection error — is the backend running?");
    } finally {
      setLoading(false);
    }
  }

  // Envoi depuis bouton / Enter (texte classique)
  const send = useCallback(() => {
    const msg = input.trim();
    if (!msg) return;
    // Texte tapé → TTS répondra dans la langue du toggle (activeLang)
    _sendMessage(msg, activeLang);
  }, [input, loading, activeLang]); // eslint-disable-line react-hooks/exhaustive-deps

  // Envoi depuis voice transcript — on mémorise la langue pour le TTS
  const sendVoice = useCallback((text: string, lang?: string) => {
    _sendMessage(text, lang);
  }, [loading]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Gestion bouton micro ──────────────────────────────────────────────────
  const handleMicClick = () => {
    if (isSpeaking) {
      cancelSpeech();
      return;
    }
    if (isListening) {
      stopListening();
      return;
    }
    if (!micBusy) {
      startListening();
    }
  };

  // ── Label tooltip micro ───────────────────────────────────────────────────
  const micTooltip = isListening  ? "Cliquez pour arrêter (ou silence 2s)"
                   : isProcessing ? "Transcription en cours…"
                   : isSpeaking   ? "Cliquez pour arrêter la lecture"
                   : "Cliquez pour parler";

  const micColor = VOICE_COLORS[voiceState] || C.p1;

  return (
    <div style={{
      width: 340, flexShrink: 0, background: C.white,
      border: `1px solid ${C.border}`, borderRadius: 20,
      padding: "18px 20px", display: "flex", flexDirection: "column",
      height: "calc(100vh - 112px)", position: "sticky", top: 72,
      boxShadow: "0 4px 24px rgba(122,63,176,.11)",
    }}>

      {/* ── Header chat ── */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14, paddingBottom: 12, borderBottom: `1px solid ${C.border}` }}>
        <div style={{ fontWeight: 800, fontSize: 14, color: C.p1, letterSpacing: -0.3 }}>💬 Career Assistant</div>

        {/* Contrôles header : langue + TTS toggle */}
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {/* Toggle langue STT/TTS */}
          <button
            title={`Langue : ${activeLang.toUpperCase()} — cliquez pour changer`}
            onClick={() => setActiveLang(l => l === "fr" ? "en" : "fr")}
            style={{
              fontSize: 10, fontWeight: 700, color: C.muted,
              background: "none", border: `1px solid ${C.border}`,
              borderRadius: 4, padding: "2px 5px", cursor: "pointer",
              letterSpacing: 0.5,
            }}
          >
            {activeLang.toUpperCase()}
          </button>

          {/* Toggle TTS on/off */}
          <button
            title={ttsEnabled ? "Désactiver la lecture vocale" : "Activer la lecture vocale"}
            onClick={() => { setTtsEnabled(v => !v); if (isSpeaking) cancelSpeech(); }}
            style={{
              background: "none", border: "none", cursor: "pointer",
              opacity: ttsEnabled ? 1 : 0.35, padding: 2,
              display: "flex", alignItems: "center",
            }}
          >
            <SpeakerIcon size={15} color={C.muted} />
          </button>
        </div>
      </div>

      {/* ── Messages ── */}
      <div style={{
        flex: 1, overflowY: "auto", display: "flex",
        flexDirection: "column", gap: 10, paddingRight: 4,
      }}>
        {msgs.length === 0 && (
          <div style={{ fontSize: 12, color: C.muted, textAlign: "center", marginTop: 32, lineHeight: 1.9, padding: "0 10px" }}>
            <div style={{ fontSize: 28, marginBottom: 10 }}>🤖</div>
            Ask me about your job matches, skills gap, roadmap or score explanations.
            <div style={{ marginTop: 10, fontSize: 11, opacity: 0.6 }}>🎙 Click the mic to speak</div>
          </div>
        )}
        {msgs.map((m, i) => (
          <div key={i} style={{
            padding: "10px 14px", borderRadius: 14, fontSize: 13, lineHeight: 1.65,
            ...(m.role === "user"
              ? { background: `linear-gradient(135deg, rgba(122,63,176,.13), rgba(122,63,176,.07))`, borderRight: `3px solid ${C.p1}`, alignSelf: "flex-end",   maxWidth: "88%", borderBottomRightRadius: 4 }
              : { background: C.bg, border: `1px solid ${C.border}`,                                 alignSelf: "flex-start", maxWidth: "95%", borderBottomLeftRadius: 4 }
            ),
          }}>
            {m.content.split("\n").map((l, j) => <div key={j}>{l || " "}</div>)}
          </div>
        ))}
        {loading && (
          <div style={{ fontSize: 11, color: "#9f8fb0", padding: "6px 12px", display: "flex", gap: 4, alignItems: "center" }}>
            <span>Thinking</span>
            {[0,150,300].map(d => (
              <div key={d} style={{
                width: 5, height: 5, borderRadius: "50%", background: "#9f8fb0",
                animation: `bounce 0.9s ${d}ms ease-in-out infinite`,
              }}/>
            ))}
          </div>
        )}
        {chatErr && <div style={{ fontSize: 11, color: C.red, padding: "4px 12px" }}>⚠ {chatErr}</div>}

        {/* Status voice */}
        {voiceState !== "idle" && (
          <div style={{
            fontSize: 10, color: micColor, padding: "4px 10px",
            background: `${micColor}12`, borderRadius: 6,
            display: "flex", alignItems: "center", gap: 6,
          }}>
            {isListening  && <><span style={{ animation: "pulse 1s ease-in-out infinite" }}>🔴</span> Écoute en cours ({activeLang.toUpperCase()})… silence 2s pour arrêter</>}
            {isProcessing && <><span>⏳</span> Transcription…</>}
            {isSpeaking   && <><span>🔊</span> Lecture vocale… <button onClick={cancelSpeech} style={{ fontSize: 9, color: C.muted, background: "none", border: "none", cursor: "pointer", textDecoration: "underline" }}>stop</button></>}
          </div>
        )}
        {voiceError && (
          <div style={{ fontSize: 10, color: C.red, padding: "4px 10px", background: "#fff0f0", borderRadius: 6 }}>
            ⚠ {voiceError}
          </div>
        )}

        <div ref={endRef} />
      </div>

      {/* ── Zone de saisie ── */}
      <div style={{ display: "flex", gap: 8, marginTop: 12, alignItems: "center", paddingTop: 12, borderTop: `1px solid ${C.border}` }}>

        {/* Bouton micro */}
        <button
          title={micTooltip}
          onClick={handleMicClick}
          disabled={isProcessing}
          style={{
            width: 38, height: 38, borderRadius: "50%", flexShrink: 0,
            border: `2px solid ${micColor}`,
            background: isListening ? `${micColor}18` : "white",
            cursor: isProcessing ? "not-allowed" : "pointer",
            display: "flex", alignItems: "center", justifyContent: "center",
            transition: "all .2s",
            animation: isListening ? "pulse 1.2s ease-in-out infinite" : "none",
            opacity: isProcessing ? 0.6 : 1,
          }}
        >
          {isSpeaking
            ? <StopIcon  size={14} color={micColor} />
            : isListening
            ? <StopIcon  size={14} color={micColor} />
            : <MicIcon   size={16} color={micColor} />
          }
        </button>

        {/* Input texte */}
        <input
          style={{ ...S.input, fontSize: 12, flex: 1 }}
          placeholder={isListening ? "Parlez maintenant…" : "Ask anything…"}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === "Enter" && !loading && send()}
          disabled={isListening || isProcessing}
        />

        {/* Bouton envoyer */}
        <button
          style={{ ...S.btn, padding: "8px 14px", fontSize: 12 }}
          onClick={send}
          disabled={loading || isListening || isProcessing}
        >
          →
        </button>
      </div>

      {/* CSS animations inline */}
      <style>{`
        @keyframes pulse {
          0%, 100% { transform: scale(1);   opacity: 1; }
          50%       { transform: scale(1.1); opacity: 0.8; }
        }
      `}</style>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  GapTab — Skills Gap (separate interface after Matches)
// ─────────────────────────────────────────────────────────────────────────────

type GapDataNormalized = {
  top_missing_skills: { skill: string; frequency: number }[];
  top_matched_skills: { skill: string; frequency: number }[];
  missing_enriched?: { skill: string; frequency: number; difficulty?: string; tip?: string; impact_pct?: number }[];
  coverage: number;
  total_market_skills: number;
  total_jobs?: number;
  cv_skills_preview?: string;
};

function normalizeGapResponse(api: any): GapDataNormalized | null {
  if (!api || typeof api !== "object") return null;
  const missing = api.missing || [];
  const matched = api.matched || [];
  const toPairs = (arr: any[]): { skill: string; frequency: number }[] =>
    arr.map((item: any) =>
      Array.isArray(item)
        ? { skill: String(item[0] ?? ""), frequency: Number(item[1] ?? 0) }
        : { skill: String(item?.skill ?? item), frequency: Number(item?.frequency ?? item?.count ?? 0) }
    ).filter((d: { skill: string; frequency: number }) => d.skill);
  const enriched = (api.missing_enriched || []).map((e: any) => ({
    skill: String(e?.skill ?? ""),
    frequency: Number(e?.count ?? e?.frequency ?? 0),
    difficulty: e?.difficulty,
    tip: e?.tip,
    impact_pct: e?.impact_pct != null ? Number(e.impact_pct) : undefined,
  })).filter((d: { skill: string }) => d.skill);
  return {
    top_missing_skills: enriched.length ? enriched.map((e: any) => ({ skill: e.skill, frequency: e.frequency })) : toPairs(api.top_missing_skills || missing).slice(0, 25),
    top_matched_skills: toPairs(api.top_missing_skills ? [] : matched).slice(0, 25),
    missing_enriched: enriched.length ? enriched : undefined,
    coverage: Number(api.coverage) || 0,
    total_market_skills: Number(api.total_market_skills) || 0,
    total_jobs: api.total_jobs != null ? Number(api.total_jobs) : undefined,
    cv_skills_preview: typeof api.cv_skills === "string" ? api.cv_skills : undefined,
  };
}

function GapTab({
  gapData,
  gapLoad,
  showLoadingPlaceholder,
  onAnalyze,
  onGoToRoadmap,
  onRefresh,
}: {
  gapData: any;
  gapLoad: boolean;
  showLoadingPlaceholder?: boolean;
  onAnalyze: () => void;
  onGoToRoadmap?: () => void;
  onRefresh?: () => void;
}) {
  const normalized = normalizeGapResponse(gapData);
  const hasData = normalized && (normalized.top_missing_skills.length > 0 || normalized.top_matched_skills.length > 0 || normalized.total_market_skills > 0);
  const showLoading = gapLoad || !!showLoadingPlaceholder;

  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 16, padding: "24px 28px", boxShadow: "0 2px 16px rgba(122,63,176,.06)" }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 18, fontWeight: 800, color: C.text, marginBottom: 4 }}>📊 Skills Gap</h2>
        <p style={{ fontSize: 13, color: C.muted }}>Top missing skills across the market vs your profile. Use this to prioritize what to learn.</p>
      </div>

      {showLoading && (
        <div style={{ textAlign: "center", padding: 48, color: C.muted }}>
          <div style={{ display: "flex", justifyContent: "center", gap: 6, marginBottom: 12 }}>
            {[0, 120, 240].map(d => <div key={d} style={{ width: 8, height: 8, borderRadius: "50%", background: C.p1, animation: `bounce 0.8s ${d}ms ease-in-out infinite` }} />)}
          </div>
          <div style={{ fontSize: 14, fontWeight: 600 }}>Analyzing skills gap…</div>
        </div>
      )}

      {!showLoading && !hasData && (
        <div style={{ textAlign: "center", padding: "48px 24px" }}>
          <div style={{ fontSize: 42, marginBottom: 16 }}>📊</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 8 }}>Top missing skills across the market</div>
          <div style={{ fontSize: 13, color: C.muted, marginBottom: 24, maxWidth: 400, margin: "0 auto 24px" }}>
            See which skills appear most in job listings but are missing from your profile. Run a scan first so we have your skills, then analyze.
          </div>
          <button style={S.btn} onClick={onAnalyze}>Analyze Skills Gap</button>
          {gapData && typeof gapData?.detail === "string" && (
            <div style={{ marginTop: 16, fontSize: 12, color: C.amber }}>{gapData.detail}</div>
          )}
        </div>
      )}

      {!showLoading && hasData && normalized && (
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <div style={{ ...S.sec, flex: 1, minWidth: 140, marginBottom: 0, textAlign: "center" }}>
                <div style={{ fontSize: 24, fontWeight: 800, color: C.p1 }}>{Math.round(normalized.coverage * 100)}%</div>
                <div style={{ fontSize: 11, color: C.muted }}>Market coverage</div>
              </div>
              <div style={{ ...S.sec, flex: 1, minWidth: 140, marginBottom: 0, textAlign: "center" }}>
                <div style={{ fontSize: 24, fontWeight: 800, color: C.text }}>{normalized.total_market_skills}</div>
                <div style={{ fontSize: 11, color: C.muted }}>Skills in market</div>
              </div>
            </div>
            <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
              {onGoToRoadmap && (
                <button style={S.btn} onClick={onGoToRoadmap}>🗺️ Build learning roadmap</button>
              )}
              {onRefresh && (
                <button style={S.btnOut} onClick={onRefresh}>🔄 Refresh analysis</button>
              )}
            </div>
          </div>

          {normalized.missing_enriched && normalized.missing_enriched.length > 0 && (
            <>
              {(() => {
                const top3 = normalized.missing_enriched.slice(0, 3);
                const sumImpact = top3.reduce((s, e) => s + (e.impact_pct ?? 0), 0);
                return sumImpact > 0 ? (
                  <div style={{ ...S.sec, marginBottom: 0, background: "rgba(122,63,176,.06)", borderColor: "rgba(122,63,176,.2)" }}>
                    <div style={{ fontSize: 12, color: C.text }}>💡 Learning the <strong>top 3</strong> missing skills could unlock <strong>~{Math.round(sumImpact)}%</strong> more jobs in the market.</div>
                  </div>
                ) : null;
              })()}

              <div style={S.sec}>
                <div style={{ fontSize: 11, color: C.muted, marginBottom: 10, textTransform: "uppercase", letterSpacing: "0.05em" }}>Learn first (priority order)</div>
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {normalized.missing_enriched.slice(0, 5).map((item, i) => {
                    const diff = (item.difficulty || "").toLowerCase();
                    const diffColor = diff === "beginner" ? C.green : diff === "advanced" ? C.amber : C.p2;
                    return (
                      <div key={item.skill} style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "flex-start", padding: "10px 12px", background: C.white, borderRadius: 8, border: `1px solid ${C.border}` }}>
                        <span style={{ fontSize: 12, fontWeight: 700, color: C.text, minWidth: 24 }}>#{i + 1}</span>
                        <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>{item.skill}</span>
                        {item.impact_pct != null && item.impact_pct > 0 && (
                          <span style={{ fontSize: 10, color: C.muted, fontFamily: MONO }}>{item.impact_pct}% of jobs</span>
                        )}
                        {item.difficulty && (
                          <span style={{ fontSize: 9, padding: "2px 6px", borderRadius: 4, background: `${diffColor}22`, color: diffColor, border: `1px solid ${diffColor}44`, fontWeight: 600 }}>{item.difficulty}</span>
                        )}
                        {item.tip && (
                          <div style={{ width: "100%", fontSize: 11, color: C.muted, marginTop: 4, paddingLeft: 32 }}>📚 {item.tip}</div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            </>
          )}

          <div>
            <div style={{ fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 12 }}>
              Top {normalized.top_missing_skills.length} missing skills across the market
            </div>
            <VerticalChart
              data={normalized.top_missing_skills}
              title="Missing skills (learn these to open more jobs)"
              valueKey="frequency"
              labelKey="skill"
              barColor={C.p0}
              height={260}
            />
          </div>

          {normalized.top_matched_skills.length > 0 && (
            <div style={S.sec}>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Your skills in demand</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                {normalized.top_matched_skills.map(({ skill, frequency }) => (
                  <span key={skill} style={{ fontSize: 11, padding: "4px 10px", borderRadius: 6, background: "rgba(22,163,74,.08)", color: C.green, border: "1px solid rgba(22,163,74,.25)" }}>
                    ✓ {skill} <span style={{ color: C.muted, fontWeight: 600 }}>({frequency})</span>
                  </span>
                ))}
              </div>
            </div>
          )}

          {normalized.cv_skills_preview && (
            <div style={S.sec}>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Your current skills</div>
              <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                {normalized.cv_skills_preview.split(",").slice(0, 24).map((s: string) => s.trim()).filter(Boolean).map((s: string) => (
                  <span key={s} style={{ fontSize: 10, padding: "2px 8px", borderRadius: 4, background: C.light, color: C.p2, border: `1px solid ${C.border}` }}>{s}</span>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  RoadmapTab — Learning roadmap (phases: beginner → intermediate → advanced)
// ─────────────────────────────────────────────────────────────────────────────

type RoadmapPhaseItem = {
  skill: string;
  jobs_count: number;
  difficulty: string;
  weeks: number;
  tip: string;
  project_ideas?: string[];
  prerequisites: string[];
  xai?: { rank: number; reason: string; market_impact_pct?: number; prereqs_met?: string[]; prereqs_missing?: string[]; llm_insight?: string };
};

function RoadmapPhaseCard({ item }: { item: RoadmapPhaseItem }) {
  const [expanded, setExpanded] = useState(false);
  const diff = (item.difficulty || "").toLowerCase();
  const diffColor = diff === "beginner" ? C.green : diff === "advanced" ? C.amber : C.p2;
  const xai = item.xai;
  return (
    <div style={{ ...S.sec, display: "flex", flexDirection: "column", gap: 8 }}>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8, alignItems: "center" }}>
        <span style={{ fontWeight: 700, fontSize: 13, color: C.text }}>{item.skill}</span>
        <span style={{ fontSize: 9, padding: "2px 7px", borderRadius: 4, background: `${diffColor}22`, color: diffColor, border: `1px solid ${diffColor}44` }}>{item.difficulty}</span>
        <span style={{ fontSize: 10, color: C.muted, fontFamily: MONO }}>~{item.weeks}w</span>
        {xai?.market_impact_pct != null && (
          <span style={{ fontSize: 10, color: C.p2 }}>{xai.market_impact_pct}% of jobs</span>
        )}
      </div>
      <div style={{ fontSize: 11, color: C.muted }}>📚 {item.tip}</div>
      {item.project_ideas && item.project_ideas.length > 0 && (
        <div style={{ fontSize: 11, color: C.p2, padding: "6px 8px", background: "rgba(122,63,176,.06)", borderRadius: 6, borderLeft: `3px solid ${C.p2}` }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>🛠️ Small project ideas</div>
          <ul style={{ margin: 0, paddingLeft: 16 }}>
            {item.project_ideas.map((idea, i) => (
              <li key={i} style={{ marginBottom: 2 }}>{idea}</li>
            ))}
          </ul>
        </div>
      )}
      {xai?.llm_insight && (
        <div style={{ fontSize: 11, color: C.p2, fontStyle: "italic", padding: "6px 8px", background: "rgba(122,63,176,.06)", borderRadius: 6, borderLeft: `3px solid ${C.p2}` }}>
          💡 {xai.llm_insight}
        </div>
      )}
      {item.prerequisites && item.prerequisites.length > 0 && (
        <div style={{ fontSize: 10, color: C.muted }}>
          Prerequisites: {item.prerequisites.join(", ")}
          {xai?.prereqs_met?.length ? <span style={{ color: C.green }}> — You have: {xai.prereqs_met.join(", ")}</span> : null}
          {xai?.prereqs_missing?.length ? <span style={{ color: C.amber }}> — Learn first: {xai.prereqs_missing.join(", ")}</span> : null}
        </div>
      )}
      {xai?.reason && (
        <>
          <button type="button" style={{ fontSize: 10, color: C.p2, background: "none", border: "none", cursor: "pointer", padding: 0, textAlign: "left", fontWeight: 600 }} onClick={() => setExpanded(!expanded)}>
            {expanded ? "▼ Hide why this order" : "▶ Why this order?"}
          </button>
          {expanded && <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.5 }}>{xai.reason}</div>}
        </>
      )}
    </div>
  );
}

function RoadmapTab({
  roadData,
  roadLoad,
  showLoadingPlaceholder,
  onGenerate,
  onRefresh,
}: {
  roadData: any;
  roadLoad: boolean;
  showLoadingPlaceholder?: boolean;
  onGenerate: () => void;
  onRefresh?: () => void;
}) {
  const showLoading = roadLoad || !!showLoadingPlaceholder;
  const phases = roadData?.phases || {};
  const beginner = (phases.beginner || []) as RoadmapPhaseItem[];
  const intermediate = (phases.intermediate || []) as RoadmapPhaseItem[];
  const advanced = (phases.advanced || []) as RoadmapPhaseItem[];
  const hasData = beginner.length > 0 || intermediate.length > 0 || advanced.length > 0;
  const totalWeeks = roadData?.total_weeks ?? 0;
  const coverage = roadData?.coverage != null ? Math.round(Number(roadData.coverage) * 100) : null;
  const message = roadData?.message || "Based on your skills gap and market demand.";

  return (
    <div style={{ background: C.white, border: `1px solid ${C.border}`, borderRadius: 16, padding: "24px 28px", boxShadow: "0 2px 16px rgba(122,63,176,.06)" }}>
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ fontSize: 18, fontWeight: 800, color: C.text, marginBottom: 4 }}>🗺️ Learning Roadmap</h2>
        <p style={{ fontSize: 13, color: C.muted }}>A phased plan from your skills gap. Start with Beginner, then Intermediate, then Advanced when you have the prerequisites.</p>
      </div>

      {showLoading && (
        <div style={{ textAlign: "center", padding: 48, color: C.muted }}>
          <div style={{ display: "flex", justifyContent: "center", gap: 6, marginBottom: 12 }}>
            {[0, 120, 240].map(d => <div key={d} style={{ width: 8, height: 8, borderRadius: "50%", background: C.p1, animation: `bounce 0.8s ${d}ms ease-in-out infinite` }} />)}
          </div>
          <div style={{ fontSize: 14, fontWeight: 600 }}>Generating roadmap…</div>
        </div>
      )}

      {!showLoading && !hasData && (
        <div style={{ textAlign: "center", padding: "48px 24px" }}>
          <div style={{ fontSize: 42, marginBottom: 16 }}>🗺️</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 8 }}>Your personalized learning roadmap</div>
          <div style={{ fontSize: 13, color: C.muted, marginBottom: 24, maxWidth: 400, margin: "0 auto 24px" }}>
            We'll build a phased plan (Beginner → Intermediate → Advanced) from your skills gap. Run a scan first so we have your skills.
          </div>
          <button style={S.btn} onClick={onGenerate}>Generate Learning Roadmap</button>
          {roadData?.detail && <div style={{ marginTop: 16, fontSize: 12, color: C.amber }}>{roadData.detail}</div>}
        </div>
      )}

      {!showLoading && hasData && (
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center", justifyContent: "space-between" }}>
            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <div style={{ ...S.sec, minWidth: 100, marginBottom: 0, textAlign: "center" }}>
                <div style={{ fontSize: 22, fontWeight: 800, color: C.p1 }}>{totalWeeks}w</div>
                <div style={{ fontSize: 10, color: C.muted }}>Total plan</div>
              </div>
              {coverage != null && (
                <div style={{ ...S.sec, minWidth: 100, marginBottom: 0, textAlign: "center" }}>
                  <div style={{ fontSize: 22, fontWeight: 800, color: C.text }}>{coverage}%</div>
                  <div style={{ fontSize: 10, color: C.muted }}>Market coverage</div>
                </div>
              )}
            </div>
            {onRefresh && <button style={S.btnOut} onClick={onRefresh}>🔄 Refresh roadmap</button>}
          </div>
          <div style={{ fontSize: 12, color: C.muted }}>{message}</div>

          {beginner.length > 0 && (
            <div>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Beginner — foundations</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {beginner.map(item => <RoadmapPhaseCard key={item.skill} item={item} />)}
              </div>
            </div>
          )}
          {intermediate.length > 0 && (
            <div>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Intermediate — core skills</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {intermediate.map(item => <RoadmapPhaseCard key={item.skill} item={item} />)}
              </div>
            </div>
          )}
          {advanced.length > 0 && (
            <div>
              <div style={{ fontSize: 11, color: C.muted, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>Advanced — specialization</div>
              <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                {advanced.map(item => <RoadmapPhaseCard key={item.skill} item={item} />)}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Dashboard
// ─────────────────────────────────────────────────────────────────────────────

function MatchesTab({ isScanning, scanJobs, jobs, jobsLoad, roleFilter, setRoleFilter, locFilter, setLocFilter, minFit, setMinFit, onSearch }: {
  isScanning: boolean; scanJobs: Job[]; jobs: Job[]; jobsLoad: boolean;
  roleFilter: string; setRoleFilter: (v: string) => void;
  locFilter:  string; setLocFilter:  (v: string) => void;
  minFit: number;     setMinFit:     (v: number) => void;
  onSearch: () => void;
}) {
  // Apply filters and sort by AI match (match_score) so list matches backend order and filters work
  const filteredAndSortedJobs = useMemo(() => {
    let list = [...jobs];
    if (roleFilter.trim()) {
      const words = roleFilter.trim().toLowerCase().split(/\s+/).filter(Boolean);
      list = list.filter(j => {
        const text = `${j.title || ""} ${j.industry || ""} ${(j.description || "")}`.toLowerCase();
        return words.some(w => text.includes(w));
      });
    }
    if (locFilter.trim()) {
      const loc = locFilter.trim().toLowerCase();
      list = list.filter(j => {
        const jLoc = (j.location || "").toLowerCase();
        const jRemote = (j.remote || "").toLowerCase();
        return jLoc.includes(loc) || (loc.includes("remote") && (jRemote.includes("remote") || jLoc.includes("remote")));
      });
    }
    if (minFit > 0) {
      list = list.filter(j => normalizeScore(j.match_score) >= minFit);
    }
    list.sort((a, b) => (normalizeScore(b.match_score) || 0) - (normalizeScore(a.match_score) || 0));
    return list;
  }, [jobs, roleFilter, locFilter, minFit]);

  return (
    <div>
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
          {isScanning ? `${scanJobs.length} matched so far…` : `${filteredAndSortedJobs.length} jobs · Cosine + AI Match`}
        </span>
      </div>

      {jobsLoad && !isScanning && <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Loading matches…</div>}

      {isScanning && scanJobs.length === 0 && (
        <div style={{ textAlign: "center", padding: "80px 20px" }}>
          <div style={{ display: "flex", justifyContent: "center", gap: 7, marginBottom: 20 }}>
            {[0, 150, 300].map(d => <div key={d} style={{ width: 11, height: 11, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />)}
          </div>
          <div style={{ fontSize: 17, fontWeight: 700, color: C.text, marginBottom: 10 }}>Scraping in progress…</div>
          <div style={{ fontSize: 13, color: C.muted }}>Scraping boards · AI scoring · Skills gap computation</div>
        </div>
      )}

      {isScanning && scanJobs.length > 0 && (
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
            <div style={{ display: "flex", gap: 4 }}>
              {[0, 100, 200].map(d => <div key={d} style={{ width: 6, height: 6, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />)}
            </div>
            <span style={{ fontSize: 13, fontWeight: 700, color: C.text }}>{scanJobs.length} job{scanJobs.length > 1 ? "s" : ""} matched · still scanning…</span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(310px,1fr))", gap: 14 }}>
            {scanJobs.map((job, i) => <div key={job.url + i} style={{ animation: "cardIn .4s ease both" }}><JobCard job={job} /></div>)}
          </div>
        </div>
      )}

      {!isScanning && !jobsLoad && jobs.length === 0 && (
        <div style={{ textAlign: "center", padding: "70px 20px", color: C.muted }}>
          <div style={{ fontSize: 36, marginBottom: 14 }}>🔍</div>
          <div style={{ fontSize: 15, fontWeight: 600, color: C.text, marginBottom: 8 }}>No jobs yet</div>
          <div style={{ fontSize: 12 }}>Go back to the home page and run a scan to populate your matches.</div>
        </div>
      )}

      {!isScanning && jobs.length > 0 && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(310px,1fr))", gap: 14 }}>
          {filteredAndSortedJobs.length === 0 ? (
            <div style={{ gridColumn: "1 / -1", textAlign: "center", padding: "40px 20px", color: C.muted }}>
              <div style={{ fontSize: 15, fontWeight: 600, color: C.text, marginBottom: 8 }}>No jobs match your filters</div>
              <div style={{ fontSize: 12 }}>Try loosening the role, location or minimum score.</div>
            </div>
          ) : (
            filteredAndSortedJobs.map((job, i) => <JobCard key={`${job.url}-${i}`} job={job} />)
          )}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Dashboard
// ─────────────────────────────────────────────────────────────────────────────

function Dashboard() {
  const router       = useRouter();
  const searchParams = useSearchParams();
  const userId       = parseInt(searchParams.get("user_id") || "0", 10);
  const shouldScan   = searchParams.get("scan") === "1";

  const [userName,   setUserName]   = useState(`User #${userId}`);
  const [isScanning, setIsScanning] = useState(false);
  const [pipeSteps,  setPipeSteps]  = useState<Record<string, PipeState>>(initPipeSteps());
  const [pipeRole,   setPipeRole]   = useState("");
  const [scanJobs,   setScanJobs]   = useState<Job[]>([]);
  const [enrichN,    setEnrichN]    = useState(0);
  const [activeTab,  setActiveTab]  = useState<Tab>("matches");
  const [jobs,       setJobs]       = useState<Job[]>([]);
  const [jobsLoad,   setJobsLoad]   = useState(false);
  const [roleFilter, setRoleFilter] = useState("");
  const [locFilter,  setLocFilter]  = useState("");
  const [minFit,     setMinFit]     = useState(0);
  const [gapData,    setGapData]    = useState<any>(null);
  const [gapLoad,    setGapLoad]    = useState(false);
  const [roadData,   setRoadData]   = useState<any>(null);
  const [roadLoad,   setRoadLoad]   = useState(false);
  const [mktData,    setMktData]    = useState<any>(null);
  const [mktLoad,    setMktLoad]    = useState(false);
  const [repData,    setRepData]    = useState("");
  const [repLoad,    setRepLoad]    = useState(false);

  useEffect(() => {
    if (!userId) { router.replace("/"); return; }
    if (shouldScan) {
      const cv = sessionStorage.getItem("jobscan_cv_text") || "";
      runScan(cv);
    } else {
      fetchUserAndJobs();
    }
  }, []);

  useEffect(() => {
    if (activeTab === "gap" && userId && !gapData) {
      setGapLoad(true);
      loadGap();
    }
    if (activeTab === "roadmap" && userId && !roadData) {
      setRoadLoad(true);
      loadRoadmap();
    }
    if (activeTab === "market"  && !mktData  && userId) loadMarket();
    if (activeTab === "report"  && !repData  && userId) loadReport();
  }, [activeTab]);

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
          if (d.event === "no_cache") break;
          if (d.event === "job")      loaded.push(d as Job);
          if (d.event === "done")   { break; }
        }
      }
      // Dédupliquer par URL (évite les doublons de la DB)
      const seen = new Set<string>();
      const unique = loaded.filter(j => {
        if (seen.has(j.url)) return false;
        seen.add(j.url); return true;
      });
      setJobs(unique);
    } finally { setJobsLoad(false); }
  }

  async function loadGap() {
    setGapLoad(true);
    try { const r = await fetch("/api/gap", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: String(userId) }) }); setGapData(await r.json()); }
    finally { setGapLoad(false); }
  }

  async function loadRoadmap() {
    setRoadLoad(true);
    try { const r = await fetch("/api/roadmap", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: String(userId) }) }); setRoadData(await r.json()); }
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
      const reader  = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = "", enriched = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const chunks = buf.split("\n\n");
        buf = chunks.pop() || "";
        for (const chunk of chunks) {
          const line = chunk.trim();
          if (!line.startsWith("data: ")) continue;
          let d: any;
          try { d = JSON.parse(line.slice(6)); } catch { continue; }
          switch (d.event) {
            case "lang_ready":  setPipeSteps(p => ({ ...p, lang: "done", title: "active" })); break;
            case "cv_title":    setPipeSteps(p => ({ ...p, title: "done", struct: "active" })); if (d.title) setPipeRole(d.title); break;
            case "cv_ready":    setPipeSteps(p => ({ ...p, struct: "done", dbuser: "active", scrape: "active", enrich: "active" })); break;
            case "user_saved":  setPipeSteps(p => ({ ...p, dbuser: "done" })); break;
            case "source_done": setPipeSteps(p => ({ ...p, [d.source]: "done" })); break;
            case "job":
              enriched++;
              setEnrichN(enriched);
              setScanJobs(prev => [...prev, d as Job]);
              if (enriched === 1) setPipeSteps(p => ({ ...p, dbjobs: "active" }));
              break;
            case "done":
              setPipeSteps(p => ({ ...p, scrape: "done", enrich: "done", dbjobs: "done" }));
              reader.cancel();
              // Keep scan result (with xai) visible after scan: copy scanJobs into jobs
              // so when isScanning becomes false we show the same cards (with xai).
              setScanJobs(prev => {
                if (prev.length) setJobs([...prev]);
                return prev;
              });
              break;
          }
        }
      }
      const r = await fetch("/api/login", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ user_id: userId }) });
      if (r.ok) { const d = await r.json(); setUserName(d.name || `User #${userId}`); }
      // Do not call loadJobs() here: we already set jobs from scanJobs above so xai stays visible.
      // User can click "Search" to refresh from DB.
    } catch (err) {
      console.error("Scan error:", err);
    } finally {
      setIsScanning(false);
    }
  }

  // ── LOGOUT : vide le chat local PUIS redirige ─────────────────────────────
  function logout() {
    // Émet un event → ChatSidebar vide ses msgs React immédiatement
    window.dispatchEvent(new Event("jobscan:logout"));
    // Vide le sessionStorage (CV, user_id, etc.)
    sessionStorage.clear();
    // Redirige vers la page de login
    router.push("/");
  }

  return (
    <div style={S.page}>
      <style>{GLOBAL_CSS}</style>

      {/* HEADER */}
      <header style={{ background: C.white, borderBottom: `1px solid ${C.border}`, padding: "0 32px", display: "flex", alignItems: "center", justifyContent: "space-between", height: 64, position: "sticky", top: 0, zIndex: 100, boxShadow: "0 1px 8px rgba(122,63,176,.06)" }}>
        <div>
          <span style={{ fontWeight: 800, fontSize: 20, background: GRAD, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", backgroundClip: "text" }}>CareerAssistant</span>
          <span style={{ fontSize: 10, color: "#9f8fb0", marginLeft: 10, fontFamily: MONO }}>JobScan AI · Cosine · BiEncoder · XAI</span>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 12, color: C.muted }}>👤 {userName}</span>
          <button style={{ ...S.btnOut, fontSize: 11 }} onClick={logout}>Logout</button>
        </div>
      </header>

      {/* BODY */}
      <div style={{ maxWidth: 1320, margin: "0 auto", padding: "24px 24px", display: "flex", gap: 20 }}>

        <ChatSidebar userId={userId} />

        <div style={{ flex: 1, minWidth: 0 }}>
          {isScanning && <ScanningBanner pipeSteps={pipeSteps} pipeRole={pipeRole} enrichN={enrichN} />}

          <div style={{ display: "flex", gap: 6, marginBottom: 18, flexWrap: "wrap" }}>
            {(["matches", "gap", "roadmap", "market", "report"] as Tab[]).map(tab => (
              <button
                key={tab}
                style={S.tab(activeTab === tab)}
                onClick={() => setActiveTab(tab)}
              >
                {{ matches: "🏆 Matches", gap: "📊 Skills Gap", roadmap: "🗺️ Roadmap", market: "📈 Market", report: "📄 Report" }[tab]}
              </button>
            ))}
          </div>

          {activeTab === "matches" && (
            <MatchesTab isScanning={isScanning} scanJobs={scanJobs} jobs={jobs} jobsLoad={jobsLoad} roleFilter={roleFilter} setRoleFilter={setRoleFilter} locFilter={locFilter} setLocFilter={setLocFilter} minFit={minFit} setMinFit={setMinFit} onSearch={loadJobs} />
          )}

          {activeTab === "gap" && (
            <GapTab
              gapData={gapData}
              gapLoad={gapLoad}
              showLoadingPlaceholder={activeTab === "gap" && !!userId && !gapData}
              onAnalyze={loadGap}
              onGoToRoadmap={() => setActiveTab("roadmap")}
              onRefresh={() => { setGapData(null); setGapLoad(true); loadGap(); }}
            />
          )}

          {activeTab === "roadmap" && (
            <RoadmapTab
              roadData={roadData}
              roadLoad={roadLoad}
              showLoadingPlaceholder={activeTab === "roadmap" && !!userId && !roadData}
              onGenerate={loadRoadmap}
              onRefresh={() => { setRoadData(null); setRoadLoad(true); loadRoadmap(); }}
            />
          )}

          {activeTab === "market" && (
            <div>
              {mktLoad ? <div style={{ textAlign: "center", padding: 40, color: C.muted }}>Loading market data…</div>
               : !mktData ? <button style={S.btn} onClick={loadMarket}>Load Market Insights</button>
               : (
                <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                  <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                    {[
                      { label: "Total Jobs",   value: mktData.total_jobs ?? "—" },
                      { label: "Avg AI Score", value: mktData.avg_ai_score != null ? `${mktData.avg_ai_score}%` : "—" },
                      { label: "Excellent",    value: mktData.score_breakdown?.excellent ?? 0 },
                      { label: "Good",         value: mktData.score_breakdown?.good ?? 0 },
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
              )}
            </div>
          )}

          {activeTab === "report" && (
            <div>
              {repLoad ? (
                <div style={{ textAlign: "center", padding: 48, color: C.muted }}>
                  <div style={{ display: "flex", justifyContent: "center", gap: 6, marginBottom: 12 }}>
                    {[0, 120, 240].map(d => <div key={d} style={{ width: 8, height: 8, borderRadius: "50%", background: C.p1, animation: `bounce 0.8s ${d}ms ease-in-out infinite` }} />)}
                  </div>
                  <div style={{ fontSize: 15, fontWeight: 600 }}>Generating your report…</div>
                  <div style={{ fontSize: 12, marginTop: 8 }}>Profile, market, skills gap, roadmap &amp; matches</div>
                </div>
              ) : repData ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 12 }}>
                    <p style={{ fontSize: 14, color: C.text, margin: 0 }}>
                      Your career report is ready. It includes your profile, market overview, skills gap, learning roadmap, and top job matches.
                    </p>
                    <button
                      style={{ ...S.btn, background: "linear-gradient(135deg,#22c55e,#16a34a)", color: "white", fontWeight: 700 }}
                      onClick={downloadPDF}
                    >
                      📄 Download PDF
                    </button>
                  </div>
                  <div style={{ ...S.sec, maxHeight: 520, overflowY: "auto" }}>
                    <pre style={{ fontSize: 11, lineHeight: 1.65, color: "#4a3f60", whiteSpace: "pre-wrap", fontFamily: MONO, margin: 0 }}>{repData}</pre>
                  </div>
                  <div style={{ display: "flex", gap: 10 }}>
                    <button style={S.btn} onClick={loadReport} disabled={repLoad}>🔄 Regenerate report</button>
                    <button style={{ ...S.btn, background: "linear-gradient(135deg,#22c55e,#16a34a)", color: "white" }} onClick={downloadPDF}>📄 Download PDF</button>
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: "center", padding: 40, color: C.muted }}>
                  <p style={{ marginBottom: 16 }}>Open the Report tab to generate your career report.</p>
                  <button style={S.btn} onClick={loadReport}>Generate report</button>
                </div>
              )}
            </div>
          )}

        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
//  Export
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