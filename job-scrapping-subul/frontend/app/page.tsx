"use client";
// ─────────────────────────────────────────────────────────────────────────────
//  app/page.tsx  —  LOGIN
//
//  Flow complet :
//    STEP 1 : saisir user ID
//      → POST /api/login
//        • 200 OK  → user EXISTS   → dashboard (/app?user_id=X)
//        • 404     → user NOUVEAU  → STEP 2
//
//    STEP 2 : saisir CV / summary
//      → POST /api/onboarding
//        • LLM extrait titre + skills + structure
//        • Enregistre le user dans la DB
//        • Retourne { cv_title, role, skills, ready_to_scan }
//      → STEP 3 : confirmation des données extraites
//
//    STEP 3 : confirmation profil extrait
//      → router.push("/app?user_id=X&scan=1")
//        • /app lance le scan SSE complet (scrape + match + save jobs)
// ─────────────────────────────────────────────────────────────────────────────

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { GRAD, FONT, MONO, C, S, GLOBAL_CSS } from "@/app/lib/theme";

// ── Types ─────────────────────────────────────────────────────────────────────
interface ExtractedProfile {
  cv_title: string;
  role: string;
  seniority: string;
  skills: string;
  summary: string;
  ready_to_scan: boolean;
}

// ── Helpers UI ────────────────────────────────────────────────────────────────
function GradBar() {
  return <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 3, background: GRAD }} />;
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <label style={{ display: "block", fontSize: 11, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 8 }}>
      {children}
    </label>
  );
}

function Spinner() {
  return (
    <>
      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      <span style={{ display: "inline-block", width: 14, height: 14, border: "2px solid #fff4", borderTop: "2px solid #fff", borderRadius: "50%", animation: "spin 0.7s linear infinite", verticalAlign: "middle" }} />
    </>
  );
}

const PIPELINE_PREVIEW = [
  "🌍 Detect language", "🤖 Extract title", "🧬 Structure CV",
  "💾 Save profile",    "🕷️ Scrape 6 boards", "📐 Cosine ≥ 0.60",
  "🧠 AI scoring",      "🗄️ Save jobs",
];

// ── Main component ────────────────────────────────────────────────────────────
export default function LoginPage() {
  const router = useRouter();

  type Step = "id" | "cv" | "extracting" | "confirm";
  const [step,      setStep]      = useState<Step>("id");
  const [uidInput,  setUidInput]  = useState("");
  const [cvText,    setCvText]    = useState("");
  const [loading,   setLoading]   = useState(false);
  const [error,     setError]     = useState("");
  const [extracted, setExtracted] = useState<ExtractedProfile | null>(null);

  // Restore session
  useEffect(() => {
    const sid = sessionStorage.getItem("jobscan_user_id");
    const scv = sessionStorage.getItem("jobscan_cv_text");
    if (sid) setUidInput(sid);
    if (scv) setCvText(scv);
  }, []);

  // ── STEP 1 : check user ID ────────────────────────────────────────────────
  async function handleCheckUser() {
    const uid = parseInt(uidInput, 10);
    if (!uid || uid < 1) { setError("Please enter a valid numeric ID"); return; }

    setLoading(true);
    setError("");

    try {
      const res = await fetch("/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: String(uid) }),
      });

      sessionStorage.setItem("jobscan_user_id", String(uid));

      if (res.ok) {
        // ✅ User EXISTS → dashboard direct
        router.push(`/app?user_id=${uid}`);
      } else if (res.status === 404) {
        // ❌ User NEW → saisir CV
        setStep("cv");
      } else {
        const body = await res.json().catch(() => ({}));
        setError(body?.detail || `Server error ${res.status}`);
      }
    } catch {
      setError("Connection error — is the Career Assistant API running?");
    } finally {
      setLoading(false);
    }
  }

  // ── STEP 2 : CV → onboarding (LLM extract + save DB) ─────────────────────
  async function handleOnboarding() {
    const uid = parseInt(uidInput, 10);
    const cv  = cvText.trim();
    if (cv.length < 30) { setError("Please paste your CV (minimum 30 characters)"); return; }

    setLoading(true);
    setError("");
    setStep("extracting");  // Show extraction loader immediately

    try {
      sessionStorage.setItem("jobscan_cv_text", cv);

      const res = await fetch("/api/onboarding", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: uid, summary: cv }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        setError(body?.detail || `Extraction error ${res.status}`);
        setStep("cv");
        return;
      }

      const data: ExtractedProfile = await res.json();
      setExtracted(data);
      setStep("confirm");   // Show extracted profile for confirmation

    } catch {
      setError("Connection error during extraction — is the API running?");
      setStep("cv");
    } finally {
      setLoading(false);
    }
  }

  // ── STEP 3 : confirm → launch scan ───────────────────────────────────────
  function handleLaunchScan() {
    const uid = parseInt(uidInput, 10);
    router.push(`/app?user_id=${uid}&scan=1`);
  }

  const charCount = cvText.trim().length;
  const charOk    = charCount >= 30;

  // ── Render ─────────────────────────────────────────────────────────────────
  return (
    <div style={{ ...S.page, display: "flex", flexDirection: "column" }}>
      <style>{GLOBAL_CSS}</style>

      {/* Header */}
      <header style={{ background: C.white, borderBottom: `1px solid ${C.border}`, height: 60, display: "flex", alignItems: "center", padding: "0 32px" }}>
        <span style={{ fontWeight: 800, fontSize: 18, background: GRAD, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", backgroundClip: "text" }}>
          CareerAssistant
        </span>
      </header>

      {/* Centered card */}
      <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", padding: "40px 16px" }}>
        <div style={{
          background: C.white, border: `1px solid ${C.border}`, borderRadius: 20,
          padding: "48px 44px", width: "100%",
          maxWidth: step === "cv" ? 560 : step === "confirm" ? 520 : 440,
          boxShadow: "0 8px 40px rgba(122,63,176,.13)",
          position: "relative", overflow: "hidden", animation: "fadeUp .4s ease",
        }}>
          <GradBar />

          {/* Logo */}
          <div style={{ textAlign: "center", marginBottom: 30 }}>
            <div style={{ fontSize: 44, marginBottom: 8 }}>🎯</div>
            <h1 style={{ fontSize: 24, fontWeight: 800, margin: 0, background: GRAD, WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent", backgroundClip: "text" }}>
              CareerAssistant
            </h1>
            <p style={{ fontSize: 12, color: "#9f8fb0", marginTop: 6 }}>
              Powered by JobScan AI · Azure OpenAI + MiniLM
            </p>
          </div>

          {/* ══════════ STEP 1 — USER ID ══════════ */}
          {step === "id" && (
            <>
              <Label>Your User ID</Label>
              <p style={{ fontSize: 11, color: "#9f8fb0", textAlign: "center", marginBottom: 14 }}>
                Existing ID → load your dashboard &nbsp;·&nbsp; New ID → create profile
              </p>
              <div style={{ position: "relative", marginBottom: 8 }}>
                <span style={{ position: "absolute", left: 14, top: "50%", transform: "translateY(-50%)", fontSize: 16, pointerEvents: "none" }}>👤</span>
                <input
                  type="number" placeholder="e.g. 1001" autoFocus
                  value={uidInput}
                  onChange={e => { setUidInput(e.target.value); setError(""); }}
                  onKeyDown={e => e.key === "Enter" && handleCheckUser()}
                  style={{ ...S.input, paddingLeft: 44, fontSize: 20, fontWeight: 700, letterSpacing: 4, borderRadius: 12 }}
                />
              </div>
              <button
                onClick={handleCheckUser} disabled={loading}
                style={{ ...S.btn, width: "100%", padding: "14px", fontSize: 15, borderRadius: 12, marginTop: 16, opacity: loading ? 0.7 : 1, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}
              >
                {loading ? <><Spinner /> Checking…</> : "Continue →"}
              </button>
            </>
          )}

          {/* ══════════ STEP 2 — CV INPUT ══════════ */}
          {step === "cv" && (
            <>
              <div style={{ marginBottom: 16 }}>
                <div style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 10, fontWeight: 700, color: C.p1, background: "rgba(195,55,155,.08)", border: "1px solid rgba(195,55,155,.2)", borderRadius: 20, padding: "3px 10px", marginBottom: 10, fontFamily: MONO }}>
                  ✨ New profile · User #{uidInput}
                </div>
                <h2 style={{ fontSize: 19, fontWeight: 800, color: C.text, margin: 0 }}>Paste your CV / Profile Summary</h2>
                <p style={{ fontSize: 12, color: "#9f8fb0", marginTop: 6, lineHeight: 1.6 }}>
                  The AI will extract your profile data, then scrape 6 job boards and score each match in real time.
                </p>
              </div>

              <Label>CV / Profile Summary</Label>
              <textarea
                rows={9} autoFocus
                placeholder={"Paste your full CV or profile summary here…\n\n• Work experience, job titles, companies\n• Technical skills: Python, SQL, Docker, AWS…\n• Education, certifications\n• Projects and achievements"}
                value={cvText}
                onChange={e => { setCvText(e.target.value); setError(""); }}
                style={{ ...S.input, fontSize: 12, fontFamily: MONO, lineHeight: 1.75, resize: "vertical", minHeight: 200, borderRadius: 12 }}
              />
              <div style={{ fontSize: 10, textAlign: "right", marginTop: 4, marginBottom: 16, fontFamily: MONO, color: charCount === 0 ? "#ccc" : charOk ? C.green : C.amber }}>
                {charCount === 0 ? "0 characters" : charOk ? `${charCount} characters ✓` : `${charCount} — need ${30 - charCount} more`}
              </div>

              {/* Pipeline preview */}
              <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 10, padding: "10px 14px", marginBottom: 20 }}>
                <div style={{ fontSize: 9, fontWeight: 700, color: "#9f8fb0", textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8, fontFamily: MONO }}>
                  Pipeline after Analyze
                </div>
                <div style={{ display: "flex", flexWrap: "wrap", gap: 5 }}>
                  {PIPELINE_PREVIEW.map(s => (
                    <span key={s} style={{ fontSize: 9, padding: "3px 8px", borderRadius: 6, border: `1px solid ${C.border}`, background: C.white, color: C.muted, fontFamily: MONO, fontWeight: 600 }}>{s}</span>
                  ))}
                </div>
              </div>

              <div style={{ display: "flex", gap: 10 }}>
                <button onClick={() => { setStep("id"); setError(""); }} style={{ ...S.btnOut, flex: 1 }}>
                  ← Back
                </button>
                <button
                  onClick={handleOnboarding} disabled={!charOk}
                  style={{ ...S.btn, flex: 2, padding: "13px", fontSize: 14, borderRadius: 12, opacity: charOk ? 1 : 0.45, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}
                >
                  🔍 Analyze my CV
                </button>
              </div>
            </>
          )}

          {/* ══════════ STEP 2.5 — EXTRACTING (loader) ══════════ */}
          {step === "extracting" && (
            <div style={{ textAlign: "center", padding: "20px 0" }}>
              {/* Animated dots */}
              <div style={{ display: "flex", justifyContent: "center", gap: 7, marginBottom: 24 }}>
                {[0, 150, 300].map(d => (
                  <div key={d} style={{ width: 10, height: 10, borderRadius: "50%", background: C.p1, animation: `bounce 0.9s ${d}ms ease-in-out infinite` }} />
                ))}
              </div>
              <div style={{ fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 8 }}>
                Extracting your profile…
              </div>
              <div style={{ fontSize: 12, color: C.muted, lineHeight: 1.7 }}>
                🌍 Detecting language<br />
                🤖 Extracting job title<br />
                🧬 Structuring CV data<br />
                💾 Saving profile to database
              </div>
            </div>
          )}

          {/* ══════════ STEP 3 — CONFIRM EXTRACTED PROFILE ══════════ */}
          {step === "confirm" && extracted && (
            <>
              {/* Success header */}
              <div style={{ textAlign: "center", marginBottom: 20 }}>
                <div style={{ fontSize: 32, marginBottom: 8 }}>✅</div>
                <div style={{ fontSize: 16, fontWeight: 800, color: C.text }}>Profile extracted successfully!</div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 4 }}>Saved to database · User #{uidInput}</div>
              </div>

              {/* Extracted data */}
              <div style={{ background: C.light, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 16px", marginBottom: 20, display: "flex", flexDirection: "column", gap: 10 }}>

                {/* Role */}
                <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
                  <span style={{ fontSize: 16 }}>👤</span>
                  <div>
                    <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.08em", fontFamily: MONO, marginBottom: 2 }}>Detected Role</div>
                    <div style={{ fontSize: 14, fontWeight: 800, color: C.text }}>{extracted.cv_title || extracted.role}</div>
                    {extracted.seniority && (
                      <span style={{ fontSize: 10, padding: "1px 8px", borderRadius: 10, background: "rgba(122,63,176,.08)", color: C.p2, border: "1px solid rgba(122,63,176,.2)", fontFamily: MONO, fontWeight: 600 }}>
                        {extracted.seniority}
                      </span>
                    )}
                  </div>
                </div>

                {/* Skills */}
                {extracted.skills && (
                  <div>
                    <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.08em", fontFamily: MONO, marginBottom: 6 }}>Extracted Skills</div>
                    <div style={{ display: "flex", flexWrap: "wrap", gap: 4 }}>
                      {extracted.skills.split(",").slice(0, 12).map(s => s.trim()).filter(Boolean).map(s => (
                        <span key={s} style={{ fontSize: 10, padding: "2px 8px", borderRadius: 6, background: "rgba(22,163,74,.07)", color: C.green, border: "1px solid rgba(22,163,74,.25)", fontFamily: MONO, fontWeight: 600 }}>
                          ✓ {s}
                        </span>
                      ))}
                      {extracted.skills.split(",").length > 12 && (
                        <span style={{ fontSize: 10, padding: "2px 8px", borderRadius: 6, background: C.bg, color: C.muted, border: `1px solid ${C.border}`, fontFamily: MONO }}>
                          +{extracted.skills.split(",").length - 12} more
                        </span>
                      )}
                    </div>
                  </div>
                )}

                {/* Summary */}
                {extracted.summary && (
                  <div>
                    <div style={{ fontSize: 9, fontWeight: 700, color: C.muted, textTransform: "uppercase", letterSpacing: "0.08em", fontFamily: MONO, marginBottom: 4 }}>Summary</div>
                    <div style={{ fontSize: 11, color: C.muted, lineHeight: 1.6, fontFamily: MONO }}>
                      {extracted.summary.slice(0, 180)}{extracted.summary.length > 180 ? "…" : ""}
                    </div>
                  </div>
                )}
              </div>

              {/* Launch scan button */}
              <button
                onClick={handleLaunchScan}
                style={{ ...S.btn, width: "100%", padding: "14px", fontSize: 15, borderRadius: 12, display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}
              >
                🚀 Launch Job Scan
              </button>

              <button
                onClick={() => { setStep("cv"); setError(""); }}
                style={{ ...S.btnOut, width: "100%", marginTop: 10, textAlign: "center" }}
              >
                ← Edit my CV
              </button>
            </>
          )}

          {/* Error */}
          {error && (
            <div style={{ marginTop: 14, fontSize: 12, color: C.red, textAlign: "center", background: "rgba(220,38,38,.06)", border: "1px solid rgba(220,38,38,.15)", borderRadius: 8, padding: "8px 12px" }}>
              ⚠ {error}
            </div>
          )}

          {/* Footer */}
          {(step === "id" || step === "cv") && (
            <div style={{ marginTop: 22, textAlign: "center", fontSize: 11, color: "#9f8fb0" }}>
              2 AI scores: <span style={{ color: C.p2, fontWeight: 600 }}>Cosine · AI Match</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}