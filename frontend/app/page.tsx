"use client";

import { useState, useEffect, useRef } from "react";
import ReactMarkdown from "react-markdown";

const API = "/api";

type Msg = { role: "user" | "assistant"; content: string };
type Profile = {
  name: string; target_role: string; experience_years: number;
  skills: string[]; preferred_locations: string[];
  open_to_remote: boolean; salary_expectation: string;
};
type Match = {
  title: string; company: string; location: string; salary: string;
  url: string; source: string; total: number; skill_pct: number;
  loc_pct: number; title_pct: number; matched: string[];
  missing: string[]; verdict: string; description: string;
};

const PROFILE_STEPS = ["name", "role", "experience", "skills", "location", "salary"] as const;
const STEP_QUESTIONS: Record<string, string> = {
  name: "What is your name?",
  role: "What job role are you targeting?\n*(e.g. Data Engineer, Full-Stack Developer, DevOps)*",
  experience: "How many years of experience do you have?\n*(just the number, e.g. 3)*",
  skills: "What are your current skills?\n*(comma-separated, e.g. python, sql, docker, linux, git)*",
  location: "Where do you prefer to work?\n*(e.g. Paris, Remote, New York — or type 'anywhere')*",
  salary: "What's your salary expectation?\n*(e.g. 60000 EUR, 90k USD — or type 'skip')*",
};

const emptyProfile: Profile = {
  name: "", target_role: "", experience_years: 0, skills: [],
  preferred_locations: [], open_to_remote: true, salary_expectation: "",
};

export default function Home() {
  const [userId, setUserId] = useState("");
  const [authenticated, setAuthenticated] = useState(false);
  const [loginInput, setLoginInput] = useState("");
  const [loginError, setLoginError] = useState("");
  const [loginLoading, setLoginLoading] = useState(false);

  const [messages, setMessages] = useState<Msg[]>([]);
  const [input, setInput] = useState("");
  const [profile, setProfile] = useState<Profile>(emptyProfile);
  const [step, setStep] = useState<number>(-1);  // -1 = loading, 0..5 = profile, 6 = ready
  const [tab, setTab] = useState("matches");
  const [matches, setMatches] = useState<Match[]>([]);
  const [gap, setGap] = useState<any>(null);
  const [roadmap, setRoadmap] = useState<any>(null);
  const [market, setMarket] = useState<any>(null);
  const [status, setStatus] = useState<any>(null);
  const [searchRole, setSearchRole] = useState("");
  const [searchLoc, setSearchLoc] = useState("");
  const [minFit, setMinFit] = useState(0);
  const [expandedJob, setExpandedJob] = useState<number | null>(null);
  const [reportMd, setReportMd] = useState("");
  const [reportLoading, setReportLoading] = useState(false);
  const [pdfLoading, setPdfLoading] = useState(false);
  const chatEnd = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const saved = sessionStorage.getItem("career_user_id");
    if (saved) { setUserId(saved); setAuthenticated(true); }
  }, []);

  async function handleLogin() {
    const uid = loginInput.trim();
    if (!uid) { setLoginError("Please enter your User ID"); return; }
    setLoginLoading(true);
    setLoginError("");
    try {
      const res = await fetch(`${API}/login`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ user_id: uid }),
      });
      if (res.status === 404) {
        setLoginError("User ID not found. Please check and try again.");
        setLoginLoading(false);
        return;
      }
      const data = await res.json();
      if (data.authenticated) {
        setUserId(uid);
        setAuthenticated(true);
        sessionStorage.setItem("career_user_id", uid);
        if (data.user) {
          setProfile(prev => ({
            ...prev,
            name: data.user.name || prev.name,
            target_role: data.user.target_role || prev.target_role,
            experience_years: data.user.experience_years || prev.experience_years,
            skills: data.user.skills?.length ? data.user.skills : prev.skills,
          }));
        }
      } else {
        setLoginError("Authentication failed. Please try again.");
      }
    } catch {
      setLoginError("Cannot connect to server. Make sure the backend is running.");
    } finally {
      setLoginLoading(false);
    }
  }

  useEffect(() => { chatEnd.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

  useEffect(() => {
    if (!authenticated) return;
    (async () => {
      const [sRes, pRes] = await Promise.all([
        fetch(`${API}/status`),
        fetch(`${API}/profile?user_id=${encodeURIComponent(userId)}`),
      ]);
      const s = await sRes.json();
      const p = await pRes.json();
      setStatus(s);
      if (p.exists && p.name && p.skills?.length) {
        setProfile({ ...p, user_id: userId });
        setStep(6);
        setMessages([{
          role: "assistant",
          content: `Welcome back, **${p.name}**! I've loaded **${s.total_jobs} jobs** from **${Object.keys(s.sources).length} sources**.\n\nAsk me anything:\n- *"Find me matching jobs"*\n- *"What skills am I missing?"*\n- *"Build me a learning roadmap"*`,
        }]);
        loadAnalysis(p);
      } else {
        setStep(0);
        setMessages([{
          role: "assistant",
          content: `👋 Hello! I'm your **Career Assistant**.\n\nI've analysed **${s.total_jobs} jobs** from **${Object.keys(s.sources).length} sources**.\n\nLet me get to know you first.\n\n**${STEP_QUESTIONS.name}**`,
        }]);
      }
    })();
  }, [authenticated]);

  async function loadAnalysis(p: Profile) {
    const [mRes, gRes, rRes, mkRes] = await Promise.all([
      fetch(`${API}/matches`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) }),
      fetch(`${API}/gap`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) }),
      fetch(`${API}/roadmap`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(p) }),
      fetch(`${API}/market`),
    ]);
    setMatches((await mRes.json()).matches || []);
    try { setGap(await gRes.json()); } catch { }
    try { setRoadmap(await rRes.json()); } catch { }
    setMarket(await mkRes.json());
  }

  function advanceProfile(msg: string): string {
    const s = PROFILE_STEPS[step];
    const p = { ...profile };

    if (s === "name") { p.name = msg.trim(); }
    else if (s === "role") { p.target_role = msg.trim(); }
    else if (s === "experience") { p.experience_years = parseInt(msg.replace(/\D/g, "")) || 0; }
    else if (s === "skills") { p.skills = msg.split(",").map(s => s.trim()).filter(Boolean); }
    else if (s === "location") {
      if (["anywhere", "any", "all"].includes(msg.trim().toLowerCase())) {
        p.preferred_locations = []; p.open_to_remote = true;
      } else {
        p.preferred_locations = msg.split(",").map(l => l.trim()).filter(Boolean);
        p.open_to_remote = p.preferred_locations.some(l => l.toLowerCase().includes("remote"));
      }
    }
    else if (s === "salary") {
      p.salary_expectation = ["skip", "no", "none"].includes(msg.trim().toLowerCase()) ? "" : msg.trim();
    }

    setProfile(p);
    const next = step + 1;

    if (next < PROFILE_STEPS.length) {
      setStep(next);
      const k = PROFILE_STEPS[next];
      const responses: Record<string, string> = {
        role: `Nice to meet you, **${p.name}**! 🙌\n\n**${STEP_QUESTIONS.role}**`,
        experience: `Great — targeting **${p.target_role}** roles.\n\n**${STEP_QUESTIONS.experience}**`,
        skills: `**${p.experience_years} years** of experience — noted.\n\n**${STEP_QUESTIONS.skills}**`,
        location: `Got it — **${p.skills.length} skills**: ${p.skills.slice(0, 8).join(", ")}\n\n**${STEP_QUESTIONS.location}**`,
        salary: `Location: **${p.preferred_locations.length ? p.preferred_locations.join(", ") : "Anywhere"}**\n\n**${STEP_QUESTIONS.salary}**`,
      };
      return responses[k] || "";
    }

    setStep(6);
    fetch(`${API}/profile`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...p, user_id: userId }),
    });
    loadAnalysis(p);
    return `✅ **Your profile is ready!**\n\n| | |\n|---|---|\n| **Name** | ${p.name} |\n| **Target** | ${p.target_role} |\n| **Experience** | ${p.experience_years} yrs |\n| **Skills** | ${p.skills.slice(0, 8).join(", ")} |\n| **Location** | ${p.preferred_locations.join(", ") || "Anywhere"} |\n\nAsk me anything:\n- *"Find me matching jobs"*\n- *"What skills am I missing?"*\n- *"Build me a learning roadmap"*`;
  }

  async function sendMessage() {
    if (!input.trim()) return;
    const userMsg = input.trim();
    setInput("");
    setMessages(prev => [...prev, { role: "user", content: userMsg }]);

    if (step < 6) {
      const reply = advanceProfile(userMsg);
      setMessages(prev => [...prev, { role: "assistant", content: reply }]);
      return;
    }

    const res = await fetch(`${API}/chat`, {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: userMsg, profile, user_id: userId }),
    });
    const data = await res.json();
    setMessages(prev => [...prev, { role: "assistant", content: data.response }]);
  }

  const filteredMatches = matches.filter(m => {
    if (m.total < minFit) return false;
    if (searchRole && !(m.title + " " + m.description).toLowerCase().includes(searchRole.toLowerCase())) return false;
    if (searchLoc && !m.location.toLowerCase().includes(searchLoc.toLowerCase())) return false;
    return true;
  });

  if (!authenticated) {
    return (
      <div className="min-h-screen flex items-center justify-center px-4">
        <div className="w-full max-w-md">
          <div className="bg-dark-800 rounded-2xl border border-dark-700 p-8 shadow-2xl">
            <div className="text-center mb-8">
              <div className="w-16 h-16 bg-brand/20 rounded-2xl flex items-center justify-center mx-auto mb-4">
                <span className="text-3xl">🎯</span>
              </div>
              <h1 className="text-2xl font-bold">Career Assistant</h1>
              <p className="text-gray-400 text-sm mt-2">Enter your User ID to access the platform</p>
            </div>
            <form onSubmit={e => { e.preventDefault(); handleLogin(); }} className="space-y-4">
              <div>
                <label className="block text-xs text-gray-400 mb-1.5 font-medium">User ID</label>
                <input
                  value={loginInput}
                  onChange={e => { setLoginInput(e.target.value); setLoginError(""); }}
                  placeholder="e.g. 1"
                  autoFocus
                  className="w-full bg-dark-900 border border-dark-700 rounded-xl px-4 py-3 text-sm focus:outline-none focus:border-brand focus:ring-1 focus:ring-brand transition"
                />
              </div>
              {loginError && (
                <p className="text-red-400 text-xs bg-red-400/10 rounded-lg px-3 py-2">{loginError}</p>
              )}
              <button
                type="submit"
                disabled={loginLoading}
                className="w-full bg-brand hover:bg-brand-light text-white py-3 rounded-xl font-semibold text-sm transition disabled:opacity-50 flex items-center justify-center gap-2"
              >
                {loginLoading ? (
                  <><svg className="animate-spin h-4 w-4" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"/></svg> Connecting...</>
                ) : "Sign In"}
              </button>
            </form>
            <p className="text-center text-gray-600 text-[11px] mt-6">Your data will be stored with this User ID for future sessions</p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="max-w-6xl mx-auto px-4 py-6">
      {/* Header */}
      <div className="text-center mb-6">
        <div className="flex items-center justify-between">
          <div />
          <div>
            <h1 className="text-3xl font-bold">🎯 Career Assistant</h1>
            <p className="text-gray-400 text-sm mt-1">Your AI career coach — chat, then explore results below</p>
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <span className="bg-dark-800 border border-dark-700 px-3 py-1.5 rounded-lg">👤 {userId}</span>
            <button
              onClick={() => { setAuthenticated(false); setUserId(""); sessionStorage.removeItem("career_user_id"); }}
              className="text-gray-500 hover:text-red-400 transition px-2 py-1.5"
              title="Sign out"
            >✕</button>
          </div>
        </div>
      </div>

      {/* Chat */}
      <div className="bg-dark-800 rounded-xl border border-dark-700 mb-6">
        <div className="h-[420px] overflow-y-auto p-4 space-y-3">
          {messages.map((m, i) => (
            <div key={i} className={`flex ${m.role === "user" ? "justify-end" : "justify-start"}`}>
              <div className={`max-w-[80%] px-4 py-3 rounded-xl text-sm leading-relaxed ${m.role === "user" ? "chat-bubble-user" : "chat-bubble-assistant"}`}>
                <ReactMarkdown>{m.content}</ReactMarkdown>
              </div>
            </div>
          ))}
          <div ref={chatEnd} />
        </div>
        <form onSubmit={e => { e.preventDefault(); sendMessage(); }} className="border-t border-dark-700 p-3 flex gap-2">
          <input
            value={input} onChange={e => setInput(e.target.value)}
            placeholder="Type your message..."
            className="flex-1 bg-dark-900 border border-dark-700 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-brand"
          />
          <button type="submit" className="bg-brand hover:bg-brand-light text-white px-6 py-2.5 rounded-lg text-sm font-semibold transition">
            Send
          </button>
        </form>
      </div>

      {/* Only show analysis sections when profile is ready */}
      {step === 6 && (
        <>
          {/* Job Search Bar */}
          <div className="mb-4">
            <h2 className="text-xl font-bold mb-3">🔍 Job Search</h2>
            <div className="flex gap-2">
              <input value={searchRole} onChange={e => setSearchRole(e.target.value)}
                placeholder="Role / keywords..." className="flex-1 bg-dark-800 border border-dark-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-brand" />
              <input value={searchLoc} onChange={e => setSearchLoc(e.target.value)}
                placeholder="Location..." className="w-48 bg-dark-800 border border-dark-700 rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-brand" />
              <select value={minFit} onChange={e => setMinFit(Number(e.target.value))}
                className="bg-dark-800 border border-dark-700 rounded-lg px-3 py-2 text-sm">
                <option value={0}>≥ 0% fit</option>
                <option value={25}>≥ 25%</option>
                <option value={50}>≥ 50%</option>
                <option value={75}>≥ 75%</option>
              </select>
            </div>
            <p className="text-gray-500 text-xs mt-1">{filteredMatches.length} jobs found</p>
          </div>

          {/* Tabs */}
          <div className="flex gap-1 mb-4 border-b border-dark-700">
            {["matches", "gap", "roadmap", "market", "report"].map(t => (
              <button key={t} onClick={() => setTab(t)}
                className={`px-4 py-2 text-sm font-medium rounded-t-lg transition ${tab === t ? "bg-dark-800 text-brand border-b-2 border-brand" : "text-gray-400 hover:text-white"}`}>
                {t === "matches" ? "💼 Matches" : t === "gap" ? "🧩 Skills Gap" : t === "roadmap" ? "🗺️ Roadmap" : t === "market" ? "📈 Market" : "📄 Report"}
              </button>
            ))}
          </div>

          {/* Tab Content */}
          <div className="bg-dark-800 rounded-xl p-5 min-h-[300px]">

            {/* Matches Tab */}
            {tab === "matches" && (
              <div className="space-y-3">
                {filteredMatches.slice(0, 20).map((m, i) => (
                  <div key={i} className="rounded-xl p-4 border-l-4 border-brand" style={{ background: "linear-gradient(135deg, #1e1e2e, #2d2d44)" }}>
                    <div className="flex justify-between items-start">
                      <div className="flex-1">
                        <h3 className="font-semibold text-white">{m.title}
                          <span className={`ml-2 inline-block px-2 py-0.5 rounded-full text-xs font-bold ${m.total >= 60 ? "bg-green-500 text-black" : m.total >= 35 ? "bg-yellow-500 text-black" : "bg-red-500 text-white"}`}>
                            {m.total}%
                          </span>
                        </h3>
                        <p className="text-brand-light font-medium text-sm">{m.company}</p>
                        <p className="text-gray-400 text-xs mt-1">📍 {m.location || "N/A"} · 💰 {m.salary || "N/A"} · 🏷️ {m.source}</p>
                      </div>
                      <span className="text-xs text-gray-500">{m.verdict}</span>
                    </div>
                    {m.matched.length > 0 && (
                      <p className="text-xs mt-2">✅ {m.matched.slice(0, 6).map(s => <code key={s} className="bg-green-900/40 text-green-300 px-1 rounded mr-1">{s}</code>)}</p>
                    )}
                    {m.missing.length > 0 && (
                      <p className="text-xs mt-1">📚 {m.missing.slice(0, 5).map(s => <code key={s} className="bg-red-900/30 text-red-300 px-1 rounded mr-1">{s}</code>)}</p>
                    )}
                    <div className="flex gap-2 mt-2">
                      {m.url && <a href={m.url} target="_blank" className="text-xs text-brand hover:underline">View Job →</a>}
                      <button onClick={() => setExpandedJob(expandedJob === i ? null : i)} className="text-xs text-gray-400 hover:text-white">
                        {expandedJob === i ? "Hide" : "Details"}
                      </button>
                    </div>
                    {expandedJob === i && (
                      <div className="mt-3 pt-3 border-t border-dark-700">
                        {/* Score breakdown bars */}
                        <div className="grid grid-cols-3 gap-3 mb-4">
                          {[
                            { label: "Skill Match", pct: m.skill_pct, weight: "55%", color: "#6c63ff" },
                            { label: "Location", pct: m.loc_pct, weight: "20%", color: "#a78bfa" },
                            { label: "Title Fit", pct: m.title_pct, weight: "25%", color: "#818cf8" },
                          ].map(b => (
                            <div key={b.label}>
                              <div className="flex justify-between text-[11px] mb-1">
                                <span className="text-gray-400">{b.label} <span className="text-gray-600">×{b.weight}</span></span>
                                <span className="font-bold" style={{ color: b.color }}>{b.pct}%</span>
                              </div>
                              <div className="w-full bg-dark-900 rounded-full h-2">
                                <div className="h-2 rounded-full transition-all" style={{ width: `${b.pct}%`, background: b.color }} />
                              </div>
                            </div>
                          ))}
                        </div>
                        {/* XAI Explanation */}
                        {(m as any).explanation && (
                          <div className="bg-dark-900 rounded-lg p-3 mb-3">
                            <p className="text-[11px] font-semibold text-brand mb-2">🔍 Why this score?</p>
                            <p className="text-[11px] text-gray-300 font-mono mb-2">{(m as any).explanation.formula}</p>
                            <div className="space-y-1.5">
                              <p className="text-[11px] text-gray-400"><span className="text-green-400 font-medium">Skills:</span> {(m as any).explanation.skill?.reason}</p>
                              <p className="text-[11px] text-gray-400"><span className="text-blue-400 font-medium">Location:</span> {(m as any).explanation.location?.reason}</p>
                              <p className="text-[11px] text-gray-400"><span className="text-purple-400 font-medium">Title:</span> {(m as any).explanation.title?.reason}</p>
                            </div>
                            <p className="text-[11px] text-yellow-300 mt-2 pt-2 border-t border-dark-700">{(m as any).explanation.verdict_reason}</p>
                          </div>
                        )}
                        {m.description && <p className="text-xs text-gray-300 leading-relaxed whitespace-pre-line">{m.description.slice(0, 1000)}</p>}
                      </div>
                    )}
                  </div>
                ))}
                {filteredMatches.length === 0 && <p className="text-gray-500 text-center py-10">No matching jobs found.</p>}
              </div>
            )}

            {/* Gap Tab */}
            {tab === "gap" && gap && (
              <div>
                <div className="grid grid-cols-3 gap-4 mb-6">
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Coverage</p>
                    <p className="text-2xl font-bold text-brand">{(gap.coverage * 100).toFixed(0)}%</p>
                  </div>
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Matched</p>
                    <p className="text-2xl font-bold text-green-400">{gap.matched?.length || 0}</p>
                  </div>
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Missing</p>
                    <p className="text-2xl font-bold text-red-400">{gap.missing?.length || 0}</p>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-6">
                  <div>
                    <h3 className="font-semibold text-green-400 mb-2">✅ Skills You Have</h3>
                    {gap.matched?.slice(0, 15).map(([s, c]: [string, number]) => (
                      <div key={s} className="flex justify-between py-1 text-sm">
                        <span>{s}</span>
                        <span className="text-gray-400">{c} jobs</span>
                      </div>
                    ))}
                  </div>
                  <div>
                    <h3 className="font-semibold text-red-400 mb-2">❌ Top Missing Skills</h3>
                    {gap.missing?.slice(0, 15).map(([s, c]: [string, number]) => (
                      <div key={s} className="flex justify-between py-1 text-sm">
                        <span>{s}</span>
                        <span className="text-gray-400">{c} jobs</span>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Skills Demand Chart */}
                {market?.top_skills && (
                  <div className="mt-6">
                    <h3 className="font-semibold mb-3">Skills Demand Chart</h3>
                    <div className="bg-dark-900 rounded-xl p-4 overflow-x-auto">
                      <div className="flex items-end gap-[6px] min-w-[800px]" style={{ height: 260 }}>
                        {market.top_skills.slice(0, 25).map((s: any) => {
                          const maxVal = market.top_skills[0]?.count || 1;
                          const pct = (s.count / maxVal) * 100;
                          const mySkills = profile.skills.map((sk: string) => sk.toLowerCase());
                          const have = mySkills.includes(s.skill.toLowerCase());
                          return (
                            <div key={s.skill} className="flex-1 flex flex-col items-center justify-end h-full min-w-[28px]">
                              <span className="text-[10px] text-gray-400 mb-1">{s.count}</span>
                              <div
                                className="w-full rounded-t transition-all"
                                style={{
                                  height: `${pct}%`,
                                  background: have ? "#1d4ed8" : "#93c5fd",
                                  minHeight: 4,
                                }}
                              />
                            </div>
                          );
                        })}
                      </div>
                      {/* X-axis labels */}
                      <div className="flex gap-[6px] min-w-[800px] mt-1" style={{ height: 80 }}>
                        {market.top_skills.slice(0, 25).map((s: any) => (
                          <div key={s.skill} className="flex-1 min-w-[28px] relative">
                            <span className="absolute top-1 left-1/2 text-[11px] text-gray-400 whitespace-nowrap origin-top-left -rotate-45 -translate-x-1/2">
                              {s.skill}
                            </span>
                          </div>
                        ))}
                      </div>
                      <div className="flex items-center justify-center gap-6 mt-2 text-xs text-gray-400">
                        <div className="flex items-center gap-1.5">
                          <div className="w-3 h-3 rounded" style={{ background: "#93c5fd" }} />
                          <span>To learn</span>
                        </div>
                        <div className="flex items-center gap-1.5">
                          <div className="w-3 h-3 rounded" style={{ background: "#1d4ed8" }} />
                          <span>You have it</span>
                        </div>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Roadmap Tab */}
            {tab === "roadmap" && roadmap && (
              <div className="space-y-6">
                <div className="bg-dark-900 rounded-xl p-4 text-center mb-4">
                  <p className="text-xs text-gray-400">Estimated Total</p>
                  <p className="text-2xl font-bold text-brand">~{roadmap.total_weeks} weeks</p>
                  <p className="text-xs text-gray-500">Many can be learned in parallel — cut by 40–60%</p>
                </div>
                {(["beginner", "intermediate", "advanced"] as const).map(phase => {
                  const items = roadmap.phases?.[phase] || [];
                  if (!items.length) return null;
                  const emoji = phase === "beginner" ? "🌱" : phase === "intermediate" ? "🔧" : "🚀";
                  const label = phase === "beginner" ? "Foundations" : phase === "intermediate" ? "Core Skills" : "Specialisation";
                  return (
                    <div key={phase}>
                      <h3 className="font-semibold text-lg mb-2">{emoji} {label}</h3>
                      <div className="space-y-2">
                        {items.map((it: any) => (
                          <div key={it.skill} className="bg-dark-900 rounded-lg p-3">
                            <div className="flex justify-between items-start">
                              <span className="font-medium">{it.skill}</span>
                              <div className="flex items-center gap-2 shrink-0">
                                {it.xai?.market_impact_pct > 0 && (
                                  <span className="text-[10px] bg-brand/20 text-brand px-1.5 py-0.5 rounded-full">{it.xai.market_impact_pct}% of market</span>
                                )}
                                <span className="text-xs text-gray-400">{it.jobs_count} jobs · ~{it.weeks} wks</span>
                              </div>
                            </div>
                            <p className="text-xs text-gray-400 mt-1">💡 {it.tip}</p>
                            {it.xai?.prereqs_met?.length > 0 && (
                              <p className="text-xs text-green-400 mt-1">✅ Prerequisites you have: {it.xai.prereqs_met.join(", ")}</p>
                            )}
                            {it.xai?.prereqs_missing?.length > 0 && (
                              <p className="text-xs text-yellow-400 mt-1">⚠️ Learn first: {it.xai.prereqs_missing.join(", ")}</p>
                            )}
                            {it.prerequisites?.length === 0 && (
                              <p className="text-xs text-green-400 mt-1">✅ No prerequisites — start immediately</p>
                            )}
                            {it.xai?.reason && (
                              <div className="mt-2 pt-2 border-t border-dark-700">
                                <p className="text-[11px] text-gray-500"><span className="text-brand font-medium">🔍 Why this recommendation:</span> {it.xai.reason}</p>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Market Tab */}
            {tab === "market" && market && (
              <div>
                <div className="grid grid-cols-3 gap-4 mb-6">
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Total Jobs</p>
                    <p className="text-2xl font-bold text-brand">{market.total_jobs}</p>
                  </div>
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Sources</p>
                    <p className="text-2xl font-bold text-brand">{Object.keys(market.sources || {}).length}</p>
                  </div>
                  <div className="bg-dark-900 rounded-xl p-4 text-center">
                    <p className="text-xs text-gray-400">Remote %</p>
                    <p className="text-2xl font-bold text-brand">{((market.remote_ratio || 0) * 100).toFixed(0)}%</p>
                  </div>
                </div>

                {/* Most In-Demand Skills Chart */}
                <h3 className="font-semibold mb-3">🔥 Most In-Demand Skills</h3>
                <div className="bg-dark-900 rounded-xl p-4 mb-6 overflow-x-auto">
                  <div className="flex items-end gap-[6px] min-w-[800px]" style={{ height: 220 }}>
                    {market.top_skills?.slice(0, 30).map((s: any) => {
                      const maxVal = market.top_skills[0]?.count || 1;
                      const pct = (s.count / maxVal) * 100;
                      return (
                        <div key={s.skill} className="flex-1 flex flex-col items-center justify-end h-full min-w-[22px]">
                          <span className="text-[10px] text-gray-400 mb-1">{s.count}</span>
                          <div className="w-full rounded-t" style={{ height: `${pct}%`, background: "#6c63ff", minHeight: 4 }} />
                        </div>
                      );
                    })}
                  </div>
                  <div className="flex gap-[6px] min-w-[800px] mt-1" style={{ height: 70 }}>
                    {market.top_skills?.slice(0, 30).map((s: any) => (
                      <div key={s.skill} className="flex-1 min-w-[22px] relative">
                        <span className="absolute top-1 left-1/2 text-[11px] text-gray-400 whitespace-nowrap origin-top-left -rotate-45 -translate-x-1/2">{s.skill}</span>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="grid grid-cols-3 gap-6">
                  {/* Top Locations */}
                  <div>
                    <h3 className="font-semibold mb-3">📍 Top Locations</h3>
                    {market.top_locations?.slice(0, 12).map((l: any) => (
                      <div key={l.location} className="flex justify-between py-1 text-sm">
                        <span>{l.location}</span>
                        <span className="text-gray-400">{l.count}</span>
                      </div>
                    ))}
                  </div>

                  {/* Top Companies */}
                  <div>
                    <h3 className="font-semibold mb-3">🏢 Top Companies</h3>
                    {market.top_companies?.slice(0, 12).map((c: any) => (
                      <div key={c.company} className="flex justify-between items-center py-1">
                        <span className="text-sm truncate mr-2">{c.company}</span>
                        <div className="flex items-center gap-2 shrink-0">
                          <div className="w-16 bg-dark-800 rounded-full h-2">
                            <div className="bg-brand-light h-2 rounded-full" style={{ width: `${(c.count / (market.top_companies[0]?.count || 1)) * 100}%` }} />
                          </div>
                          <span className="text-xs text-gray-400 w-6 text-right">{c.count}</span>
                        </div>
                      </div>
                    ))}
                  </div>

                  {/* Salaries */}
                  <div>
                    <h3 className="font-semibold mb-3">💰 Salary Ranges</h3>
                    {Object.entries(market.salaries || {}).map(([cur, v]: [string, any]) => (
                      <div key={cur} className="bg-dark-800 rounded-lg p-3 mb-2">
                        <p className="text-sm font-medium">{cur} <span className="text-gray-400 text-xs">({v.count} jobs)</span></p>
                        <div className="grid grid-cols-3 gap-1 mt-1">
                          <div className="text-center"><p className="text-[10px] text-gray-500">Min</p><p className="text-xs font-semibold">{v.min?.toLocaleString()}</p></div>
                          <div className="text-center"><p className="text-[10px] text-gray-500">Median</p><p className="text-xs font-semibold text-brand">{v.median?.toLocaleString()}</p></div>
                          <div className="text-center"><p className="text-[10px] text-gray-500">Max</p><p className="text-xs font-semibold">{v.max?.toLocaleString()}</p></div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Report Tab */}
            {tab === "report" && (
              <div>
                <h2 className="text-xl font-bold mb-2">📄 Career Report</h2>
                <p className="text-gray-400 text-sm mb-6">
                  Generate a professional PDF report for <span className="text-white font-semibold">{profile.name}</span> with
                  profile summary, job matches, skills gap analysis, learning roadmap, and market insights.
                </p>
                <div className="grid grid-cols-2 gap-3">
                  <button
                    disabled={pdfLoading}
                    onClick={async () => {
                      setPdfLoading(true);
                      try {
                        const res = await fetch(`${API}/report/pdf`, {
                          method: "POST", headers: { "Content-Type": "application/json" },
                          body: JSON.stringify(profile),
                        });
                        if (res.ok) {
                          const blob = await res.blob();
                          const url = URL.createObjectURL(blob);
                          const a = document.createElement("a");
                          a.href = url; a.download = `career_report_${profile.name}.pdf`; a.click();
                          URL.revokeObjectURL(url);
                        }
                      } finally { setPdfLoading(false); }
                    }}
                    className="bg-brand hover:bg-brand-light text-white py-3 rounded-xl font-semibold text-sm transition disabled:opacity-50 flex items-center justify-center gap-2"
                  >
                    {pdfLoading ? (
                      <><svg className="animate-spin h-4 w-4" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"/></svg> Generating PDF...</>
                    ) : (<>📄 Generate &amp; Download PDF</>)}
                  </button>
                  <button
                    disabled={reportLoading}
                    onClick={async () => {
                      setReportLoading(true);
                      try {
                        const res = await fetch(`${API}/report`, {
                          method: "POST", headers: { "Content-Type": "application/json" },
                          body: JSON.stringify(profile),
                        });
                        const data = await res.json();
                        setReportMd(data.markdown || "");
                      } finally { setReportLoading(false); }
                    }}
                    className="border border-dark-700 text-gray-300 hover:text-white py-3 rounded-xl text-sm transition disabled:opacity-50 flex items-center justify-center gap-2"
                  >
                    {reportLoading ? (
                      <><svg className="animate-spin h-4 w-4" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"/></svg> Loading...</>
                    ) : (<>👁️ Preview Report</>)}
                  </button>
                </div>
                {reportMd && (
                  <div className="mt-4 bg-dark-900 rounded-xl p-5 max-h-[600px] overflow-y-auto text-sm prose prose-invert prose-sm max-w-none">
                    <ReactMarkdown>{reportMd}</ReactMarkdown>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Sidebar profile summary */}
          {profile.name && (
            <div className="mt-6 bg-dark-800 rounded-xl p-4 flex items-center justify-between">
              <div className="text-sm">
                <span className="font-bold text-brand">{profile.name}</span>
                <span className="text-gray-400"> · {profile.target_role} · {profile.experience_years} yrs · {profile.skills.length} skills</span>
              </div>
              <button onClick={() => { setStep(0); setMessages([{ role: "assistant", content: `Let's update your profile.\n\n**${STEP_QUESTIONS.name}**` }]); setProfile(emptyProfile); }}
                className="text-xs text-brand hover:underline">✏️ Edit</button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
