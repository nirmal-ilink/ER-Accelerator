import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const GREEN_T = "#059669";
const BORDER = "#e2e8f0";

const STAGES = ["Ingestion", "Profiling", "Cleansing", "Resolution", "Survivorship", "Publishing"];

// Each stage shows for 105 frames ≈ 3.5 seconds at 30fps
// Total: 6 × 105 = 630 frames = 21 seconds
const STAGE_DUR = 105;
const getActiveStage = (frame: number) => {
    if (frame < STAGE_DUR * 1) return 0;
    if (frame < STAGE_DUR * 2) return 1;
    if (frame < STAGE_DUR * 3) return 2;
    if (frame < STAGE_DUR * 4) return 3;
    if (frame < STAGE_DUR * 5) return 4;
    return 5;
};

// ─────────────────────────────── STAGE PANELS ────────────────────────────────

const StageIngestion: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)` }}>
            {/* LIVE CONNECTION card */}
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 20, background: "white" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: GREEN_T, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 8 }}>● LIVE CONNECTION</div>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <div>
                        <div style={{ fontSize: 18, fontWeight: 800, color: SLATE, letterSpacing: "-0.02em" }}>sql-test-connection</div>
                        <div style={{ display: "inline-block", fontSize: 10, fontWeight: 700, background: "#eff6ff", color: "#1d4ed8", padding: "2px 8px", borderRadius: 4, marginTop: 4 }}>SQLSERVER</div>
                    </div>
                    <div style={{ textAlign: "right" }}>
                        <div style={{ fontSize: 10, color: SLATE_4, textTransform: "uppercase" }}>LAST SYNC</div>
                        <div style={{ fontSize: 12, fontWeight: 600, color: SLATE }}>2026-03-05 18:19</div>
                    </div>
                </div>
            </div>

            {/* DB selector row */}
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20 }}>
                <div style={{ flex: 1, height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>
                    sql-test-connection (SQLSERVER) <span style={{ color: SLATE_4 }}>▾</span>
                </div>
                <div style={{ height: 40, padding: "0 18px", border: `1px solid ${BORDER}`, borderRadius: 6, display: "flex", alignItems: "center", fontSize: 13, color: SLATE }}>Manage</div>
            </div>

            {/* DATABASE / SCHEMA / TABLE BROWSER */}
            <div style={{ marginBottom: 12 }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 10 }}>DATABASE / SCHEMA / TABLE BROWSER</div>
                <div style={{ fontSize: 12, fontWeight: 500, color: SLATE, marginBottom: 6 }}>Database</div>
                <div style={{ display: "flex", gap: 12, marginBottom: 16 }}>
                    <div style={{ flex: 1, height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>
                        mdm-db <span style={{ color: SLATE_4 }}>▾</span>
                    </div>
                    <div style={{ display: "flex", gap: 8 }}>
                        <div style={{ height: 40, padding: "0 16px", border: `1px solid ${BORDER}`, borderRadius: 6, display: "flex", alignItems: "center", fontSize: 12, color: SLATE_5 }}>↻ Refresh</div>
                        <div style={{ height: 40, padding: "0 20px", background: RED, borderRadius: 6, display: "flex", alignItems: "center", fontSize: 13, fontWeight: 700, color: "white", boxShadow: "0 3px 10px rgba(209,31,65,0.3)" }}>Fetch Metadata</div>
                    </div>
                </div>
            </div>

            {/* Table selector */}
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, overflow: "hidden", background: "white" }}>
                <div style={{ padding: "14px 18px", borderBottom: `1px solid ${BORDER}`, display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <div>
                        <div style={{ fontSize: 14, fontWeight: 700, color: SLATE }}>Select Tables &amp; Configure Load</div>
                        <div style={{ fontSize: 11, color: SLATE_5 }}>Choose tables to ingest and define load strategies</div>
                    </div>
                    <div style={{ display: "flex", gap: 12 }}>
                        <span style={{ fontSize: 11, color: SLATE_4 }}>SCHEMAS <b style={{ color: SLATE }}>2</b></span>
                        <span style={{ fontSize: 11, color: SLATE_4 }}>TABLES <b style={{ color: SLATE }}>3</b></span>
                    </div>
                </div>
                {/* Schema: dbo expanded */}
                <div style={{ padding: "12px 18px", borderBottom: `1px solid #f8fafc` }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10, fontSize: 13, fontWeight: 500, color: SLATE }}>
                        <span>▾</span> 📁 dbo <span style={{ fontSize: 11, color: SLATE_4 }}>1/2 selected</span>
                    </div>
                    <div style={{ display: "flex", gap: 10, marginBottom: 10, paddingLeft: 20 }}>
                        <div style={{ padding: "6px 14px", border: `1px solid ${BORDER}`, borderRadius: 6, fontSize: 12, color: SLATE }}>Select All</div>
                        <div style={{ padding: "6px 14px", border: `1px solid ${BORDER}`, borderRadius: 6, fontSize: 12, color: SLATE }}>Clear Selection</div>
                    </div>
                    {/* Table row — checked */}
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 220px 180px", gap: 12, padding: "8px 0 8px 20px", alignItems: "center", borderTop: `1px solid #f8fafc` }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                            <div style={{ width: 18, height: 18, borderRadius: 4, background: RED, display: "flex", alignItems: "center", justifyContent: "center" }}>
                                <svg width="11" height="11" viewBox="0 0 12 12"><polyline points="2,6 5,9 10,3" stroke="white" strokeWidth="2" fill="none" /></svg>
                            </div>
                            <span style={{ fontSize: 13, color: SLATE }}>mdm_healthcare_entity_raw</span>
                        </div>
                        <div style={{ height: 36, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 12px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 12, color: SLATE }}>Full Load <span>▾</span></div>
                        <div style={{ height: 36, border: `1px solid ${BORDER}`, borderRadius: 6, background: "white" }} />
                    </div>
                    {/* Table row — unchecked */}
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 220px 180px", gap: 12, padding: "8px 0 8px 20px", alignItems: "center", borderTop: `1px solid #f8fafc` }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                            <div style={{ width: 18, height: 18, borderRadius: 4, border: `2px solid ${BORDER}` }} />
                            <span style={{ fontSize: 13, color: SLATE }}>mdm_healthcare_facility_raw</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

const StageProfiling: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    const QUAL_CARDS = [
        { label: "Pending", sub: "Completeness", note: "Non-null values", color: "#10b981" },
        { label: "Pending", sub: "Uniqueness", note: "Distinct records", color: "#6366f1" },
        { label: "Pending", sub: "Validity", note: "Format compliance", color: "#8b5cf6" },
        { label: "Pending", sub: "Consistency", note: "Cross-field accuracy", color: "#f59e0b" },
    ];
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)` }}>
            {/* Status header */}
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 20, background: "white" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: GREEN_T, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>COMPONENT STATUS: DONE</div>
                <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>Profiling</div>
                <div style={{ fontSize: 12, color: SLATE_5, marginTop: 4 }}>Quality checks passed (99.8%)</div>
                <div style={{ position: "absolute", top: 16, right: 20, padding: "4px 12px", background: "#dcfce7", borderRadius: 20, fontSize: 11, fontWeight: 700, color: "#166534" }}>SUCCESS</div>
            </div>

            {/* Data Quality Summary */}
            <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 10 }}>DATA QUALITY SUMMARY</div>
            <div style={{ fontSize: 11, fontWeight: 600, color: SLATE_5, marginBottom: 6 }}>SELECT TABLE</div>
            <div style={{ height: 38, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white", marginBottom: 16 }}>
                mdm_healthcare_entity_raw <span style={{ color: SLATE_4 }}>▾</span>
            </div>

            {/* 4 quality dimension cards */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 14, marginBottom: 20 }}>
                {QUAL_CARDS.map((c, i) => (
                    <div key={i} style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 18px", background: "white", textAlign: "center" }}>
                        <div style={{ fontSize: 16, fontWeight: 800, color: c.color, marginBottom: 4 }}>{c.label}</div>
                        <div style={{ fontSize: 12, fontWeight: 600, color: SLATE }}>{c.sub}</div>
                        <div style={{ fontSize: 11, color: SLATE_4 }}>{c.note}</div>
                    </div>
                ))}
            </div>

            {/* Column Analysis */}
            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 10 }}>COLUMN ANALYSIS</div>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 8, overflow: "hidden", background: "white", marginBottom: 20 }}>
                <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr 1fr", padding: "10px 16px", background: "#f8fafc", borderBottom: `1px solid ${BORDER}` }}>
                    {["COLUMN NAME", "TYPE", "NULL %", "DISTINCT"].map(h => (
                        <div key={h} style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.04em" }}>{h}</div>
                    ))}
                </div>
                <div style={{ padding: "20px", textAlign: "center", fontSize: 13, color: SLATE_4 }}>No profiling data available. Click "Run Profiling" to generate analytics.</div>
            </div>

            {/* Quality Rules */}
            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 12 }}>QUALITY RULES</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 20 }}>
                {[
                    { label: "Check null values", on: true },
                    { label: "Detect outliers", on: false },
                    { label: "Validate email formats", on: false },
                    { label: "Check referential integrity", on: false },
                ].map((r, i) => (
                    <div key={i} style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 13, color: SLATE }}>
                        <div style={{ width: 38, height: 22, borderRadius: 11, background: r.on ? RED : "#e2e8f0", display: "flex", alignItems: "center", padding: "0 3px" }}>
                            <div style={{ width: 16, height: 16, borderRadius: "50%", background: "white", marginLeft: r.on ? "auto" : 0 }} />
                        </div>
                        {r.label}
                    </div>
                ))}
            </div>

            {/* Run Profiling button */}
            <div style={{ display: "flex", justifyContent: "flex-end" }}>
                <div style={{ height: 48, padding: "0 40px", background: RED, borderRadius: 8, display: "flex", alignItems: "center", fontSize: 14, fontWeight: 700, color: "white", boxShadow: "0 4px 12px rgba(209,31,65,0.3)" }}>Run Profiling</div>
            </div>
        </div>
    );
};

const StageCleansing: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    const RULE_GROUPS = [
        { group: "CONTACT INFO", rules: ["Phone Formatting — Allowed: 0-9, +, -. Null if len < 7.", "Email Validation — Lowercase, trim, valid pattern (else NULL).", "Website Standard — Lowercase, trim, ensure http/https."] },
        { group: "IDENTIFIERS", rules: ["NPI Cleaning — Digits only. Null if len ≠ 10.", "DEA Formatting — Uppercase, trim.", "License/Tax/DUNS — Trim, uppercase, alphanumeric only."] },
        { group: "NAMES & ENTITIES", rules: ["Entity Names — Trim, proper case, single internal space.", "Person Names — Trim, proper case, single internal space."] },
        { group: "ADDRESS & LOCATION", rules: ["Address Lines — Trim, proper case, single internal space.", "City/State/Country — City: Proper; State/Country: Upper.", "Postal Code — Trim, remove all internal spaces."] },
    ];
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)`, overflowY: "hidden" }}>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 16, background: "white", position: "relative" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: GREEN_T, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>COMPONENT STATUS: DONE</div>
                <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>Cleansing</div>
                <div style={{ fontSize: 12, color: SLATE_5, marginTop: 4 }}>Standardization rules applied</div>
                <div style={{ position: "absolute", top: 16, right: 20, padding: "4px 12px", background: "#dcfce7", borderRadius: 20, fontSize: 11, fontWeight: 700, color: "#166534" }}>SUCCESS</div>
            </div>

            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>TARGET TABLES</div>
            <div style={{ marginBottom: 16 }}>
                <div style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "5px 10px", border: `1px solid ${BORDER}`, borderRadius: 6, fontSize: 12, color: SLATE, background: "white" }}>
                    mdm_healthcare_…
                    <span style={{ fontSize: 13, color: "#9ca3af", cursor: "pointer" }}>×</span>
                </div>
            </div>

            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 14 }}>STANDARDIZATION RULES</div>
            {RULE_GROUPS.map((g, gi) => (
                <div key={gi} style={{ marginBottom: 14 }}>
                    <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>{g.group}</div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
                        {g.rules.map((r, ri) => {
                            const [title, ...rest] = r.split(" — ");
                            return (
                                <div key={ri} style={{ border: "1px solid #bbf7d0", borderRadius: 8, padding: "10px 12px", background: "#f0fdf4" }}>
                                    <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 4 }}>
                                        <svg width="13" height="13" viewBox="0 0 16 16"><polyline points="2,8 6,12 14,4" stroke="#16a34a" strokeWidth="2" fill="none" /></svg>
                                        <span style={{ fontSize: 12, fontWeight: 700, color: "#166534" }}>{title}</span>
                                    </div>
                                    <div style={{ fontSize: 11, color: "#4b7a5a" }}>{rest.join(" — ")}</div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            ))}
        </div>
    );
};

const StageResolution: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    const STATS = [
        { label: "TOKENS PROCESSED", value: "1.5M", sub: "Across 3 sources" },
        { label: "AI MATCHES FOUND", value: "12,941", sub: "Context based" },
        { label: "AVG. CONFIDENCE", value: "94.2%", sub: "High precision" },
        { label: "ENTITIES DISCOVERED", value: "4,102", sub: "New unique IDs" },
    ];
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)` }}>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 16, background: "white", position: "relative" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: "#f59e0b", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>COMPONENT STATUS: ACTIVE</div>
                <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>Resolution</div>
                <div style={{ fontSize: 12, color: SLATE_5, marginTop: 4 }}>Fuzzy matching (Block 4/12)</div>
                <div style={{ position: "absolute", top: 16, right: 20, padding: "4px 12px", background: "#fef3c7", borderRadius: 20, fontSize: 11, fontWeight: 700, color: "#92400e" }}>RUNNING</div>
            </div>

            {/* Tabs */}
            <div style={{ display: "flex", gap: 0, marginBottom: 20, borderBottom: `1px solid ${BORDER}` }}>
                {["Fixed Rules", "LLM based"].map((tab, i) => (
                    <div key={i} style={{ padding: "8px 18px", fontSize: 13, fontWeight: i === 1 ? 700 : 400, color: i === 1 ? RED : SLATE_5, borderBottom: i === 1 ? `2px solid ${RED}` : "none", marginBottom: -1 }}>{tab}</div>
                ))}
            </div>

            {/* LLM Card */}
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 16, background: "white" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 8 }}>
                    <div style={{ width: 32, height: 32, borderRadius: 8, background: "#7c3aed", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, color: "white", fontWeight: 700 }}>$</div>
                    <div>
                        <div style={{ fontSize: 14, fontWeight: 700, color: SLATE }}>LLM Probabilistic Matching</div>
                        <div style={{ fontSize: 11, color: "#7c3aed" }}>Leveraging OpenAI LLMs for context-aware entity resolution.</div>
                    </div>
                </div>
                <div style={{ fontSize: 11, color: SLATE_5, lineHeight: 1.5 }}>
                    The model utilizes deep learning embeddings to compare records across multiple dimensions, automatically detecting variations in names, addresses, and typos without fixed thresholding or manual blocking rules.
                </div>
            </div>

            {/* 4 stat cards with purple left borders */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 14, marginBottom: 20 }}>
                {STATS.map((s, i) => (
                    <div key={i} style={{ border: `1px solid ${BORDER}`, borderLeft: "3px solid #7c3aed", borderRadius: 8, padding: "14px 16px", background: "white" }}>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>{s.label}</div>
                        <div style={{ fontSize: 20, fontWeight: 800, color: SLATE, marginBottom: 3 }}>{s.value}</div>
                        <div style={{ fontSize: 11, color: SLATE_5 }}>{s.sub}</div>
                    </div>
                ))}
            </div>

            {/* Recent AI Resolutions */}
            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 10 }}>RECENT AI RESOLUTIONS</div>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", background: "white" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                    <span style={{ fontSize: 13, fontWeight: 600, color: SLATE }}>Confidence Score: <b>97.8%</b></span>
                    <div style={{ padding: "3px 12px", background: "#dcfce7", borderRadius: 20, fontSize: 11, fontWeight: 700, color: "#166534" }}>MATCH</div>
                </div>
                <div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: 12, alignItems: "center" }}>
                    <div style={{ border: `1px solid ${BORDER}`, borderRadius: 8, padding: "12px 14px" }}>
                        <div style={{ fontSize: 10, color: SLATE_4, marginBottom: 4 }}>RECORD A (ORA-1042)</div>
                        <div style={{ fontSize: 13, fontWeight: 700, color: SLATE }}>Apollo Hospitals Enterprises Limited</div>
                        <div style={{ fontSize: 11, color: SLATE_4 }}>Plot 1A, AECS Layout, Bangalore</div>
                    </div>
                    <div style={{ fontSize: 18, color: SLATE_4 }}>›</div>
                    <div style={{ border: `1px solid ${BORDER}`, borderRadius: 8, padding: "12px 14px" }}>
                        <div style={{ fontSize: 10, color: SLATE_4, marginBottom: 4 }}>RECORD B (SAP-3091)</div>
                        <div style={{ fontSize: 13, fontWeight: 700, color: SLATE }}>Apollo Hospitals Enterprise Ltd</div>
                        <div style={{ fontSize: 11, color: SLATE_4 }}>Plot 1A, AECS Layout, Bengaluru</div>
                    </div>
                </div>
            </div>
        </div>
    );
};

const StageSurvivorship: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    const SOURCES = [
        { label: "SALESFORCE CRM (PRIMARY)", score: 95, barColor: "#22c55e" },
        { label: "EPIC EMR (CLINICAL)", score: 85, barColor: "#22c55e" },
        { label: "LEGACY BILLING (ERP)", score: 60, barColor: "#f59e0b" },
        { label: "EXTERNAL FEEDS (3RD PARTY)", score: 40, barColor: "#ef4444" },
    ];
    const GR_ROWS = [
        { attr: "first_name", a: "John", b: "Jon", golden: "John" },
        { attr: "last_name", a: "Doe", b: "Doe", golden: "Doe" },
        { attr: "address", a: "4827 Pine Blvd", b: "4829 Pine Blvd", golden: "4827 Pine Blvd" },
        { attr: "city", a: "Phoenix", b: "Phoenix", golden: "Phoenix" },
    ];
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)` }}>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 16, background: "white", position: "relative" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>COMPONENT STATUS: PENDING</div>
                <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>Survivorship</div>
                <div style={{ fontSize: 12, color: SLATE_5, marginTop: 4 }}>Golden record rules</div>
                <div style={{ position: "absolute", top: 16, right: 20, padding: "4px 12px", background: "#f1f5f9", borderRadius: 20, fontSize: 11, fontWeight: 700, color: SLATE_5 }}>PENDING</div>
            </div>

            <div style={{ fontSize: 14, fontWeight: 700, color: SLATE, marginBottom: 4 }}>Source Confidence Tuning</div>
            <div style={{ fontSize: 11, color: SLATE_5, marginBottom: 16 }}>Adjust trust scores by source system. Higher-scored sources take precedence during conflict resolution.</div>

            {/* Source sliders */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", gap: 16, marginBottom: 24 }}>
                {SOURCES.map((s, i) => (
                    <div key={i} style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "14px 16px", background: "white" }}>
                        <div style={{ fontSize: 9, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 10 }}>{s.label}</div>
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                            <div style={{ width: 28, height: 28, border: `1px solid ${BORDER}`, borderRadius: 6, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>−</div>
                            <div style={{ fontSize: 24, fontWeight: 800, color: SLATE }}>{s.score}</div>
                            <div style={{ width: 28, height: 28, border: `1px solid ${BORDER}`, borderRadius: 6, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14 }}>+</div>
                        </div>
                        <div style={{ display: "flex", justifyContent: "space-between", fontSize: 9, color: SLATE_4, marginBottom: 4 }}>
                            <span>Low Trust</span><span>High Trust</span>
                        </div>
                        <div style={{ height: 4, borderRadius: 2, background: "#f1f5f9" }}>
                            <div style={{ height: 4, borderRadius: 2, background: s.barColor, width: `${s.score}%` }} />
                        </div>
                    </div>
                ))}
            </div>

            {/* Conflict Resolution */}
            <div style={{ fontSize: 14, fontWeight: 700, color: SLATE, marginBottom: 4 }}>Conflict Resolution Strategy</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr auto 1fr", gap: 16, alignItems: "end", marginBottom: 20 }}>
                <div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", marginBottom: 6 }}>PRIMARY RULE</div>
                    <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>Recency (LUD) <span>▾</span></div>
                </div>
                <div style={{ fontSize: 12, color: SLATE_4, paddingBottom: 10 }}>→</div>
                <div>
                    <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", marginBottom: 6 }}>FALLBACK RULE (IF TIE/NULL)</div>
                    <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>Source Priority <span>▾</span></div>
                </div>
            </div>

            {/* Golden Record Preview */}
            <div style={{ fontSize: 14, fontWeight: 700, color: SLATE, marginBottom: 4 }}>Golden Record Preview</div>
            <div style={{ fontSize: 11, color: "#7c3aed", marginBottom: 12 }}>Real-time simulation showing how current survivorship rules merge duplicate records into a single golden record.</div>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 8, overflow: "hidden" }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", background: "#f8fafc", padding: "10px 16px", borderBottom: `1px solid ${BORDER}` }}>
                    {["ATTRIBUTE", "RECORD A (SALESFORCE)", "RECORD B (EPIC)", "GOLDEN RECORD"].map((h, i) => (
                        <div key={i} style={{ fontSize: 10, fontWeight: 700, color: i === 3 ? "#166534" : SLATE_5, textTransform: "uppercase", letterSpacing: "0.04em", background: i === 3 ? "#f0fdf4" : "none", padding: i === 3 ? "0 8px" : 0, borderRadius: i === 3 ? 4 : 0 }}>{h}</div>
                    ))}
                </div>
                {GR_ROWS.map((r, i) => (
                    <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 1fr", padding: "10px 16px", borderBottom: `1px solid #f8fafc` }}>
                        <div style={{ fontSize: 12, color: SLATE_5 }}>{r.attr}</div>
                        <div style={{ fontSize: 12, color: SLATE }}>{r.a}</div>
                        <div style={{ fontSize: 12, color: SLATE }}>{r.b}</div>
                        <div style={{ fontSize: 12, fontWeight: 700, color: "#166534" }}>{r.golden}</div>
                    </div>
                ))}
            </div>
        </div>
    );
};

const StagePublishing: React.FC<{ frame: number }> = ({ frame }) => {
    const { fps } = useVideoConfig();
    const enter = spring({ frame, fps, config: { damping: 18 } });
    const TARGETS = [
        { name: "Microsoft Fabric OneLake", desc: "Primary operational store", color: "#10b981" },
        { name: "S3 Export (Parquet)", desc: "Data lake storage", color: "#6366f1" },
        { name: "Snowflake Data Warehouse", desc: "Analytics & reporting", color: "#06b6d4" },
        { name: "API Webhook", desc: "Real-time event streaming", color: "#8b5cf6" },
    ];
    return (
        <div style={{ flex: 1, opacity: enter, transform: `translateY(${(1 - enter) * 12}px)` }}>
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 16, background: "white", position: "relative" }}>
                <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>COMPONENT STATUS: PENDING</div>
                <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>Publishing</div>
                <div style={{ fontSize: 12, color: SLATE_5, marginTop: 4 }}>Downstream sync</div>
                <div style={{ position: "absolute", top: 16, right: 20, padding: "4px 12px", background: "#f1f5f9", borderRadius: 20, fontSize: 11, fontWeight: 700, color: SLATE_5 }}>PENDING</div>
            </div>

            {/* Data Publishing card */}
            <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", marginBottom: 20, background: "white", display: "flex", alignItems: "center", gap: 14 }}>
                <div style={{ width: 36, height: 36, borderRadius: 8, background: "#dcfce7", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18 }}>↑</div>
                <div>
                    <div style={{ fontSize: 14, fontWeight: 700, color: SLATE }}>Data Publishing &amp; Syndication</div>
                    <div style={{ fontSize: 11, color: "#7c3aed" }}>Configure downstream targets and deployment parameters for the golden records.</div>
                </div>
            </div>

            <div style={{ fontSize: 14, fontWeight: 700, color: SLATE, marginBottom: 6 }}>Target Destinations</div>
            <div style={{ fontSize: 11, color: SLATE_5, marginBottom: 14 }}>Select and configure integrations for data export.</div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 14, marginBottom: 20 }}>
                {TARGETS.map((t, i) => (
                    <div key={i} style={{ border: `1px solid ${BORDER}`, borderRadius: 10, padding: "14px 16px", background: "white", display: "flex", alignItems: "center", gap: 12 }}>
                        <div style={{ width: 32, height: 32, borderRadius: 8, background: `${t.color}20`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, color: t.color, fontWeight: 700 }}>
                            {i === 0 ? "F" : i === 1 ? "S3" : i === 2 ? "❄" : "🔗"}
                        </div>
                        <div>
                            <div style={{ fontSize: 13, fontWeight: 600, color: SLATE }}>{t.name}</div>
                            <div style={{ fontSize: 11, color: SLATE_5 }}>{t.desc}</div>
                        </div>
                    </div>
                ))}
            </div>

            {/* Format + Sync */}
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
                <div>
                    <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>Format Configuration</div>
                    <div style={{ fontSize: 11, fontWeight: 600, color: SLATE_5, marginBottom: 6 }}>OUTPUT FORMAT</div>
                    <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>Parquet <span>▾</span></div>
                </div>
                <div>
                    <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 12 }}>Sync Strategy</div>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{ width: 10, height: 10, borderRadius: "50%", background: RED }} />
                        <span style={{ fontSize: 12, fontWeight: 700, color: RED, textTransform: "uppercase", letterSpacing: "0.06em" }}>INCREMENTAL PUSH (APPEND ONLY)</span>
                    </div>
                </div>
            </div>
        </div>
    );
};

// ─────────────────────────────── MAIN SCENE ──────────────────────────────────

export const Scene4Inspector: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();
    const activeIdx = getActiveStage(frame);

    // Frame relative to current stage start
    const localFrame = frame - activeIdx * STAGE_DUR;

    const sideEnter = spring({ frame, fps, config: { damping: 20 } });

    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="Pipeline Inspector">
                {/* Header */}
                <div style={{ marginBottom: 16, opacity: sideEnter }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
                        <div>
                            <h1 style={{ fontSize: 28, fontWeight: 800, color: SLATE, margin: "0 0 4px", letterSpacing: "-0.02em" }}>Pipeline Inspector</h1>
                            <p style={{ fontSize: 12, color: SLATE_5, margin: 0 }}>Real-time backend process orchestration monitor</p>
                        </div>
                        {/* LIVE RUNNING + Node + Runtime */}
                        <div style={{ display: "flex", alignItems: "center", gap: 16, background: "white", border: `1px solid ${BORDER}`, borderRadius: 8, padding: "8px 16px" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                                <div style={{ width: 8, height: 8, borderRadius: "50%", background: RED, boxShadow: `0 0 6px ${RED}` }} />
                                <span style={{ fontSize: 11, fontWeight: 700, color: RED }}>LIVE RUNNING</span>
                            </div>
                            <div style={{ fontSize: 11, color: SLATE_5 }}>Node: <b style={{ color: SLATE }}>db-master-01</b></div>
                            <div style={{ fontSize: 11, color: SLATE_5 }}>Runtime: <b style={{ color: SLATE, fontFamily: "monospace" }}>04:12:08</b></div>
                        </div>
                    </div>
                </div>

                {/* Main inspector layout */}
                <div style={{ display: "flex", gap: 20, flex: 1, overflow: "hidden" }}>
                    {/* === LEFT: Stage buttons === */}
                    <div style={{ width: 340, flexShrink: 0 }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                            <span style={{ fontSize: 12, fontWeight: 700, color: SLATE_5 }}>STAGES (6)</span>
                            <span style={{ fontSize: 11, color: SLATE_4 }}>v2.4.1</span>
                        </div>
                        {STAGES.map((s, i) => {
                            const isActive = i === activeIdx;
                            return (
                                <div key={i} style={{
                                    height: 50, marginBottom: 8, borderRadius: 8,
                                    background: isActive ? RED : "white",
                                    border: isActive ? "none" : `1px solid ${BORDER}`,
                                    display: "flex", alignItems: "center", justifyContent: "center",
                                    fontSize: 14, fontWeight: isActive ? 700 : 400,
                                    color: isActive ? "white" : SLATE,
                                    boxShadow: isActive ? "0 4px 14px rgba(209,31,65,0.3)" : "0 1px 2px rgba(0,0,0,0.03)",
                                }}>{s}</div>
                            );
                        })}
                    </div>

                    {/* === RIGHT: Stage content panel === */}
                    <div style={{ flex: 1, overflow: "hidden", position: "relative", display: "flex", flexDirection: "column" }}>
                        {activeIdx === 0 && <StageIngestion frame={localFrame} />}
                        {activeIdx === 1 && <StageProfiling frame={localFrame} />}
                        {activeIdx === 2 && <StageCleansing frame={localFrame} />}
                        {activeIdx === 3 && <StageResolution frame={localFrame} />}
                        {activeIdx === 4 && <StageSurvivorship frame={localFrame} />}
                        {activeIdx === 5 && <StagePublishing frame={localFrame} />}
                    </div>
                </div>
            </AppLayout>
        </AbsoluteFill>
    );
};
