import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const BORDER = "#e2e8f0";

// Data exactly from Match Review Page.png + Match Review - Column Selector for Golden Records.png
const RECORD_FIELDS = [
    { label: "NPI", a: "5654294309", b: "5654294309", conflict: false },
    { label: "FIRST NAME", a: "Liz", b: "Elizabeth", conflict: true },
    { label: "LAST NAME", a: "Miller", b: "Miller", conflict: false },
    { label: "ADDRESS", a: "4827 Pine Blvd", b: "7804 Main Lane", conflict: true },
    { label: "CITY", a: "Phoenix", b: "Phoenix", conflict: false },
    { label: "STATE", a: "AZ", b: "AZ", conflict: false },
    { label: "ZIP CODE", a: "85001", b: "85001", conflict: false },
    { label: "PHONE", a: "357-555-7477", b: "357-555-7477", conflict: false },
    { label: "SPECIALTY", a: "Psychiatry", b: "Psychiatry", conflict: false },
];

// For multilateral comparison
const ML_ROWS = [
    { attr: "NPI", claims: "5654294309", emr: "5654294309", sap: "5654294309", npi: "5654294309" },
    { attr: "FIRST NAME", claims: "Liz", emr: "Elizabeth", sap: "Elizabeth", npi: "Liz", bold: true },
    { attr: "LAST NAME", claims: "Miller", emr: "Miller", sap: "Miller", npi: "Miller" },
    { attr: "ADDRESS", claims: "4827 Pine Blvd", emr: "7804 Main Lane", sap: "436 Oak Rd", npi: "9896 Pine Blvd", bold: true },
    { attr: "CITY", claims: "Phoenix", emr: "Phoenix", sap: "Phoenix", npi: "Phoenix" },
    { attr: "STATE", claims: "AZ", emr: "AZ", sap: "AZ", npi: "AZ" },
    { attr: "ZIP CODE", claims: "85001", emr: "85001", sap: "85001", npi: "85001" },
    { attr: "PHONE", claims: "357-555-7477", emr: "357-555-7477", sap: "357-555-7477", npi: "357-555-7477" },
    { attr: "SPECIALTY", claims: "Psychiatry", emr: "Psychiatry", sap: "Psychiatry", npi: "Psychiatry" },
];

// Golden Record builder data
const GR_ROWS = [
    { attr: "NPI", sources: { CLAIMS_DB: "5654294309", EMR: "5654294309", SAP: "5654294309", NPI_REGISTRY: "5654294309" }, selected: "CLAIMS_DB" },
    { attr: "First Name", sources: { CLAIMS_DB: "Liz", EMR: "Elizabeth", SAP: "Elizabeth", NPI_REGISTRY: "Liz" }, selected: "NPI_REGISTRY" },
    { attr: "Last Name", sources: { CLAIMS_DB: "Miller", EMR: "Miller", SAP: "Miller", NPI_REGISTRY: "Miller" }, selected: "SAP" },
    { attr: "Address", sources: { CLAIMS_DB: "4827 Pine Blvd", EMR: "7804 Main Lane", SAP: "436 Oak Rd", NPI_REGISTRY: "9896 Pine Blvd" }, selected: "CLAIMS_DB" },
    { attr: "City", sources: { CLAIMS_DB: "Phoenix", EMR: "Phoenix", SAP: "Phoenix", NPI_REGISTRY: "Phoenix" }, selected: "SAP" },
    { attr: "State", sources: { CLAIMS_DB: "AZ", EMR: "AZ", SAP: "AZ", NPI_REGISTRY: "AZ" }, selected: "CLAIMS_DB" },
    { attr: "Zip Code", sources: { CLAIMS_DB: "85001", EMR: "85001", SAP: "85001", NPI_REGISTRY: "85001" }, selected: "SAP" },
    { attr: "Phone", sources: { CLAIMS_DB: "357-555-7477", EMR: "357-555-7477", SAP: "357-555-7477", NPI_REGISTRY: "357-555-7477" }, selected: "CLAIMS_DB" },
    { attr: "Specialty", sources: { CLAIMS_DB: "Psychiatry", EMR: "Psychiatry", SAP: "Psychiatry", NPI_REGISTRY: "Psychiatry" }, selected: "CLAIMS_DB" },
];

const DonutChart: React.FC<{ pct: number; size: number }> = ({ pct, size }) => {
    const r = (size - 14) / 2;
    const circ = 2 * Math.PI * r;
    const dash = (pct / 100) * circ;
    return (
        <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`}>
            <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#f1f5f9" strokeWidth={10} />
            <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={RED} strokeWidth={10}
                strokeDasharray={`${dash} ${circ - dash}`}
                strokeLinecap="round"
                transform={`rotate(-90 ${size / 2} ${size / 2})`}
            />
            <text x={size / 2} y={size / 2 + 2} textAnchor="middle" dominantBaseline="middle" fontSize={18} fontWeight={800} fill={SLATE}>{pct}%</text>
        </svg>
    );
};

export const Scene5MatchReview: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    // Phase: 0-90 = comparison view, 91+ = golden record builder
    const showGoldenRecord = frame >= 91;

    const hdr = spring({ frame, fps, config: { damping: 20 } });
    const cards = spring({ frame: Math.max(0, frame - 15), fps, config: { damping: 18 } });
    const mlComp = spring({ frame: Math.max(0, frame - 50), fps, config: { damping: 16 } });
    const grEnter = spring({ frame: Math.max(0, frame - 91), fps, config: { damping: 20 } });

    if (showGoldenRecord) {
        // Golden Record Builder view
        return (
            <AbsoluteFill style={{ width: 1920, height: 1080 }}>
                <AppLayout activeNav="Match Review">
                    <div style={{ opacity: grEnter, transform: `translateY(${(1 - grEnter) * 10}px)` }}>
                        <h1 style={{ fontSize: 26, fontWeight: 800, color: SLATE, margin: "0 0 4px", letterSpacing: "-0.02em" }}>Build Golden Record</h1>
                        <p style={{ fontSize: 12, color: SLATE_5, margin: "0 0 20px" }}>Select the authoritative source for each attribute to compose the master record.</p>

                        <div style={{ display: "grid", gridTemplateColumns: "160px 1fr", gap: 16, alignItems: "start" }}>
                            <div style={{ fontSize: 12, fontWeight: 700, color: SLATE_5, paddingTop: 10 }}>Attribute</div>
                            <div style={{ fontSize: 12, fontWeight: 700, color: SLATE_5, paddingTop: 10 }}>Authoritative Source Selection</div>

                            {GR_ROWS.map((row, ri) => (
                                <React.Fragment key={ri}>
                                    <div style={{ fontSize: 14, fontWeight: 600, color: SLATE, paddingTop: 8 }}>{row.attr}</div>
                                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                                        {(Object.entries(row.sources) as [string, string][]).map(([source, val]) => {
                                            const selected = source === row.selected;
                                            return (
                                                <div key={source} style={{
                                                    padding: "6px 14px", borderRadius: 24,
                                                    border: `1.5px solid ${selected ? RED : BORDER}`,
                                                    fontSize: 12, fontWeight: selected ? 700 : 400,
                                                    color: selected ? RED : SLATE_5,
                                                    background: selected ? "#fff5f6" : "white",
                                                }}>
                                                    {source}: {val}
                                                </div>
                                            );
                                        })}
                                    </div>
                                </React.Fragment>
                            ))}
                        </div>

                        {/* Bottom buttons */}
                        <div style={{ display: "flex", gap: 12, marginTop: 24 }}>
                            {["← Previous", "Approve Match", "Reject Match", "Defer Resolution"].map((b, i) => (
                                <div key={i} style={{
                                    flex: 1, height: 44, border: `1px solid ${BORDER}`, borderRadius: 8,
                                    display: "flex", alignItems: "center", justifyContent: "center",
                                    fontSize: 13, fontWeight: 500, color: SLATE, background: "white"
                                }}>{b}</div>
                            ))}
                        </div>
                    </div>
                </AppLayout>
            </AbsoluteFill>
        );
    }

    // Main comparison view
    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="Match Review">
                {/* Header with red left border */}
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12, opacity: hdr }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 0 }}>
                        <div style={{ width: 4, height: 42, background: RED, borderRadius: 2, marginRight: 16 }} />
                        <div>
                            <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
                                <h1 style={{ fontSize: 28, fontWeight: 800, color: SLATE, margin: 0, letterSpacing: "-0.02em" }}>Match Review Center</h1>
                                <div style={{ fontSize: 14, color: SLATE_5 }}>| CLUSTER: CL010</div>
                            </div>
                        </div>
                    </div>
                    <div style={{ textAlign: "right" }}>
                        <div style={{ fontSize: 10, fontWeight: 600, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em" }}>RESOLUTION STATUS</div>
                        <div style={{ fontSize: 22, fontWeight: 800, color: SLATE }}>1 <span style={{ fontSize: 14, color: SLATE_4 }}>/ 6</span></div>
                    </div>
                </div>

                {/* Red progress bar */}
                <div style={{ height: 4, background: "#f1f5f9", borderRadius: 2, marginBottom: 14, opacity: hdr }}>
                    <div style={{ height: 4, width: "17%", background: RED, borderRadius: 2 }} />
                </div>

                {/* Source pills */}
                <div style={{ display: "flex", alignItems: "center", gap: 24, marginBottom: 16, opacity: hdr }}>
                    <span style={{ fontSize: 12, color: SLATE_5, fontWeight: 600 }}>COMPARE REFERENCE AGAINST:</span>
                    {[{ n: "CLINICAL EMR (73%)", c: RED }, { n: "SAP ERP (73%)", c: SLATE }, { n: "NPI REGISTRY (89%)", c: SLATE }].map((s, i) => (
                        <div key={i} style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, fontWeight: 600, color: s.c }}>
                            <div style={{ width: 8, height: 8, borderRadius: "50%", background: s.c }} /> {s.n}
                        </div>
                    ))}
                </div>

                {/* Two record cards + donut center */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 160px 1fr", gap: 16, marginBottom: 16, opacity: cards, transform: `translateY(${(1 - cards) * 12}px)` }}>
                    {/* Left: Liz Miller */}
                    <div style={{ border: `1px solid ${BORDER}`, borderRadius: 12, overflow: "hidden", background: "white" }}>
                        <div style={{ padding: "14px 16px", borderBottom: `1px solid ${BORDER}`, display: "flex", justifyContent: "space-between" }}>
                            <div>
                                <div style={{ fontSize: 16, fontWeight: 800, color: SLATE }}>Liz Miller</div>
                                <div style={{ fontSize: 11, color: SLATE_4 }}>UID: CLA482</div>
                            </div>
                            <div style={{ padding: "3px 8px", background: "#f1f5f9", borderRadius: 4, fontSize: 10, fontWeight: 700, color: SLATE_5 }}>CLAIMS_DB</div>
                        </div>
                        {RECORD_FIELDS.map((f, i) => (
                            <div key={i} style={{ display: "grid", gridTemplateColumns: "100px 1fr", padding: "7px 14px", borderBottom: `1px solid #f8fafc`, alignItems: "center" }}>
                                <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.04em" }}>{f.label}</div>
                                <div style={{
                                    fontSize: 12, fontWeight: f.conflict ? 700 : 400, color: f.conflict ? "white" : SLATE,
                                    textAlign: "right", padding: f.conflict ? "3px 8px" : 0, borderRadius: f.conflict ? 4 : 0,
                                    background: f.conflict ? RED : "none",
                                }}>{f.a}</div>
                            </div>
                        ))}
                    </div>

                    {/* Center donut */}
                    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 10 }}>
                        <DonutChart pct={73} size={120} />
                        <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", textAlign: "center", letterSpacing: "0.06em" }}>MATCH CONFIDENCE</div>
                        <div style={{ padding: "4px 12px", background: "#fef3c7", borderRadius: 20, fontSize: 10, fontWeight: 700, color: "#92400e" }}>REVIEW REQUESTED</div>
                    </div>

                    {/* Right: Elizabeth Miller */}
                    <div style={{ border: `1px solid ${BORDER}`, borderRadius: 12, overflow: "hidden", background: "white" }}>
                        <div style={{ padding: "14px 16px", borderBottom: `1px solid ${BORDER}`, display: "flex", justifyContent: "space-between" }}>
                            <div>
                                <div style={{ fontSize: 16, fontWeight: 800, color: SLATE }}>Elizabeth Miller</div>
                                <div style={{ fontSize: 11, color: SLATE_4 }}>UID: EMR412</div>
                            </div>
                            <div style={{ padding: "3px 8px", background: "#fef2f2", borderRadius: 4, fontSize: 10, fontWeight: 700, color: RED }}>EMR</div>
                        </div>
                        {RECORD_FIELDS.map((f, i) => (
                            <div key={i} style={{ display: "grid", gridTemplateColumns: "100px 1fr", padding: "7px 14px", borderBottom: `1px solid #f8fafc`, alignItems: "center" }}>
                                <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.04em" }}>{f.label}</div>
                                <div style={{
                                    fontSize: 12, fontWeight: f.conflict ? 700 : 400, color: f.conflict ? "white" : SLATE,
                                    textAlign: "right", padding: f.conflict ? "3px 8px" : 0, borderRadius: f.conflict ? 4 : 0,
                                    background: f.conflict ? RED : "none",
                                }}>{f.b}</div>
                            </div>
                        ))}
                    </div>
                </div>

                {/* Multilateral Comparison */}
                <div style={{ opacity: mlComp, transform: `translateY(${(1 - mlComp) * 10}px)` }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
                        <div style={{ fontSize: 14, fontWeight: 700, color: SLATE }}>Multilateral Comparison</div>
                        <div style={{ fontSize: 12, fontWeight: 700, color: RED }}>4 DATA VECTORS</div>
                    </div>
                    <div style={{ border: `1px solid ${BORDER}`, borderRadius: 10, overflow: "hidden" }}>
                        {/* Header */}
                        <div style={{ display: "grid", gridTemplateColumns: "120px 1fr 1fr 1fr 1fr", padding: "8px 14px", background: "#f8fafc", borderBottom: `1px solid ${BORDER}` }}>
                            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase" }}>ATTRIBUTE</div>
                            {[
                                { n: "CLAIMS_DB", bg: "#f1f5f9", c: SLATE }, { n: "EMR", bg: "#fef2f2", c: RED },
                                { n: "SAP", bg: "#f1f5f9", c: SLATE }, { n: "NPI_REGISTRY", bg: "#eff6ff", c: "#1d4ed8" }
                            ].map((h, i) => (
                                <div key={i} style={{ padding: "2px 8px", background: h.bg, borderRadius: 4, fontSize: 10, fontWeight: 700, color: h.c, textTransform: "uppercase" }}>{h.n}</div>
                            ))}
                        </div>
                        {ML_ROWS.map((r, i) => (
                            <div key={i} style={{ display: "grid", gridTemplateColumns: "120px 1fr 1fr 1fr 1fr", padding: "8px 14px", borderBottom: `1px solid #f8fafc` }}>
                                <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.04em" }}>{r.attr}</div>
                                {[r.claims, r.emr, r.sap, r.npi].map((v, j) => (
                                    <div key={j} style={{ fontSize: 12, fontWeight: r.bold ? 700 : 400, color: SLATE }}>{v}</div>
                                ))}
                            </div>
                        ))}
                    </div>
                </div>
            </AppLayout>
        </AbsoluteFill>
    );
};
