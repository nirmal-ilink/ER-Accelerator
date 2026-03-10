import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const BORDER = "#e2e8f0";

const KPIS = [
    { label: "TOTAL LOGS", value: "1,248", color: SLATE },
    { label: "SYSTEM HEALTH", value: "98.4%", color: "#16a34a" },
    { label: "SECURITY FLAGS", value: "12", color: RED },
    { label: "ACTIVE USERS", value: "18", color: SLATE },
];

// Exact log rows from Audit Log - Page.png screenshot
const LOG_ROWS = [
    { time: "Mar 06, 13:02", user: "admin", activity: "User Login", sub: "Auth", status: "Success", detail: "Session started for admin" },
    { time: "Mar 05, 18:56", user: "Administrator", activity: "User Logout", sub: "Auth", status: "Success", detail: "User signed out manually" },
    { time: "Mar 05, 18:39", user: "System", activity: "Triggered Profiling", sub: "Connectors", status: "Success", detail: "Triggered /Shared/ER_aligned/nb_mdm_profiling for ID dd809c8c-feee…" },
    { time: "Mar 05, 17:51", user: "System", activity: "Triggered Profiling", sub: "Connectors", status: "Success", detail: "Triggered /Shared/ER_aligned/nb_mdm_profiling for ID dd809c8c-feee…" },
    { time: "Mar 05, 17:37", user: "admin", activity: "User Login", sub: "Auth", status: "Success", detail: "Session started for admin" },
    { time: "Mar 05, 08:33", user: "admin", activity: "User Login", sub: "Auth", status: "Success", detail: "Session started for admin" },
    { time: "Mar 05, 08:21", user: "admin", activity: "User Login", sub: "Auth", status: "Success", detail: "Session started for admin" },
    { time: "Mar 04, 17:13", user: "System", activity: "Triggered Profiling", sub: "Connectors", status: "Success", detail: "Triggered /Shared/ER_aligned/nb_mdm_profiling for ID dd809c8c-feee…" },
    { time: "Mar 04, 16:58", user: "System", activity: "Triggered Ingestion", sub: "Connectors", status: "Success", detail: "Triggered /Shared/ER_aligned/nb_brz_ingestion for ID dd809c8c-feee…" },
    { time: "Mar 04, 16:44", user: "System", activity: "Triggered Ingestion", sub: "Connectors", status: "Failed", detail: "Failed to Ingest TERMINATED…" },
];

export const Scene7Audit: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const hdr = spring({ frame, fps, config: { damping: 20 } });
    const kpi = spring({ frame: Math.max(0, frame - 12), fps, config: { damping: 18 } });
    const filters = spring({ frame: Math.max(0, frame - 28), fps, config: { damping: 18 } });
    const table = spring({ frame: Math.max(0, frame - 44), fps, config: { damping: 16 } });

    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="Audit Logs">
                {/* Header */}
                <div style={{ marginBottom: 20, opacity: hdr, transform: `translateY(${(1 - hdr) * 10}px)` }}>
                    <h1 style={{ fontSize: 30, fontWeight: 800, color: SLATE, margin: "0 0 4px", letterSpacing: "-0.02em" }}>Audit &amp; Governance Trail</h1>
                    <p style={{ fontSize: 13, color: SLATE_5, margin: 0 }}>Comprehensive immutable ledger of all system and user interactions.</p>
                </div>

                {/* KPI cards */}
                <div style={{ display: "flex", gap: 16, marginBottom: 20, opacity: kpi, transform: `translateY(${(1 - kpi) * 10}px)` }}>
                    {KPIS.map((k, i) => (
                        <div key={i} style={{ flex: 1, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "16px 20px", background: "white", boxShadow: "0 1px 3px rgba(0,0,0,0.03)" }}>
                            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>{k.label}</div>
                            <div style={{ fontSize: 26, fontWeight: 700, color: k.color, lineHeight: 1 }}>{k.value}</div>
                        </div>
                    ))}
                </div>

                {/* Filters row */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16, opacity: filters }}>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>SEARCH</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", fontSize: 13, color: SLATE_4, background: "white" }}>Search by user, action, or module...</div>
                    </div>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>STATUS</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>All Categories <span style={{ color: SLATE_4 }}>▾</span></div>
                    </div>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>DATE RANGE</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>2026/02/27 – 2026/03/06 <span style={{ color: SLATE_4 }}>╮</span></div>
                    </div>
                </div>

                {/* Row count + download button */}
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12, opacity: table }}>
                    <div style={{ fontSize: 13, color: SLATE_5 }}>480 entries found in current view</div>
                    <div style={{ height: 38, padding: "0 20px", background: RED, borderRadius: 8, display: "flex", alignItems: "center", fontSize: 13, fontWeight: 700, color: "white" }}>Download Audit Trail (CSV)</div>
                </div>

                {/* Dark table */}
                <div style={{ flex: 1, overflow: "hidden", borderRadius: 10, opacity: table, transform: `translateY(${(1 - table) * 10}px)` }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                        <thead>
                            <tr style={{ background: "#1e293b" }}>
                                {["Time", "Integrator / User", "Activity", "Subsystem", "Execution Status", "Technical Details"].map((h, i) => (
                                    <th key={i} style={{ padding: "12px 14px", textAlign: "left", fontWeight: 600, color: "#94a3b8", fontSize: 11, textTransform: "uppercase", letterSpacing: "0.04em", whiteSpace: "nowrap" }}>{h}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {LOG_ROWS.map((r, i) => {
                                const rowDelay = 50 + i * 6;
                                const rowOp = Math.min(1, Math.max(0, (frame - rowDelay) / 12));
                                return (
                                    <tr key={i} style={{ background: "#1e293b", borderBottom: "1px solid rgba(255,255,255,0.05)", opacity: rowOp }}>
                                        <td style={{ padding: "9px 14px", color: "#94a3b8", whiteSpace: "nowrap" }}>{r.time}</td>
                                        <td style={{ padding: "9px 14px", color: "#e2e8f0", fontWeight: 500 }}>{r.user}</td>
                                        <td style={{ padding: "9px 14px", color: "#e2e8f0" }}>{r.activity}</td>
                                        <td style={{ padding: "9px 14px", color: "#94a3b8" }}>{r.sub}</td>
                                        <td style={{ padding: "9px 14px", color: r.status === "Failed" ? RED : "#22c55e", fontWeight: 500 }}>{r.status}</td>
                                        <td style={{ padding: "9px 14px", color: "#64748b", overflow: "hidden", maxWidth: 350, textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{r.detail}</td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </AppLayout>
        </AbsoluteFill>
    );
};
