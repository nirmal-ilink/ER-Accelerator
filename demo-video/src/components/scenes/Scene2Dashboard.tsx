import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const GREEN = "#22c55e";
const BORDER = "#e2e8f0";

// Exact data from Dashboard screenshot + dashboard.py source
const KPIS = [
    { label: "RECORDS GOVERNED", value: "14.2M", trend: "+ 4.2%", good: true },
    { label: "GOLDEN CONSISTENCY", value: "96.4%", trend: "+ 1.8%", good: true },
    { label: "DUPLICATE RISK", value: "0.42%", trend: "↓ 12%", good: false },
    { label: "THROUGHPUT / HR", value: "84.2K", trend: "+ 9.1%", good: true },
];

// Exact months + scores from dashboard.py
const TRUST_MONTHS = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb"];
const TRUST_SCORES = [78.5, 79.2, 78.8, 81.5, 83.2, 85.8, 88.4, 91.2, 92.8, 93.5, 96.4];

// Risk vectors exact from screenshot
const RISKS = [
    { tag: "LATE", color: "#ef4444", bg: "#fee2e2", label: "SLA BREACH: EPIC-01", sub: "High Severity" },
    { tag: "PEND", color: "#f59e0b", bg: "#fef3c7", label: "DUPLICATE CLUSTER: PHX", sub: "Review Req" },
    { tag: "SYNC", color: "#3b82f6", bg: "#eff6ff", label: "SYSTEM SYNC DELAY", sub: "Delta Lake" },
    { tag: "ERR", color: "#ef4444", bg: "#fee2e2", label: "INCONSISTENT PROVIDER ID", sub: "Manual Resolve" },
];

const TrustChart: React.FC<{ progress: number }> = ({ progress }) => {
    const W = 660, H = 200;
    const minY = 75, maxY = 100;
    const pts = TRUST_SCORES.map((v, i) => ({
        x: 36 + (i / (TRUST_SCORES.length - 1)) * (W - 46),
        y: H - 30 - ((v - minY) / (maxY - minY)) * (H - 50)
    }));
    const vis = Math.max(1, Math.round(progress * (pts.length - 1)));
    const vpts = pts.slice(0, vis + 1);

    let line = "";
    if (vpts.length > 1) {
        line = `M ${vpts[0].x} ${vpts[0].y}`;
        for (let i = 1; i < vpts.length; i++) {
            const cpx = (vpts[i - 1].x + vpts[i].x) / 2;
            line += ` C ${cpx} ${vpts[i - 1].y} ${cpx} ${vpts[i].y} ${vpts[i].x} ${vpts[i].y}`;
        }
    }
    const fill = line + ` L ${vpts[vpts.length - 1]?.x ?? 36} ${H - 30} L 36 ${H - 30} Z`;

    return (
        <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} style={{ overflow: "visible" }}>
            {[80, 85, 90, 95, 100].map(v => {
                const y = H - 30 - ((v - minY) / (maxY - minY)) * (H - 50);
                return (
                    <g key={v}>
                        <line x1={36} y1={y} x2={W - 10} y2={y} stroke="#f1f5f9" strokeWidth={1} />
                        <text x={28} y={y + 4} textAnchor="end" fontSize={10} fill={SLATE_4}>{v}</text>
                    </g>
                );
            })}
            {vpts.length > 1 && <path d={fill} fill={`${RED}15`} />}
            {line && <path d={line} fill="none" stroke={RED} strokeWidth={2.5} strokeLinecap="round" />}
            {vpts.length > 0 && (
                <circle cx={vpts[vpts.length - 1].x} cy={vpts[vpts.length - 1].y} r={5} fill={RED} />
            )}
            {TRUST_MONTHS.map((m, i) => {
                const x = 36 + (i / (TRUST_SCORES.length - 1)) * (W - 46);
                return <text key={i} x={x} y={H - 8} textAnchor="middle" fontSize={10} fill={SLATE_4}>{m}</text>;
            })}
        </svg>
    );
};

export const Scene2Dashboard: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const hdr = spring({ frame, fps, config: { damping: 20 } });
    const kpi = spring({ frame: Math.max(0, frame - 15), fps, config: { damping: 18 } });
    const grid = spring({ frame: Math.max(0, frame - 40), fps, config: { damping: 16 } });
    const chartProg = Math.min(1, Math.max(0, (frame - 50) / 100));

    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="Dashboard">
                {/* === HEADER === */}
                <div style={{ marginBottom: 20, opacity: hdr, transform: `translateY(${(1 - hdr) * 12}px)` }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
                        <div style={{ width: 28, height: 3, background: RED, borderRadius: 2 }} />
                        <span style={{ fontSize: 11, fontWeight: 600, color: RED, textTransform: "uppercase", letterSpacing: "0.12em" }}>Enterprise Intelligence</span>
                    </div>
                    <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
                        <h1 style={{ fontSize: 36, fontWeight: 800, color: SLATE, margin: 0, letterSpacing: "-0.03em" }}>Operational Command</h1>
                        {/* LIVE signal — top right */}
                        <div style={{ display: "flex", alignItems: "center", gap: 8, background: "rgba(34,197,94,0.08)", border: "1px solid rgba(34,197,94,0.2)", padding: "5px 14px", borderRadius: 50, fontSize: 10.5, fontWeight: 600, color: "#166534" }}>
                            <div style={{ width: 7, height: 7, borderRadius: "50%", background: GREEN }} />
                            SIGNAL: LIVE ENTERPRISE FEED
                        </div>
                    </div>
                </div>

                {/* === COMMAND STRIP === */}
                <div style={{ display: "flex", gap: 12, marginBottom: 24, opacity: kpi }}>
                    {["Last 7 Days ▾", "All Platforms ▾", "Execute Global Linkage", "Advanced Pipeline"].map((label, i) => (
                        <div key={i} style={{
                            height: 40, padding: "0 20px",
                            background: "white", border: `1px solid ${BORDER}`,
                            borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center",
                            fontSize: 13, fontWeight: 500, color: SLATE,
                            minWidth: i < 2 ? 200 : 220,
                            boxShadow: "0 1px 2px rgba(0,0,0,0.04)"
                        }}>{label}</div>
                    ))}
                </div>

                {/* === KPI CARDS === */}
                <div style={{ display: "flex", gap: 18, marginBottom: 22, opacity: kpi, transform: `translateY(${(1 - kpi) * 14}px)` }}>
                    {KPIS.map((k, i) => (
                        <div key={i} style={{
                            flex: 1, background: "white",
                            border: `1px solid ${BORDER}`, borderRadius: 12,
                            padding: "20px 24px",
                            boxShadow: "0 1px 3px rgba(0,0,0,0.03)",
                        }}>
                            <div style={{ fontSize: 10, fontWeight: 600, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 10 }}>{k.label}</div>
                            <div style={{ display: "flex", alignItems: "flex-end", justifyContent: "space-between" }}>
                                <div style={{ fontSize: 30, fontWeight: 700, color: SLATE, lineHeight: 1 }}>{k.value}</div>
                                <div style={{ fontSize: 12, fontWeight: 700, color: k.good ? "#16a34a" : RED }}>{k.trend}</div>
                            </div>
                        </div>
                    ))}
                </div>

                {/* === MAIN GRID: Chart + Risk Vectors === */}
                <div style={{ display: "flex", gap: 20, flex: 1, overflow: "hidden", opacity: grid, transform: `translateY(${(1 - grid) * 14}px)` }}>
                    {/* Trust Trajectory chart */}
                    <div style={{ flex: 2.5, background: "white", border: `1px solid ${BORDER}`, borderRadius: 12, padding: "22px 24px", boxShadow: "0 1px 3px rgba(0,0,0,0.03)", overflow: "hidden" }}>
                        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 4 }}>
                            <div>
                                <div style={{ fontSize: 14, fontWeight: 700, color: SLATE }}>Enterprise Trust Trajectory</div>
                                <div style={{ fontSize: 11, color: SLATE_4 }}>Synthesized data health index over 12 months</div>
                            </div>
                            <div style={{ fontSize: 10, fontWeight: 600, color: SLATE_5, letterSpacing: "0.05em", textTransform: "uppercase" }}>VECTOR: GLOBAL RECONCILIATION</div>
                        </div>
                        <TrustChart progress={chartProg} />
                    </div>

                    {/* High Risk Vectors */}
                    <div style={{ width: 340, background: "white", border: `1px solid ${BORDER}`, borderRadius: 12, padding: "22px 24px", boxShadow: "0 1px 3px rgba(0,0,0,0.03)", display: "flex", flexDirection: "column" }}>
                        <div style={{ fontSize: 14, fontWeight: 700, color: SLATE, marginBottom: 4 }}>High Risk Vectors</div>
                        <div style={{ fontSize: 11, color: SLATE_4, marginBottom: 18 }}>Urgent items requiring intervention</div>
                        {RISKS.map((r, i) => {
                            const rowOp = Math.min(1, Math.max(0, (frame - (55 + i * 14)) / 18));
                            return (
                                <div key={i} style={{ display: "flex", alignItems: "center", gap: 14, padding: "12px 0", borderBottom: i < RISKS.length - 1 ? `1px solid #f8fafc` : "none", opacity: rowOp }}>
                                    <div style={{ fontSize: 9, fontWeight: 800, background: r.bg, color: r.color, padding: "3px 6px", borderRadius: 4, letterSpacing: "0.05em", flexShrink: 0 }}>{r.tag}</div>
                                    <div>
                                        <div style={{ fontSize: 12.5, fontWeight: 700, color: SLATE }}>{r.label}</div>
                                        <div style={{ fontSize: 11, color: SLATE_4 }}>{r.sub}</div>
                                    </div>
                                    <div style={{ marginLeft: "auto", fontSize: 14, color: SLATE_4 }}>›</div>
                                </div>
                            );
                        })}
                        <div style={{ marginTop: "auto", paddingTop: 14 }}>
                            <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13, fontWeight: 500, color: SLATE }}>
                                Open Operational Review
                            </div>
                        </div>
                    </div>
                </div>
            </AppLayout>
        </AbsoluteFill>
    );
};
