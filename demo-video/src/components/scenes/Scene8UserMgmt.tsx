import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const BORDER = "#e2e8f0";

// Exact from User Management - Page.png
const KPIS = [
    { label: "TOTAL USERS", value: "4", sub: "Registered accounts", color: SLATE },
    { label: "ACTIVE USERS", value: "4", sub: "100% of total", color: "#16a34a" },
    { label: "ADMINISTRATORS", value: "1", sub: "Admin access", color: RED },
    { label: "LICENSE CAPACITY", value: "46", sub: "Available seats", color: "#3b82f6" },
];

// Exact rows from screenshot
const USERS = [
    { id: "admin", name: "Administrator", email: "admin@icore.io", role: "Admin", status: "Active", activity: "2 mins ago" },
    { id: "exec", name: "C-Suite User", email: "exec@icore.io", role: "Executive", status: "Active", activity: "1 hour ago" },
    { id: "steward", name: "Data Steward", email: "steward@icore.io", role: "Steward", status: "Active", activity: "Just now" },
    { id: "dev", name: "Tech Lead", email: "dev@icore.io", role: "Developer", status: "Active", activity: "15 mins ago" },
];

const ROLE_COLORS: Record<string, string> = {
    Admin: "#7c3aed",
    Executive: "#0ea5e9",
    Steward: "#16a34a",
    Developer: "#f59e0b",
};

export const Scene8UserMgmt: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const hdr = spring({ frame, fps, config: { damping: 20 } });
    const kpi = spring({ frame: Math.max(0, frame - 14), fps, config: { damping: 18 } });
    const dir = spring({ frame: Math.max(0, frame - 28), fps, config: { damping: 18 } });
    const table = spring({ frame: Math.max(0, frame - 44), fps, config: { damping: 16 } });

    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="User Management">
                {/* Header */}
                <div style={{ marginBottom: 24, opacity: hdr, transform: `translateY(${(1 - hdr) * 10}px)` }}>
                    <h1 style={{ fontSize: 30, fontWeight: 800, color: SLATE, margin: "0 0 4px", letterSpacing: "-0.02em" }}>Identity Control Center</h1>
                    <p style={{ fontSize: 13, color: SLATE_5, margin: 0 }}>Manage platform users, roles, and access permissions with precision.</p>
                </div>

                {/* KPI cards */}
                <div style={{ display: "flex", gap: 16, marginBottom: 24, opacity: kpi, transform: `translateY(${(1 - kpi) * 10}px)` }}>
                    {KPIS.map((k, i) => (
                        <div key={i} style={{ flex: 1, border: `1px solid ${BORDER}`, borderRadius: 10, padding: "18px 20px", background: "white", boxShadow: "0 1px 3px rgba(0,0,0,0.03)" }}>
                            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_4, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 8 }}>{k.label}</div>
                            <div style={{ fontSize: 28, fontWeight: 800, color: k.color, lineHeight: 1, marginBottom: 4 }}>{k.value}</div>
                            <div style={{ fontSize: 11, color: SLATE_4 }}>{k.sub}</div>
                        </div>
                    ))}
                </div>

                {/* "Directory Management" section */}
                <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16, opacity: dir }}>
                    <div style={{ width: 3, height: 20, background: RED, borderRadius: 2 }} />
                    <div style={{ fontSize: 16, fontWeight: 700, color: SLATE }}>Directory Management</div>
                </div>

                {/* Filters row + Add New User */}
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr auto", gap: 12, marginBottom: 16, alignItems: "end", opacity: dir }}>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>SEARCH USERS</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", fontSize: 13, color: SLATE_4, background: "white" }}>Search by name, email, or team...</div>
                    </div>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>FILTER BY ROLE</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>All Roles <span style={{ color: SLATE_4 }}>▾</span></div>
                    </div>
                    <div>
                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>FILTER BY STATUS</div>
                        <div style={{ height: 40, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", fontSize: 13, color: SLATE, background: "white" }}>All Status <span style={{ color: SLATE_4 }}>▾</span></div>
                    </div>
                    <div style={{ height: 40, padding: "0 24px", background: RED, borderRadius: 24, display: "flex", alignItems: "center", fontSize: 13, fontWeight: 700, color: "white", boxShadow: "0 4px 12px rgba(209,31,65,0.3)" }}>Add New User</div>
                </div>

                {/* User table — dark */}
                <div style={{ borderRadius: 10, overflow: "hidden", opacity: table, transform: `translateY(${(1 - table) * 10}px)` }}>
                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
                        <thead>
                            <tr style={{ background: "#1e293b" }}>
                                {["Login ID", "User Name", "Email", "Role", "Access Status", "Last Activity"].map((h, i) => (
                                    <th key={i} style={{ padding: "13px 16px", textAlign: "left", fontWeight: 600, color: "#94a3b8", fontSize: 11, letterSpacing: "0.04em" }}>
                                        {h !== "Login ID" ? <span style={{ display: "flex", alignItems: "center", gap: 4 }}><span style={{ fontSize: 10 }}>⇅</span> {h}</span> : h}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {USERS.map((u, i) => {
                                const rowDelay = 50 + i * 10;
                                const rowOp = Math.min(1, Math.max(0, (frame - rowDelay) / 14));
                                return (
                                    <tr key={i} style={{ background: "#1e293b", borderBottom: "1px solid rgba(255,255,255,0.05)", opacity: rowOp }}>
                                        <td style={{ padding: "11px 16px", color: "#94a3b8", fontFamily: "monospace" }}>{u.id}</td>
                                        <td style={{ padding: "11px 16px", color: "#e2e8f0", fontWeight: 500 }}>{u.name}</td>
                                        <td style={{ padding: "11px 16px", color: "#94a3b8" }}>{u.email}</td>
                                        <td style={{ padding: "11px 16px" }}>
                                            <span style={{ fontSize: 11, fontWeight: 700, color: ROLE_COLORS[u.role] || SLATE_5 }}>{u.role}</span>
                                        </td>
                                        <td style={{ padding: "11px 16px", color: "#22c55e", fontWeight: 500 }}>{u.status}</td>
                                        <td style={{ padding: "11px 16px", color: "#64748b" }}>{u.activity}</td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>

                {/* Footer */}
                <div style={{ textAlign: "center", marginTop: "auto", paddingTop: 16, fontSize: 12, color: SLATE_4, opacity: table }}>
                    © 2026 iCORE Enterprise Identity Management
                </div>
            </AppLayout>
        </AbsoluteFill>
    );
};
