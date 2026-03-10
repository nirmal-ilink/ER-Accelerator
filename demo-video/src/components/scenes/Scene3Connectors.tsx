import React from "react";
import { AbsoluteFill, spring, useCurrentFrame, useVideoConfig, staticFile } from "remotion";
import { AppLayout } from "../layout/AppLayout";

const RED = "#D11F41";
const SLATE = "#0F172A";
const SLATE_5 = "#64748B";
const SLATE_4 = "#94a3b8";
const BORDER = "#e2e8f0";

const CONNECTOR_LOGOS: Record<string, string> = {
    "SQL Server": staticFile("sqlserver_logo.png"),
    "Snowflake": staticFile("snowflake_logo.png"),
    "Databricks": staticFile("databricks_logo.png"),
    "Oracle DB": staticFile("oracle_logo.png"),
    "SAP HANA": staticFile("sap_logo.png"),
    "Delta Lake": staticFile("deltalake_logo.png"),
    "Microsoft Fabric": staticFile("fabric_logo.png"),
};

const CONNECTORS = ["SQL Server", "Snowflake", "Databricks", "Oracle DB", "SAP HANA", "Delta Lake", "Microsoft Fabric"];

export const Scene3Connectors: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    // Phase: 0-25 header, 26-70 dropdown open + selection, 71-85 dropdown closes, 86+ form shows
    const headerEnter = spring({ frame, fps, config: { damping: 22, stiffness: 100 } });
    const dropdownOpenAmt = frame >= 26 && frame <= 85
        ? Math.min(1, (frame - 26) / 12)
        : frame > 85 ? Math.max(0, 1 - (frame - 85) / 10) : 0;
    const dropdownIsOpen = dropdownOpenAmt > 0.01;

    // Highlight scrolls: SQL Server first, then jumps to Microsoft Fabric at frame 60
    const highlightIdx = frame < 60 ? 0 : 6;
    const selectedLabel = frame < 86 ? "SQL Server" : "Microsoft Fabric";
    const formEnter = spring({ frame: Math.max(0, frame - 95), fps, config: { damping: 20 } });

    return (
        <AbsoluteFill style={{ width: 1920, height: 1080 }}>
            <AppLayout activeNav="Connectors">
                {/* Header */}
                <div style={{ marginBottom: 28, opacity: headerEnter, transform: `translateY(${(1 - headerEnter) * 10}px)` }}>
                    <h1 style={{ fontSize: 30, fontWeight: 800, color: SLATE, margin: "0 0 4px", letterSpacing: "-0.02em" }}>Data Connectors</h1>
                    <p style={{ fontSize: 13, color: SLATE_5, margin: 0 }}>Configure and save your enterprise data source connections.</p>
                </div>

                {/* Dropdown — zIndex keeps it above everything */}
                <div style={{ position: "relative", zIndex: 200, maxWidth: 500, marginBottom: dropdownIsOpen ? 0 : 24, opacity: headerEnter }}>
                    <div style={{
                        height: 44, border: `1px solid ${dropdownIsOpen ? RED : BORDER}`,
                        borderRadius: dropdownIsOpen ? "8px 8px 0 0" : 8,
                        padding: "0 16px", background: "white",
                        display: "flex", alignItems: "center", justifyContent: "space-between",
                        fontSize: 14, fontWeight: 500, color: SLATE,
                    }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                            <img src={CONNECTOR_LOGOS[selectedLabel]} alt={selectedLabel} style={{ width: 20, height: 20, objectFit: "contain" }} />
                            {selectedLabel}
                        </div>
                        <span style={{ color: "#9ca3af", fontSize: 11 }}>▾</span>
                    </div>

                    {dropdownIsOpen && (
                        <div style={{
                            position: "absolute", top: 44, left: 0, width: "100%", zIndex: 300,
                            background: "white", borderRadius: "0 0 10px 10px",
                            border: `1px solid ${BORDER}`, borderTop: "none",
                            boxShadow: "0 12px 32px rgba(0,0,0,0.14)",
                            opacity: dropdownOpenAmt,
                        }}>
                            <div style={{ padding: 6 }}>
                                {CONNECTORS.map((c, i) => {
                                    const isHl = i === highlightIdx;
                                    return (
                                        <div key={i} style={{
                                            display: "flex", alignItems: "center", gap: 12,
                                            padding: "11px 14px", borderRadius: 7,
                                            background: isHl ? "#fef2f2" : "white",
                                            fontSize: 14, fontWeight: isHl ? 600 : 400,
                                            color: isHl ? RED : "#374151",
                                        }}>
                                            <img src={CONNECTOR_LOGOS[c]} alt={c} style={{ width: 22, height: 22, objectFit: "contain" }} />
                                            {c}
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    )}
                </div>

                {/* Form — only renders after dropdown closed */}
                {!dropdownIsOpen && (
                    <div style={{ opacity: formEnter, transform: `translateY(${(1 - formEnter) * 16}px)` }}>
                        {/* Connector name + logo */}
                        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
                            <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
                                <img src={CONNECTOR_LOGOS["Microsoft Fabric"]} alt="Fabric" style={{ width: 44, height: 44, objectFit: "contain" }} />
                                <div>
                                    <div style={{ fontSize: 18, fontWeight: 700, color: SLATE }}>Microsoft Fabric</div>
                                    <div style={{ fontSize: 12, color: SLATE_5 }}>Data Warehouse (Warehouse)</div>
                                </div>
                            </div>
                            <div style={{ fontSize: 12, color: SLATE_4 }}>● Inactive</div>
                        </div>

                        <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 6 }}>STEP 1: CONNECTION CONFIGURATION</div>
                        <div style={{ fontSize: 11, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 8 }}>CONNECTION NAME</div>
                        <div style={{ height: 48, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 16px", display: "flex", alignItems: "center", fontSize: 14, color: SLATE_4, background: "white", marginBottom: 20 }}>
                            e.g. Production SQL, Dev Warehouse
                        </div>

                        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
                            {[
                                { label: "TENANT ID", pw: false }, { label: "CLIENT ID", pw: false },
                                { label: "WORKSPACE ID", pw: false }, { label: "CLIENT SECRET", pw: true },
                            ].map((f, i) => (
                                <div key={i}>
                                    <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>{f.label}</div>
                                    <div style={{ height: 44, border: `1px solid ${BORDER}`, borderRadius: 6, padding: "0 14px", display: "flex", alignItems: "center", justifyContent: "space-between", background: "white" }}>
                                        {f.pw && <span style={{ letterSpacing: 3, color: SLATE }}>••••••••••</span>}
                                        {f.pw && <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" /><circle cx="12" cy="12" r="3" /></svg>}
                                    </div>
                                </div>
                            ))}
                        </div>

                        <div style={{ marginBottom: 36 }}>
                            <div style={{ fontSize: 10, fontWeight: 700, color: SLATE_5, textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 6 }}>TABLE NAME</div>
                            <div style={{ height: 44, border: `1px solid ${BORDER}`, borderRadius: 6, background: "white" }} />
                        </div>

                        <div style={{ display: "flex", justifyContent: "space-between" }}>
                            <div style={{ width: "30%", height: 48, background: "white", border: `1px solid ${BORDER}`, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, fontWeight: 500, color: SLATE }}>Test Connection</div>
                            <div style={{ width: "30%", height: 48, background: RED, borderRadius: 8, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14, fontWeight: 700, color: "white", boxShadow: "0 4px 14px rgba(209,31,65,0.35)" }}>Save Connection</div>
                        </div>
                    </div>
                )}
            </AppLayout>
        </AbsoluteFill>
    );
};
