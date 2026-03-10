import React from "react";
import { AbsoluteFill, staticFile } from "remotion";

const BRAND_RED = "#D11F41";


// ------------------------------------------------------------------
// Nav icon filenames — real PNGs from assets/ copied to public/
// ------------------------------------------------------------------
const NAV_ICONS: Record<string, string> = {
    "Dashboard": staticFile("layout-dashboard.png"),
    "Connectors": staticFile("plug.png"),
    "Pipeline Inspector": staticFile("workflow.png"),
    "Match Review": staticFile("compare.png"),
    "Audit Logs": staticFile("file-text.png"),
    "User Management": staticFile("shield-user.png"),
};

interface AppLayoutProps {
    children: React.ReactNode;
    activeNav?: string;
}

export const AppLayout: React.FC<AppLayoutProps> = ({
    children,
    activeNav = "Dashboard"
}) => {
    const rootStyle: React.CSSProperties = {
        fontFamily: "'Outfit', 'Inter', system-ui, -apple-system, sans-serif",
        backgroundColor: "#ffffff",
        display: "flex",
        flexDirection: "row",
        overflow: "hidden",
        width: 1920,
        height: 1080,
        color: "#0f172a"
    };

    const sidebarStyle: React.CSSProperties = {
        width: 220,
        minWidth: 220,
        backgroundColor: "#f8f9fa",
        borderRight: "1px solid #e9ecef",
        display: "flex",
        flexDirection: "column",
        flexShrink: 0,
        height: 1080,
        boxSizing: "border-box",
        position: "relative",
    };

    const mainAreaStyle: React.CSSProperties = {
        flex: 1,
        display: "flex",
        flexDirection: "column",
        padding: "36px 44px 28px 44px",
        position: "relative",
        height: 1080,
        backgroundColor: "#ffffff",
        overflow: "hidden",
        boxSizing: "border-box"
    };

    const navItems = [
        "Dashboard",
        "Connectors",
        "Pipeline Inspector",
        "Match Review",
        "Audit Logs",
        "User Management",
    ];

    return (
        <AbsoluteFill style={rootStyle}>
            {/* ====== SIDEBAR ====== */}
            <div style={sidebarStyle}>
                {/* Collapse button */}
                <div style={{
                    position: "absolute", top: 16, right: -15, zIndex: 20,
                    width: 28, height: 28, background: "white",
                    border: "1px solid #dee2e6", borderRadius: 8,
                    display: "flex", alignItems: "center", justifyContent: "center",
                    fontSize: 11, color: "#6b7280", fontWeight: 700,
                    boxShadow: "0 2px 6px rgba(0,0,0,0.08)"
                }}>«</div>

                {/* Logo — real app_logo.png */}
                <div style={{ padding: "20px 20px 16px", borderBottom: "1px solid #e9ecef", display: "flex", alignItems: "center", gap: 10 }}>
                    <img
                        src={staticFile("app_logo.png")}
                        alt="iCORE Logo"
                        style={{ width: 44, height: 44, objectFit: "contain" }}
                    />
                    <div>
                        <div style={{ fontSize: 18, fontWeight: 800, color: "#0f172a", letterSpacing: "-0.02em", lineHeight: 1 }}>iCORE</div>
                        <div style={{ fontSize: 9, fontWeight: 700, color: "#94a3b8", letterSpacing: "0.15em", textTransform: "uppercase" }}>iLink Digital</div>
                    </div>
                </div>

                {/* Nav Items */}
                <div style={{ flex: 1, padding: "12px 14px", display: "flex", flexDirection: "column", gap: 2 }}>
                    {navItems.map((name, i) => {
                        const isActive = name === activeNav;
                        const iconSrc = NAV_ICONS[name];
                        return (
                            <div key={i} style={{
                                display: "flex", alignItems: "center", gap: 12,
                                padding: "10px 14px", borderRadius: 10,
                                background: isActive ? "white" : "transparent",
                                border: isActive ? "1px solid #fecdd3" : "1px solid transparent",
                                boxShadow: isActive ? "0 2px 8px rgba(209,31,65,0.12), 0 0 0 1px rgba(209,31,65,0.15)" : "none",
                                cursor: "pointer",
                            }}>
                                {iconSrc && (
                                    <img
                                        src={iconSrc}
                                        alt={name}
                                        style={{
                                            width: 18, height: 18,
                                            objectFit: "contain",
                                            filter: isActive
                                                ? "invert(15%) sepia(90%) saturate(5000%) hue-rotate(335deg) brightness(85%)"
                                                : "invert(60%) sepia(0%) saturate(0%) brightness(90%)",
                                        }}
                                    />
                                )}
                                <span style={{
                                    fontSize: 13.5, fontWeight: isActive ? 700 : 400,
                                    color: isActive ? BRAND_RED : "#374151",
                                }}>
                                    {name}
                                </span>
                            </div>
                        );
                    })}
                </div>

                {/* User + Sign Out */}
                <div style={{ padding: "14px 16px", borderTop: "1px solid #e9ecef" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 12 }}>
                        <div style={{
                            width: 36, height: 36, borderRadius: "50%",
                            background: "#e2e8f0", color: "#475569",
                            display: "flex", alignItems: "center", justifyContent: "center",
                            fontWeight: 700, fontSize: 15, flexShrink: 0
                        }}>A</div>
                        <div>
                            <div style={{ fontSize: 12, fontWeight: 700, color: "#1e293b" }}>Administrator</div>
                            <div style={{ fontSize: 11, color: "#94a3b8" }}>Admin</div>
                        </div>
                    </div>
                    <div style={{
                        width: "100%", height: 42, background: BRAND_RED,
                        borderRadius: 24, display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 13, fontWeight: 700, color: "white", cursor: "pointer",
                        boxShadow: "0 4px 12px rgba(209,31,65,0.3)"
                    }}>Sign Out</div>
                </div>
            </div>

            {/* ====== MAIN CONTENT ====== */}
            <div style={mainAreaStyle}>
                {/* Top-right: Deploy + ⋮ */}
                <div style={{ position: "absolute", top: 14, right: 20, display: "flex", gap: 8, alignItems: "center", zIndex: 5 }}>
                    <div style={{ fontSize: 12, color: "#94a3b8" }}>Deploy</div>
                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none">
                        <circle cx="12" cy="5" r="1.5" fill="#94a3b8" />
                        <circle cx="12" cy="12" r="1.5" fill="#94a3b8" />
                        <circle cx="12" cy="19" r="1.5" fill="#94a3b8" />
                    </svg>
                </div>
                <div style={{ width: "100%", height: "100%", position: "relative" }}>
                    {children}
                </div>
            </div>
        </AbsoluteFill>
    );
};
