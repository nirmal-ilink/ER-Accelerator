import React from "react";
import { AbsoluteFill, staticFile, spring, useCurrentFrame, useVideoConfig } from "remotion";

export const Scene1Login: React.FC = () => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    // Card enter animation
    const cardEnter = spring({ frame, fps, config: { damping: 20, stiffness: 80 } });
    const logoEnter = spring({ frame: Math.max(0, frame - 8), fps, config: { damping: 18 } });
    const fieldsEnter = spring({ frame: Math.max(0, frame - 20), fps, config: { damping: 18 } });

    // Particle positions (static seeds for consistent rendering)
    const particles = [
        { x: 120, y: 80, r: 5, op: 0.6 },
        { x: 1050, y: 200, r: 3, op: 0.4 },
        { x: 900, y: 650, r: 4, op: 0.5 },
        { x: 200, y: 500, r: 3, op: 0.35 },
        { x: 1700, y: 100, r: 6, op: 0.3 },
        { x: 60, y: 700, r: 4, op: 0.4 },
        { x: 1800, y: 800, r: 3, op: 0.5 },
        { x: 700, y: 900, r: 5, op: 0.25 },
        { x: 1400, y: 400, r: 4, op: 0.35 },
        { x: 350, y: 300, r: 3, op: 0.3 },
    ];

    return (
        <AbsoluteFill style={{
            fontFamily: "'Outfit', 'Inter', sans-serif",
            width: 1920, height: 1080,
            background: "#f0f4f8",
            display: "flex", alignItems: "center", justifyContent: "center",
        }}>
            {/* Subtle background particles */}
            <svg style={{ position: "absolute", inset: 0, width: 1920, height: 1080 }}>
                {particles.map((p, i) => (
                    <circle key={i} cx={p.x} cy={p.y} r={p.r} fill="#D11F41" opacity={p.op * 0.5} />
                ))}
                {/* Faint connecting lines */}
                <line x1="120" y1="80" x2="350" y2="300" stroke="#D11F41" strokeWidth="0.5" opacity="0.1" />
                <line x1="1700" y1="100" x2="1400" y2="400" stroke="#D11F41" strokeWidth="0.5" opacity="0.1" />
                <line x1="200" y1="500" x2="60" y2="700" stroke="#D11F41" strokeWidth="0.5" opacity="0.1" />
            </svg>

            {/* White login card — matches screenshot exactly */}
            <div style={{
                width: 480, background: "white",
                borderRadius: 16, padding: "48px 44px 40px",
                boxShadow: "0 20px 60px rgba(0,0,0,0.09), 0 4px 16px rgba(0,0,0,0.06)",
                opacity: cardEnter, transform: `translateY(${(1 - cardEnter) * 24}px)`,
                display: "flex", flexDirection: "column", alignItems: "center",
            }}>
                {/* Logo — centered */}
                <div style={{ opacity: logoEnter, transform: `scale(${0.8 + 0.2 * logoEnter})`, textAlign: "center", marginBottom: 20 }}>
                    <img src={staticFile("app_logo.png")} alt="iCORE" style={{ width: 72, height: 72, objectFit: "contain" }} />
                    <div style={{ fontSize: 28, fontWeight: 800, color: "#0f172a", letterSpacing: "-0.03em", marginTop: 8, lineHeight: 1 }}>iCORE</div>
                    <div style={{ fontSize: 10, fontWeight: 600, color: "#94a3b8", letterSpacing: "0.15em", textTransform: "uppercase", marginTop: 4 }}>
                        Central Operational Resolution Engine
                    </div>
                    {/* Red divider line */}
                    <div style={{ width: 36, height: 2, background: "#D11F41", borderRadius: 2, margin: "12px auto 8px" }} />
                    <div style={{ fontSize: 13, color: "#64748b", fontStyle: "italic" }}>The Core of Business Truth</div>
                </div>

                {/* Form fields */}
                <div style={{ width: "100%", opacity: fieldsEnter, transform: `translateY(${(1 - fieldsEnter) * 16}px)` }}>
                    {/* USERNAME */}
                    <div style={{ marginBottom: 16 }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: "#374151", letterSpacing: "0.06em", textTransform: "uppercase", marginBottom: 6 }}>Username</div>
                        <div style={{
                            height: 48, border: "1px solid #d1d5db", borderRadius: 8,
                            padding: "0 16px", display: "flex", alignItems: "center",
                            fontSize: 14, color: "#9ca3af", background: "white"
                        }}>Enter username</div>
                    </div>

                    {/* PASSWORD */}
                    <div style={{ marginBottom: 24 }}>
                        <div style={{ fontSize: 11, fontWeight: 700, color: "#374151", letterSpacing: "0.06em", textTransform: "uppercase", marginBottom: 6 }}>Password</div>
                        <div style={{
                            height: 48, border: "1px solid #d1d5db", borderRadius: 8,
                            padding: "0 16px", display: "flex", alignItems: "center", justifyContent: "space-between",
                            fontSize: 14, color: "#9ca3af", background: "white"
                        }}>
                            <span>Enter password</span>
                            {/* Eye icon */}
                            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" strokeWidth="2">
                                <path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z" />
                                <circle cx="12" cy="12" r="3" />
                            </svg>
                        </div>
                    </div>

                    {/* Sign In button — red pill, full width */}
                    <div style={{
                        width: "100%", height: 52, background: "#D11F41",
                        borderRadius: 50, display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 16, fontWeight: 700, color: "white",
                        boxShadow: "0 8px 24px rgba(209,31,65,0.35)",
                        letterSpacing: "0.02em",
                    }}>Sign In</div>
                </div>

                {/* Footer */}
                <div style={{ marginTop: 28, textAlign: "center", opacity: fieldsEnter }}>
                    <div style={{ fontSize: 10, fontWeight: 600, color: "#94a3b8", letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>
                        Enterprise Entity Resolution Platform
                    </div>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "center", gap: 8 }}>
                        <span style={{ fontSize: 12, color: "#94a3b8", fontWeight: 500 }}>Powered by</span>
                        <img src={staticFile("ilink_logo.png")} alt="iLink Digital" style={{ height: 18, objectFit: "contain" }} />
                    </div>
                </div>
            </div>
        </AbsoluteFill>
    );
};
