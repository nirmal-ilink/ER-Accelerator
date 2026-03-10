import React from "react";
import { AbsoluteFill, Sequence, spring, useCurrentFrame, useVideoConfig } from "remotion";
import { Scene1Login } from "./components/scenes/Scene1Login";
import { Scene2Dashboard } from "./components/scenes/Scene2Dashboard";
import { Scene3Connectors } from "./components/scenes/Scene3Connectors";
import { Scene4Inspector } from "./components/scenes/Scene4Inspector";
import { Scene5MatchReview } from "./components/scenes/Scene5MatchReview";
import { Scene7Audit } from "./components/scenes/Scene7Audit";
import { Scene8UserMgmt } from "./components/scenes/Scene8UserMgmt";

const FPS = 30;

// Scene durations in seconds → frames
const SCENES = [
    { dur: 6 * FPS },   // 180 — Login
    { dur: 10 * FPS },  // 300 — Dashboard
    { dur: 9 * FPS },   // 270 — Connectors
    { dur: 21 * FPS },  // 630 — Pipeline Inspector (6 stages × 3.5s each)
    { dur: 12 * FPS },  // 360 — Match Review (comparison + golden record)
    { dur: 7 * FPS },   // 210 — Audit Log
    { dur: 6 * FPS },   // 180 — User Management
];

// Compute cumulative start frames
const starts = SCENES.reduce<number[]>((acc, _s, i) => {
    acc.push(i === 0 ? 0 : (acc[i - 1] + SCENES[i - 1].dur));
    return acc;
}, []);

const TOTAL = starts[starts.length - 1] + SCENES[SCENES.length - 1].dur;

// Smooth cross-fade wrapper
const CrossFade: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();
    const progress = spring({
        frame,
        fps,
        config: { damping: 22, stiffness: 110 },
    });
    return (
        <AbsoluteFill style={{
            opacity: progress,
            transform: `scale(${0.98 + 0.02 * progress})`,
        }}>
            {children}
        </AbsoluteFill>
    );
};

export const MainVideo: React.FC = () => {
    return (
        <AbsoluteFill style={{ backgroundColor: "#ffffff" }}>
            {/* Scene 1: Login */}
            <Sequence from={starts[0]} durationInFrames={SCENES[0].dur}>
                <CrossFade><Scene1Login /></CrossFade>
            </Sequence>

            {/* Scene 2: Dashboard */}
            <Sequence from={starts[1]} durationInFrames={SCENES[1].dur}>
                <CrossFade><Scene2Dashboard /></CrossFade>
            </Sequence>

            {/* Scene 3: Data Connectors */}
            <Sequence from={starts[2]} durationInFrames={SCENES[2].dur}>
                <CrossFade><Scene3Connectors /></CrossFade>
            </Sequence>

            {/* Scene 4: Pipeline Inspector (cycles through 6 sub-stages) */}
            <Sequence from={starts[3]} durationInFrames={SCENES[3].dur}>
                <CrossFade><Scene4Inspector /></CrossFade>
            </Sequence>

            {/* Scene 5: Match Review (comparison → golden record builder) */}
            <Sequence from={starts[4]} durationInFrames={SCENES[4].dur}>
                <CrossFade><Scene5MatchReview /></CrossFade>
            </Sequence>

            {/* Scene 6: Audit Log */}
            <Sequence from={starts[5]} durationInFrames={SCENES[5].dur}>
                <CrossFade><Scene7Audit /></CrossFade>
            </Sequence>

            {/* Scene 7: User Management */}
            <Sequence from={starts[6]} durationInFrames={SCENES[6].dur}>
                <CrossFade><Scene8UserMgmt /></CrossFade>
            </Sequence>
        </AbsoluteFill>
    );
};

// Export total frames for Root.tsx
export const TOTAL_FRAMES = TOTAL;
