import React from "react";
import { useCurrentFrame, useVideoConfig, spring, interpolate } from "remotion";

export const ScaleIn: React.FC<{ children: React.ReactNode; delay?: number }> = ({ children, delay = 0 }) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const animationProgress = spring({
        frame: frame - delay,
        fps,
        config: {
            damping: 12,
            mass: 0.5,
            stiffness: 100
        },
    });

    const scale = interpolate(animationProgress, [0, 1], [0.8, 1]);
    const opacity = interpolate(animationProgress, [0, 0.5, 1], [0, 1, 1]);

    return (
        <div style={{ opacity, transform: `scale(${scale})` }}>
            {children}
        </div>
    );
};
