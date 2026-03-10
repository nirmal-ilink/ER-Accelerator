import React from "react";
import { useCurrentFrame, useVideoConfig, spring, interpolate } from "remotion";

export const FadeInUp: React.FC<{ children: React.ReactNode; delay?: number }> = ({ children, delay = 0 }) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const animationProgress = spring({
        frame: frame - delay,
        fps,
        config: {
            damping: 14,
            mass: 0.5,
        },
    });

    const translateY = interpolate(animationProgress, [0, 1], [40, 0]);
    const opacity = interpolate(animationProgress, [0, 0.5, 1], [0, 1, 1]);

    return (
        <div style={{ opacity, transform: `translateY(${translateY}px)` }}>
            {children}
        </div>
    );
};
