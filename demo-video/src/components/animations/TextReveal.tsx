import React from "react";
import { useCurrentFrame, useVideoConfig, spring, interpolate } from "remotion";

export const TextReveal: React.FC<{ text: string; delay?: number }> = ({ text, delay = 0 }) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const words = text.split(" ");

    return (
        <div style={{ display: "flex", flexWrap: "wrap", justifyContent: "center", gap: "10px" }}>
            {words.map((word, i) => {
                const animationProgress = spring({
                    frame: frame - delay - i * 3, // slightly stagger each word
                    fps,
                    config: {
                        damping: 16,
                    },
                });

                const translateY = interpolate(animationProgress, [0, 1], [20, 0]);
                const opacity = interpolate(animationProgress, [0, 1], [0, 1]);

                return (
                    <span
                        key={i}
                        style={{
                            opacity,
                            transform: `translateY(${translateY}px)`,
                            display: "inline-block",
                        }}
                    >
                        {word}
                    </span>
                );
            })}
        </div>
    );
};
