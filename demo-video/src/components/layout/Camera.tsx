import React from "react";
import { useCurrentFrame, useVideoConfig, spring, interpolate } from "remotion";

interface CameraProps {
    children: React.ReactNode;
    targetScale: number;
    targetX: number;
    targetY: number;
    startZoomFrame?: number;
    endZoomFrame?: number;
}

export const Camera: React.FC<CameraProps> = ({
    children,
    targetScale = 2.5,
    targetX = 0,
    targetY = 0,
    startZoomFrame = 45, // Wait 1.5 seconds before zooming in
    endZoomFrame = 210   // Start zooming out at 7 seconds
}) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    // Create smooth, non-bouncy spring for zooming in
    const zoomInProgress = spring({
        frame: Math.max(0, frame - startZoomFrame),
        fps,
        config: {
            damping: 100, // Very high damping to prevent bounce
            stiffness: 100, // Slow, highly controlled movement
            mass: 2
        }
    });

    // Create smooth spring for zooming back out
    const zoomOutProgress = spring({
        frame: Math.max(0, frame - endZoomFrame),
        fps,
        config: {
            damping: 100,
            stiffness: 100,
            mass: 2
        }
    });

    // Calculate current scale: 1 -> targetScale -> 1
    const currentScale = interpolate(
        zoomInProgress - zoomOutProgress,
        [0, 1],
        [1, targetScale],
        { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
    );

    // Calculate X and Y pan concurrently with zoom
    const currentX = interpolate(
        zoomInProgress - zoomOutProgress,
        [0, 1],
        [0, targetX],
        { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
    );

    const currentY = interpolate(
        zoomInProgress - zoomOutProgress,
        [0, 1],
        [0, targetY],
        { extrapolateLeft: "clamp", extrapolateRight: "clamp" }
    );

    const containerStyle: React.CSSProperties = {
        width: "100%",
        height: "100%",
        position: "absolute",
        left: 0,
        top: 0,
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        transformOrigin: "center center",
        transform: `scale(${currentScale}) translate(${currentX}px, ${currentY}px)`,
        /* 
          We use will-change for performance since we are animating highly complex DOM trees.
        */
        willChange: "transform"
    };

    return (
        <div style={containerStyle}>
            {children}
        </div>
    );
};
