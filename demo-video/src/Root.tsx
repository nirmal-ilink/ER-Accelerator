import { Composition } from "remotion";
import { MainVideo, TOTAL_FRAMES } from "./MainVideo";

export const RemotionRoot: React.FC = () => {
    return (
        <>
            <Composition
                id="DemoVideo"
                component={MainVideo}
                durationInFrames={TOTAL_FRAMES}
                fps={30}
                width={1920}
                height={1080}
            />
        </>
    );
};
