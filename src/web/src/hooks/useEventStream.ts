import { useEffect, useRef, useState } from "react";

import { streamUrl } from "../api/client";
import type { Anomaly, Candle } from "../api/types";

export type StreamEvent =
  | { type: "candle"; data: Candle }
  | { type: "anomaly"; data: Anomaly };

export type StreamStatus = "connecting" | "live" | "down";

export function useEventStream(onEvent: (event: StreamEvent) => void): StreamStatus {
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const handlerRef = useRef(onEvent);
  handlerRef.current = onEvent;

  useEffect(() => {
    const source = new EventSource(streamUrl());

    source.onopen = () => setStatus("live");
    source.onerror = () => setStatus("down");

    const onCandle = (event: MessageEvent<string>) => {
      try {
        handlerRef.current({ type: "candle", data: JSON.parse(event.data) as Candle });
      } catch {
        /* ignore malformed frame */
      }
    };
    const onAnomaly = (event: MessageEvent<string>) => {
      try {
        handlerRef.current({ type: "anomaly", data: JSON.parse(event.data) as Anomaly });
      } catch {
        /* ignore malformed frame */
      }
    };

    source.addEventListener("candle", onCandle as EventListener);
    source.addEventListener("anomaly", onAnomaly as EventListener);

    return () => {
      source.removeEventListener("candle", onCandle as EventListener);
      source.removeEventListener("anomaly", onAnomaly as EventListener);
      source.close();
    };
  }, []);

  return status;
}
