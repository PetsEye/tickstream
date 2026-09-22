import {
  createChart,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useRef } from "react";

import type { Candle } from "../api/types";

const UP = "#0ecb81";
const DOWN = "#f6465d";
const VWAP = "#f0b90b";

function toTime(iso: string): UTCTimestamp {
  return Math.floor(new Date(iso).getTime() / 1000) as UTCTimestamp;
}

export default function CandleChart({ candles }: { candles: Candle[] }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const vwapSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const chart: IChartApi = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "transparent" },
        textColor: "#8b98a5",
        fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
      },
      grid: {
        vertLines: { color: "rgba(255, 255, 255, 0.04)" },
        horzLines: { color: "rgba(255, 255, 255, 0.04)" },
      },
      rightPriceScale: { borderColor: "rgba(255, 255, 255, 0.08)" },
      timeScale: {
        borderColor: "rgba(255, 255, 255, 0.08)",
        timeVisible: true,
        secondsVisible: false,
      },
    });

    candleSeriesRef.current = chart.addCandlestickSeries({
      upColor: UP,
      downColor: DOWN,
      borderUpColor: UP,
      borderDownColor: DOWN,
      wickUpColor: UP,
      wickDownColor: DOWN,
    });
    vwapSeriesRef.current = chart.addLineSeries({
      color: VWAP,
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    volumeSeriesRef.current = chart.addHistogramSeries({
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });

    chartRef.current = chart;

    return () => {
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      vwapSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, []);

  useEffect(() => {
    const candleSeries = candleSeriesRef.current;
    const vwapSeries = vwapSeriesRef.current;
    const volumeSeries = volumeSeriesRef.current;
    if (!candleSeries || !vwapSeries || !volumeSeries) return;

    candleSeries.setData(
      candles.map((candle) => ({
        time: toTime(candle.window_start),
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
      })),
    );
    vwapSeries.setData(
      candles.map((candle) => ({ time: toTime(candle.window_start), value: candle.vwap })),
    );
    volumeSeries.setData(
      candles.map((candle) => ({
        time: toTime(candle.window_start),
        value: candle.volume,
        color: candle.close >= candle.open ? "rgba(14, 203, 129, 0.35)" : "rgba(246, 70, 93, 0.35)",
      })),
    );
    chartRef.current?.timeScale().fitContent();
  }, [candles]);

  return <div className="chart" ref={containerRef} />;
}
