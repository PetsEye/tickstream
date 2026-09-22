import { useCallback, useEffect, useRef, useState } from "react";

import { api } from "./api/client";
import type { Anomaly, Candle, SymbolSummary } from "./api/types";
import AnomalyFeed from "./components/AnomalyFeed";
import CandleChart from "./components/CandleChart";
import StatusBadge from "./components/StatusBadge";
import TickerTape from "./components/TickerTape";
import Watchlist from "./components/Watchlist";
import { useEventStream, type StreamEvent } from "./hooks/useEventStream";
import { formatCount, formatPct, formatPrice } from "./lib/format";

const MAX_CANDLES = 300;
const MAX_ANOMALIES = 50;

const timeKey = (iso: string): number => Math.floor(new Date(iso).getTime() / 1000);

function upsertSummary(prev: SymbolSummary[], candle: Candle): SymbolSummary[] {
  const next: SymbolSummary = {
    symbol: candle.symbol,
    window_start: candle.window_start,
    open: candle.open,
    high: candle.high,
    low: candle.low,
    close: candle.close,
    volume: candle.volume,
    vwap: candle.vwap,
    trade_count: candle.trade_count,
  };
  const index = prev.findIndex((item) => item.symbol === candle.symbol);
  if (index === -1) {
    return [...prev, next].sort((a, b) => a.symbol.localeCompare(b.symbol));
  }
  const copy = prev.slice();
  copy[index] = next;
  return copy;
}

function mergeCandle(prev: Candle[], candle: Candle): Candle[] {
  const key = timeKey(candle.window_start);
  const index = prev.findIndex((item) => timeKey(item.window_start) === key);
  if (index === -1) {
    return [...prev, candle].slice(-MAX_CANDLES);
  }
  const copy = prev.slice();
  copy[index] = candle;
  return copy;
}

export default function App() {
  const [summary, setSummary] = useState<SymbolSummary[]>([]);
  const [symbols, setSymbols] = useState<string[]>([]);
  const [selected, setSelected] = useState("");
  const [candles, setCandles] = useState<Candle[]>([]);
  const [anomalies, setAnomalies] = useState<Anomaly[]>([]);
  const selectedRef = useRef(selected);
  selectedRef.current = selected;

  useEffect(() => {
    let cancelled = false;
    async function load() {
      const [loadedSymbols, loadedSummary, loadedAnomalies] = await Promise.all([
        api.symbols(),
        api.summary(),
        api.anomalies(MAX_ANOMALIES),
      ]);
      if (cancelled) return;
      setSymbols(loadedSymbols);
      setSummary(loadedSummary);
      setAnomalies(loadedAnomalies);
      setSelected((current) => current || loadedSymbols[0] || "");
    }
    load().catch((error) => console.error("initial load failed", error));
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!selected) return;
    let cancelled = false;
    api
      .candles(selected, MAX_CANDLES)
      .then((data) => {
        if (!cancelled) setCandles(data);
      })
      .catch((error) => console.error("candle load failed", error));
    return () => {
      cancelled = true;
    };
  }, [selected]);

  const handleEvent = useCallback((event: StreamEvent) => {
    if (event.type === "candle") {
      const candle = event.data;
      setSummary((prev) => upsertSummary(prev, candle));
      setSymbols((prev) =>
        prev.includes(candle.symbol) ? prev : [...prev, candle.symbol].sort(),
      );
      if (candle.symbol === selectedRef.current) {
        setCandles((prev) => mergeCandle(prev, candle));
      }
    } else {
      setAnomalies((prev) => [event.data, ...prev].slice(0, MAX_ANOMALIES));
    }
  }, []);

  const status = useEventStream(handleEvent);

  const active = summary.find((item) => item.symbol === selected);
  const change = active && active.open ? ((active.close - active.open) / active.open) * 100 : 0;

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <span className="brand-mark">▮</span>
          <h1>tickstream</h1>
          <span className="brand-sub">Kafka → Spark → TimescaleDB</span>
        </div>
        <StatusBadge status={status} />
      </header>

      <TickerTape summary={summary} selected={selected} onSelect={setSelected} />

      <main className="main">
        <section className="panel chart-panel">
          <header className="panel-header">
            <div>
              <h2>{selected || "—"}</h2>
              <span className="panel-sub">
                {active
                  ? `${formatPrice(active.close)} · vwap ${formatPrice(active.vwap)}`
                  : "waiting for data"}
              </span>
            </div>
            <div className="chart-stats">
              <div className={`big ${change >= 0 ? "up" : "down"}`}>{formatPct(change)}</div>
              <div className="muted">
                {active ? `${formatCount(active.trade_count)} trades / window` : ""}
              </div>
            </div>
          </header>
          <CandleChart candles={candles} />
          <footer className="legend">
            <span className="legend-item">
              <i className="swatch up" /> close
            </span>
            <span className="legend-item">
              <i className="swatch vwap" /> vwap
            </span>
            <span className="legend-item">
              <i className="swatch vol" /> volume
            </span>
            <span className="muted">{symbols.length} symbols</span>
          </footer>
        </section>

        <aside className="sidebar">
          <Watchlist summary={summary} selected={selected} onSelect={setSelected} />
          <AnomalyFeed anomalies={anomalies} />
        </aside>
      </main>
    </div>
  );
}
