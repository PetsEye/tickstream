import type { Anomaly, Candle, SymbolSummary } from "./types";

const API_BASE: string = import.meta.env.VITE_API_BASE ?? "";

async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return (await response.json()) as T;
}

export const api = {
  symbols: () => getJson<string[]>("/api/symbols"),
  summary: () => getJson<SymbolSummary[]>("/api/summary"),
  candles: (symbol: string, limit = 300) =>
    getJson<Candle[]>(`/api/candles?symbol=${encodeURIComponent(symbol)}&limit=${limit}`),
  anomalies: (limit = 50) => getJson<Anomaly[]>(`/api/anomalies?limit=${limit}`),
};

export const streamUrl = (): string => `${API_BASE}/api/stream`;
