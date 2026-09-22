export interface Candle {
  symbol: string;
  window_start: string;
  window_end: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  buy_volume: number;
  sell_volume: number;
  vwap: number;
  trade_count: number;
}

export interface Anomaly {
  symbol: string;
  window_start: string;
  window_end: string;
  metric: string;
  value: number;
  mean: number;
  stddev: number;
  zscore: number;
  detected_at: string | null;
}

export interface SymbolSummary {
  symbol: string;
  window_start: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  trade_count: number;
}
