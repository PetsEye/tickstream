import type { SymbolSummary } from "../api/types";
import { formatPct, formatPrice } from "../lib/format";

interface Props {
  summary: SymbolSummary[];
  selected: string;
  onSelect: (symbol: string) => void;
}

export default function TickerTape({ summary, selected, onSelect }: Props) {
  if (summary.length === 0) {
    return <div className="ticker-tape empty">waiting for the first candles…</div>;
  }

  return (
    <div className="ticker-tape">
      {summary.map((item) => {
        const change = item.open ? ((item.close - item.open) / item.open) * 100 : 0;
        const direction = change >= 0 ? "up" : "down";
        return (
          <button
            key={item.symbol}
            type="button"
            className={`ticker ${selected === item.symbol ? "active" : ""}`}
            onClick={() => onSelect(item.symbol)}
          >
            <span className="ticker-symbol">{item.symbol}</span>
            <span className="ticker-price">{formatPrice(item.close)}</span>
            <span className={`ticker-change ${direction}`}>{formatPct(change)}</span>
          </button>
        );
      })}
    </div>
  );
}
