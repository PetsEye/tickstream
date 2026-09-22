import type { SymbolSummary } from "../api/types";
import { formatCount, formatPrice, formatQuantity } from "../lib/format";

interface Props {
  summary: SymbolSummary[];
  selected: string;
  onSelect: (symbol: string) => void;
}

export default function Watchlist({ summary, selected, onSelect }: Props) {
  return (
    <section className="panel">
      <header className="panel-header">
        <h2>Watchlist</h2>
        <span className="panel-sub">latest 1m window</span>
      </header>
      <table className="watchlist">
        <thead>
          <tr>
            <th>Symbol</th>
            <th className="num">Close</th>
            <th className="num">VWAP</th>
            <th className="num">Vol</th>
            <th className="num">Trades</th>
          </tr>
        </thead>
        <tbody>
          {summary.map((item) => (
            <tr
              key={item.symbol}
              className={selected === item.symbol ? "active" : ""}
              onClick={() => onSelect(item.symbol)}
            >
              <td>{item.symbol}</td>
              <td className="num">{formatPrice(item.close)}</td>
              <td className="num muted">{formatPrice(item.vwap)}</td>
              <td className="num muted">{formatQuantity(item.volume)}</td>
              <td className="num muted">{formatCount(item.trade_count)}</td>
            </tr>
          ))}
          {summary.length === 0 && (
            <tr>
              <td colSpan={5} className="empty">
                no data yet
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </section>
  );
}
