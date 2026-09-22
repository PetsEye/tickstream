import type { Anomaly } from "../api/types";
import { formatCount, formatPrice, formatTime, formatZscore } from "../lib/format";

export default function AnomalyFeed({ anomalies }: { anomalies: Anomaly[] }) {
  return (
    <section className="panel">
      <header className="panel-header">
        <h2>Anomalies</h2>
        <span className="panel-sub">notional z-score ≥ threshold</span>
      </header>
      <ul className="anomalies">
        {anomalies.map((anomaly) => (
          <li key={`${anomaly.symbol}-${anomaly.window_start}-${anomaly.metric}`}>
            <div className="anomaly-top">
              <span className="anomaly-symbol">{anomaly.symbol}</span>
              <span className="anomaly-z">z {formatZscore(anomaly.zscore)}</span>
            </div>
            <div className="anomaly-bottom">
              <span>{formatTime(anomaly.window_start)}</span>
              <span className="muted">
                trade {formatPrice(anomaly.value)} vs mean {formatPrice(anomaly.mean)}
              </span>
            </div>
          </li>
        ))}
        {anomalies.length === 0 && <li className="empty">no anomalies detected yet</li>}
      </ul>
      {anomalies.length > 0 && (
        <footer className="panel-footer muted">
          showing {formatCount(anomalies.length)} most recent
        </footer>
      )}
    </section>
  );
}
