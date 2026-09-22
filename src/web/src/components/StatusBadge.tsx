import type { StreamStatus } from "../hooks/useEventStream";

const LABELS: Record<StreamStatus, string> = {
  connecting: "connecting",
  live: "live",
  down: "reconnecting",
};

export default function StatusBadge({ status }: { status: StreamStatus }) {
  return (
    <span className={`status status-${status}`}>
      <span className="status-dot" />
      {LABELS[status]}
    </span>
  );
}
