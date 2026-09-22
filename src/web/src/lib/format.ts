const priceFormatters = new Map<number, Intl.NumberFormat>();

function priceFormatter(value: number): Intl.NumberFormat {
  const decimals = value >= 1 ? 2 : 6;
  let formatter = priceFormatters.get(decimals);
  if (!formatter) {
    formatter = new Intl.NumberFormat("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: decimals,
    });
    priceFormatters.set(decimals, formatter);
  }
  return formatter;
}

export function formatPrice(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return priceFormatter(value).format(value);
}

export function formatQuantity(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 4 }).format(value);
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatPct(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return `${value >= 0 ? "+" : ""}${value.toFixed(2)}%`;
}

export function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString("en-US", { hour12: false });
}

export function formatZscore(value: number): string {
  return value.toFixed(2);
}
