/** Pure formatting helpers (unit-tested). */
export function bytes(n: number, locale = 'en-CA'): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let v = n;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i += 1;
  }
  return `${new Intl.NumberFormat(locale, { maximumFractionDigits: i === 0 ? 0 : 1 }).format(v)} ${units[i]}`;
}

export function duration(seconds: number | null): string {
  if (seconds === null || Number.isNaN(seconds)) return '—';
  const s = Math.round(seconds);
  if (s < 60) return `${s} s`;
  if (s < 3600) return `${Math.floor(s / 60)} min ${s % 60} s`;
  const h = Math.floor(s / 3600);
  return h < 48 ? `${h} h ${Math.floor((s % 3600) / 60)} min` : `${Math.floor(h / 24)} d`;
}

export function dateTime(iso: string | null, locale = 'en-CA'): string {
  if (!iso) return '—';
  return new Intl.DateTimeFormat(locale, { dateStyle: 'short', timeStyle: 'medium', timeZone: 'UTC' }).format(new Date(iso)) + ' UTC';
}

export function pct(ratio: number, locale = 'en-CA'): string {
  return new Intl.NumberFormat(locale, { style: 'percent', maximumFractionDigits: 1 }).format(ratio);
}

export function daysUntil(iso: string, now: Date = new Date()): number {
  return Math.floor((new Date(iso).getTime() - now.getTime()) / 86_400_000);
}

/** Audit export to CSV (RFC 4180 quoting). */
export function toCsv(rows: Record<string, unknown>[], columns: string[]): string {
  const cell = (v: unknown): string => {
    const s = v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  return [columns.join(','), ...rows.map((r) => columns.map((c) => cell(r[c])).join(','))].join('\r\n') + '\r\n';
}

export function download(filename: string, content: string, mime: string): void {
  const url = URL.createObjectURL(new Blob([content], { type: mime }));
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
