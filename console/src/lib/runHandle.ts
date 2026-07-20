/**
 * The Run handle's timestamp fallback chain: explicit `scheduled_at`, else
 * the derived `triggered_at`, else the first transition's timestamp — so
 * every instance has a handle from the moment it exists (a signal-triggered
 * instance with no schedule still resolves one).
 */
export function runHandleSource(snapshot: {
  scheduled_at?: string | null;
  triggered_at?: string | null;
  transitions?: Array<{ at: string }>;
}): string | null {
  return snapshot.scheduled_at ?? snapshot.triggered_at ?? snapshot.transitions?.[0]?.at ?? null;
}

/**
 * The Run handle: a workflow instance's display identity. An absolute
 * timestamp with explicit UTC offset (`YYYY-MM-DD HH:MM:SS +HH:MM`) in the
 * viewer's local timezone — instances are told apart by when they fired,
 * never by a run counter (none exists in the substrate) and never by a
 * truncated hex id.
 */
export function formatRunHandle(iso: string | null | undefined): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  const pad = (n: number) => String(n).padStart(2, '0');
  const offsetMin = -d.getTimezoneOffset();
  const sign = offsetMin >= 0 ? '+' : '-';
  const abs = Math.abs(offsetMin);
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
    `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())} ` +
    `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
  );
}
