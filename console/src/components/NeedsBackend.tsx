import { PlugZap } from 'lucide-react';

interface NeedsBackendProps {
  /** The surface being declared unbuilt, e.g. "DAG view" or "Event log". */
  surface: string;
  /** The exact endpoint / data the backend must expose, e.g. "GET /api/events". */
  endpoint: string;
  /** One line on what the surface needs from that endpoint. Optional. */
  need?: string;
  /** Render flush inside a tab/card (no min-height) instead of as a full-page block. */
  inline?: boolean;
}

/**
 * The single placeholder for any surface the backend doesn't support yet.
 *
 * Product rule: unbuilt surfaces are shown but clearly marked — we name the exact
 * missing endpoint and never fake data to paper over the gap. Every "this needs
 * backend support" state in the app routes through this one component so the
 * honesty rule lives in exactly one place.
 */
export function NeedsBackend({ surface, endpoint, need, inline }: NeedsBackendProps) {
  return (
    <div
      role="note"
      data-needs-backend
      className={
        'flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border bg-muted/30 px-6 text-center ' +
        (inline ? 'py-10' : 'min-h-[16rem] py-12')
      }
    >
      <PlugZap size={22} className="text-muted-foreground" aria-hidden />
      <div className="space-y-1">
        <div className="text-base font-semibold">{surface}</div>
        <p className="t-muted mx-auto max-w-md text-sm text-muted-foreground">
          {need ?? 'This surface needs backend support that isn’t available yet.'}
        </p>
      </div>
      <div className="flex items-center gap-2 text-xs">
        <span className="text-muted-foreground">Needs endpoint</span>
        <code className="rounded bg-muted px-2 py-0.5 font-mono text-foreground">{endpoint}</code>
      </div>
      <p className="max-w-md text-xs text-muted-foreground/80">
        Shown but intentionally unbuilt — Tickr marks missing surfaces honestly and never fakes data.
      </p>
    </div>
  );
}
