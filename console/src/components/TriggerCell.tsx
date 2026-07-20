import { Calendar, Radio, Zap } from 'lucide-react';
import type { Trigger } from '@/api/client';

/**
 * Renders a workflow's firing trigger honestly, discriminating on the
 * canonical `Trigger` serde shape:
 *   - `{ Cron: "<expr>" }`  → Calendar icon + the cron expression (tabular-nums)
 *   - `{ WaitsOnSignal: _ }` → Radio glyph + "on signal" (muted)
 *   - `"FireNow"`            → Zap icon + "fire-now" (muted)
 *
 * The Radio glyph is the one canonical "signal" icon shared with the signal
 * gate / signal emit / waits-on-signal surfaces. Non-cron triggers never
 * render a fabricated clock time.
 *
 * Shared by the Workflow list page's Trigger column and the Workflow detail
 * page's meta-grid header — one component, two surfaces (DC-0010 / DC-0014).
 */
export function TriggerCell({ trigger }: { trigger: Trigger }) {
  if (typeof trigger === 'string') {
    // "FireNow"
    return (
      <span className="inline-flex items-center gap-1.5 text-muted-foreground">
        <Zap size={14} aria-label="fire-now" />
        fire-now
      </span>
    );
  }
  if ('Cron' in trigger) {
    return (
      <span className="inline-flex items-center gap-1.5">
        <Calendar size={14} aria-label="cron schedule" />
        <span className="tabular-nums">{trigger.Cron}</span>
      </span>
    );
  }
  // WaitsOnSignal
  return (
    <span className="inline-flex items-center gap-1.5 text-muted-foreground">
      <Radio size={14} aria-label="waits on signal" />
      on signal
    </span>
  );
}
