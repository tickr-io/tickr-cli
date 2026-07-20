// The single event-log row renderer. One projection of an event into a row —
// time · category dot · event_type · summary · typed id-chips · relative age —
// shared by the cross-workflow Event log and the per-instance Events sections,
// so no surface grows a parallel row component.

import type { Event as ApiEvent } from '@/api/client';
import { eventToken, type EventLogToken, describeEvent, clockTime, relTime } from '@/lib/events';
import { cn } from '@/lib/utils';

/** Tailwind colour classes per event-log token. The five shared tokens use the
 * same status vocabulary the state palette resolves into, so an outcome event
 * and its state read the same hue; the two event-log-local lifecycle hues
 * (workflow-violet / task-cyan) are the event log's own, no status surface
 * resolves them. */
const TOKEN_DOT: Record<EventLogToken, string> = {
  success: 'bg-success',
  info: 'bg-info',
  warning: 'bg-warning',
  destructive: 'bg-destructive',
  neutral: 'bg-muted-foreground',
  'workflow-lifecycle': 'bg-event-workflow',
  'task-lifecycle': 'bg-event-task',
};
const TOKEN_TEXT: Record<EventLogToken, string> = {
  success: 'text-success',
  info: 'text-info',
  warning: 'text-warning',
  destructive: 'text-destructive',
  neutral: 'text-muted-foreground',
  'workflow-lifecycle': 'text-event-workflow',
  'task-lifecycle': 'text-event-task',
};

export function EventRow({
  ev,
  nowMs,
  isNew,
}: {
  ev: ApiEvent;
  nowMs: number;
  isNew: boolean;
}) {
  const token = eventToken(ev.event_type);
  const { summary, chips } = describeEvent(ev);
  return (
    <div
      className={cn(
        'flex items-center gap-3 border-b border-border/60 px-4 py-2 text-sm',
        isNew && 'animate-in fade-in slide-in-from-top-1 duration-300',
      )}
      data-testid="event-row"
    >
      <span className="w-16 shrink-0 font-mono text-xs tabular-nums text-muted-foreground">
        {clockTime(ev.ts)}
      </span>
      <span className={cn('h-2 w-2 shrink-0 rounded-full', TOKEN_DOT[token])} />
      <span className={cn('w-44 shrink-0 truncate font-mono text-xs', TOKEN_TEXT[token])}>
        {ev.event_type}
      </span>
      <span className="min-w-0 flex-1 truncate">{summary}</span>
      <span className="hidden shrink-0 gap-1.5 lg:flex">
        {chips.map((c, i) => (
          <span
            key={`${c.label}-${i}`}
            className="rounded border border-border bg-muted/40 px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground"
          >
            <span className="mr-1 opacity-60">{c.label}</span>
            {c.value}
          </span>
        ))}
      </span>
      <span className="w-16 shrink-0 text-right text-xs tabular-nums text-muted-foreground">
        {relTime(ev.ts, nowMs)}
      </span>
    </div>
  );
}
