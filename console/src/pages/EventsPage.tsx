// Event log — the cross-workflow live tail (DC-0007). Polls
// GET /api/events?after=<seq> every 5s while open, buffers the newest ~200
// rows, and renders each as the real event vocabulary: time · category dot ·
// event_type · summary · typed id-chips · relative age. Non-tenant data
// never reach the data plane, so there is no Cluster filter here.

import { useEffect, useState } from 'react';
import { useEventTail, EVENT_BUFFER_CAP } from '@/api/hooks';
import { FILTERS, type EventFilter, matchesFilter } from '@/lib/events';
import { EventRow } from '@/components/EventRow';
import { Button } from '@/components/ui/button';
import { QueryError, EmptyState, TableLoading } from '@/components/QueryStates';
import { cn } from '@/lib/utils';

export function EventsPage() {
  const [paused, setPaused] = useState(false);
  const [filter, setFilter] = useState<EventFilter>('All');
  const [nowMs, setNowMs] = useState(() => Date.now());
  const { events, newSeqs, isLoading, error } = useEventTail(!paused);

  // Tick relative ages once a second — a pure client tick, not a poll.
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1_000);
    return () => clearInterval(id);
  }, []);

  const shown = events.filter((e) => matchesFilter(e.event_type, filter));

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Event log</h1>
        <p className="text-sm text-muted-foreground">
          Cross-workflow event stream — the newest-first activity feed for the whole system.
        </p>
      </div>

      <div className="rounded-lg border border-border bg-card">
        <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-2.5">
          <span
            className={cn(
              'flex items-center gap-2 text-xs',
              paused ? 'text-muted-foreground' : 'text-success',
            )}
          >
            <span
              className={cn(
                'h-2 w-2 rounded-full',
                paused ? 'bg-muted-foreground' : 'animate-pulse bg-success',
              )}
            />
            {paused ? 'Paused' : 'Live'} · polling /api/events every 5s
          </span>
          <div className="ml-auto flex items-center gap-1">
            {FILTERS.map((f) => (
              <button
                key={f}
                onClick={() => setFilter(f)}
                className={cn(
                  'rounded px-2.5 py-1 text-xs transition-colors',
                  filter === f
                    ? 'bg-secondary font-medium text-secondary-foreground'
                    : 'text-muted-foreground hover:text-foreground',
                )}
              >
                {f}
              </button>
            ))}
          </div>
          <Button variant="outline" size="sm" onClick={() => setPaused((p) => !p)}>
            {paused ? 'Resume' : 'Pause'}
          </Button>
        </div>

        {error && events.length === 0 ? (
          <div className="p-6">
            <QueryError error={error} />
          </div>
        ) : isLoading ? (
          <div className="p-4">
            <TableLoading rows={8} cols={5} />
          </div>
        ) : shown.length === 0 ? (
          <div className="py-10">
            <EmptyState
              title={events.length === 0 ? 'No events yet' : 'No events match'}
              description={
                events.length === 0
                  ? 'Events appear here as workflows run.'
                  : 'Try a different filter.'
              }
            />
          </div>
        ) : (
          <div className="max-h-[65vh] overflow-y-auto" data-testid="event-stream">
            {shown.map((ev) => (
              <EventRow key={ev.seq} ev={ev} nowMs={nowMs} isNew={newSeqs.has(ev.seq)} />
            ))}
          </div>
        )}

        <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-2 text-xs text-muted-foreground">
          <span>
            Showing {shown.length} of {events.length} buffered (cap {EVENT_BUFFER_CAP})
          </span>
          <span>
            Events arrive within ~10–17s of occurring (5s archive sweep + 2s stability watermark +
            5s pull + 5s poll) · history is durable
          </span>
        </div>
      </div>
    </div>
  );
}
