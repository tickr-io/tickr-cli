// The per-instance Events sections — the workflow-instance page's Events tab
// and the task-instance page's Events tab. Both poll the same tenant events
// projection scoped to their id (single fetch on tab open + 5s incremental
// poll on the highest `seq` seen) and render through the shared event-log row
// renderer, so there is no parallel row component.

import { useEffect, useState } from 'react';
import { useInstanceEventTail, useTaskEventTail, EVENT_BUFFER_CAP } from '@/api/hooks';
import type { EventTail } from '@/api/hooks';
import { EventRow } from '@/components/EventRow';
import { QueryError, EmptyState, TableLoading } from '@/components/QueryStates';

/** Presentational tail: loading / error / empty / the newest-first list. */
function EventStream({ tail, emptyHint }: { tail: EventTail; emptyHint: string }) {
  const { events, newSeqs, isLoading, error } = tail;
  const [nowMs, setNowMs] = useState(() => Date.now());

  // Tick relative ages once a second — a pure client tick, not a poll.
  useEffect(() => {
    const id = setInterval(() => setNowMs(Date.now()), 1_000);
    return () => clearInterval(id);
  }, []);

  if (error && events.length === 0) {
    return (
      <div className="p-6">
        <QueryError error={error} />
      </div>
    );
  }
  if (isLoading && events.length === 0) {
    return (
      <div className="p-4">
        <TableLoading rows={6} cols={4} />
      </div>
    );
  }
  if (events.length === 0) {
    return (
      <div className="py-10">
        <EmptyState title="No events yet" description={emptyHint} />
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-card">
      <div className="max-h-[60vh] overflow-y-auto" data-testid="event-stream">
        {events.map((ev) => (
          <EventRow key={ev.seq} ev={ev} nowMs={nowMs} isNew={newSeqs.has(ev.seq)} />
        ))}
      </div>
      <div className="border-t border-border px-4 py-2 text-xs text-muted-foreground">
        Showing {events.length} buffered (cap {EVENT_BUFFER_CAP}) · polling every 5s while open
      </div>
    </div>
  );
}

/** Workflow-instance page Events tab. */
export function InstanceEventsTab({
  instanceId,
  active,
}: {
  instanceId: string | undefined;
  active: boolean;
}) {
  const tail = useInstanceEventTail(instanceId, active);
  return (
    <EventStream
      tail={tail}
      emptyHint="Orchestration events for this instance appear here as it runs."
    />
  );
}

/** Task-instance page Events tab. */
export function TaskEventsTab({
  instanceId,
  taskId,
  active,
}: {
  instanceId: string | undefined;
  taskId: string | undefined;
  active: boolean;
}) {
  const tail = useTaskEventTail(instanceId, taskId, active);
  return (
    <EventStream tail={tail} emptyHint="Events for this task appear here as it runs." />
  );
}
