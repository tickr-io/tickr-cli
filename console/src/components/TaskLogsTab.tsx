import { useCallback, useEffect, useRef, useState } from 'react';
import { ArrowDown, Download } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { api, unwrap, type SnapshotTaskInstance, type TaskLogPage } from '@/api/client';
import { useTaskLogs } from '@/api/hooks';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import {
  isTerminalTaskState,
  isAbnormalEnd,
  markerLine,
  boundedTail,
  ABNORMAL_END_LINE,
} from '@/lib/taskSlice';

/** Poll cadence for the live tail. */
const POLL_MS = 2_000;
/** Batches per tail-first load and per "load earlier" page. */
const TAIL_BATCHES = 200;
/** Bounded viewport: lines rendered at most; the banner reports the rest. */
const MAX_LINES = 5_000;
/** Client-side buffer cap — a chatty long tail trims from the front and the
 * trimmed content stays reachable via "load earlier" / download. */
const MAX_BUFFER_BATCHES = 4_000;

interface Batch {
  seq: number;
  text: string;
}

interface MarkerInfo {
  exitStatus?: number;
  exitReason?: string;
}

const LOGS_PATH = '/api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_instance_id}/logs' as const;

function downloadHref(workflowId: string, instanceId: string, taskInstanceId: string): string {
  return `/api/workflows/${workflowId}/instances/${instanceId}/tasks/${taskInstanceId}/logs?download=true`;
}

/** The marker / abnormal-end terminal line, styled apart from log text. */
function StreamEndLine({
  markerPresent,
  marker,
  taskState,
}: {
  markerPresent: boolean;
  marker: MarkerInfo;
  taskState: string;
}) {
  if (markerPresent) {
    const ok = (marker.exitStatus ?? -1) === 0;
    return (
      <div
        className={`mt-1 border-t border-border pt-1 font-mono text-xs ${ok ? 'text-muted-foreground' : 'text-destructive'}`}
      >
        {markerLine(marker.exitStatus, marker.exitReason)}
      </div>
    );
  }
  if (isAbnormalEnd(taskState, false)) {
    return (
      <div className="mt-1 border-t border-border pt-1 font-mono text-xs text-destructive">
        {ABNORMAL_END_LINE}
      </div>
    );
  }
  return null;
}

/**
 * Live tail of a non-terminal Attempt: tail-first load, then a 2s cursor
 * poll appending only new batches. The tail ends naturally — the
 * End-of-stream marker (or task-terminal with no further batches) stops the
 * poll with no extra fetch. Follow mode auto-scrolls like a terminal,
 * pausing when the operator scrolls up.
 */
function LiveLogStream({
  workflowId,
  instanceId,
  taskInstance,
}: {
  workflowId: string;
  instanceId: string;
  taskInstance: SnapshotTaskInstance;
}) {
  const [batches, setBatches] = useState<Batch[]>([]);
  const [hasEarlier, setHasEarlier] = useState(false);
  const [markerPresent, setMarkerPresent] = useState(false);
  const [marker, setMarker] = useState<MarkerInfo>({});
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [follow, setFollow] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);
  // The poll reads these without re-arming the interval.
  const cursorRef = useRef<number | null>(null);
  const stoppedRef = useRef(false);

  const taskTerminal = isTerminalTaskState(taskInstance.state);

  const absorb = useCallback((data: TaskLogPage, mode: 'append' | 'prepend' | 'replace') => {
    const incoming: Batch[] = (data.batches ?? []).map((b) => ({ seq: b.seq, text: b.text }));
    setBatches((prev) => {
      const next =
        mode === 'replace' ? incoming : mode === 'append' ? [...prev, ...incoming] : [...incoming, ...prev];
      return next.length > MAX_BUFFER_BATCHES ? next.slice(next.length - MAX_BUFFER_BATCHES) : next;
    });
    if (mode !== 'prepend') {
      const maxSeq = incoming.length ? incoming[incoming.length - 1].seq : null;
      if (maxSeq != null && (cursorRef.current == null || maxSeq > cursorRef.current)) {
        cursorRef.current = maxSeq;
      }
      if (data.last_seq != null && (cursorRef.current == null || data.last_seq > cursorRef.current)) {
        cursorRef.current = data.last_seq;
      }
    }
    if (data.marker_present) {
      setMarkerPresent(true);
      setMarker({ exitStatus: data.exit_status ?? undefined, exitReason: data.exit_reason ?? undefined });
      stoppedRef.current = true;
    }
  }, []);

  // Tail-first load.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await unwrap(
          api.GET(LOGS_PATH, {
            params: {
              path: { workflow_id: workflowId, workflow_instance_id: instanceId, task_instance_id: taskInstance.id },
              query: { tail_batches: TAIL_BATCHES },
            },
          }),
        ) as TaskLogPage;
        if (cancelled) return;
        absorb(data, 'replace');
        setHasEarlier(!!data.has_earlier);
        setLoaded(true);
      } catch (e) {
        if (!cancelled) {
          setError(e as Error);
          setLoaded(true);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [workflowId, instanceId, taskInstance.id, absorb]);

  // Cursor poll — ends at the marker, or at task-terminal once a poll
  // returns nothing further.
  useEffect(() => {
    if (!loaded || stoppedRef.current) return;
    const t = setInterval(() => {
      void (async () => {
        if (stoppedRef.current) return;
        try {
          const data = await unwrap(
            api.GET(LOGS_PATH, {
              params: {
                path: { workflow_id: workflowId, workflow_instance_id: instanceId, task_instance_id: taskInstance.id },
                query: { after_seq: cursorRef.current ?? 0 },
              },
            }),
          ) as TaskLogPage;
          absorb(data, 'append');
          if (taskTerminal && !(data.batches ?? []).length && !data.marker_present) {
            // Terminal with nothing further on the subject — the abnormal-end
            // case. Stop; no extra fetch.
            stoppedRef.current = true;
          }
        } catch {
          // Transient poll failure — keep the interval; the next tick retries.
        }
      })();
    }, POLL_MS);
    if (markerPresent || (taskTerminal && stoppedRef.current)) clearInterval(t);
    return () => clearInterval(t);
  }, [loaded, workflowId, instanceId, taskInstance.id, absorb, taskTerminal, markerPresent]);

  const loadEarlier = async () => {
    const firstSeq = batches[0]?.seq;
    if (firstSeq == null) return;
    try {
      const data = await unwrap(
        api.GET(LOGS_PATH, {
          params: {
            path: { workflow_id: workflowId, workflow_instance_id: instanceId, task_instance_id: taskInstance.id },
            query: { tail_batches: TAIL_BATCHES, before_seq: firstSeq },
          },
        }),
      ) as TaskLogPage;
      setHasEarlier(!!data.has_earlier);
      absorb(data, 'prepend');
    } catch {
      // Leave the affordance in place; the next click retries.
    }
  };

  // Follow mode: stick to the bottom on new content.
  const text = batches.map((b) => b.text).join('');
  useEffect(() => {
    const el = scrollRef.current;
    if (follow && el) el.scrollTop = el.scrollHeight;
  }, [text, follow, markerPresent]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 24;
    if (!atBottom && follow) setFollow(false);
  };

  if (!loaded) return <TableLoading rows={8} cols={1} />;
  if (error && batches.length === 0) return <QueryError error={error} />;

  const { shown, hidden } = boundedTail(text.split('\n'), MAX_LINES);

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        {hasEarlier && (
          <Button variant="outline" size="sm" onClick={() => void loadEarlier()}>
            Load earlier
          </Button>
        )}
        {hidden > 0 && (
          <span className="text-xs text-muted-foreground">
            showing last {MAX_LINES.toLocaleString()} lines ({hidden.toLocaleString()} earlier
            lines hidden)
          </span>
        )}
        <span className="ml-auto inline-flex items-center gap-2">
          {!follow && !stoppedRef.current && (
            <Button variant="outline" size="sm" onClick={() => setFollow(true)}>
              <ArrowDown size={14} aria-hidden className="mr-1" />
              Follow
            </Button>
          )}
          <a href={downloadHref(workflowId, instanceId, taskInstance.id)} download>
            <Button variant="ghost" size="sm" title="Download the full log">
              <Download size={14} aria-hidden />
            </Button>
          </a>
        </span>
      </div>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        className="max-h-[60vh] overflow-auto rounded-md bg-muted/40 p-4"
      >
        {batches.length === 0 && !markerPresent ? (
          <span className="text-xs text-muted-foreground">No output yet — tailing…</span>
        ) : (
          <pre className="whitespace-pre-wrap break-words text-xs leading-relaxed">
            {shown.join('\n')}
          </pre>
        )}
        <StreamEndLine
          markerPresent={markerPresent}
          marker={marker}
          taskState={taskInstance.state}
        />
      </div>
    </div>
  );
}

/**
 * Logs of an already-terminal Attempt: one no-cursor fetch — stream replay
 * while the instance is live, the archived blob once compacted; the
 * resolver's probe order hides the handoff. No polling.
 */
function TerminalLogView({
  workflowId,
  instanceId,
  taskInstance,
}: {
  workflowId: string;
  instanceId: string;
  taskInstance: SnapshotTaskInstance;
}) {
  const { data, isLoading, error, refetch } = useTaskLogs(workflowId, instanceId, taskInstance.id);
  const notFound = error instanceof Error && /404/.test(error.message);

  if (isLoading) return <TableLoading rows={8} cols={1} />;
  if (error && !notFound) return <QueryError error={error as Error} onRetry={() => refetch()} />;

  const content = data?.logs ?? '';
  const markerPresent = data?.marker_present ?? false;
  if (!content && !markerPresent && (notFound || data)) {
    return (
      <div className="space-y-2">
        <EmptyState
          title="No logs"
          description="This attempt's log subject holds no batches and no end-of-stream marker."
        />
        <StreamEndLine markerPresent={false} marker={{}} taskState={taskInstance.state} />
      </div>
    );
  }

  const { shown, hidden } = boundedTail(content.split('\n'), MAX_LINES);
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        {hidden > 0 && (
          <span className="text-xs text-muted-foreground">
            showing last {MAX_LINES.toLocaleString()} lines ({hidden.toLocaleString()} earlier
            lines hidden)
          </span>
        )}
        <span className="ml-auto">
          <a href={downloadHref(workflowId, instanceId, taskInstance.id)} download>
            <Button variant="ghost" size="sm" title="Download the full log">
              <Download size={14} aria-hidden />
            </Button>
          </a>
        </span>
      </div>
      <div className="max-h-[60vh] overflow-auto rounded-md bg-muted/40 p-4">
        <pre className="whitespace-pre-wrap break-words text-xs leading-relaxed">
          {shown.join('\n')}
        </pre>
        <StreamEndLine
          markerPresent={markerPresent}
          marker={{
            exitStatus: data?.exit_status ?? undefined,
            exitReason: data?.exit_reason ?? undefined,
          }}
          taskState={taskInstance.state}
        />
      </div>
    </div>
  );
}

/**
 * The Logs tab — a live tail for a running Attempt, a single replay for a
 * terminal one. The mode is fixed at mount per Attempt: a task that goes
 * terminal mid-tail ends through the live stream's marker (or abnormal-end)
 * path rather than refetching.
 */
export function TaskLogsTab({
  workflowId,
  instanceId,
  taskInstance,
}: {
  workflowId: string;
  instanceId: string;
  taskInstance: SnapshotTaskInstance;
}) {
  // Capture terminality at mount: arriving on an already-finished Attempt
  // takes the one-fetch path; watching one finish stays on the stream.
  const [terminalAtMount] = useState(() => isTerminalTaskState(taskInstance.state));
  return terminalAtMount ? (
    <TerminalLogView workflowId={workflowId} instanceId={instanceId} taskInstance={taskInstance} />
  ) : (
    <LiveLogStream workflowId={workflowId} instanceId={instanceId} taskInstance={taskInstance} />
  );
}
