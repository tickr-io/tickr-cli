import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseQueryOptions,
} from '@tanstack/react-query';
import { api, unwrap } from './client';
import type {
  Event as ApiEvent,
  Workflow,
  WorkflowDetail,
  WorkflowInstance,
  InstanceSnapshot,
  InstanceContext,
  TaskInstance,
  ClockResponse,
  CalendarResponse,
  UpcomingInstance,
  TaskLogs,
  PatchSource,
  ReplayResult,
  ReplayRow,
  TenantInfo,
} from './client';
import { normalizeState } from './normalize';
import { normalizeWorkflowDetail } from './workflowDefinition';
import { displayOrdered } from '@/lib/events';
import {
  reduceHealth,
  initialHealthState,
  type HealthDisplay,
  type HealthReading,
  type HealthResponse,
  type HealthState,
} from '@/lib/health';

type Opts<T> = Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, 'queryKey' | 'queryFn'>;

export function useWorkflows(opts?: Opts<Workflow[]>) {
  return useQuery({
    queryKey: ['workflows'],
    queryFn: () => unwrap(api.GET('/api/workflows', {})),
    ...opts,
  });
}

/** The tenant this API component serves — slug, derived UUID, and the count of
 * workflow definitions registered under it. Read-only; single-tenant per process. */
export function useTenant(opts?: Opts<TenantInfo>) {
  return useQuery({
    queryKey: ['tenant'],
    queryFn: () => unwrap(api.GET('/api/tenant', {})),
    ...opts,
  });
}

/**
 * The Workflow detail page payload for one (workflow_id, version). When
 * `version` is undefined the server resolves the Default version; passing an
 * explicit version re-scopes the per-version header cells and tabs. The version
 * is part of the query key so a picker move refetches cleanly.
 */
export function useWorkflowDetail(
  workflowId: string | undefined,
  version?: number,
  opts?: Opts<WorkflowDetail>,
) {
  return useQuery({
    queryKey: ['workflowDetail', workflowId, version ?? null],
    queryFn: async () =>
      normalizeWorkflowDetail(
        await unwrap(
          api.GET('/api/workflows/{workflow_id}', {
            params: {
              path: { workflow_id: workflowId! },
              query: version != null ? { version: String(version) } : {},
            },
          }),
        ),
      ),
    enabled: !!workflowId,
    ...opts,
  });
}

export function useWorkflowInstances(
  workflowId: string | undefined,
  args: { date?: string; tz?: string } = {},
  opts?: Opts<WorkflowInstance[]>,
) {
  const { date, tz } = args;
  return useQuery({
    queryKey: ['workflowInstances', workflowId, date ?? null, tz ?? null],
    queryFn: () =>
      unwrap(
        api.GET('/api/workflows/{id}/instances', {
          params: {
            path: { id: workflowId! },
            query: date ? { date, tz } : {},
          },
        }),
      ),
    enabled: !!workflowId,
    ...opts,
  });
}

/**
 * The Run calendar's per-day counts for `year`, bucketed in the client's IANA
 * `tz`. The response carries `live_data_available` so the UI can tell "no
 * scheduled/in-progress runs" apart from "live source degraded".
 */
export function useWorkflowCalendar(
  workflowId: string | undefined,
  year: number,
  tz: string,
  opts?: Opts<CalendarResponse>,
) {
  return useQuery({
    queryKey: ['workflowCalendar', workflowId, year, tz],
    queryFn: () =>
      unwrap(
        api.GET('/api/workflows/{id}/calendar', {
          params: { path: { id: workflowId! }, query: { year, tz } },
        }),
      ),
    enabled: !!workflowId,
    ...opts,
  });
}

/** A workflow instance state with no further transitions — polling stops here. */
function isTerminalInstanceState(state: string | undefined): boolean {
  const s = normalizeState(state);
  return s === 'completed' || s === 'failed';
}

/**
 * The instance snapshot — the one polled query behind the whole instance
 * detail page (DC-0015): enriched instance + minted task instances + tasks
 * definition map + hypergraph with gate states + routing variables.
 * Polls at 5s while the instance is live; polling stops at terminal state
 * so finished instances generate no load.
 */
export function useInstanceSnapshot(instanceId: string | undefined, opts?: Opts<InstanceSnapshot>) {
  return useQuery({
    queryKey: ['workflowInstance', instanceId],
    queryFn: () =>
      unwrap(api.GET('/api/workflows/instances/{id}', { params: { path: { id: instanceId! } } })),
    enabled: !!instanceId,
    refetchInterval: (query) =>
      isTerminalInstanceState(query.state.data?.state) ? false : 5_000,
    ...opts,
  });
}

/**
 * The Context tab's payload — fetched on tab focus (pass `enabled`), and
 * re-polled on the page cadence only while the instance is live. The query
 * key is separate from the snapshot's so tab switches never refetch the
 * snapshot itself.
 */
export function useInstanceContext(
  instanceId: string | undefined,
  args: { enabled: boolean; live: boolean },
  opts?: Opts<InstanceContext>,
) {
  return useQuery({
    queryKey: ['instanceContext', instanceId],
    queryFn: () =>
      unwrap(
        api.GET('/api/workflows/instances/{id}/context', {
          params: { path: { id: instanceId! } },
        }),
      ),
    enabled: !!instanceId && args.enabled,
    refetchInterval: args.live ? 5_000 : false,
    ...opts,
  });
}

/**
 * One Patch's retained authored source — the verbatim bytes the author
 * submitted (Nickel for an external patch, the JSON document for a self-patch),
 * fetched by `patch_id` from the Conductor's read path. The source is immutable
 * once retained, so it is fetched once (no polling) and cached indefinitely.
 * Inert until `patchId` is known; the code tab only mounts it for a patch that
 * actually applied.
 */
export function usePatchSource(patchId: string | undefined, opts?: Opts<PatchSource>) {
  return useQuery({
    queryKey: ['patchSource', patchId],
    queryFn: () =>
      unwrap(api.GET('/api/patches/{patch_id}/source', { params: { path: { patch_id: patchId! } } })),
    enabled: !!patchId,
    staleTime: Infinity, // the retained source is immutable — fetch once
    ...opts,
  });
}

export function useTaskInstances(instanceId: string | undefined, opts?: Opts<TaskInstance[]>) {
  return useQuery({
    queryKey: ['taskInstances', instanceId],
    queryFn: () =>
      unwrap(api.GET('/api/workflows/instances/{id}/tasks', { params: { path: { id: instanceId! } } })),
    enabled: !!instanceId,
    ...opts,
  });
}

/**
 * Task-level cancel: `POST /workflows/instances/{id}/tasks/{task_id}/cancel`.
 * The cancel succeeds at the state level immediately (a forced attempt-failure
 * that respawns or grounds `Cancelled` per the retry budget), so on success we
 * invalidate the instance snapshot and task list to repaint the task's badge.
 */
export function useCancelTask(instanceId: string | undefined) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (taskId: string) =>
      unwrap(
        api.POST('/api/workflows/instances/{id}/tasks/{task_id}/cancel', {
          params: { path: { id: instanceId!, task_id: taskId } },
          body: {},
        }),
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['workflowInstance', instanceId] });
      void qc.invalidateQueries({ queryKey: ['taskInstances', instanceId] });
    },
  });
}

/**
 * Workflow-level cancel: `POST /workflows/instances/{id}/cancel` (`node_id`
 * omitted). One atomic pass grounds the whole non-grounded frontier and drives
 * the run to the distinct terminal `Cancelled` outcome. It succeeds at the
 * state level immediately, so on success we invalidate the instance snapshot to
 * repaint the run's badge (and the task list, whose cards flip to `Cancelled`).
 */
export function useCancelRun(instanceId: string | undefined) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () =>
      unwrap(
        api.POST('/api/workflows/instances/{id}/cancel', {
          params: { path: { id: instanceId! } },
          body: {},
        }),
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['workflowInstance', instanceId] });
      void qc.invalidateQueries({ queryKey: ['taskInstances', instanceId] });
    },
  });
}

/** The shadow-lever body a replay may carry. The one-click Resume button sends
 * only `resume_from`; the richer fields are for the API-driven paths. */
export interface ReplayRequestBody {
  resume_from?: string[];
  name?: string;
  inputs?: Record<string, unknown>;
  idempotency_key?: string;
}

/**
 * Replay a terminal run from its archive: `POST /workflows/instances/{id}/replay`.
 * The one-click Resume path passes `resume_from: [nodeId]` for the failed
 * HyperNode the operator is standing on. Returns the `replay_instance_id` (the
 * new run's page) and any `doomed` HyperNodes left blocked. On success we
 * invalidate the source run's replay list so its reverse link refreshes.
 */
export function useReplayRun(instanceId: string | undefined) {
  const qc = useQueryClient();
  return useMutation<ReplayResult, Error, ReplayRequestBody | undefined>({
    mutationFn: (body) =>
      unwrap(
        api.POST('/api/workflows/instances/{id}/replay', {
          params: { path: { id: instanceId! } },
          body: body ?? {},
        }),
      ),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['instanceReplays', instanceId] });
    },
  });
}

/**
 * The reverse link — the replays spawned from a source run
 * (`GET /workflows/instances/{id}/replays`), newest first. Served from the
 * indexed pipeline row, so this is cheap; fetched once per page (no polling).
 */
export function useInstanceReplays(instanceId: string | undefined, opts?: Opts<ReplayRow[]>) {
  return useQuery({
    queryKey: ['instanceReplays', instanceId],
    queryFn: () =>
      unwrap(api.GET('/api/workflows/instances/{id}/replays', { params: { path: { id: instanceId! } } })),
    enabled: !!instanceId,
    ...opts,
  });
}

export function useTaskLogs(
  workflowId: string | undefined,
  instanceId: string | undefined,
  taskId: string | undefined,
  opts?: Opts<TaskLogs>,
) {
  return useQuery({
    queryKey: ['taskLogs', workflowId, instanceId, taskId],
    queryFn: () =>
      unwrap(
        api.GET('/api/workflows/{workflow_id}/instances/{workflow_instance_id}/tasks/{task_instance_id}/logs', {
          params: {
            path: {
              workflow_id: workflowId!,
              workflow_instance_id: instanceId!,
              task_instance_id: taskId!,
            },
          },
        }),
      ) as Promise<TaskLogs>,
    enabled: !!workflowId && !!instanceId && !!taskId,
    ...opts,
  });
}

/** Health page poll cadence. Individually-cheap per-request checks, and the
 * 2-read debounce means a real flip lands within ~2 cadences. */
const HEALTH_POLL_MS = 10_000;

/** What the Health page consumes: the debounced per-row display, the last
 * successful `checked_at`, whether the last read reached the endpoint, first-load
 * flag, and a `recheck` that re-hits the endpoint. */
export interface HealthTail {
  display: HealthDisplay | null;
  checkedAt: string | null;
  reachable: boolean;
  isLoading: boolean;
  recheck: () => void;
}

/**
 * Poll `GET /api/health` and fold each reading through the UI-owned cascade +
 * 2-consecutive-read debounce (`reduceHealth`). Not a `useQuery`: the display is
 * an accumulation over successive readings (each read may only *hold* a row),
 * which react-query's replace-on-refetch model fights — the same reason the
 * event tail hand-rolls its poll.
 *
 * The endpoint is hand-rolled in the `api` crate (not in the generated OpenAPI
 * types), so it is fetched directly rather than through the typed client. A
 * failed fetch is the API-unreachable signal that drives the cascade.
 */
export function useHealth(pollMs = HEALTH_POLL_MS): HealthTail {
  const [state, setState] = useState<HealthState>(initialHealthState);
  const [checkedAt, setCheckedAt] = useState<string | null>(null);
  const [reachable, setReachable] = useState(true);
  const [isLoading, setIsLoading] = useState(true);

  const poll = useCallback(async () => {
    let reading: HealthReading;
    try {
      const res = await fetch('/api/health', { headers: { accept: 'application/json' } });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const response = (await res.json()) as HealthResponse;
      reading = { ok: true, response };
      setCheckedAt(response.checked_at);
      setReachable(true);
    } catch {
      // The endpoint did not answer — that IS the API-down signal (DC-0013), so
      // the cascade reports every row unhealthy on a sustained outage.
      reading = { ok: false };
      setReachable(false);
    }
    setIsLoading(false);
    setState((s) => reduceHealth(s, reading));
  }, []);

  // First load runs immediately so the page is useful before the first tick.
  useEffect(() => {
    void poll();
  }, [poll]);

  useEffect(() => {
    const id = setInterval(() => void poll(), pollMs);
    return () => clearInterval(id);
  }, [poll, pollMs]);

  return { display: state.display, checkedAt, reachable, isLoading, recheck: () => void poll() };
}

interface DashboardClockArgs {
  startSeconds?: number;
  endSeconds?: number;
  /** Pause polling for historical / future windows. Default true. */
  live?: boolean;
}

/**
 * The day-clock's instance list for the selected calendar day (live ∪ archive,
 * merged server-side). Keyed on the day boundaries so a date-picker change
 * invalidates cleanly; polls at 30s only when the selected day is today.
 */
export function useDashboardClock(args: DashboardClockArgs = {}, opts?: Opts<ClockResponse>) {
  const { startSeconds, endSeconds, live = true } = args;
  return useQuery({
    queryKey: ['dashboardClock', startSeconds, endSeconds, live],
    queryFn: () =>
      unwrap(
        api.GET('/api/dashboard/clock', {
          params: {
            query: {
              start_time: startSeconds,
              end_time: endSeconds,
            },
          },
        }),
      ),
    refetchInterval: live ? 30_000 : false,
    ...opts,
  });
}

/**
 * The "Up next" strip — the next `limit` scheduled instances, refreshed on a
 * 30s poll (the dashboard's freshness budget). The lead chip's per-second
 * countdown is a pure client tick against `next_run_at`, not a poll.
 */
export function useUpcoming(limit = 20, opts?: Opts<UpcomingInstance[]>) {
  return useQuery({
    queryKey: ['upcoming', limit],
    queryFn: () => unwrap(api.GET('/api/dashboard/upcoming', { params: { query: { limit } } })),
    refetchInterval: 30_000,
    ...opts,
  });
}

/** What the Event log's live tail exposes to the page. */
export interface EventTail {
  /** Newest-first buffer, capped at `EVENT_BUFFER_CAP`. */
  events: ApiEvent[];
  /** `seq`s that arrived on the most recent poll — the rows to animate in. */
  newSeqs: Set<number>;
  /** True until the first load resolves (success or failure). */
  isLoading: boolean;
  /** Last poll's failure, cleared by the next success. */
  error: Error | null;
}

/** DC-0007 buffer semantics: cap ~200, newest at top, oldest drop. */
export const EVENT_BUFFER_CAP = 200;
const EVENT_POLL_MS = 5_000;

/**
 * Fetch one newest-first page of events strictly newer than `after` (the
 * `seq` cursor; `null` on first load). The cross-workflow Event log and the
 * per-instance Events sections differ only in this fetcher — same projection,
 * same `seq` cursor, same arrival ordering — so they share the tail machinery
 * below and only vary the page they pull.
 */
type EventPager = (after: number | null) => Promise<ApiEvent[]>;

/**
 * Shared live-tail engine over a paged events endpoint. First load fetches
 * the latest batch (newest-first); while `active`, polls every 5s passing the
 * highest `seq` seen as the cursor, so each poll returns only never-seen
 * rows. Pausing (or unmounting) stops the poll; resuming catches up from the
 * held cursor — the buffer and cursor survive the pause, so no gaps and no
 * duplicates.
 *
 * Not a `useQuery`: the buffer is append-only keyed by a moving cursor, which
 * react-query's replace-on-refetch model fights.
 */
function useEventPoll(fetchPage: EventPager, active: boolean): EventTail {
  const [events, setEvents] = useState<ApiEvent[]>([]);
  const [newSeqs, setNewSeqs] = useState<Set<number>>(() => new Set());
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);
  // The poll cursor: highest seq in the buffer. A ref, not state — polls
  // read it without re-arming the interval.
  const cursorRef = useRef<number | null>(null);

  const poll = useCallback(
    async (isFirst: boolean) => {
      try {
        const fresh = await fetchPage(cursorRef.current);
        setError(null);
        if (fresh.length > 0) {
          cursorRef.current = fresh[0].seq; // newest-first: [0] is the max
          setEvents((prev) => [...fresh, ...prev].slice(0, EVENT_BUFFER_CAP));
          setNewSeqs(isFirst ? new Set() : new Set(fresh.map((e) => e.seq)));
        }
      } catch (e) {
        setError(e instanceof Error ? e : new Error(String(e)));
      } finally {
        setIsLoading(false);
      }
    },
    [fetchPage],
  );

  // First load happens once, poll-state independent: the page is useful
  // immediately without waiting for the first interval tick.
  useEffect(() => {
    void poll(true);
  }, [poll]);

  useEffect(() => {
    if (!active) return undefined;
    const id = setInterval(() => void poll(false), EVENT_POLL_MS);
    return () => clearInterval(id);
  }, [active, poll]);

  // Delivery stays on arrival (`seq`) order — the cursor and buffer retention
  // above are untouched — but rows are *displayed* newest-first by occurrence
  // time (`ts`). A derived view over the arrival-ordered buffer, so the
  // gap-free/duplicate-free paging guarantee is unaffected; the client sort is
  // what honours occurrence order, not the cursor.
  const displayEvents = useMemo(() => displayOrdered(events), [events]);
  return { events: displayEvents, newSeqs, isLoading, error };
}

/** Live tail of the cross-workflow `GET /api/events`. */
export function useEventTail(active: boolean): EventTail {
  const fetchPage = useCallback<EventPager>(
    (after) =>
      unwrap(api.GET('/api/events', { params: { query: after !== null ? { after } : {} } })),
    [],
  );
  return useEventPoll(fetchPage, active);
}

/**
 * Live tail of one workflow instance's events
 * (`GET /api/workflows/instances/{id}/events`). Same projection and cursor as
 * the Event log, scoped to the instance. Inert (never fetches) until `id` is
 * known.
 */
export function useInstanceEventTail(id: string | undefined, active: boolean): EventTail {
  const fetchPage = useCallback<EventPager>(
    (after) => {
      if (!id) return Promise.resolve([]);
      return unwrap(
        api.GET('/api/workflows/instances/{id}/events', {
          params: { path: { id }, query: after !== null ? { after } : {} },
        }),
      );
    },
    [id],
  );
  return useEventPoll(fetchPage, active);
}

/**
 * Live tail of one task instance's events
 * (`GET /api/workflows/instances/{id}/tasks/{task_id}/events`). Filtered to
 * the task instance; the parent instance id only nests the route.
 */
export function useTaskEventTail(
  id: string | undefined,
  taskId: string | undefined,
  active: boolean,
): EventTail {
  const fetchPage = useCallback<EventPager>(
    (after) => {
      if (!id || !taskId) return Promise.resolve([]);
      return unwrap(
        api.GET('/api/workflows/instances/{id}/tasks/{task_id}/events', {
          params: { path: { id, task_id: taskId }, query: after !== null ? { after } : {} },
        }),
      );
    },
    [id, taskId],
  );
  return useEventPoll(fetchPage, active);
}
