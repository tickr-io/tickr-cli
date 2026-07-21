import { useEffect, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Check, Copy, RotateCcw } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useCancelTask, useInstanceSnapshot, useReplayRun, useTaskLogs } from '@/api/hooks';
import {
  ApiError,
  type InstanceSnapshot,
  type ReplayResult,
  type SnapshotTaskInstance,
} from '@/api/client';
import { canResumeFromTask, doomedLabels } from '@/lib/replay';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import { TaskEventsTab } from '@/components/InstanceEventsTab';
import { StateBadge } from '@/components/StateBadge';
import { TaskLogsTab } from '@/components/TaskLogsTab';
import { TaskContextTab } from '@/components/TaskContextTab';
import { TaskGatesTab } from '@/components/TaskGatesTab';
import { normalizeState, killConfirmationLabel } from '@/api/normalize';
import { taskTypeView } from '@/lib/taskType';
import {
  findTaskInstance,
  siblingAttempts,
  taskDefFor,
  isTerminalTaskState,
  isAbnormalEnd,
  attemptDurationMs,
} from '@/lib/taskSlice';

function fmtDate(iso: string | null | undefined) {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtDuration(ms: number): string {
  const sec = Math.max(0, Math.floor(ms / 1000));
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  if (m < 60) return s ? `${m}m ${s}s` : `${m}m`;
  return `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** Re-renders every `intervalMs` while `enabled` — drives the live duration
 * tick between snapshot polls. */
function useNow(intervalMs: number, enabled: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!enabled) return;
    const t = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(t);
  }, [intervalMs, enabled]);
  return now;
}

function CopyButton({ value, label }: { value: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Button
      variant="ghost"
      size="sm"
      className="h-6 w-6 p-0 text-muted-foreground"
      aria-label={label}
      onClick={() => {
        void navigator.clipboard.writeText(value);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
    >
      {copied ? <Check size={12} aria-hidden /> : <Copy size={12} aria-hidden />}
    </Button>
  );
}

function StorageIndicator({ storage }: { storage: InstanceSnapshot['storage'] }) {
  return storage === 'live' ? (
    <Badge variant="outline" className="font-mono text-xs">
      live
    </Badge>
  ) : (
    <span className="font-mono text-xs text-muted-foreground">archived state</span>
  );
}

/**
 * Sibling-Attempt chips: every Attempt of this task within the instance, the
 * current one highlighted, each other one linking to its own page. A
 * single-Attempt task renders no chips — there is nothing to hop between.
 */
function AttemptChips({
  siblings,
  currentId,
  workflowId,
  instanceId,
}: {
  siblings: SnapshotTaskInstance[];
  currentId: string;
  workflowId: string;
  instanceId: string;
}) {
  if (siblings.length <= 1) return null;
  return (
    <span className="inline-flex flex-wrap items-center gap-1.5">
      {siblings.map((s) =>
        s.id === currentId ? (
          <span
            key={s.id}
            aria-current="page"
            className="inline-flex items-center rounded-md border border-primary bg-primary/10 px-2 py-0.5 font-mono text-xs"
          >
            {s.attempt + 1}
          </span>
        ) : (
          <Link
            key={s.id}
            to={`/workflows/${workflowId}/instances/${instanceId}/tasks/${s.id}`}
            title={`Attempt ${s.attempt + 1} · ${s.state}`}
            className="inline-flex items-center rounded-md border border-border px-2 py-0.5 font-mono text-xs text-muted-foreground hover:text-foreground"
          >
            {s.attempt + 1}
          </Link>
        ),
      )}
    </span>
  );
}

/**
 * Abnormal-end callout: the task is terminal but its log subject was never
 * closed with an End-of-stream marker — "the executor died" stated as fact.
 * Marker state comes from the same cached one-shot logs fetch the Logs tab
 * uses (shared query key, one request per page visit); a 404 means no
 * batches and no marker anywhere, which is marker-absent.
 */
function AbnormalEndCallout({
  workflowId,
  instanceId,
  taskInstance,
}: {
  workflowId: string;
  instanceId: string;
  taskInstance: SnapshotTaskInstance;
}) {
  const terminal = isTerminalTaskState(taskInstance.state);
  const logsQ = useTaskLogs(workflowId, instanceId, taskInstance.id, { enabled: terminal });
  const markerPresent: boolean | undefined = logsQ.data
    ? (logsQ.data.marker_present ?? false)
    : logsQ.error instanceof ApiError && logsQ.error.status === 404
      ? false
      : undefined;
  if (!isAbnormalEnd(taskInstance.state, markerPresent)) return null;
  return (
    <Card className="border-destructive/50">
      <CardHeader>
        <CardTitle className="text-base text-destructive">Abnormal end</CardTitle>
        <CardDescription>
          The stream ended without an end-of-stream marker — the executor may have died. The
          staged logs (if any) were archived as-is.
        </CardDescription>
      </CardHeader>
    </Card>
  );
}

function OverviewTab({
  snapshot,
  taskInstance,
  workflowId,
  instanceId,
}: {
  snapshot: InstanceSnapshot;
  taskInstance: SnapshotTaskInstance;
  workflowId: string;
  instanceId: string;
}) {
  const running = !!taskInstance.started_at && !taskInstance.completed_at;
  const now = useNow(1_000, running);
  const durationMs = attemptDurationMs(taskInstance, now);
  const siblings = siblingAttempts(snapshot, taskInstance);

  const cells: Array<[string, React.ReactNode]> = [
    [
      'Task instance id',
      <span key="id" className="inline-flex items-center gap-1.5">
        <span className="break-all font-mono text-xs">{taskInstance.id}</span>
        <CopyButton value={taskInstance.id} label="Copy task instance id" />
      </span>,
    ],
    ['Started', <span key="sa">{fmtDate(taskInstance.started_at)}</span>],
    ['Completed', <span key="ca">{fmtDate(taskInstance.completed_at)}</span>],
    [
      'Duration',
      <span key="d" className="tabular-nums">
        {durationMs == null ? '—' : fmtDuration(durationMs)}
      </span>,
    ],
    [
      'Executor',
      taskInstance.executor_id ? (
        <span key="e" className="font-mono text-xs text-muted-foreground">
          {taskInstance.executor_id}
        </span>
      ) : (
        <span key="e" className="text-muted-foreground">
          —
        </span>
      ),
    ],
    ['Storage', <StorageIndicator key="st" storage={snapshot.storage} />],
  ];
  if (siblings.length > 1) {
    cells.splice(1, 0, [
      'Attempts',
      <AttemptChips
        key="at"
        siblings={siblings}
        currentId={taskInstance.id}
        workflowId={workflowId}
        instanceId={instanceId}
      />,
    ]);
  }

  return (
    <div className="space-y-4">
      <AbnormalEndCallout
        workflowId={workflowId}
        instanceId={instanceId}
        taskInstance={taskInstance}
      />
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3">
        {cells.map(([k, v]) => (
          <Card key={k}>
            <CardHeader>
              <CardDescription>{k}</CardDescription>
              <CardTitle className="text-base font-normal">{v}</CardTitle>
            </CardHeader>
          </Card>
        ))}
      </div>
    </div>
  );
}

/**
 * The Task instance detail page — one TaskInstance, one Attempt, one page
 * (DC-0012). The route param is the task-instance id; sibling Attempts are
 * sibling pages cross-linked by attempt chips. Everything renders from the
 * same polled instance snapshot as the Workflow instance detail page
 * (shared query cache key); polling stops once this task is terminal, even
 * while the instance runs on.
 */
export function TaskDetailPage() {
  const { workflowId, instanceId, taskId } = useParams<{
    workflowId: string;
    instanceId: string;
    taskId: string;
  }>();

  const inst = useInstanceSnapshot(instanceId, {
    refetchInterval: (query) => {
      const snap = query.state.data;
      if (!snap) return 5_000;
      const ti = taskId ? findTaskInstance(snap, taskId) : null;
      if (ti && isTerminalTaskState(ti.state)) return false;
      // Instance-terminal implies nothing further changes either.
      const s = normalizeState(snap.state);
      if (s === 'completed' || s === 'failed') return false;
      return 5_000;
    },
  });
  const [tab, setTab] = useState('overview');

  const taskInstance = inst.data && taskId ? findTaskInstance(inst.data, taskId) : null;
  const taskDef = inst.data && taskInstance ? taskDefFor(inst.data, taskInstance) : null;
  // Kind pill reads from the true `task_type` enum via the shared taxonomy
  // source of truth, so a ShadowTask shows visibly instead of the raw variant.
  const taskType = taskInstance ? taskTypeView(taskInstance.task_type) : null;

  const errorStatus = inst.error instanceof ApiError ? inst.error.status : undefined;

  // "Cancel task" targets the graph node (the task DEFINITION id), not the
  // per-attempt instance id — the cancel is a forced attempt-failure on the
  // node. Offered only while the task is non-terminal (still live).
  const cancelTask = useCancelTask(instanceId);
  const cancellable = !!taskInstance && !isTerminalTaskState(taskInstance.state);

  // "Resume" replays the run from THIS task's failed HyperNode — the page the
  // operator is standing on IS the selection, so there is no node-picker. The
  // button enables iff this task's HyperNode is Grounded(Failed) (a
  // cascade-Cancelled sibling is excluded); it fires `resume_from: [node_id]`.
  const navigate = useNavigate();
  const replayRun = useReplayRun(instanceId);
  const [replayResult, setReplayResult] = useState<ReplayResult | null>(null);
  const canResume =
    !!inst.data && !!taskInstance && canResumeFromTask(inst.data, taskInstance.task_id);
  const onResume = () => {
    if (!taskInstance) return;
    setReplayResult(null);
    replayRun.mutate(
      { resume_from: [taskInstance.task_id] },
      {
        onSuccess: (res) => {
          // A doom enumeration means a sibling failure's subtree stays blocked —
          // surface it as a confirmation with a link to the replay, rather than
          // silently navigating away. No doomed nodes → open the replay directly.
          if (res.doomed && res.doomed.length > 0) {
            setReplayResult(res);
          } else {
            navigate(`/workflows/${workflowId}/instances/${res.replay_instance_id}`);
          }
        },
      },
    );
  };

  return (
    <div className="space-y-6">
      {/* Minimal identity header — one thin row so the tab viewport stays tall. */}
      <div className="flex flex-wrap items-center gap-3">
        <h1 className="text-2xl font-semibold tracking-tight">
          {taskInstance?.name ?? 'Task'}
        </h1>
        {taskInstance && (
          <StateBadge state={taskInstance.state} reason={taskInstance.cancel_reason} />
        )}
        {taskInstance && killConfirmationLabel(taskInstance.kill_confirmation) && (
          <Badge variant="outline" className="text-xs">
            {killConfirmationLabel(taskInstance.kill_confirmation)}
          </Badge>
        )}
        {taskType && (
          <span className={`taskdef-type ${taskType.cls}`} title={taskType.title}>
            {taskType.label}
          </span>
        )}
        {taskInstance && (
          <span className="font-mono text-xs text-muted-foreground">
            attempt {taskInstance.attempt + 1}
            {taskDef ? ` / ${taskDef.max_attempts}` : ''}
          </span>
        )}
        {cancellable && (
          <Button
            variant="destructive"
            size="sm"
            className="ml-auto h-7 px-3 text-xs"
            disabled={cancelTask.isPending}
            onClick={() => cancelTask.mutate(taskInstance!.task_id)}
          >
            {cancelTask.isPending ? 'Cancelling…' : 'Cancel task'}
          </Button>
        )}
        {canResume && (
          <Button
            variant="default"
            size="sm"
            className={cancellable ? 'h-7 px-3 text-xs' : 'ml-auto h-7 px-3 text-xs'}
            disabled={replayRun.isPending}
            title="Replay the run, resuming from this failed task"
            onClick={onResume}
          >
            <RotateCcw size={12} aria-hidden className="mr-1" />
            {replayRun.isPending ? 'Resuming…' : 'Resume from here'}
          </Button>
        )}
      </div>

      {replayRun.error && (
        <Card className="border-destructive/50">
          <CardHeader>
            <CardTitle className="text-base text-destructive">Replay failed</CardTitle>
            <CardDescription>
              {replayRun.error instanceof ApiError
                ? replayRun.error.message
                : 'The replay request could not be started.'}
            </CardDescription>
          </CardHeader>
        </Card>
      )}

      {replayResult && inst.data && (
        <Card className="border-amber-500/50">
          <CardHeader>
            <CardTitle className="text-base">Replay started · some HyperNodes stay blocked</CardTitle>
            <CardDescription>
              Resuming from this failure leaves a sibling failure's subtree
              permanently blocked. These HyperNodes will not run in the replay:
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="flex flex-wrap gap-1.5">
              {doomedLabels(inst.data, replayResult.doomed ?? []).map((code) => (
                <span
                  key={code}
                  className="inline-flex items-center rounded-md border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 font-mono text-xs"
                >
                  {code}
                </span>
              ))}
            </div>
            <Button
              variant="default"
              size="sm"
              className="h-7 px-3 text-xs"
              onClick={() =>
                navigate(
                  `/workflows/${workflowId}/instances/${replayResult.replay_instance_id}`,
                )
              }
            >
              Open replay
            </Button>
          </CardContent>
        </Card>
      )}

      {errorStatus === 503 ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Control plane unreachable</CardTitle>
            <CardDescription>
              The live store could not be reached. This task instance may still exist — retry once
              the control plane is back.
            </CardDescription>
          </CardHeader>
        </Card>
      ) : errorStatus === 404 ? (
        <EmptyState
          title="Instance not found"
          description="No workflow instance matches this id in the archive or the live cluster."
        />
      ) : inst.error ? (
        <QueryError error={inst.error as Error} onRetry={() => inst.refetch()} />
      ) : inst.data && !taskInstance ? (
        <EmptyState
          title="Task instance not found"
          description="This run's snapshot holds no task instance with this id — the link may be stale, or the task was never minted."
        />
      ) : null}

      {taskInstance && instanceId && workflowId && inst.data && (
        <Tabs value={tab} onValueChange={setTab} className="space-y-4">
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="logs">Logs</TabsTrigger>
            <TabsTrigger value="context">Context</TabsTrigger>
            <TabsTrigger value="gates">Gates</TabsTrigger>
            <TabsTrigger value="events">Events</TabsTrigger>
          </TabsList>

          <TabsContent value="overview">
            <OverviewTab
              snapshot={inst.data}
              taskInstance={taskInstance}
              workflowId={workflowId}
              instanceId={instanceId}
            />
          </TabsContent>

          <TabsContent value="logs">
            <TaskLogsTab
              workflowId={workflowId}
              instanceId={instanceId}
              taskInstance={taskInstance}
            />
          </TabsContent>

          <TabsContent value="context">
            <TaskContextTab
              snapshot={inst.data}
              taskInstance={taskInstance}
              instanceId={instanceId}
              active={tab === 'context'}
            />
          </TabsContent>

          <TabsContent value="gates">
            <TaskGatesTab snapshot={inst.data} taskInstance={taskInstance} />
          </TabsContent>

          <TabsContent value="events">
            <TaskEventsTab
              instanceId={instanceId}
              taskId={taskId}
              active={tab === 'events'}
            />
          </TabsContent>
        </Tabs>
      )}

      {!inst.data && !inst.error && <TableLoading rows={3} cols={3} />}
    </div>
  );
}
