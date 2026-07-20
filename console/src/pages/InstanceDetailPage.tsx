import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Calendar, Check, Copy, Play, Radio, RotateCcw, Zap, type LucideIcon } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useCancelRun, useInstanceReplays, useInstanceSnapshot, useTaskLogs } from '@/api/hooks';
import { normalizeState } from '@/api/normalize';
import { ApiError, type InstanceSnapshot } from '@/api/client';
import { replaySummary } from '@/lib/replay';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import { InstanceEventsTab } from '@/components/InstanceEventsTab';
import { StateBadge } from '@/components/StateBadge';
import { InstanceTaskCards } from '@/components/InstanceTaskCards';
import { InstanceGraphTab } from '@/components/InstanceGraphTab';
import { InstanceTimelineTab } from '@/components/InstanceTimelineTab';
import { InstanceGatesTab } from '@/components/InstanceGatesTab';
import { InstanceContextTab } from '@/components/InstanceContextTab';
import { InstanceCodeTab } from '@/components/InstanceCodeTab';
import { formatRunHandle, runHandleSource } from '@/lib/runHandle';

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

const TRIGGER_ICON: Record<string, LucideIcon> = {
  Cron: Calendar,
  Manual: Play,
  External: Zap,
  Wakeup: Radio,
  Replay: RotateCcw,
};

/**
 * The replay subtitle: renders "replay of ⟨source⟩ from ⟨…⟩" beneath the run
 * heading when this run is a replay, so the operator never confuses it with the
 * original. The source run links back to its own detail page. Renders nothing
 * for a non-replay run.
 */
function ReplaySubtitle({
  snapshot,
  workflowId,
}: {
  snapshot: InstanceSnapshot;
  workflowId: string | undefined;
}) {
  const summary = replaySummary(snapshot.triggered_by);
  if (!summary) return null;
  const wf = workflowId ?? snapshot.workflow_id;
  return (
    <span className="inline-flex flex-wrap items-center gap-1 text-xs text-muted-foreground">
      <RotateCcw size={12} aria-hidden />
      <span>replay of</span>
      <Link
        to={`/workflows/${wf}/instances/${summary.sourceId}`}
        className="font-mono underline-offset-2 hover:underline"
        title={`Open the source run ${summary.sourceId}`}
      >
        {summary.sourceCode}
      </Link>
      <span>· {summary.suffix}</span>
    </span>
  );
}

/**
 * The reverse link — the replays spawned from this run. Rendered only when the
 * run has replays. Served from the indexed pipeline row, so a run with none is
 * the common case and shows nothing. Each row links to the replay's own page.
 */
function ReplaysCard({
  instanceId,
  workflowId,
  workflowFallback,
}: {
  instanceId: string | undefined;
  workflowId: string | undefined;
  workflowFallback: string;
}) {
  const replays = useInstanceReplays(instanceId);
  const rows = replays.data ?? [];
  if (rows.length === 0) return null;
  const wf = workflowId ?? workflowFallback;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Replays</CardTitle>
        <CardDescription>Runs replayed from this one, newest first.</CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {rows.map((r) => (
          <div key={r.replay_instance_id} className="flex flex-wrap items-center gap-2 text-sm">
            <RotateCcw size={12} aria-hidden className="text-muted-foreground" />
            <Link
              to={`/workflows/${wf}/instances/${r.replay_instance_id}`}
              className="font-mono text-xs underline-offset-2 hover:underline"
            >
              {r.name ?? r.replay_instance_id.slice(0, 8)}
            </Link>
            <Badge variant="outline" className="text-xs">
              {r.status}
            </Badge>
            {r.resume_from.length > 0 && (
              <span className="text-xs text-muted-foreground">
                from{' '}
                {r.resume_from.length === 1
                  ? r.resume_from[0].code
                  : `${r.resume_from.length} HyperNodes`}
              </span>
            )}
            {r.shadowed_keys.length > 0 && (
              <span
                className="text-xs text-muted-foreground"
                title={`Shadowed captures: ${r.shadowed_keys.join(', ')}`}
              >
                · shadowed {r.shadowed_keys.length}
              </span>
            )}
          </div>
        ))}
      </CardContent>
    </Card>
  );
}

/**
 * Trigger provenance cell: kind with icon; for the signal-borne kinds the
 * originating signal id renders short-mono and copyable, linking to the
 * Context tab's trigger scope (the link target tab is built by the Context
 * slice — until then it lands on the tab's stub).
 */
function TriggerCellValue({
  snapshot,
  onOpenContext,
}: {
  snapshot: InstanceSnapshot;
  onOpenContext: () => void;
}) {
  const p = snapshot.triggered_by;
  if (!p) return <span className="text-muted-foreground">Unknown</span>;
  const Icon = TRIGGER_ICON[p.kind] ?? Calendar;
  return (
    <span className="inline-flex flex-wrap items-center gap-2">
      <span className="inline-flex items-center gap-1.5">
        <Icon size={14} aria-hidden className="text-muted-foreground" />
        {p.kind === 'Wakeup' && p.name ? `Wakeup · ${p.name}` : p.kind}
      </span>
      {p.signal_id && (
        <span className="inline-flex items-center gap-0.5">
          <button
            type="button"
            className="font-mono text-xs text-muted-foreground underline-offset-2 hover:underline"
            title="Open the Context tab at the trigger scope"
            onClick={onOpenContext}
          >
            {p.signal_id.slice(0, 8)}
          </button>
          <CopyButton value={p.signal_id} label="Copy originating signal id" />
        </span>
      )}
    </span>
  );
}

/** Author tags as key=value chips; `tickr/*` system tags muted/mono.
 * The full merged map ships and renders because ByTag addressing matches
 * against all of it — hiding the system half would misrepresent what a tag
 * filter could have matched. */
function TagsRow({ tags }: { tags: Record<string, string> }) {
  const entries = Object.entries(tags).sort(([a], [b]) => a.localeCompare(b));
  if (entries.length === 0) return null;
  const author = entries.filter(([k]) => !k.startsWith('tickr/'));
  const system = entries.filter(([k]) => k.startsWith('tickr/'));
  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {author.map(([k, v]) => (
        <span
          key={k}
          className="inline-flex items-center gap-1 rounded-md border border-border bg-muted/40 px-2 py-0.5 text-xs"
        >
          <span className="font-medium text-muted-foreground">{k}=</span>
          {v}
        </span>
      ))}
      {system.map(([k, v]) => (
        <span
          key={k}
          className="inline-flex items-center rounded-md bg-muted/30 px-2 py-0.5 font-mono text-[11px] text-muted-foreground"
        >
          {k}={v}
        </span>
      ))}
    </div>
  );
}

function StorageIndicator({ storage }: { storage: InstanceSnapshot['storage'] }) {
  return storage === 'live' ? (
    <Badge variant="outline" className="font-mono text-xs">
      live
    </Badge>
  ) : (
    <span className="font-mono text-xs text-muted-foreground">archived · Postgres</span>
  );
}

function OverviewTab({
  snapshot,
  workflowId,
  instanceId,
  onOpenContext,
}: {
  snapshot: InstanceSnapshot;
  workflowId: string | undefined;
  instanceId: string | undefined;
  onOpenContext: () => void;
}) {
  const startedMs = snapshot.started_at ? new Date(snapshot.started_at).getTime() : null;
  const completedMs = snapshot.completed_at ? new Date(snapshot.completed_at).getTime() : null;
  // Duration is derived, never served: completed−started when terminal,
  // ticking now−started while running.
  const now = useNow(1_000, startedMs != null && completedMs == null);
  const duration =
    startedMs != null ? fmtDuration((completedMs ?? now) - startedMs) : '—';

  const cells: Array<[string, React.ReactNode]> = [
    ['State', <StateBadge key="s" state={snapshot.state} />],
    [
      'Trigger',
      <TriggerCellValue key="t" snapshot={snapshot} onOpenContext={onOpenContext} />,
    ],
    [
      'Version',
      <Link
        key="v"
        className="font-mono text-sm underline-offset-2 hover:underline"
        to={`/workflows/${workflowId ?? snapshot.workflow_id}?version=${encodeURIComponent(snapshot.workflow_version)}&tab=definition`}
        title="Open this version's definition"
      >
        {snapshot.workflow_version}
      </Link>,
    ],
    ['Scheduled', <span key="sc">{fmtDate(snapshot.scheduled_at)}</span>],
    ['Started', <span key="sa">{fmtDate(snapshot.started_at)}</span>],
    ['Completed', <span key="ca">{fmtDate(snapshot.completed_at)}</span>],
    [
      'Duration',
      <span key="d" className="tabular-nums">
        {duration}
      </span>,
    ],
    [
      'Tasks',
      <span key="n" className="tabular-nums">
        {snapshot.completed_tasks} / {snapshot.task_count}
      </span>,
    ],
    ['Storage', <StorageIndicator key="st" storage={snapshot.storage} />],
  ];
  return (
    <div className="space-y-4">
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
      <TagsRow tags={snapshot.tags} />
      <ReplaysCard
        instanceId={instanceId}
        workflowId={workflowId}
        workflowFallback={snapshot.workflow_id}
      />
    </div>
  );
}

function TaskLogsPanel({
  workflowId,
  instanceId,
  taskId,
  taskName,
  onClose,
}: {
  workflowId: string;
  instanceId: string;
  taskId: string;
  taskName: string;
  onClose: () => void;
}) {
  const { data, isLoading, error, refetch } = useTaskLogs(workflowId, instanceId, taskId);
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <div>
          <CardTitle className="text-base">Logs · {taskName}</CardTitle>
          <CardDescription>
            Static snapshot — log streaming requires backend support.
          </CardDescription>
        </div>
        <Button variant="ghost" size="sm" onClick={onClose}>
          Close
        </Button>
      </CardHeader>
      <CardContent>
        {error ? (
          <QueryError error={error as Error} onRetry={() => refetch()} />
        ) : isLoading ? (
          <TableLoading rows={8} cols={1} />
        ) : !data?.logs ? (
          <EmptyState title="No logs available" />
        ) : (
          <pre className="max-h-[420px] overflow-auto rounded-md bg-muted/40 p-4 text-xs leading-relaxed">
            {data.logs}
          </pre>
        )}
      </CardContent>
    </Card>
  );
}

function TasksTab({
  snapshot,
  workflowId,
  instanceId,
}: {
  snapshot: InstanceSnapshot;
  workflowId: string | undefined;
  instanceId: string | undefined;
}) {
  const [openLogs, setOpenLogs] = useState<{ taskId: string; name: string } | null>(null);
  return (
    <>
      <InstanceTaskCards
        snapshot={snapshot}
        workflowId={workflowId}
        instanceId={instanceId}
        onLogs={(ti) => setOpenLogs({ taskId: ti.id, name: ti.name })}
      />

      {openLogs && workflowId && instanceId && (
        <div className="mt-4">
          <TaskLogsPanel
            workflowId={workflowId}
            instanceId={instanceId}
            taskId={openLogs.taskId}
            taskName={openLogs.name}
            onClose={() => setOpenLogs(null)}
          />
        </div>
      )}
    </>
  );
}

// Canonical vocabulary: a live materialisation of a workflow is an instance,
// not a run. Its display identity is the Run handle — the absolute scheduled
// timestamp — because no run counter exists and a hex id tells an operator
// nothing about when the instance fired.
export function InstanceDetailPage() {
  const { workflowId, instanceId } = useParams<{ workflowId: string; instanceId: string }>();

  // One polled query drives the whole page; the per-tab renderings below are
  // pure functions of this snapshot. Polling stops at terminal state.
  const inst = useInstanceSnapshot(instanceId);
  const [tab, setTab] = useState('overview');
  // The version picked in the patch-history timeline (Graph tab), lifted here so
  // the Code tab focuses the same patch. `null` = the live/current version.
  const [selectedVersion, setSelectedVersion] = useState<number | null>(null);
  // True when the user arrived on the Context tab via the Overview's
  // originating-signal-id link — the trigger grouping highlights once.
  const [contextFocus, setContextFocus] = useState(false);
  const openContextAtTrigger = () => {
    setContextFocus(true);
    setTab('context');
  };
  const changeTab = (next: string) => {
    setContextFocus(false);
    setTab(next);
  };

  const handle = formatRunHandle(inst.data ? runHandleSource(inst.data) : null);

  // "Cancel run" is a workflow-level cancel (no node narrowing) that resolves
  // the whole instance to the distinct terminal `Cancelled` outcome. Offered
  // only while the run is non-terminal (still live or scheduled).
  const cancelRun = useCancelRun(instanceId);
  const runState = inst.data ? normalizeState(inst.data.state) : undefined;
  const cancellable =
    !!inst.data && !['completed', 'failed', 'cancelled'].includes(runState ?? '');

  // "Instance missing" and "live store unreachable" are different facts and
  // render differently — a 503 must never read as "your instance is gone".
  const errorStatus = inst.error instanceof ApiError ? inst.error.status : undefined;

  return (
    <div className="space-y-6">
      <div className="space-y-1">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">{inst.data?.name ?? '—'}</h1>
          {inst.data && <StateBadge state={inst.data.state} />}
          {cancellable && (
            <Button
              variant="destructive"
              size="sm"
              className="ml-auto h-7 px-3 text-xs"
              disabled={cancelRun.isPending}
              onClick={() => cancelRun.mutate()}
            >
              {cancelRun.isPending ? 'Cancelling…' : 'Cancel run'}
            </Button>
          )}
        </div>
        {instanceId && (
          <span className="inline-flex items-center gap-1.5">
            <span className="font-mono text-xs text-muted-foreground">{instanceId}</span>
            <CopyButton value={instanceId} label="Copy instance id" />
            {handle && (
              <span className="text-xs text-muted-foreground tabular-nums">· {handle}</span>
            )}
          </span>
        )}
        {inst.data && <ReplaySubtitle snapshot={inst.data} workflowId={workflowId} />}
      </div>

      {errorStatus === 503 ? (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Control plane unreachable</CardTitle>
            <CardDescription>
              The live store could not be reached. This instance may still exist — retry once the
              control plane is back.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" size="sm" onClick={() => inst.refetch()}>
              Retry
            </Button>
          </CardContent>
        </Card>
      ) : errorStatus === 404 ? (
        <EmptyState
          title="Instance not found"
          description="No workflow instance matches this id in the archive or the live cluster."
        />
      ) : inst.error ? (
        <QueryError error={inst.error as Error} onRetry={() => inst.refetch()} />
      ) : null}

      <Tabs value={tab} onValueChange={changeTab} className="space-y-4">
        <TabsList>
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="tasks">Tasks</TabsTrigger>
          <TabsTrigger value="graph">Graph</TabsTrigger>
          <TabsTrigger value="timeline">Timeline</TabsTrigger>
          <TabsTrigger value="gates">Gates</TabsTrigger>
          <TabsTrigger value="events">Events</TabsTrigger>
          <TabsTrigger value="context">Context</TabsTrigger>
          <TabsTrigger value="code">Code</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          {inst.data ? (
            <OverviewTab
              snapshot={inst.data}
              workflowId={workflowId}
              instanceId={instanceId}
              onOpenContext={openContextAtTrigger}
            />
          ) : (
            !inst.error && <TableLoading rows={3} cols={3} />
          )}
        </TabsContent>

        <TabsContent value="tasks">
          {inst.data ? (
            <TasksTab snapshot={inst.data} workflowId={workflowId} instanceId={instanceId} />
          ) : (
            !inst.error && <TableLoading rows={6} cols={5} />
          )}
        </TabsContent>

        <TabsContent value="graph">
          {inst.data ? (
            <InstanceGraphTab
              snapshot={inst.data}
              selectedVersion={selectedVersion}
              onSelectVersion={setSelectedVersion}
            />
          ) : (
            !inst.error && <TableLoading rows={4} cols={3} />
          )}
        </TabsContent>
        <TabsContent value="timeline">
          {inst.data ? (
            <InstanceTimelineTab snapshot={inst.data} />
          ) : (
            !inst.error && <TableLoading rows={4} cols={3} />
          )}
        </TabsContent>
        <TabsContent value="gates">
          {inst.data ? (
            <InstanceGatesTab snapshot={inst.data} />
          ) : (
            !inst.error && <TableLoading rows={4} cols={5} />
          )}
        </TabsContent>
        <TabsContent value="events">
          <InstanceEventsTab instanceId={instanceId} active={tab === 'events'} />
        </TabsContent>
        <TabsContent value="context">
          {inst.data ? (
            <InstanceContextTab
              snapshot={inst.data}
              instanceId={instanceId}
              active={tab === 'context'}
              focusTrigger={contextFocus}
            />
          ) : (
            !inst.error && <TableLoading rows={4} cols={2} />
          )}
        </TabsContent>
        <TabsContent value="code">
          {inst.data ? (
            <InstanceCodeTab
              snapshot={inst.data}
              workflowId={workflowId}
              active={tab === 'code'}
              selectedVersion={selectedVersion}
            />
          ) : (
            !inst.error && <TableLoading rows={6} cols={1} />
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
