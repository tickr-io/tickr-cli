import { useMemo, useState } from 'react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import { Check, Copy, CalendarDays, X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Skeleton } from '@/components/ui/skeleton';
import { useWorkflowCalendar, useWorkflowDetail, useWorkflowInstances } from '@/api/hooks';
import { ApiError } from '@/api/client';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import { MetaGridHeader } from '@/components/MetaGridHeader';
import { StateBadge } from '@/components/StateBadge';
import { InstancesTable } from '@/components/InstancesTable';
import { RunCalendar } from '@/components/RunCalendar';
import { DefinitionTab } from '@/components/DefinitionTab';
import { TaskGraphTab } from '@/components/TaskGraphTab';

const cx = (...a: (string | false | null | undefined)[]) => a.filter(Boolean).join(' ');

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

/** Seed the calendar's initial view from the workflow's latest fired run, so a
 *  workflow dormant since a prior year opens on its latest active month with
 *  zero clicks. Falls back to the current month for a never-run workflow. */
function deriveInitialView(latestRunAt: string | null | undefined): { year: number; month: number } {
  if (latestRunAt) {
    const d = new Date(latestRunAt);
    if (!Number.isNaN(d.getTime())) return { year: d.getFullYear(), month: d.getMonth() };
  }
  const now = new Date();
  return { year: now.getFullYear(), month: now.getMonth() };
}

/** The run calendar, co-located above the run list it filters. Month steps are a
 *  client slice over the fetched year; a year step refetches via the hook. A
 *  never-run workflow (no fired run and no day history) shows a placeholder. */
function CalendarSection({
  workflowId,
  tz,
  latestRunAt,
  selectedDate,
  onDayClick,
}: {
  workflowId: string;
  tz: string;
  latestRunAt: string | null | undefined;
  selectedDate?: string | null;
  onDayClick: (date: string | null) => void;
}) {
  const [view, setView] = useState(() => deriveInitialView(latestRunAt));
  const calendar = useWorkflowCalendar(workflowId, view.year, tz);
  const days = calendar.data?.days ?? [];

  // "No runs yet" applies only to a genuinely never-run workflow: no fired run
  // to seed a year, and the landing year carries no day history. A later
  // navigation to an empty year still renders the grid so the user can step back.
  const neverRun = !latestRunAt && days.length === 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Run calendar</CardTitle>
        <CardDescription>One cell per day, coloured by outcome. Click a day to filter the Instances tab to that date.</CardDescription>
      </CardHeader>
      <CardContent>
        {calendar.isLoading ? (
          <Skeleton className="h-48 w-full max-w-[320px] rounded-md" />
        ) : neverRun ? (
          <EmptyState
            title="No runs yet"
            description="This workflow has not fired. Its run history will appear here once it executes."
          />
        ) : (
          <RunCalendar
            days={days}
            year={view.year}
            month={view.month}
            selectedDate={selectedDate ?? null}
            onDayClick={onDayClick}
            onNavigate={setView}
          />
        )}
      </CardContent>
    </Card>
  );
}

/** The run list — owns the Live-only toggle; the card head carries the title, a
 *  dynamic description, and the live-only / date-filter chip. */
function RunListCard({
  workflowId,
  date,
  tz,
  onClearDate,
}: {
  workflowId: string;
  date?: string;
  tz: string;
  onClearDate: () => void;
}) {
  const [liveOnly, setLiveOnly] = useState(false);
  const { data, isLoading, error, refetch } = useWorkflowInstances(workflowId, { date, tz });
  const count = data?.length ?? 0;

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3 space-y-0">
        <div className="space-y-1.5">
          <CardTitle className="text-base">Instance runs</CardTitle>
          <CardDescription>
            {date
              ? `Runs on ${date} — ${count} found.`
              : `${count} runs — newest first. Live and archived runs are shown together.`}
          </CardDescription>
        </div>
        {date ? (
          <button
            className="drill-chip active"
            style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}
            onClick={onClearDate}
            title="Clear date filter"
          >
            <CalendarDays size={13} aria-hidden />
            {date}
            <X size={13} aria-hidden />
          </button>
        ) : (
          <button className={cx('drill-chip', liveOnly && 'active')} onClick={() => setLiveOnly((v) => !v)}>
            Live only
          </button>
        )}
      </CardHeader>
      <CardContent className="p-0">
        {error ? (
          <div className="p-4">
            <QueryError error={error as Error} onRetry={() => refetch()} />
          </div>
        ) : isLoading ? (
          <div className="p-4">
            <TableLoading rows={6} cols={6} />
          </div>
        ) : count === 0 ? (
          <div className="p-6">
            <EmptyState
              title={date ? 'No runs on this day' : 'No instances yet'}
              description={
                date
                  ? 'Clear the date filter to see the full history.'
                  : 'Instances of this workflow will appear here once they execute.'
              }
            />
          </div>
        ) : (
          <InstancesTable workflowId={workflowId} instances={data!} liveOnly={liveOnly} />
        )}
      </CardContent>
    </Card>
  );
}

export function WorkflowDetailPage() {
  const { workflowId } = useParams<{ workflowId: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const tz = useMemo(() => Intl.DateTimeFormat().resolvedOptions().timeZone, []);

  const version = searchParams.get('version') ?? undefined;
  const tab = searchParams.get('tab') ?? 'overview';
  const date = searchParams.get('date') ?? undefined;

  const { data, isLoading, error, refetch } = useWorkflowDetail(
    workflowId,
    version != null ? Number(version) : undefined,
  );

  const setParams = (changes: Record<string, string | null>) => {
    const next = new URLSearchParams(searchParams);
    for (const [k, v] of Object.entries(changes)) {
      if (v === null) next.delete(k);
      else next.set(k, v);
    }
    setSearchParams(next);
  };

  const name = (data?.workflow_definition as Record<string, unknown> | undefined)?.name as
    | string
    | undefined;
  const is404 = error instanceof ApiError && error.status === 404;

  return (
    <div className="stack space-y-6">
      {/* Thin persistent header (shared with the instance shell): name, a badge
          bound to the workflow-aggregate latest-run state, the namespace.slug
          identity, and a copyable UUID. */}
      <div className="space-y-1">
        <div className="flex items-center gap-3">
          <h1 className="text-2xl font-semibold tracking-tight">{name ?? 'Workflow'}</h1>
          {data?.latest_run_state && <StateBadge state={data.latest_run_state} />}
        </div>
        {data && (
          <p className="mono text-xs text-muted-foreground">
            {data.namespace}.{data.slug}
          </p>
        )}
        {workflowId && (
          <span className="inline-flex items-center gap-1.5">
            <span className="mono text-xs text-muted-foreground">{workflowId}</span>
            <CopyButton value={workflowId} label="Copy workflow id" />
          </span>
        )}
      </div>

      {is404 ? (
        <div className="space-y-4">
          <EmptyState
            title="Workflow not found"
            description="No workflow matches this id (or version). It may have been removed, or the link is stale."
          />
          <button onClick={() => navigate('/workflows')} className="text-sm text-primary hover:underline">
            ← Back to workflows
          </button>
        </div>
      ) : error ? (
        <QueryError error={error as Error} onRetry={() => refetch()} />
      ) : isLoading || !data ? (
        <Skeleton className="h-28 w-full rounded-lg" />
      ) : (
        <Tabs value={tab} onValueChange={(v) => setParams({ tab: v })}>
          <TabsList>
            <TabsTrigger value="overview">Overview</TabsTrigger>
            <TabsTrigger value="instances">Instances</TabsTrigger>
            <TabsTrigger value="definition">Definition</TabsTrigger>
            <TabsTrigger value="taskgraph">Task graph</TabsTrigger>
          </TabsList>

          <TabsContent value="overview" className="mt-4 space-y-6">
            <MetaGridHeader
              detail={data}
              hasExplicitVersion={!!version}
              onVersionChange={(v) => setParams({ version: String(v) })}
            />
            {workflowId && (
              <CalendarSection
                workflowId={workflowId}
                tz={tz}
                latestRunAt={data.latest_run_at}
                selectedDate={date ?? null}
                onDayClick={(d) => setParams(d ? { date: d, tab: 'instances' } : { date: null })}
              />
            )}
          </TabsContent>

          <TabsContent value="instances" className="mt-4 space-y-6">
            {workflowId && (
              <RunListCard
                workflowId={workflowId}
                date={date}
                tz={tz}
                onClearDate={() => setParams({ date: null })}
              />
            )}
          </TabsContent>

          <TabsContent value="definition" className="mt-4">
            <DefinitionTab detail={data} />
          </TabsContent>

          <TabsContent value="taskgraph" className="mt-4">
            <TaskGraphTab definition={data.workflow_definition as Record<string, unknown>} />
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}
