import { useEffect, useRef } from 'react';
import { KeyRound } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useInstanceContext } from '@/api/hooks';
import { ApiError, type CtxEntry, type InstanceSnapshot } from '@/api/client';
import { EmptyState, QueryError, TableLoading } from '@/components/QueryStates';

function EntryRow({ entry }: { entry: CtxEntry }) {
  return (
    <div className="rounded-md border border-border/60 p-2.5" data-ctx-entry={entry.name}>
      <div className="flex items-center gap-2 text-xs">
        <span className="font-mono font-medium">{entry.name}</span>
        <span className="text-muted-foreground">{entry.kind}</span>
        <span className="ml-auto text-muted-foreground">{entry.producer}</span>
      </div>
      <pre className="mt-1.5 whitespace-pre-wrap break-words rounded bg-muted/40 p-2 font-mono text-xs leading-relaxed">
        {entry.secret ? (
          <span className="inline-flex items-center gap-1.5 text-muted-foreground">
            <KeyRound size={12} aria-hidden /> •••••• (secret — masked)
          </span>
        ) : !entry.present ? (
          <span className="text-muted-foreground italic">absent — capture matched nothing</span>
        ) : typeof entry.value === 'string' ? (
          // Raw text so real newlines render as line breaks — JSON-encoding a
          // string escapes them to a literal `\n`.
          entry.value
        ) : (
          JSON.stringify(entry.value, null, 2)
        )}
      </pre>
    </div>
  );
}

function Grouping({
  title,
  description,
  entries,
  highlight,
}: {
  title: string;
  description: string;
  entries: CtxEntry[];
  highlight?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (highlight) ref.current?.scrollIntoView({ block: 'nearest' });
  }, [highlight]);
  return (
    <Card ref={ref} className={highlight ? 'ring-2 ring-ring' : undefined}>
      <CardHeader>
        <CardTitle className="text-base">{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-2">
        {entries.length === 0 ? (
          <p className="text-sm text-muted-foreground">No values in this scope.</p>
        ) : (
          entries.map((e) => <EntryRow key={e.name} entry={e} />)
        )}
      </CardContent>
    </Card>
  );
}

/**
 * The Context tab: the run's tickr-ctx scope in run / trigger / gate
 * groupings. Fetches on tab focus; re-polls on the page cadence while the
 * instance is live. `focusTrigger` (the Overview's originating-signal-id
 * deep-link) highlights the trigger grouping on arrival.
 */
export function InstanceContextTab({
  snapshot,
  instanceId,
  active,
  focusTrigger,
}: {
  snapshot: InstanceSnapshot;
  instanceId: string | undefined;
  active: boolean;
  focusTrigger: boolean;
}) {
  const live = snapshot.storage === 'live' && snapshot.completed_at == null;
  const ctx = useInstanceContext(instanceId, { enabled: active, live });

  if (ctx.error) {
    const status = ctx.error instanceof ApiError ? ctx.error.status : undefined;
    if (status === 503) {
      return (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Context store unreachable</CardTitle>
            <CardDescription>
              The ctx store could not be reached — this is not the same as the run having no
              values. Retry once the data plane is back.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" size="sm" onClick={() => ctx.refetch()}>
              Retry
            </Button>
          </CardContent>
        </Card>
      );
    }
    return <QueryError error={ctx.error as Error} onRetry={() => ctx.refetch()} />;
  }
  if (!ctx.data) return <TableLoading rows={4} cols={2} />;

  const { run, trigger, gates } = ctx.data;
  const empty = run.length === 0 && trigger.length === 0 && gates.length === 0;

  return (
    <div className="space-y-4">
      {empty ? (
        <EmptyState
          title="No context values"
          description="Nothing has been published to this run's tickr-ctx scope yet."
        />
      ) : (
        <>
          <Grouping
            title="Run scope"
            description="Task outputs published to the run's shared namespace."
            entries={run}
          />
          <Grouping
            title="Trigger scope"
            description="Captures extracted from the triggering signal's payload."
            entries={trigger}
            highlight={focusTrigger}
          />
          {gates.map((g) => (
            <Grouping
              key={g.signal_id}
              title={`Gate scope · ${g.signal_id.slice(0, 8)}`}
              description="Captures from the wakeup that satisfied this signal gate."
              entries={g.entries}
            />
          ))}
        </>
      )}
    </div>
  );
}
