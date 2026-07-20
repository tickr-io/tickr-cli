import { KeyRound } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { useInstanceContext } from '@/api/hooks';
import { ApiError, type InstanceSnapshot, type SnapshotTaskInstance } from '@/api/client';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import { InputChip } from '@/components/InstanceTaskCards';
import { taskDefFor, taskCtxSlice, type TaskCtxInput, type TaskCtxOutput } from '@/lib/taskSlice';

function ValueBlock({ entry }: { entry: TaskCtxInput['entry'] }) {
  if (!entry) {
    return (
      <pre className="mt-1.5 rounded bg-muted/40 p-2 font-mono text-xs leading-relaxed">
        <span className="italic text-muted-foreground">awaiting — not yet produced</span>
      </pre>
    );
  }
  if (!entry.present) {
    return (
      <pre className="mt-1.5 rounded bg-muted/40 p-2 font-mono text-xs leading-relaxed">
        <span className="italic text-muted-foreground">absent — capture matched nothing</span>
      </pre>
    );
  }
  return (
    <pre className="mt-1.5 whitespace-pre-wrap break-words rounded bg-muted/40 p-2 font-mono text-xs leading-relaxed">
      {typeof entry.value === 'string'
        ? // Raw text so real newlines render as line breaks — JSON-encoding a
          // string escapes them to a literal `\n`.
          entry.value
        : JSON.stringify(entry.value, null, 2)}
    </pre>
  );
}

function InputRow({ input }: { input: TaskCtxInput }) {
  return (
    <div className="rounded-md border border-border/60 p-2.5" data-ctx-entry={input.name}>
      <div className="flex items-center gap-2 text-xs">
        <InputChip input={{ name: input.name, source: input.source }} />
        {input.entry && <span className="ml-auto text-muted-foreground">{input.entry.producer}</span>}
      </div>
      <ValueBlock entry={input.entry} />
    </div>
  );
}

function OutputRow({ output }: { output: TaskCtxOutput }) {
  return (
    <div className="rounded-md border border-border/60 p-2.5" data-ctx-entry={output.name}>
      <div className="flex items-center gap-2 text-xs">
        <span className="font-mono font-medium">{output.name}</span>
        {output.isRoutingVar && (
          <span className="rounded bg-muted/60 px-1.5 py-0.5 text-[10px] uppercase tracking-wide text-muted-foreground">
            routing var
          </span>
        )}
        {output.entry && (
          <span className="ml-auto text-muted-foreground">{output.entry.kind}</span>
        )}
      </div>
      <ValueBlock entry={output.entry} />
    </div>
  );
}

/**
 * The Context tab: this task's slice of the run's tickr-ctx scope — Inputs
 * (declared names with their source chips) and Outputs (declared names
 * including routing variables) — projected client-side from the same
 * instance Context endpoint the instance page uses (shared query key, never
 * a second source of truth). Secrets render as key names only; secret
 * values never render. This is the task's I/O — there is no separate I/O
 * tab.
 */
export function TaskContextTab({
  snapshot,
  taskInstance,
  instanceId,
  active,
}: {
  snapshot: InstanceSnapshot;
  taskInstance: SnapshotTaskInstance;
  instanceId: string | undefined;
  active: boolean;
}) {
  const live = snapshot.storage === 'live' && snapshot.completed_at == null;
  const ctx = useInstanceContext(instanceId, { enabled: active, live });
  const def = taskDefFor(snapshot, taskInstance);

  if (!def) {
    return (
      <EmptyState
        title="No task definition"
        description="The snapshot holds no definition for this task — its declared inputs and outputs are unknown."
      />
    );
  }

  if (ctx.error) {
    const status = ctx.error instanceof ApiError ? ctx.error.status : undefined;
    if (status === 503) {
      return (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Context store unreachable</CardTitle>
            <CardDescription>
              The ctx store could not be reached — this is not the same as the task having no
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

  const slice = taskCtxSlice(def, ctx.data);
  const empty =
    slice.inputs.length === 0 && slice.outputs.length === 0 && slice.secretNames.length === 0;

  if (empty) {
    return (
      <EmptyState
        title="No declared context"
        description="This task declares no inputs, outputs, or secrets — nothing flows through tickr-ctx for it."
      />
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Inputs</CardTitle>
          <CardDescription>
            The keys this task reads, each with its declared source.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-2">
          {slice.inputs.length === 0 ? (
            <p className="text-sm text-muted-foreground">No declared inputs.</p>
          ) : (
            slice.inputs.map((i) => <InputRow key={i.name} input={i} />)
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Outputs</CardTitle>
          <CardDescription>
            The keys this task writes — including its routing variables.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-2">
          {slice.outputs.length === 0 ? (
            <p className="text-sm text-muted-foreground">No declared outputs.</p>
          ) : (
            slice.outputs.map((o) => <OutputRow key={o.name} output={o} />)
          )}
        </CardContent>
      </Card>

      {slice.secretNames.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Secrets</CardTitle>
            <CardDescription>Key names only — secret values never render.</CardDescription>
          </CardHeader>
          <CardContent className="flex flex-wrap gap-2">
            {slice.secretNames.map((name) => (
              <span
                key={name}
                className="inline-flex items-center gap-1.5 rounded-md border border-border bg-muted/40 px-2 py-0.5 font-mono text-xs text-muted-foreground"
              >
                <KeyRound size={12} aria-hidden />
                {name}
              </span>
            ))}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
