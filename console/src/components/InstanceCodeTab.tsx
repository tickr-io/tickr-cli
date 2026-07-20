import { useEffect, useRef } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { useWorkflowDetail, usePatchSource } from '@/api/hooks';
import { ApiError, type AppliedPatchView, type InstanceSnapshot, type PatchOpView } from '@/api/client';
import { EmptyState, QueryError, TableLoading } from '@/components/QueryStates';
import { SyntaxBlock } from '@/components/SyntaxBlock';

function fmtDate(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** A short, readable form of a UUID-ish id for the lowered-op list. The full id
 * rides the `title`, so nothing is lost — this only trims the visible width. */
function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…` : id;
}

/** One lowered primitive op, phrased for reading: the verb plus the structures
 * it touched. This is *what the patch did* to the live graph — the effect the
 * server recorded, beside the intent the author wrote. */
function LoweredOp({ op }: { op: PatchOpView }) {
  const idTag = (id: string) => (
    <span className="font-mono text-xs text-foreground" title={id}>
      {shortId(id)}
    </span>
  );
  let detail: React.ReactNode = null;
  switch (op.op) {
    case 'AddNode':
    case 'RemoveNode':
      detail = op.node_id ? idTag(op.node_id) : null;
      break;
    case 'AddEdge':
      detail = (
        <span className="inline-flex flex-wrap items-center gap-1">
          {op.sources.map((s) => idTag(s))}
          <span className="text-muted-foreground">→</span>
          {op.targets.map((t) => idTag(t))}
        </span>
      );
      break;
    case 'RemoveEdge':
      detail = op.edge_id ? idTag(op.edge_id) : null;
      break;
  }
  return (
    <li className="flex items-center gap-2 py-1">
      <Badge variant="outline" className="font-mono text-[11px]">
        {op.op}
      </Badge>
      {detail}
    </li>
  );
}

/**
 * One applied patch, rendered as the two reading halves the code tab joins: the
 * **authored patch** (what the operator submitted, fetched verbatim from the
 * Conductor's retention path — Nickel for an external patch, the JSON document
 * for a self-patch) under a provenance/reason/timestamp header, and **what it
 * did** (the server's lowered primitive ops). The two are joined by the patch
 * key / version, so intent and effect sit side by side.
 */
function AppliedPatchSection({ patch, focused }: { patch: AppliedPatchView; focused: boolean }) {
  const source = usePatchSource(patch.patch_key);
  // When the timeline (Graph tab) selects this version, bring its section into
  // view and ring it — the shared selection joins the two reading surfaces.
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (focused) ref.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }, [focused]);
  // Nickel highlights through the shared block; a self-patch JSON document is
  // shown as pretty-printed monospace (pretty-printed only if it parses — a raw
  // submission is never reshaped, so an unparseable body is shown as-is).
  const isNickel = source.data?.source_format === 'nickel';
  let body: React.ReactNode;
  if (source.isLoading) {
    body = <TableLoading rows={4} cols={1} />;
  } else if (source.error) {
    const status = source.error instanceof ApiError ? source.error.status : undefined;
    body =
      status === 404 ? (
        <EmptyState
          title="Authored source unavailable"
          description="The Conductor retained no source for this patch."
        />
      ) : (
        <QueryError error={source.error as Error} onRetry={() => source.refetch()} />
      );
  } else if (!source.data?.source) {
    body = (
      <EmptyState
        title="Authored source unavailable"
        description="The Conductor retained no source for this patch."
      />
    );
  } else if (isNickel) {
    body = <SyntaxBlock code={source.data.source} />;
  } else {
    let text = source.data.source;
    try {
      text = JSON.stringify(JSON.parse(text), null, 2);
    } catch {
      // Not JSON — render the verbatim submission untouched.
    }
    body = <pre className="pre">{text}</pre>;
  }

  return (
    <Card ref={ref} data-patch-version={patch.version} data-focused={focused ? 'true' : undefined}
      className={focused ? 'ring-2 ring-primary/60' : undefined}>
      <CardHeader className="space-y-2">
        <div className="flex flex-wrap items-center gap-2">
          <CardTitle className="text-base">
            Patch → version <span className="font-mono font-normal">{patch.version}</span>
          </CardTitle>
          <Badge variant={patch.provenance === 'self' ? 'info' : 'secondary'}>
            {patch.provenance === 'self' ? 'self-patch' : 'external'}
          </Badge>
          <span className="text-xs text-muted-foreground tabular-nums">
            v{patch.prior_version} → v{patch.version} · {fmtDate(patch.applied_at)}
          </span>
        </div>
        {patch.reason && <CardDescription>{patch.reason}</CardDescription>}
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Authored patch{' '}
            {source.data?.source_format && (
              <span className="font-mono normal-case">· {source.data.source_format}</span>
            )}
          </div>
          {body}
        </div>
        <div className="space-y-2">
          <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            What it did · lowered ops
          </div>
          {patch.ops.length === 0 ? (
            <p className="text-sm text-muted-foreground">No primitive ops recorded.</p>
          ) : (
            <ul className="rounded-md border border-border bg-muted/20 px-3 py-1">
              {patch.ops.map((op, i) => (
                <LoweredOp key={i} op={op} />
              ))}
            </ul>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

/**
 * The Code tab: for this instance's evolution, three stacked regions in reading
 * order — the **base Nickel** (the definition the instance bound, collapsed by
 * default so a patched graph leads with what changed), then, per applied patch,
 * the **authored patch** and **what it did**. Base source is fetched once per
 * instance by `(workflow_id, bound version)` from the registration archive; each
 * patch's authored source is fetched by patch key from the Conductor. A missing
 * base source renders an honest empty state naming the version it looked for.
 */
export function InstanceCodeTab({
  snapshot,
  workflowId,
  active,
  selectedVersion = null,
}: {
  snapshot: InstanceSnapshot;
  workflowId: string | undefined;
  active: boolean;
  /** The version picked in the Graph tab's patch-history timeline — its section
   * scrolls into view and rings. `null` = no focus. */
  selectedVersion?: number | null;
}) {
  const boundVersion = snapshot.workflow_version;
  const detail = useWorkflowDetail(active ? workflowId : undefined, boundVersion, {
    staleTime: Infinity, // the bound version's source is immutable — fetch once
  });
  const patches = snapshot.applied_patches ?? [];

  if (!active) return null;
  if (detail.isLoading) return <TableLoading rows={6} cols={1} />;

  let baseRegion: React.ReactNode;
  if (detail.error) {
    const status = detail.error instanceof ApiError ? detail.error.status : undefined;
    baseRegion =
      status === 404 ? (
        <EmptyState
          title={`No source for version ${boundVersion}`}
          description="The registration archive has no Nickel source row for this instance's bound version. Showing another version's source would be a lie, so nothing is shown."
        />
      ) : (
        <QueryError error={detail.error as Error} onRetry={() => detail.refetch()} />
      );
  } else if (!detail.data || !detail.data.nickel_source) {
    baseRegion = (
      <EmptyState
        title={`No source for version ${boundVersion}`}
        description="The registration archive has no Nickel source row for this instance's bound version."
      />
    );
  } else {
    // Collapsed by default: on a patched instance the interesting reading is the
    // patch stack below, so the base definition folds away until asked for.
    baseRegion = (
      <details open={patches.length === 0}>
        <summary className="cursor-pointer text-sm text-muted-foreground hover:text-foreground">
          Base definition · <span className="font-mono">{boundVersion}</span>
        </summary>
        <div className="mt-3">
          <SyntaxBlock code={detail.data.nickel_source} />
        </div>
      </details>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">
            Base Nickel · <span className="font-mono font-normal">{boundVersion}</span>
          </CardTitle>
          <CardDescription>
            The authored bytes this instance bound — not necessarily the latest registration.
            {patches.length > 0 && ' The patches below reshaped it live.'}
          </CardDescription>
        </CardHeader>
        <CardContent>{baseRegion}</CardContent>
      </Card>

      {patches.map((p) => (
        <AppliedPatchSection
          key={`${p.patch_key}-${p.version}`}
          patch={p}
          focused={p.version === selectedVersion}
        />
      ))}
    </div>
  );
}
