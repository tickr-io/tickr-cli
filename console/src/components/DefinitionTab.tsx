import { useState } from 'react';
import { Radio, ArrowDownLeft, ArrowUpRight, Braces, Key } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { SyntaxBlock } from '@/components/SyntaxBlock';
import { TriggerCell } from './TriggerCell';
import { routingVarNames } from '@/lib/routingVars';
import { taskTypeView } from '@/lib/taskType';
import type { WorkflowDetail, Trigger } from '@/api/client';

interface TaskView {
  id?: string;
  name?: string;
  nix_expression_path?: string;
  task_type?: string;
  max_attempts?: number;
  timeout_secs?: number | null;
  inputs?: string[];
  outputs?: string[];
  secrets?: string[];
  emits?: unknown[];
  // Object-shaped on the wire (`{ name, var_type }[]`); never read inline —
  // always projected to names through `routingVarNames` so a decl object can't
  // reach a React child.
  routing_vars?: unknown[];
}

function emitLabel(e: unknown): string {
  if (typeof e === 'string') return e;
  const o = e as { signal?: string; kind?: string };
  return `${o?.signal ?? 'emit'}${o?.kind === 'on-failure' ? ' (on fail)' : ''}`;
}

function TaskDefCard({ t }: { t: TaskView }) {
  const tt = taskTypeView(t.task_type);
  const routingVars = routingVarNames(t.routing_vars);
  const hasIO =
    (t.inputs?.length ?? 0) +
      (t.outputs?.length ?? 0) +
      (t.secrets?.length ?? 0) +
      routingVars.length +
      (t.emits?.length ?? 0) >
    0;
  return (
    <div className="taskdef">
      <div className="taskdef-head">
        <span className={`taskdef-type ${tt.cls}`} title={tt.title}>{tt.label}</span>
        <span className="taskdef-name">{t.name}</span>
        <span className="taskdef-meta tabular-nums">
          {t.max_attempts ?? 1} attempt{(t.max_attempts ?? 1) === 1 ? '' : 's'}
          {t.timeout_secs != null ? ` · ${t.timeout_secs}s` : ''}
        </span>
      </div>
      <div className="taskdef-nix mono">{t.nix_expression_path}</div>
      {hasIO && (
        <div className="taskdef-io">
          {t.inputs?.map((x) => (
            <span key={`i${x}`} className="io-chip io-in"><ArrowDownLeft size={12} />{x}</span>
          ))}
          {t.outputs?.map((x) => (
            <span key={`o${x}`} className="io-chip io-out"><ArrowUpRight size={12} />{x}</span>
          ))}
          {routingVars.map((v) => (
            <span key={`r${v}`} className="io-chip io-route"><Braces size={12} />{v}</span>
          ))}
          {t.secrets?.map((x) => (
            <span key={`s${x}`} className="io-chip io-secret"><Key size={12} />{x}</span>
          ))}
          {t.emits?.map((e, i) => (
            <span key={`e${i}`} className="io-chip io-emit"><Radio size={12} />{emitLabel(e)}</span>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * The Definition tab (DC-0005): a compact meta grid (trigger · status · build ·
 * version · timeout), tags, and a flat list of per-task cards, plus a
 * head-anchored **View Nickel source** toggle revealing the exact
 * author-submitted source. Matches the kit's `ov-grid` / `taskdef` / `io-chip`
 * structure; the trigger reuses the shared compact `TriggerCell`.
 */
export function DefinitionTab({ detail }: { detail: WorkflowDetail }) {
  const [showSrc, setShowSrc] = useState(false);
  const def = detail.workflow_definition as Record<string, unknown>;

  const trigger = def.trigger as Trigger | undefined;
  const status = (def.status as string | undefined) ?? '';
  const timeoutSecs = (def.timeout_secs as number | null) ?? null;
  const tags = (def.tags as Record<string, string> | undefined) ?? {};
  const tasks = Object.values((def.tasks as Record<string, TaskView> | undefined) ?? {});
  const build = detail.available_versions.find((v) => v.version === detail.version)?.status;
  const buildReady = build === 'Ready' || build === 'Submitted';

  return (
    <div className="space-y-4">
      <div className="ov-grid">
        <div className="ov-cell">
          <div className="ov-k">Trigger</div>
          <div className="ov-v">
            {trigger ? <TriggerCell trigger={trigger} /> : <span className="muted">—</span>}
          </div>
        </div>
        <div className="ov-cell">
          <div className="ov-k">Status</div>
          <div className="ov-v">
            {status.toLowerCase() === 'active' ? (
              <Badge variant="success">active</Badge>
            ) : (
              <Badge variant="secondary">inactive</Badge>
            )}
          </div>
        </div>
        <div className="ov-cell">
          <div className="ov-k">Build</div>
          <div className="ov-v">
            {buildReady ? <span className="muted">ready</span> : <Badge variant="destructive">{build ?? 'building'}</Badge>}
          </div>
        </div>
        <div className="ov-cell">
          <div className="ov-k">Version</div>
          <div className="ov-v mono">v{detail.version}</div>
        </div>
        <div className="ov-cell">
          <div className="ov-k">Timeout</div>
          <div className="ov-v">{timeoutSecs != null ? `${timeoutSecs}s` : <span className="muted">none</span>}</div>
        </div>
      </div>

      {Object.keys(tags).length > 0 && (
        <div>
          <div className="ov-k" style={{ marginBottom: 6 }}>Tags</div>
          <div className="def-tags">
            {Object.entries(tags).map(([k, v]) => (
              <span key={k} className="tag-chip">
                <span className="tag-k">{k}</span>
                {v}
              </span>
            ))}
          </div>
        </div>
      )}

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <div className="space-y-1.5">
            <CardTitle className="text-base">Tasks</CardTitle>
            <CardDescription>{tasks.length} tasks — each names a Nix derivation.</CardDescription>
          </div>
          <Button variant="outline" size="sm" onClick={() => setShowSrc((v) => !v)}>
            {showSrc ? 'Hide source' : 'View Nickel source'}
          </Button>
        </CardHeader>
        <CardContent>
          {showSrc ? (
            <SyntaxBlock code={detail.nickel_source} />
          ) : (
            <div className="taskdef-list">
              {tasks.map((t, i) => (
                <TaskDefCard key={t.id ?? i} t={t} />
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
