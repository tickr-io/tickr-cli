import { ArrowDownLeft, ArrowUpRight, Key, Radio, Zap } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { StateBadge } from '@/components/StateBadge';
import { killConfirmationLabel } from '@/api/normalize';
import { taskTypeView } from '@/lib/taskType';
import type { InstanceSnapshot, SnapshotTaskDef, SnapshotTaskInstance } from '@/api/client';

/**
 * One input chip with its provenance affordance: a source-kind icon for
 * signal-bound, trigger-bound, and upstream-task-bound inputs; bare ambient
 * names render plain. Identical on started and not-started cards, so "where
 * is this input supposed to come from" is answerable before a run.
 */
export function InputChip({ input }: { input: SnapshotTaskDef['inputs'][number] }) {
  const src = input.source;
  let icon = null;
  let title: string | undefined;
  if (src?.kind === 'task') {
    icon = <ArrowDownLeft size={12} aria-hidden />;
    title = `from upstream task ${src.task ?? ''}`.trim();
  } else if (src?.kind === 'trigger') {
    icon = <Zap size={12} aria-hidden />;
    title = 'from the triggering signal';
  } else if (src?.kind === 'signal') {
    icon = <Radio size={12} aria-hidden />;
    title = `from signal gate ${src.signal_name ?? ''}`.trim();
  }
  return (
    <span className="io-chip io-in" title={title}>
      {icon}
      {input.name}
    </span>
  );
}

/**
 * One task card: definition facts plus the per-instance overlay (state badge
 * and attempts-used). Tasks whose instance was never minted render as
 * neutral "not started" cards from the topology, so the tab always shows the
 * complete workflow shape.
 */
function InstanceTaskCard({
  def,
  current,
  onOpen,
  onLogs,
}: {
  def: SnapshotTaskDef;
  current: SnapshotTaskInstance | undefined;
  onOpen: (() => void) | undefined;
  onLogs: (() => void) | undefined;
}) {
  const tt = taskTypeView(def.task_type);
  const hasIO =
    def.inputs.length + def.outputs.length + def.secrets.length + def.emits.length > 0;
  return (
    <div
      className={`taskdef ${onOpen ? 'cursor-pointer transition-colors hover:border-foreground/30' : ''}`}
      onClick={onOpen}
      role={onOpen ? 'link' : undefined}
    >
      <div className="taskdef-head">
        <span className={`taskdef-type ${tt.cls}`} title={tt.title}>{tt.label}</span>
        <span className="taskdef-name">{def.name}</span>
        {current ? (
          <StateBadge state={current.state} reason={current.cancel_reason} />
        ) : (
          <Badge variant="outline">not started</Badge>
        )}
        {current && killConfirmationLabel(current.kill_confirmation) && (
          <Badge variant="outline" className="text-xs">
            {killConfirmationLabel(current.kill_confirmation)}
          </Badge>
        )}
        <span className="taskdef-meta tabular-nums">
          {current != null
            ? `${current.attempt + 1} / ${def.max_attempts} attempts`
            : `${def.max_attempts} attempt${def.max_attempts === 1 ? '' : 's'}`}
          {def.timeout_secs != null ? ` · ${def.timeout_secs}s` : ''}
        </span>
        {onLogs && (
          <Button
            variant="ghost"
            size="sm"
            className="h-6 px-2 text-xs"
            onClick={(e) => {
              e.stopPropagation();
              onLogs();
            }}
          >
            Logs
          </Button>
        )}
      </div>
      <div className="taskdef-nix mono">{def.nix_expression_path}</div>
      {hasIO && (
        <div className="taskdef-io">
          {def.inputs.map((input) => (
            <InputChip key={`i${input.name}`} input={input} />
          ))}
          {def.outputs.map((x) => (
            <span key={`o${x}`} className="io-chip io-out">
              <ArrowUpRight size={12} aria-hidden />
              {x}
            </span>
          ))}
          {def.secrets.map((x) => (
            <span key={`s${x}`} className="io-chip io-secret">
              <Key size={12} aria-hidden />
              {x}
            </span>
          ))}
          {def.emits.map((e, i) => (
            <span key={`e${i}`} className="io-chip io-emit">
              <Radio size={12} aria-hidden />
              {e.signal_name}
              {e.kind === 'on_failure' ? ' (on fail)' : ''}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * The Tasks tab body: one rich card per task definition in the snapshot's
 * tasks definition map, overlaid with the latest attempt's instance state.
 * Reads only the polled snapshot — no extra fetches.
 */
export function InstanceTaskCards({
  snapshot,
  workflowId,
  instanceId,
  onLogs,
}: {
  snapshot: InstanceSnapshot;
  workflowId: string | undefined;
  instanceId: string | undefined;
  onLogs: (taskInstance: SnapshotTaskInstance) => void;
}) {
  const navigate = useNavigate();
  // Latest attempt per task definition — earlier attempts stay reachable
  // through the Timeline; the card overlays the current truth.
  const currentByTask = new Map<string, SnapshotTaskInstance>();
  for (const ti of snapshot.task_instances) {
    const seen = currentByTask.get(ti.task_id);
    if (!seen || ti.attempt > seen.attempt) currentByTask.set(ti.task_id, ti);
  }
  return (
    <div className="taskdef-list">
      {snapshot.tasks.map((def) => {
        const current = currentByTask.get(def.id);
        return (
          <InstanceTaskCard
            key={def.id}
            def={def}
            current={current}
            onOpen={
              current && workflowId && instanceId
                ? () => navigate(`/workflows/${workflowId}/instances/${instanceId}/tasks/${current.id}`)
                : undefined
            }
            onLogs={current ? () => onLogs(current) : undefined}
          />
        );
      })}
      {snapshot.tasks.length === 0 && (
        <div className="text-sm text-muted-foreground">This workflow declares no tasks.</div>
      )}
    </div>
  );
}
