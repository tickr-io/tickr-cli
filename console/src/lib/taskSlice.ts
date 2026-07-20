/**
 * Pure selectors projecting one TaskInstance's slice out of the instance
 * snapshot. The Task instance detail page is one Attempt of one task —
 * everything it shows is derived from the same snapshot the instance page
 * polls, never fetched separately (no second source of truth).
 */
import type {
  CtxEntry,
  InstanceContext,
  InstanceSnapshot,
  SnapshotTaskDef,
  SnapshotTaskInstance,
} from '@/api/client';
import { normalizeState } from '@/api/normalize';

/** The snapshot row for this page's TaskInstance, or null when the id isn't
 * in the snapshot (stale deep link, or a task node never minted). */
export function findTaskInstance(
  snapshot: InstanceSnapshot,
  taskInstanceId: string,
): SnapshotTaskInstance | null {
  return snapshot.task_instances.find((ti) => ti.id === taskInstanceId) ?? null;
}

/**
 * Every Attempt of the same task within this instance — the same `task_id`,
 * each its own TaskInstance — ordered by attempt. Always includes the given
 * instance itself.
 */
export function siblingAttempts(
  snapshot: InstanceSnapshot,
  taskInstance: SnapshotTaskInstance,
): SnapshotTaskInstance[] {
  return snapshot.task_instances
    .filter((ti) => ti.task_id === taskInstance.task_id)
    .sort((a, b) => a.attempt - b.attempt);
}

/** The task definition behind this TaskInstance (max_attempts, declared
 * inputs/outputs/secrets), or null if the snapshot lacks it. */
export function taskDefFor(snapshot: InstanceSnapshot, taskInstance: SnapshotTaskInstance) {
  return snapshot.tasks.find((t) => t.id === taskInstance.task_id) ?? null;
}

/** A task state with no further transitions — the page's snapshot polling
 * stops here even while the instance runs on. */
export function isTerminalTaskState(state: string | null | undefined): boolean {
  const s = normalizeState(state);
  return s === 'completed' || s === 'failed' || s === 'cancelled' || s === 'killed';
}

/** One declared input with its resolved ctx entry (null = awaiting). */
export interface TaskCtxInput {
  name: string;
  source: SnapshotTaskDef['inputs'][number]['source'];
  entry: CtxEntry | null;
}

/** One declared output with its resolved ctx entry (null = not yet written). */
export interface TaskCtxOutput {
  name: string;
  isRoutingVar: boolean;
  entry: CtxEntry | null;
}

/** This task's slice of the run's tickr-ctx scope. Secrets carry names
 * only — by construction the slice holds no entry (and so no value) for a
 * secret name, so secret material cannot reach the DOM through it. */
export interface TaskCtxSlice {
  inputs: TaskCtxInput[];
  outputs: TaskCtxOutput[];
  secretNames: string[];
}

function findEntry(entries: CtxEntry[], name: string): CtxEntry | null {
  return entries.find((e) => e.name === name) ?? null;
}

/**
 * Project this task's Inputs and Outputs out of the run-level Context
 * payload by the task's declared names — a pure client-side narrowing of
 * the instance Context endpoint, never a second source of truth.
 *
 * Input resolution follows the declared source: trigger-bound names read
 * the trigger scope, signal-bound names the gate scopes, upstream-task and
 * bare ambient names the run scope (falling back to trigger and gate
 * scopes the way the ambient resolver walks them). Outputs — including
 * routing variables — are produced into the run scope.
 */
export function taskCtxSlice(def: SnapshotTaskDef, ctx: InstanceContext): TaskCtxSlice {
  const gateEntries = ctx.gates.flatMap((g) => g.entries);
  const secretNames = new Set(def.secrets);

  const inputs: TaskCtxInput[] = def.inputs
    .filter((i) => !secretNames.has(i.name))
    .map((i) => {
      let entry: CtxEntry | null;
      if (i.source?.kind === 'trigger') {
        entry = findEntry(ctx.trigger, i.name);
      } else if (i.source?.kind === 'signal') {
        entry = findEntry(gateEntries, i.name);
      } else {
        entry =
          findEntry(ctx.run, i.name) ??
          findEntry(ctx.trigger, i.name) ??
          findEntry(gateEntries, i.name);
      }
      return { name: i.name, source: i.source ?? null, entry };
    });

  const routingVarNames = new Set(def.routing_vars.map((rv) => rv.name));
  const outputNames = [
    ...def.outputs.filter((o) => !routingVarNames.has(o)),
    ...def.routing_vars.map((rv) => rv.name),
  ];
  const outputs: TaskCtxOutput[] = outputNames
    .filter((name) => !secretNames.has(name))
    .map((name) => ({
      name,
      isRoutingVar: routingVarNames.has(name),
      entry: findEntry(ctx.run, name),
    }));

  return { inputs, outputs, secretNames: def.secrets };
}

/**
 * Abnormal-end derivation: the task ran to a terminal outcome but its log
 * stream was never closed with an End-of-stream marker — the executor died
 * rather than exiting. Cancelled/killed tasks are excluded: they may never
 * have run, so a missing marker there is expected, not abnormal.
 * `markerPresent: undefined` means the marker state is unknown (logs not
 * fetched yet) — never report abnormal on ignorance.
 */
export function isAbnormalEnd(
  state: string | null | undefined,
  markerPresent: boolean | undefined,
): boolean {
  if (markerPresent !== false) return false;
  const s = normalizeState(state);
  return s === 'completed' || s === 'failed';
}

/** The styled terminal line for an End-of-stream marker. */
export function markerLine(exitStatus: number | undefined, exitReason: string | undefined): string {
  const status = exitStatus ?? -1;
  const base = `── end of stream · exit ${status} ──`;
  return exitReason ? `${base} (${exitReason})` : base;
}

/** The styled terminal line for a terminal task whose stream has no marker. */
export const ABNORMAL_END_LINE =
  '── stream ended without end-of-stream marker — executor may have died ──';

/**
 * Bounded viewport window: the last `max` lines plus how many were hidden —
 * drives the honest "showing last N lines" banner. Huge logs neither lie
 * nor crash the page.
 */
export function boundedTail(lines: string[], max: number): { shown: string[]; hidden: number } {
  if (lines.length <= max) return { shown: lines, hidden: 0 };
  return { shown: lines.slice(lines.length - max), hidden: lines.length - max };
}

/**
 * Attempt duration in milliseconds: completed−started when terminal,
 * now−started while running, null before the first Running transition.
 * `started_at` / `completed_at` are the server's transition-history
 * derivations — facts, not guesses.
 */
export function attemptDurationMs(
  taskInstance: Pick<SnapshotTaskInstance, 'started_at' | 'completed_at'>,
  nowMs: number,
): number | null {
  if (!taskInstance.started_at) return null;
  const started = new Date(taskInstance.started_at).getTime();
  if (Number.isNaN(started)) return null;
  const end = taskInstance.completed_at ? new Date(taskInstance.completed_at).getTime() : nowMs;
  return Math.max(0, end - started);
}
