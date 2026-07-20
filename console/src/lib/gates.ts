/**
 * Gates-tab row model — a pure function from the instance snapshot to the
 * table the Gates tab renders: every gate on every HyperEdge exactly once,
 * with its declared Expression, the joined Current value for predicate
 * gates, and the Dispatched-gate honesty annotation.
 *
 * The annotation derives only from gate state plus value-presence — the UI
 * never re-evaluates predicates (no second evaluator to drift from the
 * server's). The inference "Dispatched + value present ⇒ predicate false"
 * is sound because the server merges routing variables and re-evaluates
 * Dispatched gates in one event-handler application before persisting. The
 * reject time is computed client-side as the gate's Dispatched timestamp
 * (from its transition history) + the declared timeout — the scheduler's
 * absolute deadline is deliberately off the wire.
 */

import type { InstanceSnapshot, GateView, RoutingValueView } from '@/api/client';

export interface GateRowModel {
  key: string;
  edgeId: string;
  /** Source / target task names, sorted. */
  sources: string[];
  targets: string[];
  kind: GateView['kind'];
  /** The declared condition, rendered. */
  expression: string;
  state: GateView['state'];
  /** Distinguishing copy for the state cell. */
  stateCopy: string | null;
  /** Predicate gates: the routing variable's current typed value, when produced. */
  currentValue: RoutingValueView | null;
  /** Predicate gates with no value yet: the single declaring task's name. */
  awaitingProducer: string | null;
  /** The Dispatched + value-present honesty line, when applicable. */
  annotation: string | null;
}

function fmtValue(v: RoutingValueView): string {
  return `${JSON.stringify(v.value)} (${v.kind})`;
}

export function gateExpression(gate: GateView): string {
  if (gate.kind === 'predicate') {
    const val = gate.value != null ? JSON.stringify(gate.value.value) : '?';
    return `${gate.routing_var ?? '?'} ${gate.op ?? '?'} ${val}`;
  }
  if (gate.kind === 'signal') {
    const parts = [gate.signal_name ?? 'signal'];
    if (gate.predicate) parts.push(gate.predicate);
    if (gate.captures.length > 0) parts.push(`captures: ${gate.captures.join(', ')}`);
    return parts.join(' · ');
  }
  if (gate.kind === 'timer') {
    return gate.duration_secs != null ? `after ${gate.duration_secs}s` : 'timer';
  }
  return gate.kind;
}

const STATE_COPY: Partial<Record<string, string>> = {
  Rejected: 'own deadline elapsed',
  Cancelled: 'abandoned mid-flight',
};

function dispatchedAtMs(gate: GateView): number | null {
  const rec = gate.transitions.find((t) => t.to === 'Dispatched');
  if (!rec) return null;
  const t = new Date(rec.at).getTime();
  return Number.isNaN(t) ? null : t;
}

export function buildGateRows(snapshot: InstanceSnapshot): GateRowModel[] {
  const taskName = new Map(snapshot.tasks.map((t) => [t.id, t.name]));
  const producerOf = new Map<string, string>();
  for (const t of snapshot.tasks) {
    for (const rv of t.routing_vars) producerOf.set(rv.name, t.name);
  }
  const names = (ids: string[]) =>
    ids
      .map((id) => taskName.get(id))
      .filter((n): n is string => !!n)
      .sort();

  const rows: GateRowModel[] = [];
  for (const edge of snapshot.graph.edges) {
    edge.gates.forEach((gate, i) => {
      let currentValue: RoutingValueView | null = null;
      let awaitingProducer: string | null = null;
      let annotation: string | null = null;

      if (gate.kind === 'predicate' && gate.routing_var) {
        currentValue = snapshot.routing_variables[gate.routing_var] ?? null;
        if (currentValue == null) {
          awaitingProducer = producerOf.get(gate.routing_var) ?? null;
        } else if (gate.state === 'Dispatched') {
          // Value present yet not Satisfied ⇒ the server evaluated false.
          const dispatched = dispatchedAtMs(gate);
          if (gate.timeout_secs != null && dispatched != null) {
            const rejectAt = new Date(dispatched + gate.timeout_secs * 1000);
            annotation = `predicate false against current value — will reject at ${rejectAt.toLocaleString()}`;
          } else {
            annotation = 'predicate false against current value — no timeout declared';
          }
        }
      }

      rows.push({
        key: `${edge.id}:${i}`,
        edgeId: edge.id,
        sources: names(edge.sources),
        targets: names(edge.targets),
        kind: gate.kind,
        expression: gateExpression(gate),
        state: gate.state,
        stateCopy: STATE_COPY[gate.state] ?? null,
        currentValue,
        awaitingProducer,
        annotation,
      });
    });
  }
  return rows;
}

/** The gates incident to one task, split by role. A gate whose edge carries
 * the task in both its source and target sets appears in both groups — no
 * dedup; "what does this wait on" and "what does this release" are
 * different operator questions. */
export interface IncidentGateRows {
  /** Gates on edges whose target set includes this task — what it waits on. */
  gatedBy: GateRowModel[];
  /** Gates on edges fed by this task's completion — what it releases. */
  gatesDownstream: GateRowModel[];
}

/**
 * Split the instance's gate rows down to the ones incident to one task.
 * Incidence means the task's `task_id` (the hypergraph node id) appears in
 * the HyperEdge's sources or targets. Row rendering is the instance page's
 * rows wholesale — same model, narrowed.
 */
export function incidentGateRows(snapshot: InstanceSnapshot, taskId: string): IncidentGateRows {
  const rows = buildGateRows(snapshot);
  const edgeById = new Map(snapshot.graph.edges.map((e) => [e.id, e]));
  return {
    gatedBy: rows.filter((r) => edgeById.get(r.edgeId)?.targets.includes(taskId) ?? false),
    gatesDownstream: rows.filter((r) => edgeById.get(r.edgeId)?.sources.includes(taskId) ?? false),
  };
}

export { fmtValue as formatRoutingValue };
