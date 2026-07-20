/**
 * The one canonical projection of producer↔gate adjacency: which task produces
 * each routing variable, and which predicate gates read it. Derived from
 * definition topology only — routing-var producers (single-producer by parser
 * rule) and the routing vars each predicate gate reads — so it is
 * liveness-invariant: the same workflow yields the identical adjacency for the
 * static detail graph and for every running instance.
 *
 * Both the detail-page graph builder and the instance-page graph builder
 * resolve through here rather than re-deriving the mapping inline, so the two
 * surfaces cannot drift on "which gate does this task arm?" — the live tab's
 * producer→gate selection highlight reads the same adjacency the static tab
 * does. Callers extract the per-edge `reads` from their own gate shape (the raw
 * blob's `Predicate.reads` vs the snapshot's typed `GateView.routing_var`); the
 * adjacency match itself lives here once.
 */
export interface SelectionGraph {
  /** producing task id → edge ids of predicate gates reading its routing var(s) */
  taskToGates: Record<string, string[]>;
  /** predicate-gate edge id → producing task id */
  gateToTask: Record<string, string>;
}

export interface AdjacencyTask {
  id: string;
  /** routing-var names this task produces */
  routingVars: string[];
}

export interface AdjacencyGateEdge {
  id: string;
  /** routing-var names this edge's predicate gate(s) read */
  reads: string[];
}

export function buildProducerGateAdjacency(
  tasks: AdjacencyTask[],
  gateEdges: AdjacencyGateEdge[],
): SelectionGraph {
  const varProducer: Record<string, string> = {};
  for (const t of tasks) for (const v of t.routingVars) varProducer[v] = t.id;

  const taskToGates: Record<string, string[]> = {};
  const gateToTask: Record<string, string> = {};
  for (const e of gateEdges) {
    for (const v of e.reads) {
      const producer = varProducer[v];
      if (producer) {
        (taskToGates[producer] ??= []).push(e.id);
        gateToTask[e.id] = producer;
      }
    }
  }
  return { taskToGates, gateToTask };
}
