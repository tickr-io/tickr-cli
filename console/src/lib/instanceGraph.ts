/**
 * Instance-graph layout model — the snapshot-driven sibling of
 * `buildHyperGraphModel` (ADR-0035's UI-side translator precedent). Builds
 * the same layout/render shape from the instance snapshot's hypergraph and
 * overlays per-instance liveness: HyperNode state from the latest attempt,
 * gate badges carrying live gate state, and running-task start timestamps
 * for the client-side elapsed tick.
 */

import type { InstanceSnapshot, SnapshotGraph, GateView } from '@/api/client';
import { detectChainsCore, type ChainFold, type LayoutGraph } from './hyperGraph';
import { routingVarNames } from './routingVars';
import { buildProducerGateAdjacency, type SelectionGraph } from './producerGates';
import { detectRingLanes, sequenceLoopEdges, type RingLane } from './loopLane';

export interface LiveGateBadge {
  kind: GateView['kind'];
  state: GateView['state'];
  /** Compact declared-condition label (signal name / predicate / duration). */
  label: string;
}

export interface LiveTaskNodeView {
  id: string;
  /** The HyperNode's identity code — the short handle an operator reads off the
   * graph to name this node when authoring a patch. Projected server-side from
   * the node UUID; overlaid by the graph tab's identity-code toggle. */
  code: string;
  name: string;
  nix: string;
  routingVars: string[];
  /** Latest attempt's substrate state; undefined = never minted (neutral). */
  state?: string;
  /** Latest attempt's derived start, for the running elapsed tick. */
  startedAt?: string;
  /** Latest attempt's derived completion (first terminal transition), for the
   * settled time-to-complete shown on a finished node. */
  completedAt?: string;
  running: boolean;
  /** Latest attempt's task-instance id — the target of opening this node's task
   * instance detail page. Undefined for a never-minted node (nothing to open). */
  taskInstanceId?: string;
  /** A ghost node — grounded without a task instance ever running (a loop reap
   * or cancel cascade grounded a never-minted sibling). It carries no `state`
   * (nothing ran), so the renderer colours it from `ground` and marks it
   * distinct from a never-reached node. Derived server-side. */
  ghost: boolean;
  /** A carried-forward HyperNode of a replay — inherited grounded from the
   * source run, never minted a task instance here. It is also a `ghost`;
   * `preGrounded` is what draws it distinctly from a genuine reaped ghost.
   * Derived server-side from the replay provenance. */
  preGrounded: boolean;
  /** The node's ground kind (`success` / `failed` / `cancelled` / `pending`).
   * A ghost node has no attempt state, so its hue comes from here instead. */
  ground?: string;
}

export interface LiveJunctionView {
  id: string;
  edgeId: string;
  /** The HyperEdge's identity code — a fan-in/fan-out edge is rendered as a
   * junction, so its code is overlaid here. */
  code: string;
  gates: LiveGateBadge[];
}

export interface LiveEdgeView {
  id: string;
  from: string;
  to: string;
  /** The HyperEdge's identity code, for the identity-code toggle. A 1→1 edge
   * carries its own code; a junction leg (`${edgeId}:…`) carries none — the
   * whole HyperEdge's code rides its junction, not each leg. */
  code?: string;
  gates: LiveGateBadge[];
  /** A `kind = loop` edge — styled as a loop lane (dashed) rather than a plain
   * dependency arrow. */
  isLoop?: boolean;
  /** Draw this loop edge as an over-the-top arc (a self-loop, or the ring's
   * back-edge) instead of an inline bezier. Kept out of the dagre layout so the
   * layout graph stays acyclic. */
  loopArc?: boolean;
  /** A frontier edge: its source has grounded-success and its target has not
   * started, so this edge is the live boundary of the run. Statically
   * brightened (no animation — honest under the 5s poll). */
  frontier?: boolean;
  /** Completed loop turns, on a back-edge arc only: the count of transitions
   * into `Parked` on the arc endpoints' latest attempts. A loop turn parks and
   * re-queues the SAME instance (attempt never increments on a turn), so the
   * transition history is the one honest turn record the snapshot carries. */
  turns?: number;
}

export interface InstanceGraphModel {
  layout: LayoutGraph;
  tasks: LiveTaskNodeView[];
  junctions: LiveJunctionView[];
  edges: LiveEdgeView[];
  /** Detected loop rings oriented as lanes (entry first, deterministic) — the
   * renderer collapses each multi-member lane into one dagre placeholder and
   * expands it as a single horizontal row, identical to the static tab. */
  ringLanes: RingLane[];
  /** producer↔gate adjacency for the producer→gate selection highlight. Derived
   * from definition topology only (the shared helper), so it is identical for
   * every instance of this workflow — liveness plays no part. */
  selection: SelectionGraph;
  /** Maximal linear chains, folded into serpentine blocks by the shared layout.
   * Detected off the snapshot's own edge kind + arity, so the live graph folds a
   * long spine identically to the static definition graph. */
  chains: ChainFold[];
}

export function gateLabel(g: GateView): string {
  if (g.kind === 'signal') return g.signal_name ?? 'signal';
  if (g.kind === 'predicate')
    return `${g.routing_var ?? '?'} ${g.op ?? ''} ${g.value != null ? JSON.stringify(g.value.value) : ''}`.trim();
  if (g.kind === 'timer') return g.duration_secs != null ? `${g.duration_secs}s` : 'timer';
  return g.kind;
}

function liveGates(gates: GateView[]): LiveGateBadge[] {
  return gates.map((g) => ({ kind: g.kind, state: g.state, label: gateLabel(g) }));
}

/**
 * Build the layout/render model for an instance's graph. By default it renders
 * the live `graph`; pass a stored `version_snapshots` entry as `graph` to render
 * a past Instance version's shape directly (the evolution view's version
 * navigator) — liveness (task state, gate state, elapsed) still overlays from
 * the instance's own attempts, so an older shape reads with current status.
 */
export function buildInstanceGraphModel(
  snapshot: InstanceSnapshot,
  graph: SnapshotGraph = snapshot.graph,
): InstanceGraphModel {
  const defById = new Map(snapshot.tasks.map((t) => [t.id, t]));

  // Latest attempt per task definition. `parks` counts its transitions into
  // Parked — the loop-turn record (a turn re-queues the same instance).
  const currentByTask = new Map<
    string,
    {
      id: string;
      state: string;
      started_at?: string | null;
      completed_at?: string | null;
      attempt: number;
      parks: number;
    }
  >();
  for (const ti of snapshot.task_instances) {
    const seen = currentByTask.get(ti.task_id);
    if (!seen || ti.attempt > seen.attempt) {
      currentByTask.set(ti.task_id, {
        id: ti.id,
        state: ti.state,
        started_at: ti.started_at,
        completed_at: ti.completed_at,
        attempt: ti.attempt,
        parks: (ti.transitions ?? []).filter((tr) => tr.to === 'Parked').length,
      });
    }
  }

  const taskIds = new Set(graph.nodes.filter((n) => n.kind === 'task').map((n) => n.id));
  // Identity codes travel on the snapshot's own graph structures (projected
  // server-side from each UUID); keep them keyed by id so every rendered
  // node/edge can carry the exact code the HTTP view and ctx graph expose.
  const codeByNode = new Map(graph.nodes.map((n) => [n.id, n.code]));
  // The snapshot node carries the derived ghost distinction and ground kind —
  // both instance-level facts the renderer needs to colour a grounded-never-run
  // node in its outcome hue instead of the neutral never-reached one.
  const nodeById = new Map(graph.nodes.map((n) => [n.id, n]));

  const tasks: LiveTaskNodeView[] = [...taskIds].map((id) => {
    const def = defById.get(id);
    const cur = currentByTask.get(id);
    const node = nodeById.get(id);
    return {
      id,
      code: codeByNode.get(id) ?? '',
      name: def?.name ?? id,
      nix: def?.nix_expression_path ?? '',
      routingVars: routingVarNames(def?.routing_vars),
      state: cur?.state,
      startedAt: cur?.started_at ?? undefined,
      completedAt: cur?.completed_at ?? undefined,
      running: cur?.state === 'Running',
      taskInstanceId: cur?.id,
      ghost: node?.ghost ?? false,
      preGrounded: node?.pre_grounded ?? false,
      ground: node?.ground,
    };
  });

  const layoutNodes = new Set<string>(taskIds);
  const layoutEdges: { from: string; to: string }[] = [];
  const junctions: LiveJunctionView[] = [];
  const edges: LiveEdgeView[] = [];
  // 1→1 loop edges are deferred to a second pass: only once the acyclic base
  // graph is built can a forward ring step be told from the cycle's back-edge.
  const loopEdges: { id: string; code: string; from: string; to: string; gates: LiveGateBadge[] }[] =
    [];

  // Entry hints for ring-lane orientation: a member targeted by any non-loop
  // edge (including one from the start sentinel) is where control enters.
  const entryHints = new Set<string>();
  for (const e of graph.edges) {
    if (e.kind === 'loop') continue;
    for (const t of e.targets) if (taskIds.has(t)) entryHints.add(t);
  }

  for (const e of graph.edges) {
    // Start/end sentinels are structural and not drawn.
    const sources = e.sources.filter((s) => taskIds.has(s));
    const targets = e.targets.filter((t) => taskIds.has(t));
    if (sources.length === 0 || targets.length === 0) continue;

    const gates = liveGates(e.gates);

    if (e.kind === 'loop' && sources.length === 1 && targets.length === 1) {
      loopEdges.push({ id: e.id, code: e.code, from: sources[0], to: targets[0], gates });
      continue;
    }

    const isHyper = sources.length > 1 || targets.length > 1;

    if (isHyper) {
      const jid = `junction:${e.id}`;
      // The whole HyperEdge's identity code rides its junction; its legs carry
      // none, so the toggle labels each edge exactly once.
      junctions.push({ id: jid, edgeId: e.id, code: e.code, gates });
      layoutNodes.add(jid);
      for (const s of sources) {
        layoutEdges.push({ from: s, to: jid });
        edges.push({ id: `${e.id}:${s}->j`, from: s, to: jid, gates: [] });
      }
      for (const t of targets) {
        layoutEdges.push({ from: jid, to: t });
        edges.push({ id: `${e.id}:j->${t}`, from: jid, to: t, gates: [] });
      }
    } else {
      layoutEdges.push({ from: sources[0], to: targets[0] });
      edges.push({ id: e.id, code: e.code, from: sources[0], to: targets[0], gates });
    }
  }

  // Sequence the loop edges. A detected ring becomes a lane: forward steps
  // enter the layout (collapsed into the lane placeholder by the renderer),
  // and the DETERMINISTIC back-edge — last member into the entry — becomes the
  // over-the-top arc, carrying the turn counter (the loop's one temporal
  // answer, derived from the Parked history of the arc's endpoints). Loop
  // edges that don't form a clean ring fall back to reachability sequencing.
  const ringLanes = detectRingLanes(loopEdges, (id) => entryHints.has(id));
  const laneMember = new Set(ringLanes.flatMap((r) => r.members));
  const backIds = new Set(ringLanes.map((r) => r.backEdgeId));
  for (const le of loopEdges) {
    if (laneMember.has(le.from) && !backIds.has(le.id)) {
      layoutEdges.push({ from: le.from, to: le.to });
    }
  }
  const rest = loopEdges.filter((le) => !laneMember.has(le.from));
  const restArcs = sequenceLoopEdges(layoutEdges, rest);
  const restArcById = new Map(rest.map((le, i) => [le.id, restArcs[i]]));
  const parks = (id: string) => currentByTask.get(id)?.parks ?? 0;
  for (const le of loopEdges) {
    const arc = laneMember.has(le.from) ? backIds.has(le.id) : (restArcById.get(le.id) ?? false);
    const turns = arc ? Math.max(parks(le.from), parks(le.to)) : 0;
    edges.push({
      id: le.id,
      code: le.code,
      from: le.from,
      to: le.to,
      gates: le.gates,
      isLoop: true,
      loopArc: arc,
      turns: turns > 0 ? turns : undefined,
    });
  }

  // Frontier edges: a grounded-success source feeding a not-yet-started target —
  // the live boundary of the run. Marked here (liveness-derived) so the renderer
  // brightens them statically; junction legs never qualify (a junction id is not
  // a task id, so it is neither completed nor never-minted).
  const isCompleted = (id: string) => currentByTask.get(id)?.state === 'Completed';
  const isNeverStarted = (id: string) => taskIds.has(id) && !currentByTask.has(id);
  for (const e of edges) {
    if (isCompleted(e.from) && isNeverStarted(e.to)) e.frontier = true;
  }

  // selection-graph: producer↔gate adjacency via the same shared helper the
  // static detail graph reads, so the live producer→gate highlight can't drift
  // from the static one. This surface extracts each edge's predicate `reads`
  // from the typed `GateView.routing_var`, keyed by the substrate edge id.
  const selection = buildProducerGateAdjacency(
    tasks,
    graph.edges.map((e) => ({
      id: e.id,
      reads: e.gates
        .filter((g) => g.kind === 'predicate' && g.routing_var)
        .map((g) => g.routing_var as string),
    })),
  );

  // Chain folding off the snapshot's own edges — same detector the definition
  // graph runs, so a long serial spine folds into the same serpentine here.
  const chains = detectChainsCore(
    graph.edges.map((e) => ({
      sources: e.sources,
      targets: e.targets,
      isLoop: e.kind === 'loop',
    })),
    taskIds,
    graph.nodes.filter((n) => n.kind === 'task').map((n) => n.id),
  );

  return {
    layout: { nodes: [...layoutNodes], edges: layoutEdges },
    tasks,
    junctions,
    edges,
    selection,
    ringLanes,
    chains,
  };
}

