/**
 * HyperGraphLayoutModel — the UI-side translation layer between the opaque
 * `workflow_definition` blob and the Task-graph / Definition renderers
 * (DC-0004 / DC-0005). Lives in the UI only; never reaches the wire.
 *
 * One builder constructs the model from the blob; consumers read its three
 * projections without re-walking the blob:
 *   - layout-graph  → dagre input (topology + synthetic junctions for hyperedges)
 *   - render-graph  → task nodes, junction nodes, edges, gate badges, routing chips
 *   - selection-graph → producer↔gate adjacency for highlighting
 *
 * Each renderable element carries the stable id it inherits from substrate
 * (task UUID, edge UUID, `junction:{edge.id}`) — no UI-side id minting.
 */

import { routingVarNames } from './routingVars';
import { buildProducerGateAdjacency, type SelectionGraph } from './producerGates';
import { detectRingLanes, sequenceLoopEdges, type RingLane } from './loopLane';

export type { SelectionGraph } from './producerGates';

export type GateKind = 'signal' | 'timer' | 'predicate' | 'unknown';

export interface GateBadge {
  kind: GateKind;
  raw: unknown;
}

export interface TaskNodeView {
  id: string;
  name: string;
  nix: string;
  routingVars: string[];
}

export interface JunctionNodeView {
  id: string; // `junction:{edgeId}`
  edgeId: string;
  gate?: GateBadge;
}

export interface RenderEdgeView {
  id: string;
  from: string;
  to: string;
  /** Gate riding on a 1→1 edge (hyperedge gates live on the junction). */
  gate?: GateBadge;
  /** A `kind = Loop` edge — styled as the loop lane (dashed) rather than a
   * plain dependency arrow. */
  isLoop?: boolean;
  /** The cycle's back-edge (or a self-loop) — kept out of the dagre layout so
   * it stays acyclic, and drawn as an over-the-top arc. */
  loopArc?: boolean;
}

export interface LayoutGraph {
  nodes: string[];
  edges: { from: string; to: string }[];
}

export interface RenderGraph {
  tasks: TaskNodeView[];
  junctions: JunctionNodeView[];
  edges: RenderEdgeView[];
}

export interface HyperGraphLayoutModel {
  layout: LayoutGraph;
  render: RenderGraph;
  selection: SelectionGraph;
  /** Detected loop rings, oriented as lanes (entry first, deterministic). The
   * renderer collapses each multi-member lane into one dagre placeholder and
   * expands it as a single horizontal row. */
  ringLanes: RingLane[];
  /** Maximal linear chains folded into serpentine blocks by the shared layout —
   * computed here so the two graph surfaces read the same `chains` field. */
  chains: ChainFold[];
}

interface RawEdge {
  id?: string;
  sources?: string[];
  targets?: string[];
  gates?: unknown[];
  /** Author-declared edge role: `Control` | `Data` | `Loop` (PascalCase on the
   *  definition wire). Chain detection reads it to never fold across a loop
   *  back-edge; the rest of the model ignores it. */
  kind?: string;
}

/** Classify a substrate gate (a serde-tagged enum object) into a render kind.
 *  The variant names are the wire's `Gate` enum (`server/src/task_graph/edge.rs`):
 *  `SignalReceived` / `PredicateHolds` / `TimerElapsed`, plus `Satisfied` (a
 *  signal gate already satisfied — still signal-kinded). */
function gateBadge(gates: unknown[] | undefined): GateBadge | undefined {
  if (!gates || gates.length === 0) return undefined;
  if (gates.length > 1) {
    // The substrate allows multiple ANDed gates per edge, but no DSL
    // constructor emits more than one today. Surface the case if it ever lands.
    console.warn(`edge has ${gates.length} gates; rendering the first (multi-gate UX deferred)`);
  }
  const g = gates[0];
  let kind: GateKind = 'unknown';
  if (g && typeof g === 'object') {
    const key = Object.keys(g as Record<string, unknown>)[0];
    if (key === 'SignalReceived' || key === 'Satisfied') kind = 'signal';
    else if (key === 'TimerElapsed') kind = 'timer';
    else if (key === 'PredicateHolds') kind = 'predicate';
  }
  return { kind, raw: g };
}

/** Extract the routing-var a predicate gate reads — the wire's
 *  `PredicateHolds.routing_var` (a single string; the single-producer rule
 *  means one var per predicate gate). */
function gateReads(gates: unknown[] | undefined): string[] {
  if (!gates) return [];
  const out: string[] = [];
  for (const g of gates) {
    const pred = (g as Record<string, unknown> | null)?.['PredicateHolds'] as
      | Record<string, unknown>
      | undefined;
    const rv = pred?.['routing_var'];
    if (typeof rv === 'string') out.push(rv);
  }
  return out;
}

export function buildHyperGraphModel(
  definition: Record<string, unknown> | undefined,
): HyperGraphLayoutModel {
  const tasksRaw = (definition?.tasks as Record<string, Record<string, unknown>>) ?? {};
  const tg = (definition?.task_graph as Record<string, unknown>) ?? {};
  const edgesRaw = (tg.edges as Record<string, RawEdge>) ?? {};

  const taskIds = new Set(Object.keys(tasksRaw));
  const tasks: TaskNodeView[] = Object.entries(tasksRaw).map(([id, t]) => ({
    id,
    name: (t.name as string) ?? id,
    nix: (t.nix_expression_path as string) ?? '',
    routingVars: routingVarNames(t.routing_vars),
  }));

  const layoutNodes = new Set<string>(taskIds);
  const layoutEdges: { from: string; to: string }[] = [];
  const junctions: JunctionNodeView[] = [];
  const renderEdges: RenderEdgeView[] = [];
  // 1→1 loop edges are deferred to a second pass: only once the acyclic base
  // graph is built can a forward ring step be told from the cycle's back-edge.
  const loopEdges: { id: string; from: string; to: string; gate?: GateBadge }[] = [];

  // Entry hints for ring-lane orientation: a member targeted by any non-loop
  // edge is where control enters the ring. Read from the RAW edges (before
  // sentinel filtering) because the entry edge usually comes FROM the start
  // sentinel and would otherwise be invisible here.
  const entryHints = new Set<string>();
  for (const e of Object.values(edgesRaw)) {
    if ((e.kind ?? '').toLowerCase() === 'loop') continue;
    for (const t of e.targets ?? []) if (taskIds.has(t)) entryHints.add(t);
  }

  for (const [edgeId, e] of Object.entries(edgesRaw)) {
    // Keep only task↔task endpoints; edges touching the start/end sentinels are
    // structural and not drawn.
    const sources = (e.sources ?? []).filter((s) => taskIds.has(s));
    const targets = (e.targets ?? []).filter((t) => taskIds.has(t));
    if (sources.length === 0 || targets.length === 0) continue;

    const gate = gateBadge(e.gates);

    if ((e.kind ?? '').toLowerCase() === 'loop' && sources.length === 1 && targets.length === 1) {
      loopEdges.push({ id: edgeId, from: sources[0], to: targets[0], gate });
      continue;
    }

    const isHyper = sources.length > 1 || targets.length > 1;

    if (isHyper) {
      const jid = `junction:${edgeId}`;
      junctions.push({ id: jid, edgeId, gate });
      layoutNodes.add(jid);
      for (const s of sources) {
        layoutEdges.push({ from: s, to: jid });
        renderEdges.push({ id: `${edgeId}:${s}->j`, from: s, to: jid });
      }
      for (const t of targets) {
        layoutEdges.push({ from: jid, to: t });
        renderEdges.push({ id: `${edgeId}:j->${t}`, from: jid, to: t });
      }
    } else {
      layoutEdges.push({ from: sources[0], to: targets[0] });
      renderEdges.push({ id: edgeId, from: sources[0], to: targets[0], gate });
    }
  }

  // Sequence the loop edges. A detected ring becomes a lane: its forward steps
  // enter the layout (and are later collapsed into the lane placeholder by the
  // renderer), and its DETERMINISTIC back-edge — last member into the entry,
  // never blob-iteration-order — becomes the over-the-top arc. Loop edges that
  // don't form a clean ring fall back to reachability sequencing.
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
  for (const le of loopEdges) {
    renderEdges.push({
      id: le.id,
      from: le.from,
      to: le.to,
      gate: le.gate,
      isLoop: true,
      loopArc: laneMember.has(le.from) ? backIds.has(le.id) : restArcById.get(le.id),
    });
  }

  // selection-graph: routing-var producer (task) ↔ predicate gate edge reading
  // it. Derived through the shared adjacency helper — the same liveness-
  // invariant projection the live instance graph reads — so the two surfaces
  // can't drift on which gate a producer arms. This surface extracts the
  // per-edge `reads` from the raw blob's `Predicate.reads` shape.
  const selection: SelectionGraph = buildProducerGateAdjacency(
    tasks,
    Object.entries(edgesRaw).map(([edgeId, e]) => ({ id: edgeId, reads: gateReads(e.gates) })),
  );

  return {
    layout: { nodes: [...layoutNodes], edges: layoutEdges },
    render: { tasks, junctions, edges: renderEdges },
    selection,
    ringLanes,
    chains: detectChains(definition),
  };
}

// ─── Structure-aware chain folding ──────────────────────────────────────────
// Pure helpers (no React, no dagre): detect maximal linear chains in a workflow
// definition, then place a chain's members as a compact serpentine (boustrophedon)
// grid block. The renderer collapses each chain to one dagre node, lays out the
// skeleton, then expands the placeholder back into these positions — so a long
// `impl_01 → … → verify → commit` spine fills the viewport instead of sprawling
// along one axis. All geometry is unit-testable without a layout engine.

/** Card metrics + intra-block gaps for a serpentine grid. */
export interface Cell {
  w: number;
  h: number;
  gapX: number;
  gapY: number;
}

/** A foldable linear chain: ordered head→tail task ids. */
export interface ChainFold {
  members: string[];
}

/** Connector kind between two consecutive serpentine members. */
export type SerpKind = 'h-lr' | 'h-rl' | 'turn-r' | 'turn-l';

/**
 * Detect maximal linear chains over the RAW definition blob (chain detection
 * needs `edge.kind` and true source/target arity, both dropped by
 * `buildHyperGraphModel`'s layout projection). A chain interior node has exactly
 * one simple (1→1, non-loop) in-edge and one simple out-edge and touches no
 * hyperedge or loop edge; a chain's endpoints may attach to anything (that is how
 * the spine joins a fan or a loop). A mid-chain gate does NOT break the chain (a
 * gate is an edge property, not a structural fan). Only runs of `minLen`+ fold.
 * Loop-ring members never fold: every `mkLoop` ring step is tagged `kind:Loop`,
 * so each member touches a loop edge and is excluded here.
 */
export function detectChains(
  definition: Record<string, unknown> | undefined,
  minLen = 5,
): ChainFold[] {
  const tasksRaw = (definition?.tasks as Record<string, unknown>) ?? {};
  const tg = (definition?.task_graph as Record<string, unknown>) ?? {};
  const edgesRaw = (tg.edges as Record<string, RawEdge>) ?? {};
  const taskIds = new Set(Object.keys(tasksRaw));
  const edges: NormChainEdge[] = Object.values(edgesRaw).map((e) => ({
    sources: e.sources ?? [],
    targets: e.targets ?? [],
    isLoop: (e.kind ?? '').toLowerCase() === 'loop',
  }));
  return detectChainsCore(edges, taskIds, Object.keys(tasksRaw), minLen);
}

/** The edge shape chain detection reads: endpoints + whether it is a loop step.
 * Both the definition blob and the instance snapshot normalize into it, so one
 * detector serves both surfaces (the instance graph gained chain folding this
 * way — the snapshot already carries edge kind and arity). */
export interface NormChainEdge {
  sources: string[];
  targets: string[];
  isLoop: boolean;
}

/**
 * Chain detection over already-normalized edges. `taskIds` bounds the endpoints
 * (sentinels are excluded); `taskOrder` is the deterministic head-iteration
 * order so the fold is stable regardless of map/array ordering.
 */
export function detectChainsCore(
  edges: NormChainEdge[],
  taskIds: Set<string>,
  taskOrder: string[],
  minLen = 5,
): ChainFold[] {
  const simpleOut = new Map<string, string[]>();
  const simpleIn = new Map<string, string[]>();
  const touchesHyperOrLoop = new Set<string>();

  for (const e of edges) {
    const sources = e.sources.filter((s) => taskIds.has(s));
    const targets = e.targets.filter((t) => taskIds.has(t));
    if (sources.length === 0 || targets.length === 0) continue;
    const isHyper = sources.length > 1 || targets.length > 1;
    if (isHyper || e.isLoop) {
      for (const s of sources) touchesHyperOrLoop.add(s);
      for (const t of targets) touchesHyperOrLoop.add(t);
      continue;
    }
    const u = sources[0];
    const v = targets[0];
    if (!simpleOut.has(u)) simpleOut.set(u, []);
    simpleOut.get(u)!.push(v);
    if (!simpleIn.has(v)) simpleIn.set(v, []);
    simpleIn.get(v)!.push(u);
  }

  const outDeg = (id: string) => (simpleOut.get(id) ?? []).length;
  const inDeg = (id: string) => (simpleIn.get(id) ?? []).length;
  // An interior node is a pure 1→1 pass-through untouched by any fan/loop.
  const interiorEligible = (id: string) =>
    inDeg(id) === 1 && outDeg(id) === 1 && !touchesHyperOrLoop.has(id);

  const visited = new Set<string>();
  const chains: ChainFold[] = [];
  // Deterministic head order so the fold is stable regardless of map iteration.
  for (const s of taskOrder) {
    if (visited.has(s)) continue;
    // A head has one simple out but cannot itself be a chain interior (in-deg ≠ 1,
    // or it is a fan/loop endpoint) — i.e. the linear run starts here.
    if (outDeg(s) !== 1 || interiorEligible(s)) continue;
    const chain = [s];
    visited.add(s);
    let cur = s;
    while (outDeg(cur) === 1) {
      const nxt = simpleOut.get(cur)![0];
      if (visited.has(nxt)) break; // cycle / consumed
      chain.push(nxt);
      visited.add(nxt);
      if (interiorEligible(nxt)) cur = nxt;
      else break; // nxt is the tail (fan/loop endpoint or branches)
    }
    if (chain.length >= minLen) chains.push({ members: chain });
  }
  return chains;
}

/** Column count for a chain of `n`, minimizing |aspect − target| (landscape). */
export function chainColumns(n: number, cell: Cell, targetAspect = 2.0): number {
  let best = 2;
  let bestErr = Infinity;
  const hi = Math.max(2, n - 1);
  for (let k = 2; k <= hi; k++) {
    const { width, height } = serpentineBlockSize(n, k, cell, 0);
    const err = Math.abs(width / height - targetAspect);
    if (err < bestErr) {
      bestErr = err;
      best = k;
    }
  }
  return best;
}

/**
 * Serpentine member CENTERS in boustrophedon order: even rows L→R, odd rows R→L,
 * so the row turn drops straight down (`col` mirrors on odd rows keeping turns
 * vertical). `origin` is the top-left of the member grid.
 */
export function serpentinePositions(
  n: number,
  k: number,
  origin: { x: number; y: number },
  cell: Cell,
): { x: number; y: number }[] {
  const out: { x: number; y: number }[] = [];
  for (let i = 0; i < n; i++) {
    const row = Math.floor(i / k);
    const inRow = i % k;
    const col = row % 2 === 0 ? inRow : k - 1 - inRow;
    out.push({
      x: origin.x + cell.w / 2 + col * (cell.w + cell.gapX),
      y: origin.y + cell.h / 2 + row * (cell.h + cell.gapY),
    });
  }
  return out;
}

/** Padded bounding box for the placeholder (grid + U-bend turn margins L/R). */
export function serpentineBlockSize(
  n: number,
  k: number,
  cell: Cell,
  turnMargin: number,
): { width: number; height: number } {
  const cols = Math.min(n, k);
  const rows = Math.ceil(n / k);
  const gridW = cols * cell.w + (cols - 1) * cell.gapX;
  const gridH = rows * cell.h + (rows - 1) * cell.gapY;
  return { width: gridW + 2 * turnMargin, height: gridH };
}

/** Connector kind per consecutive member pair (length n-1). */
export function serpentineConnectors(n: number, k: number): SerpKind[] {
  const out: SerpKind[] = [];
  for (let i = 0; i < n - 1; i++) {
    const row = Math.floor(i / k);
    const isRowEnd = i % k === k - 1; // last column of this row (turn down)
    if (isRowEnd) out.push(row % 2 === 0 ? 'turn-r' : 'turn-l');
    else out.push(row % 2 === 0 ? 'h-lr' : 'h-rl');
  }
  return out;
}
