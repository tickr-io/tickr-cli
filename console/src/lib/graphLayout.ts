/**
 * The one graph layout engine, shared by BOTH the static definition graph
 * (`TaskGraphTab`) and the live instance graph (`InstanceGraphTab`). Given a
 * pure-topology input — layout nodes/edges, render edges, detected loop rings
 * and linear chains — it runs the collapse → dagre → expand pipeline once and
 * returns node positions, ring circles, serpentine connector paths, loop-arc
 * lanes and the canvas size. The two tabs differ only in how they PAINT the
 * result (static kind vs live state), never in how it is laid out — so a long
 * spine folds into the same serpentine, and a loop into the same circle, on
 * both surfaces. Before this module the two tabs hand-copied the pipeline and
 * had drifted: chain folding lived only in the definition tab.
 */

import dagre from '@dagrejs/dagre';
import {
  chainColumns,
  serpentineBlockSize,
  serpentineConnectors,
  serpentinePositions,
  type Cell,
  type ChainFold,
  type SerpKind,
} from './hyperGraph';
import {
  assignArcLanes,
  loopArcPath,
  ringCircleBlock,
  ringCirclePositions,
  ringCircleRadius,
  ringCircleWaypoints,
  type ArcSpan,
  type Box,
  type RingLane,
} from './loopLane';

export const TASK_W = 178;
export const TASK_H = 54;
export const JUNCTION = 20;
const SERP_GAP_X = 56;
const SERP_GAP_Y = 48;
const SERP_TURN = 42;
const SERP_TURN_R = 15;
export const CHAIN_MIN = 5;
const CELL: Cell = { w: TASK_W, h: TASK_H, gapX: SERP_GAP_X, gapY: SERP_GAP_Y };

export type { Box } from './loopLane';

/** SVG path + a label midpoint. Ring steps carry an empty path (the circle
 *  draws them) and only the chip-anchor midpoint. */
export interface Seg {
  path: string;
  mid: { x: number; y: number };
  serpKind?: SerpKind;
}

/** One loop ring, expanded to a circle. `backEdgeId` lets a live consumer look
 *  up the turn counter for the ring; the static consumer ignores it. */
export interface RingLayout {
  center: { x: number; y: number };
  r: number;
  members: string[];
  edgeIds: string[];
  chevrons: { x: number; y: number; rotDeg: number }[];
  backEdgeId: string;
}

/** Minimal render-edge shape the layout needs: the id keys ring steps and arc
 *  lanes; `loopArc` marks an over-the-top arc kept out of the dagre layout. */
export interface LayoutEdgeInput {
  id: string;
  from: string;
  to: string;
  isLoop?: boolean;
  loopArc?: boolean;
}

export interface GraphLayoutInput {
  /** All layout-graph node ids (tasks + synthetic junctions). */
  nodes: string[];
  /** The acyclic layout edges dagre ranks (loop back-edges excluded). */
  layoutEdges: { from: string; to: string }[];
  /** Render edges — for ring-step detection, serpentine keying, arc drawing. */
  renderEdges: LayoutEdgeInput[];
  /** Detected loop rings (multi-member ones fold into circles). */
  ringLanes: RingLane[];
  /** Detected linear chains (fold into serpentine blocks). */
  chains: ChainFold[];
  /** Which layout nodes are synthetic hyperedge junctions (sized small). */
  junctionIds: Set<string>;
  /** Real footprint per junction, keyed by junction id. A gated junction
   * renders as a wide gate-chip pill, so it is sized to that pill (not the 20px
   * dot) — dagre reserves the room AND edge touchdowns land on the pill's actual
   * perimeter, spread across it, instead of piling onto the centre. Ungated
   * junctions fall back to the dot. */
  junctionBox?: Map<string, { width: number; height: number }>;
}

export interface GraphLayout {
  pos: Record<string, Box>;
  width: number;
  height: number;
  arcLanes: Map<string, number>;
  rings: RingLayout[];
  /** Serpentine connector paths + ring-step chip anchors, keyed by edge id. */
  internalSeg: Map<string, Seg>;
  /** Edge ids that are a ring step — drawn by the circle, never as a path. */
  ringStepIds: Set<string>;
  /** Routed paths for plain (non-fold, non-arc) edges: fan-spread where several
   * edges share an endpoint, bowed around a loop ring when one endpoint sits on
   * it. Keyed by edge id; a consumer falls back to a plain bezier on a miss. */
  edgeRoutes: Map<string, Seg>;
}

/** Estimate a gated junction's footprint from its gate-chip label(s), mirroring
 *  the `.hg-gate` CSS (mono ~11px, leading icon, 10px side padding, capped at the
 *  168px max-width) so the layout reserves the pill's real width — chips lay out
 *  side by side, so multiple gates sum. Height covers one chip row. */
export function gateChipBox(labels: string[]): { width: number; height: number } {
  const chip = (t: string) => Math.min(176, 52 + t.length * 6.4);
  const width = labels.reduce((s, t) => s + chip(t), 0) + Math.max(0, labels.length - 1) * 6;
  return { width: Math.max(width, JUNCTION), height: 30 };
}

/** SVG path + label midpoint for a serpentine connector between member centers
 *  `a`→`b`. Bypasses the plain bezier's left-to-right assumption so R→L rows and
 *  the vertical U-bend turns draw correctly. */
export function serpSegment(
  kind: SerpKind,
  a: { x: number; y: number },
  b: { x: number; y: number },
): Seg {
  const hw = TASK_W / 2;
  if (kind === 'h-lr') {
    const ax = a.x + hw;
    const bx = b.x - hw;
    const d = Math.max(18, (bx - ax) * 0.4);
    return { path: `M ${ax} ${a.y} C ${ax + d} ${a.y}, ${bx - d} ${b.y}, ${bx} ${b.y}`, mid: { x: (ax + bx) / 2, y: (a.y + b.y) / 2 } };
  }
  if (kind === 'h-rl') {
    const ax = a.x - hw;
    const bx = b.x + hw;
    const d = Math.max(18, (ax - bx) * 0.4);
    return { path: `M ${ax} ${a.y} C ${ax - d} ${a.y}, ${bx + d} ${b.y}, ${bx} ${b.y}`, mid: { x: (ax + bx) / 2, y: (a.y + b.y) / 2 } };
  }
  // A serpentine row-turn: the two members are stacked (same x), so the
  // connector is a rounded U bulging out past the row end — exit the near member
  // horizontally, round down the outside, round back into the far member
  // horizontally with the arrow landing head-on. The straight verticals with
  // arc corners read unmistakably as "the row folds here" (a shallow bezier hook
  // did not). `r` clamps to the vertical half-span so short folds stay smooth.
  const r = Math.min(SERP_TURN_R, Math.abs(b.y - a.y) / 2);
  if (kind === 'turn-r') {
    const ax = a.x + hw;
    const px = Math.max(ax, b.x + hw) + SERP_TURN;
    return {
      path: `M ${ax} ${a.y} L ${px - r} ${a.y} Q ${px} ${a.y} ${px} ${a.y + r} L ${px} ${b.y - r} Q ${px} ${b.y} ${px - r} ${b.y} L ${b.x + hw} ${b.y}`,
      mid: { x: px, y: (a.y + b.y) / 2 },
    };
  }
  // turn-l
  const ax = a.x - hw;
  const px = Math.min(ax, b.x - hw) - SERP_TURN;
  return {
    path: `M ${ax} ${a.y} L ${px + r} ${a.y} Q ${px} ${a.y} ${px} ${a.y + r} L ${px} ${b.y - r} Q ${px} ${b.y} ${px + r} ${b.y} L ${b.x - hw} ${b.y}`,
    mid: { x: px, y: (a.y + b.y) / 2 },
  };
}

/** Boundary point of a node's box toward (tx,ty) plus the unit exit direction —
 *  lets an edge leave a node toward its actual neighbour instead of always the
 *  right edge, so loop fan-in legs and off-axis edges stay tidy. */
function boundary(c: Box, tx: number, ty: number) {
  let dx = tx - c.x;
  let dy = ty - c.y;
  const L = Math.hypot(dx, dy) || 1;
  dx /= L;
  dy /= L;
  const sx = dx !== 0 ? c.width / 2 / Math.abs(dx) : Infinity;
  const sy = dy !== 0 ? c.height / 2 / Math.abs(dy) : Infinity;
  const s = Math.min(sx, sy);
  return { x: c.x + dx * s, y: c.y + dy * s, dx, dy };
}

/** A boundary-aware bezier between two positioned nodes. */
export function bezier(pos: Record<string, Box>, from: string, to: string): Seg | null {
  const a = pos[from];
  const b = pos[to];
  if (!a || !b) return null;
  const s = boundary(a, b.x, b.y);
  const e = boundary(b, a.x, a.y);
  const k = Math.max(22, Math.hypot(e.x - s.x, e.y - s.y) * 0.4);
  return {
    path: `M ${s.x} ${s.y} C ${s.x + s.dx * k} ${s.y + s.dy * k}, ${e.x + e.dx * k} ${e.y + e.dy * k}, ${e.x} ${e.y}`,
    mid: { x: (s.x + e.x) / 2, y: (s.y + e.y) / 2 },
  };
}

/** A self-loop / non-ring loop edge as the shared over-the-top arc, lifted by
 *  its stagger lane. */
export function loopArc(
  pos: Record<string, Box>,
  arcLanes: Map<string, number>,
  id: string,
  from: string,
  to: string,
) {
  const a = pos[from];
  const b = pos[to];
  if (!a || !b) return null;
  return loopArcPath(a, b, arcLanes.get(id) ?? 0);
}

/**
 * Route the plain (non-fold, non-arc) edges with global awareness the per-edge
 * bezier lacks: (1) fan-spread — when several edges share a target (or source),
 * their attach points spread along that node's boundary, sorted by the opposite
 * endpoint's angle, so arrowheads fan out instead of stacking on one point; and
 * (2) loop bow — when exactly one endpoint sits on a loop ring, both control
 * points are pushed radially outward from that ring's centre, so the edge arcs
 * around the circle instead of slicing straight through it. Simple 1→1 edges
 * with no shared endpoint and no ring get the plain boundary bezier unchanged.
 */
export function computeEdgeRoutes(
  renderEdges: LayoutEdgeInput[],
  pos: Record<string, Box>,
  rings: RingLayout[],
  ringStepIds: Set<string>,
  internalSeg: Map<string, Seg>,
): { routes: Map<string, Seg>; maxX: number; maxY: number } {
  const memberCenter = new Map<string, { x: number; y: number }>();
  for (const rg of rings) for (const m of rg.members) memberCenter.set(m, rg.center);

  const plain = renderEdges.filter(
    (e) => !e.loopArc && !ringStepIds.has(e.id) && !internalSeg.has(e.id) && pos[e.from] && pos[e.to],
  );
  const angleFrom = (hub: string, other: string) =>
    Math.atan2(pos[other].y - pos[hub].y, pos[other].x - pos[hub].x);
  const inByTarget = new Map<string, LayoutEdgeInput[]>();
  const outBySource = new Map<string, LayoutEdgeInput[]>();
  for (const e of plain) {
    if (!inByTarget.has(e.to)) inByTarget.set(e.to, []);
    inByTarget.get(e.to)!.push(e);
    if (!outBySource.has(e.from)) outBySource.set(e.from, []);
    outBySource.get(e.from)!.push(e);
  }
  for (const [hub, list] of inByTarget) list.sort((a, b) => angleFrom(hub, a.from) - angleFrom(hub, b.from));
  for (const [hub, list] of outBySource) list.sort((a, b) => angleFrom(hub, a.to) - angleFrom(hub, b.to));

  const SPREAD = 13;
  const BOW = 44;
  const routes = new Map<string, Seg>();
  let maxX = 0;
  let maxY = 0;
  for (const e of plain) {
    const A = pos[e.from];
    const B = pos[e.to];
    let s = boundary(A, B.x, B.y);
    let t = boundary(B, A.x, A.y);
    // Spread the entry among edges sharing this target (perpendicular to entry).
    const inSibs = inByTarget.get(e.to)!;
    if (inSibs.length > 1) {
      const frac = inSibs.indexOf(e) - (inSibs.length - 1) / 2;
      t = { x: t.x - t.dy * frac * SPREAD, y: t.y + t.dx * frac * SPREAD, dx: t.dx, dy: t.dy };
    }
    // Spread the exit among edges sharing this source.
    const outSibs = outBySource.get(e.from)!;
    if (outSibs.length > 1) {
      const frac = outSibs.indexOf(e) - (outSibs.length - 1) / 2;
      s = { x: s.x - s.dy * frac * SPREAD, y: s.y + s.dx * frac * SPREAD, dx: s.dx, dy: s.dy };
    }
    const k = Math.max(22, Math.hypot(t.x - s.x, t.y - s.y) * 0.4);
    let c1x = s.x + s.dx * k;
    let c1y = s.y + s.dy * k;
    let c2x = t.x + t.dx * k;
    let c2y = t.y + t.dy * k;
    // Loop bow: exactly one endpoint on a ring → push controls radially outward.
    const fromRing = memberCenter.get(e.from);
    const toRing = memberCenter.get(e.to);
    const cen = fromRing && !toRing ? fromRing : toRing && !fromRing ? toRing : undefined;
    if (cen) {
      let ox = (s.x + t.x) / 2 - cen.x;
      let oy = (s.y + t.y) / 2 - cen.y;
      const L = Math.hypot(ox, oy) || 1;
      ox /= L;
      oy /= L;
      c1x += ox * BOW;
      c1y += oy * BOW;
      c2x += ox * BOW;
      c2y += oy * BOW;
    }
    routes.set(e.id, {
      path: `M ${s.x} ${s.y} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${t.x} ${t.y}`,
      mid: { x: (s.x + t.x) / 2, y: (s.y + t.y) / 2 },
    });
    maxX = Math.max(maxX, s.x, t.x, c1x, c2x);
    maxY = Math.max(maxY, s.y, t.y, c1y, c2y);
  }
  return { routes, maxX, maxY };
}

/**
 * Run the collapse → dagre → expand pipeline. Groups (linear chains and
 * multi-member loop rings) each collapse to one placeholder dagre lays out;
 * every placeholder then expands back — a chain into its serpentine grid, a
 * ring into its circle. Self-loop and non-ring loop arcs stay out of dagre and
 * are staggered into lanes with the whole layout pushed down so the highest arc
 * is not clipped.
 */
export function computeGraphLayout(input: GraphLayoutInput): GraphLayout {
  const { nodes, layoutEdges, renderEdges, ringLanes, chains, junctionIds, junctionBox } = input;

  // Stage 1 — the groups to fold: chains (serpentine) and multi-member rings
  // (circles). Ring members never chain-fold (chain detection excludes them).
  type Group = { members: string[]; ring: boolean };
  const groups: Group[] = [
    ...chains.map((c) => ({ members: c.members, ring: false })),
    ...ringLanes.filter((r) => r.members.length > 1).map((r) => ({ members: r.members, ring: true })),
  ];
  const memberToGroup = new Map<string, number>();
  groups.forEach((grp, gi) => grp.members.forEach((m) => memberToGroup.set(m, gi)));
  const placeholderId = (gi: number) => `grp:${groups[gi].members[0]}`;
  const groupBox = (gi: number) => {
    const { members, ring } = groups[gi];
    const n = members.length;
    return ring
      ? ringCircleBlock(ringCircleRadius(n, TASK_W), TASK_W, TASK_H)
      : serpentineBlockSize(n, chainColumns(n, CELL), CELL, SERP_TURN);
  };

  const renderEdgeByPair = new Map<string, string>();
  for (const e of renderEdges) renderEdgeByPair.set(`${e.from} ${e.to}`, e.id);
  // Ring-step edge ids — these draw as the circle, never as separate paths.
  const ringStepIds = new Set<string>();
  for (const r of ringLanes) {
    if (r.members.length <= 1) continue;
    r.members.forEach((m, i) => {
      const id = renderEdgeByPair.get(`${m} ${r.members[(i + 1) % r.members.length]}`);
      if (id) ringStepIds.add(id);
    });
  }

  const g = new dagre.graphlib.Graph();
  g.setGraph({ rankdir: 'LR', nodesep: 36, ranksep: 80, marginx: 24, marginy: 24 });
  g.setDefaultEdgeLabel(() => ({}));

  // Stage 2 — collapse: non-member nodes as-is; each group one placeholder. A
  // gated junction is sized to its gate-chip pill (via junctionBox) so dagre
  // reserves the room and edge touchdowns land on the pill's real perimeter.
  for (const n of nodes) {
    if (memberToGroup.has(n)) continue;
    const box = junctionIds.has(n)
      ? junctionBox?.get(n) ?? { width: JUNCTION, height: JUNCTION }
      : { width: TASK_W, height: TASK_H };
    g.setNode(n, box);
  }
  groups.forEach((_, gi) => {
    const box = groupBox(gi);
    g.setNode(placeholderId(gi), { width: box.width, height: box.height });
  });

  // Drop intra-group edges; reroute a member endpoint to its placeholder; dedupe.
  const mapNode = (id: string) => (memberToGroup.has(id) ? placeholderId(memberToGroup.get(id)!) : id);
  const seen = new Set<string>();
  for (const e of layoutEdges) {
    const ca = memberToGroup.get(e.from);
    const cb = memberToGroup.get(e.to);
    if (ca !== undefined && ca === cb) continue; // internal — drawn inside the block
    const from = mapNode(e.from);
    const to = mapNode(e.to);
    if (from === to) continue;
    const key = `${from} ${to}`;
    if (seen.has(key)) continue;
    seen.add(key);
    g.setEdge(from, to);
  }

  dagre.layout(g);

  const p: Record<string, Box> = {};
  for (const n of nodes) {
    if (memberToGroup.has(n)) continue;
    const node = g.node(n);
    if (node) p[n] = { x: node.x, y: node.y, width: node.width, height: node.height };
  }

  // Self-loop / non-ring arcs draw above dagre-placed nodes and outside its
  // bounds; assign stagger lanes and push the layout down so the highest arc
  // stays inside the canvas. Ring steps are excluded (their block reserves the
  // room). Computed BEFORE expansion so member and circle coordinates are minted
  // post-shift and never desync.
  const arcSpans: ArcSpan[] = [];
  for (const e of renderEdges) {
    if (!e.loopArc || ringStepIds.has(e.id)) continue;
    const a = p[e.from];
    const b = p[e.to];
    if (!a || !b) continue;
    arcSpans.push({ id: e.id, lo: Math.min(a.x, b.x), hi: Math.max(a.x, b.x) });
  }
  const arcLanes = assignArcLanes(arcSpans);
  let minTop = 0;
  for (const e of renderEdges) {
    if (!e.loopArc || ringStepIds.has(e.id)) continue;
    const a = p[e.from];
    const b = p[e.to];
    if (!a || !b) continue;
    minTop = Math.min(minTop, loopArcPath(a, b, arcLanes.get(e.id) ?? 0).top);
  }
  const offsetY = minTop < 8 ? 8 - minTop : 0;
  if (offsetY) for (const id of Object.keys(p)) p[id] = { ...p[id], y: p[id].y + offsetY };

  // Stage 3 — expand each placeholder into member positions: a ring into a
  // circle (cards on the rim, gate-chip anchors + chevrons along it), a chain
  // into the serpentine grid with its connector segments (keyed by the real
  // render-edge id). A lookup miss is LOUD — silent degradation is how the old
  // ring fold shipped broken unnoticed.
  const internalSeg = new Map<string, Seg>();
  const rings: RingLayout[] = [];
  groups.forEach((grp, gi) => {
    const ph = g.node(placeholderId(gi));
    if (!ph) return;
    const phy = ph.y + offsetY;
    const n = grp.members.length;
    if (grp.ring) {
      const lane = ringLanes.find((r) => r.members[0] === grp.members[0]);
      const r = ringCircleRadius(n, TASK_W);
      const center = { x: ph.x, y: phy };
      const centers = ringCirclePositions(n, center, r);
      grp.members.forEach((m, i) => {
        p[m] = { x: centers[i].x, y: centers[i].y, width: TASK_W, height: TASK_H };
      });
      const chipAt = ringCircleWaypoints(n, center, r, 0.5);
      const edgeIds: string[] = [];
      for (let i = 0; i < n; i++) {
        const id = renderEdgeByPair.get(`${grp.members[i]} ${grp.members[(i + 1) % n]}`);
        if (!id) {
          console.warn(
            `ring circle: no render edge for ${grp.members[i]} → ${grp.members[(i + 1) % n]}; its gate chip is dropped`,
          );
          continue;
        }
        edgeIds.push(id);
        // Empty path: the circle draws the step; only the chip anchor is needed.
        // The +26 cancels the badge renderer's fixed -26 offset so the chip
        // centers on the rim.
        internalSeg.set(id, { path: '', mid: { x: chipAt[i].x, y: chipAt[i].y + 26 } });
      }
      rings.push({
        center,
        r,
        members: grp.members,
        edgeIds,
        chevrons: ringCircleWaypoints(n, center, r, 0.25),
        backEdgeId: lane?.backEdgeId ?? '',
      });
      return;
    }
    const k = chainColumns(n, CELL);
    const box = serpentineBlockSize(n, k, CELL, SERP_TURN);
    const origin = { x: ph.x - box.width / 2 + SERP_TURN, y: phy - box.height / 2 };
    const centers = serpentinePositions(n, k, origin, CELL);
    grp.members.forEach((m, i) => {
      p[m] = { x: centers[i].x, y: centers[i].y, width: TASK_W, height: TASK_H };
    });
    const kinds = serpentineConnectors(n, k);
    for (let i = 0; i < n - 1; i++) {
      const id = renderEdgeByPair.get(`${grp.members[i]} ${grp.members[i + 1]}`);
      if (!id) {
        console.warn(
          `serpentine fold: no render edge for ${grp.members[i]} → ${grp.members[i + 1]}; falling back to a generic bezier`,
        );
        continue;
      }
      internalSeg.set(id, { ...serpSegment(kinds[i], centers[i], centers[i + 1]), serpKind: kinds[i] });
    }
  });

  // Canvas: dagre already sized for the placeholder boxes; grow defensively to
  // cover expanded members and the arc offset.
  const gr = g.graph();
  let width = gr.width ?? 400;
  let height = (gr.height ?? 200) + offsetY;
  for (const id of Object.keys(p)) {
    width = Math.max(width, p[id].x + p[id].width / 2 + 24);
    height = Math.max(height, p[id].y + p[id].height / 2 + 24);
  }

  // Route plain edges with fan-spread + loop-bow, then grow the canvas so a
  // curve bowed outside the node extents is not clipped.
  const { routes: edgeRoutes, maxX, maxY } = computeEdgeRoutes(renderEdges, p, rings, ringStepIds, internalSeg);
  width = Math.max(width, maxX + 24);
  height = Math.max(height, maxY + 24);

  return { pos: p, width, height, arcLanes, rings, internalSeg, ringStepIds, edgeRoutes };
}
