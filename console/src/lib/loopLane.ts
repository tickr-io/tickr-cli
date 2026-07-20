/**
 * The loop lane — the one shared visual language for `kind = Loop` edges,
 * consumed by BOTH graph surfaces (static TaskGraphTab and live
 * InstanceGraphTab): a ring's forward steps lie flat in the dagre layout like
 * the chain they are, and the cycle's back-edge stays out of the layout and
 * draws as an over-the-top arc. One module owns the sequencing and the arc
 * geometry so the two surfaces cannot drift — the ring-fold audit's failure
 * mode was exactly a bespoke loop layout living in one tab, verified nowhere.
 */

export interface Box {
  x: number;
  y: number;
  width: number;
  height: number;
}

/** Extra vertical lift per arc lane, so overlapping loop arcs stagger. */
export const ARC_STAGGER = 24;

/** A loop ring rendered as a lane: members on ONE horizontal row in execution
 * order (entry first), the closing step drawn as the over-the-top arc. */
export interface RingLane {
  /** Ring members in execution order, entry first. */
  members: string[];
  /** The loop edge closing the cycle (last member → entry) — the arc. */
  backEdgeId: string;
}

/**
 * Detect loop rings and orient them as lanes. Works on the 1→1 loop edges of
 * the mkLoop shape (each member has exactly one loop successor); anything
 * messier — a member with two loop out-edges, a walk that never closes — is
 * left out for the caller's `sequenceLoopEdges` fallback.
 *
 * The entry (lane start) is chosen DETERMINISTICALLY, never by blob iteration
 * order: the first member in cycle order with an external non-loop in-edge
 * (`isEntry` — where control actually enters the ring), falling back to the
 * lexicographically smallest member id. The back-edge is then the loop edge
 * from the last member into the entry.
 */
export function detectRingLanes(
  loops: readonly { id: string; from: string; to: string }[],
  isEntry: (id: string) => boolean,
): RingLane[] {
  const next = new Map<string, { id: string; to: string }>();
  for (const l of loops) {
    if (next.has(l.from)) return []; // two loop successors — not the mkLoop shape
    next.set(l.from, { id: l.id, to: l.to });
  }
  const lanes: RingLane[] = [];
  const consumed = new Set<string>();
  for (const l of loops) {
    if (consumed.has(l.from)) continue;
    const path = [l.from];
    const seen = new Set([l.from]);
    let cur = next.get(l.from);
    let closed = false;
    while (cur) {
      if (cur.to === path[0]) {
        closed = true;
        break;
      }
      if (seen.has(cur.to)) break; // enters a cycle that excludes the start — not clean
      path.push(cur.to);
      seen.add(cur.to);
      cur = next.get(cur.to);
    }
    path.forEach((m) => consumed.add(m));
    if (!closed) continue;
    let k = path.findIndex(isEntry);
    if (k < 0) k = path.indexOf([...path].sort()[0]);
    const members = [...path.slice(k), ...path.slice(0, k)];
    lanes.push({ members, backEdgeId: next.get(members[members.length - 1])!.id });
  }
  return lanes;
}

/**
 * Sequence 1→1 loop edges against an acyclic base layout. A forward ring step
 * is APPENDED to `layoutEdges` (mutating it) so its endpoints rank
 * left-to-right like the chain they are; an edge whose target can already
 * reach its source closes the cycle — the back-edge — and stays out of the
 * layout. A self-edge is always a back-edge. Returns one arc flag per entry
 * of `loops`, aligned by index (`true` = draw as an over-the-top arc).
 */
export function sequenceLoopEdges(
  layoutEdges: { from: string; to: string }[],
  loops: readonly { from: string; to: string }[],
): boolean[] {
  const canReach = (start: string, goal: string): boolean => {
    const stack = [start];
    const seen = new Set<string>();
    while (stack.length) {
      const cur = stack.pop() as string;
      if (cur === goal) return true;
      if (seen.has(cur)) continue;
      seen.add(cur);
      for (const ed of layoutEdges) if (ed.from === cur) stack.push(ed.to);
    }
    return false;
  };
  return loops.map((le) => {
    const arc = le.from === le.to || canReach(le.to, le.from);
    if (!arc) layoutEdges.push({ from: le.from, to: le.to });
    return arc;
  });
}

/**
 * The over-the-top arc for a loop back-edge: a self-loop lane when the boxes
 * coincide, or an arc bowing above both nodes when they differ. `lane` lifts
 * the peak so overlapping loops stagger. `top` is the highest (smallest-y)
 * point the drawing reaches — including room for a gate badge — so the caller
 * can grow the canvas to keep the arc unclipped.
 */
export function loopArcPath(
  a: Box,
  b: Box,
  lane = 0,
): { path: string; mid: { x: number; y: number }; top: number } {
  const lift = lane * ARC_STAGGER;
  if (a.x === b.x && a.y === b.y) {
    const topY = a.y - a.height / 2;
    const peakY = topY - 30 - lift;
    return {
      path: `M ${a.x - 16} ${topY} C ${a.x - 44} ${peakY}, ${a.x + 44} ${peakY}, ${a.x + 16} ${topY}`,
      mid: { x: a.x, y: peakY - 2 },
      top: peakY - 2 - 26,
    };
  }
  const ay = a.y - a.height / 2;
  const by = b.y - b.height / 2;
  // Rise scales with the horizontal span so an arc sweeping a whole ring lane
  // bows visibly instead of hugging the node tops.
  const rise = 40 + Math.min(64, Math.abs(b.x - a.x) * 0.07);
  const peakY = Math.min(ay, by) - rise - lift;
  return {
    path: `M ${a.x} ${ay} C ${a.x} ${peakY}, ${b.x} ${peakY}, ${b.x} ${by}`,
    mid: { x: (a.x + b.x) / 2, y: peakY },
    top: peakY - 26,
  };
}

// ─── Ring circle geometry ────────────────────────────────────────────────────
// A multi-member ring renders as a literal circle: the full circle IS the loop
// lane (drawn dashed under the cards — the old ring fold failed precisely
// because the circle was never drawn, leaving 3 cards on an invisible circle
// to read as a triangle), members sit on it clockwise in execution order with
// the entry at 12 o'clock, chevrons on the rim give direction, and the center
// carries the loop glyph / turn counter.

/** Radius so adjacent member cards on the circle keep `gap` px between their
 *  centers' chord beyond one card width. */
export function ringCircleRadius(n: number, cardW: number, gap = 90, minR = 120): number {
  if (n <= 1) return 0;
  return Math.max(minR, (cardW + gap) / (2 * Math.sin(Math.PI / n)));
}

/** Member centers on the circle: entry (index 0) at 12 o'clock, clockwise in
 *  execution order. */
export function ringCirclePositions(
  n: number,
  center: { x: number; y: number },
  radius: number,
): { x: number; y: number }[] {
  const out: { x: number; y: number }[] = [];
  for (let i = 0; i < n; i++) {
    const t = -Math.PI / 2 + (i * 2 * Math.PI) / n;
    out.push({ x: center.x + radius * Math.cos(t), y: center.y + radius * Math.sin(t) });
  }
  return out;
}

/** Points on the rim between consecutive members (member i → i+1), at
 *  `fraction` of the angular step (0.5 = halfway). `rotDeg` is the clockwise
 *  tangent direction at that point — the rotation for a direction chevron. */
export function ringCircleWaypoints(
  n: number,
  center: { x: number; y: number },
  radius: number,
  fraction = 0.5,
): { x: number; y: number; rotDeg: number }[] {
  const out: { x: number; y: number; rotDeg: number }[] = [];
  for (let i = 0; i < n; i++) {
    const t = -Math.PI / 2 + ((i + fraction) * 2 * Math.PI) / n;
    out.push({
      x: center.x + radius * Math.cos(t),
      y: center.y + radius * Math.sin(t),
      rotDeg: (t * 180) / Math.PI + 90,
    });
  }
  return out;
}

/** Bounding box a ring circle needs in the layout: the circle plus card
 *  extents plus rim-chip overhang. */
export function ringCircleBlock(
  radius: number,
  cardW: number,
  cardH: number,
): { width: number; height: number } {
  return { width: 2 * radius + cardW + 48, height: 2 * radius + cardH + 48 };
}

/** A loop arc's horizontal span on the laid-out canvas. */
export interface ArcSpan {
  id: string;
  lo: number;
  hi: number;
}

/**
 * Assign each loop arc a lane index so arcs whose horizontal spans overlap never
 * draw at the same height — the consumer raises an arc's peak by its lane so two
 * disjoint loops sharing a span read as distinct, staggered arcs. Greedy
 * interval colouring: a single ring (one span) is always lane 0, order-
 * independent; non-overlapping spans reuse lane 0; overlapping spans climb.
 */
export function assignArcLanes(arcs: ArcSpan[]): Map<string, number> {
  const lanes = new Map<string, number>();
  // Stable order: leftmost start first, then by id so equal starts are
  // deterministic regardless of input edge order.
  const sorted = [...arcs].sort((a, b) => a.lo - b.lo || (a.id < b.id ? -1 : 1));
  const laneEnds: number[] = [];
  for (const s of sorted) {
    let lane = laneEnds.findIndex((end) => end <= s.lo);
    if (lane === -1) {
      lane = laneEnds.length;
      laneEnds.push(s.hi);
    } else {
      laneEnds[lane] = s.hi;
    }
    lanes.set(s.id, lane);
  }
  return lanes;
}
