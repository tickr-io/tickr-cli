import { describe, it, expect } from 'vitest';
import {
  assignArcLanes,
  detectRingLanes,
  loopArcPath,
  ringCircleBlock,
  ringCirclePositions,
  ringCircleRadius,
  ringCircleWaypoints,
  sequenceLoopEdges,
  type ArcSpan,
} from './loopLane';

describe('ring circle geometry', () => {
  it('places the entry at 12 o’clock and proceeds clockwise', () => {
    const r = 200;
    const p = ringCirclePositions(4, { x: 0, y: 0 }, r);
    // top (entry), right, bottom, left — clockwise on screen (y down).
    expect(p[0].x).toBeCloseTo(0);
    expect(p[0].y).toBeCloseTo(-r);
    expect(p[1].x).toBeCloseTo(r);
    expect(p[1].y).toBeCloseTo(0);
    expect(p[2].y).toBeCloseTo(r);
    expect(p[3].x).toBeCloseTo(-r);
  });

  it('scales the radius with member count and clamps to a minimum', () => {
    expect(ringCircleRadius(2, 178)).toBeGreaterThanOrEqual(120);
    expect(ringCircleRadius(8, 178)).toBeGreaterThan(ringCircleRadius(3, 178));
    expect(ringCircleRadius(1, 178)).toBe(0);
  });

  it('waypoints sit between members with a clockwise tangent rotation', () => {
    const w = ringCircleWaypoints(4, { x: 0, y: 0 }, 100, 0.5);
    expect(w).toHaveLength(4);
    // First waypoint is at 45° past the top; its chevron points down-right.
    expect(w[0].x).toBeCloseTo(100 * Math.cos(-Math.PI / 4));
    expect(w[0].y).toBeCloseTo(100 * Math.sin(-Math.PI / 4));
    expect(w[0].rotDeg).toBeCloseTo(45);
  });

  it('reserves a block covering the circle, the cards, and the rim chips', () => {
    // diameter (300) + card extents + 48px rim-chip overhang
    expect(ringCircleBlock(150, 178, 54)).toEqual({ width: 526, height: 402 });
  });
});

describe('detectRingLanes — ring orientation is deterministic', () => {
  const ring = [
    { id: 'l0', from: 'judge', to: 'grilly' },
    { id: 'l1', from: 'grilly', to: 'griller' },
    { id: 'l2', from: 'griller', to: 'judge' },
  ];

  it('rotates the lane to start at the external-entry member; back-edge = last → entry', () => {
    const lanes = detectRingLanes(ring, (id) => id === 'judge');
    expect(lanes).toEqual([{ members: ['judge', 'grilly', 'griller'], backEdgeId: 'l2' }]);
  });

  it('is independent of edge order', () => {
    for (const order of [ring, [...ring].reverse(), [ring[2], ring[0], ring[1]]]) {
      expect(detectRingLanes(order, (id) => id === 'judge')).toEqual([
        { members: ['judge', 'grilly', 'griller'], backEdgeId: 'l2' },
      ]);
    }
  });

  it('falls back to the lexicographically smallest member when nothing enters the ring', () => {
    const lanes = detectRingLanes(ring, () => false);
    expect(lanes).toEqual([{ members: ['griller', 'judge', 'grilly'], backEdgeId: 'l1' }]);
  });

  it('yields a single-member lane for a self-loop', () => {
    expect(detectRingLanes([{ id: 'self', from: 'A', to: 'A' }], () => false)).toEqual([
      { members: ['A'], backEdgeId: 'self' },
    ]);
  });

  it('bails on a member with two loop successors (not the mkLoop shape)', () => {
    expect(
      detectRingLanes(
        [
          { id: 'a', from: 'A', to: 'B' },
          { id: 'b', from: 'A', to: 'C' },
        ],
        () => false,
      ),
    ).toEqual([]);
  });

  it('ignores a loop walk that never closes (falls to the sequencing fallback)', () => {
    expect(
      detectRingLanes(
        [
          { id: 'a', from: 'A', to: 'B' },
          { id: 'b', from: 'B', to: 'C' },
        ],
        () => false,
      ),
    ).toEqual([]);
  });
});

describe('sequenceLoopEdges — forward steps vs the back-edge', () => {
  it('marks the cycle-closing edge as the arc and feeds the rest into the layout', () => {
    const layout: { from: string; to: string }[] = [];
    const arcs = sequenceLoopEdges(layout, [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
      { from: 'C', to: 'A' },
    ]);
    expect(arcs).toEqual([false, false, true]);
    expect(layout).toEqual([
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
    ]);
  });

  it('always arcs a self-edge', () => {
    const layout: { from: string; to: string }[] = [];
    expect(sequenceLoopEdges(layout, [{ from: 'A', to: 'A' }])).toEqual([true]);
    expect(layout).toEqual([]);
  });

  it('closes exactly one arc per ring regardless of edge order', () => {
    const ring = [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
      { from: 'C', to: 'A' },
    ];
    for (const order of [ring, [...ring].reverse(), [ring[2], ring[0], ring[1]]]) {
      const layout: { from: string; to: string }[] = [];
      const arcs = sequenceLoopEdges(layout, order);
      expect(arcs.filter(Boolean)).toHaveLength(1);
      expect(layout).toHaveLength(2); // N − 1 forward steps
    }
  });

  it('respects pre-existing base-layout reachability', () => {
    // Base already carries A→B; a loop edge B→A is therefore the back-edge.
    const layout = [{ from: 'A', to: 'B' }];
    expect(sequenceLoopEdges(layout, [{ from: 'B', to: 'A' }])).toEqual([true]);
    expect(layout).toHaveLength(1);
  });
});

describe('loopArcPath — over-the-top arc geometry', () => {
  const box = (x: number, y: number) => ({ x, y, width: 178, height: 54 });

  it('bows above both nodes and reports the highest point it reaches', () => {
    const seg = loopArcPath(box(400, 100), box(100, 100));
    expect(seg.path.startsWith('M ')).toBe(true);
    expect(seg.path).toContain(' C '); // cubic arc, never an SVG `A` circle arc
    expect(seg.mid.y).toBeLessThan(100 - 27); // peak above the node tops
    expect(seg.top).toBeLessThan(seg.mid.y); // room for a badge above the peak
  });

  it('lifts the peak by the stagger lane', () => {
    const flat = loopArcPath(box(400, 100), box(100, 100), 0);
    const lifted = loopArcPath(box(400, 100), box(100, 100), 2);
    expect(lifted.mid.y).toBeLessThan(flat.mid.y);
  });

  it('draws a self-loop lane when the boxes coincide', () => {
    const seg = loopArcPath(box(100, 100), box(100, 100));
    expect(seg.mid.x).toBe(100);
    expect(seg.mid.y).toBeLessThan(100 - 27);
  });
});

describe('assignArcLanes — overlapping loop arcs stagger', () => {
  it('puts a single arc on lane 0', () => {
    const lanes = assignArcLanes([{ id: 'a', lo: 0, hi: 100 }]);
    expect(lanes.get('a')).toBe(0);
  });

  it('lifts two horizontally overlapping arcs onto distinct lanes', () => {
    const arcs: ArcSpan[] = [
      { id: 'a', lo: 0, hi: 100 },
      { id: 'b', lo: 50, hi: 150 },
    ];
    const lanes = assignArcLanes(arcs);
    expect(new Set([lanes.get('a'), lanes.get('b')]).size).toBe(2);
  });

  it('reuses lane 0 for two disjoint, non-overlapping arcs', () => {
    const arcs: ArcSpan[] = [
      { id: 'a', lo: 0, hi: 40 },
      { id: 'b', lo: 60, hi: 100 },
    ];
    const lanes = assignArcLanes(arcs);
    expect(lanes.get('a')).toBe(0);
    expect(lanes.get('b')).toBe(0);
  });

  it('is order-independent for the lane count', () => {
    const arcs: ArcSpan[] = [
      { id: 'a', lo: 0, hi: 100 },
      { id: 'b', lo: 50, hi: 150 },
    ];
    const lanesFwd = assignArcLanes(arcs);
    const lanesRev = assignArcLanes([...arcs].reverse());
    expect(Math.max(...lanesFwd.values())).toBe(Math.max(...lanesRev.values()));
  });
});
