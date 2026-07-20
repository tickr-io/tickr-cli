import { describe, it, expect } from 'vitest';
import {
  buildHyperGraphModel,
  detectChains,
  serpentinePositions,
  serpentineBlockSize,
  serpentineConnectors,
  chainColumns,
  type Cell,
} from './hyperGraph';

// A definition exercising: a plain 1→1 edge, a multi-source hyperedge, a
// 1→1 gated edge (signal), and a predicate gate reading a routing var that a
// task produces. Gate payloads use the REAL wire variants (`Gate` in
// `server/src/task_graph/edge.rs`): SignalReceived / PredicateHolds /
// TimerElapsed — fixtures keyed to invented shapes are how the gate
// classifier shipped dead.
const definition = {
  tasks: {
    A: { name: 'a', routing_vars: ['x'] },
    B: { name: 'b' },
    C: { name: 'c' },
    D: { name: 'd' },
  },
  task_graph: {
    edges: {
      e1: { id: 'e1', sources: ['A'], targets: ['B'], gates: [] },
      e2: { id: 'e2', sources: ['B', 'C'], targets: ['D'], gates: [] }, // hyperedge (2 sources)
      e3: {
        id: 'e3',
        sources: ['A'],
        targets: ['C'],
        gates: [{ SignalReceived: { signal_name: 's', state: 'Idle', transitions: [] } }],
      },
      e4: {
        id: 'e4',
        sources: ['C'],
        targets: ['D'],
        gates: [
          {
            PredicateHolds: {
              routing_var: 'x',
              op: 'Eq',
              value: { String: 'go' },
              state: 'Idle',
              transitions: [],
            },
          },
        ],
      },
      // structural edge touching a sentinel — must be dropped.
      e5: { id: 'e5', sources: ['start-node'], targets: ['A'], gates: [] },
    },
  },
};

describe('buildHyperGraphModel', () => {
  const m = buildHyperGraphModel(definition);

  it('layout-graph injects one junction per hyperedge with sources+targets synthetic edges', () => {
    expect(m.layout.nodes).toContain('junction:e2');
    // Synthetic edges for e2: B→j, C→j, j→D (2 sources + 1 target = 3).
    const e2Edges = m.layout.edges.filter((e) => e.from === 'junction:e2' || e.to === 'junction:e2');
    expect(e2Edges).toHaveLength(3);
    expect(e2Edges).toContainEqual({ from: 'B', to: 'junction:e2' });
    expect(e2Edges).toContainEqual({ from: 'C', to: 'junction:e2' });
    expect(e2Edges).toContainEqual({ from: 'junction:e2', to: 'D' });
  });

  it('plain 1→1 edges land in dagre directly; sentinel-touching edges are dropped', () => {
    expect(m.layout.edges).toContainEqual({ from: 'A', to: 'B' }); // e1
    expect(m.layout.edges).toContainEqual({ from: 'A', to: 'C' }); // e3
    // e5 touches a non-task sentinel → not present.
    expect(m.layout.edges.some((e) => e.from === 'start-node')).toBe(false);
  });

  it('render-graph carries stable ids and classifies gate kinds', () => {
    expect(m.render.tasks.map((t) => t.id).sort()).toEqual(['A', 'B', 'C', 'D']);
    expect(m.render.junctions).toEqual([{ id: 'junction:e2', edgeId: 'e2', gate: undefined }]);

    const e3 = m.render.edges.find((e) => e.id === 'e3');
    expect(e3?.gate?.kind).toBe('signal');
    const e4 = m.render.edges.find((e) => e.id === 'e4');
    expect(e4?.gate?.kind).toBe('predicate');

    // The producing task carries its routing var (the braces chip source).
    expect(m.render.tasks.find((t) => t.id === 'A')?.routingVars).toEqual(['x']);
  });

  it('selection-graph links the producing task to the predicate gate both ways', () => {
    // A produces 'x'; e4's predicate reads 'x'.
    expect(m.selection.taskToGates['A']).toEqual(['e4']);
    expect(m.selection.gateToTask['e4']).toBe('A');
  });

  it('handles an empty / missing definition without throwing', () => {
    const empty = buildHyperGraphModel(undefined);
    expect(empty.layout.nodes).toEqual([]);
    expect(empty.render.tasks).toEqual([]);
  });
});

// Linear chain `A→B→C→D→E` plus optional extra structure, in the raw-blob shape.
const chainDef = (
  edges: Record<string, { sources: string[]; targets: string[]; gates?: unknown[]; kind?: string }>,
  tasks = ['A', 'B', 'C', 'D', 'E'],
) => ({
  tasks: Object.fromEntries(tasks.map((t) => [t, { name: t.toLowerCase() }])),
  task_graph: {
    edges: Object.fromEntries(
      Object.entries(edges).map(([id, e]) => [id, { id, gates: [], ...e }]),
    ),
  },
});

const spine = {
  ab: { sources: ['A'], targets: ['B'] },
  bc: { sources: ['B'], targets: ['C'] },
  cd: { sources: ['C'], targets: ['D'] },
  de: { sources: ['D'], targets: ['E'] },
};

describe('detectChains', () => {
  it('folds a 5-node pure chain into one ordered run', () => {
    expect(detectChains(chainDef(spine))).toEqual([{ members: ['A', 'B', 'C', 'D', 'E'] }]);
  });

  it('does not fold a chain shorter than minLen', () => {
    const short = chainDef({ ab: spine.ab, bc: spine.bc, cd: spine.cd }, ['A', 'B', 'C', 'D']);
    expect(detectChains(short)).toEqual([]); // 4 < 5
  });

  it('stops the chain at a node that feeds a fan (hyperedge endpoint)', () => {
    // E and G fan into H; the spine ends at E (its tail), the fan stays out.
    const def = chainDef(
      { ...spine, fan: { sources: ['E', 'G'], targets: ['H'] } },
      ['A', 'B', 'C', 'D', 'E', 'G', 'H'],
    );
    expect(detectChains(def)).toEqual([{ members: ['A', 'B', 'C', 'D', 'E'] }]);
  });

  it('never folds across a loop back-edge', () => {
    // E→A closes a ring as kind:Loop; the linear spine still folds unchanged.
    const def = chainDef({ ...spine, back: { sources: ['E'], targets: ['A'], kind: 'Loop' } });
    expect(detectChains(def)).toEqual([{ members: ['A', 'B', 'C', 'D', 'E'] }]);
  });

  it('never folds a mkLoop ring — every member touches a loop edge', () => {
    // A 6-node ring, every step kind:Loop (the shape mkLoop emits). Its forward
    // path would otherwise read as a foldable chain.
    const def = chainDef(
      {
        l0: { sources: ['h'], targets: ['a'], kind: 'Loop' },
        l1: { sources: ['a'], targets: ['b'], kind: 'Loop' },
        l2: { sources: ['b'], targets: ['c'], kind: 'Loop' },
        l3: { sources: ['c'], targets: ['d'], kind: 'Loop' },
        l4: { sources: ['d'], targets: ['e'], kind: 'Loop' },
        l5: { sources: ['e'], targets: ['h'], kind: 'Loop' },
      },
      ['h', 'a', 'b', 'c', 'd', 'e'],
    );
    expect(detectChains(def, 5)).toEqual([]);
  });

  it('keeps the chain intact across a mid-chain gate', () => {
    const def = chainDef({
      ...spine,
      cd: {
        sources: ['C'],
        targets: ['D'],
        gates: [{ PredicateHolds: { routing_var: 'x', op: 'Eq', value: { String: 'go' } } }],
      },
    });
    expect(detectChains(def)).toEqual([{ members: ['A', 'B', 'C', 'D', 'E'] }]);
  });

  it('does not fold across a branch (out-degree > 1)', () => {
    const def = chainDef(
      {
        ab: { sources: ['A'], targets: ['B'] },
        bc: { sources: ['B'], targets: ['C'] },
        bd: { sources: ['B'], targets: ['D'] }, // B branches → no run ≥5
        ce: { sources: ['C'], targets: ['E'] },
        df: { sources: ['D'], targets: ['F'] },
      },
      ['A', 'B', 'C', 'D', 'E', 'F'],
    );
    expect(detectChains(def)).toEqual([]);
  });
});

describe('serpentine geometry', () => {
  const cell: Cell = { w: 178, h: 54, gapX: 56, gapY: 48 };

  it('places members boustrophedon: even rows L→R, odd rows R→L, turns vertical', () => {
    const p = serpentinePositions(7, 3, { x: 0, y: 0 }, cell);
    expect(p).toEqual([
      { x: 89, y: 27 },
      { x: 323, y: 27 },
      { x: 557, y: 27 },
      { x: 557, y: 129 },
      { x: 323, y: 129 },
      { x: 89, y: 129 },
      { x: 89, y: 231 },
    ]);
    // row 1 strictly right-to-left
    expect(p[3].x > p[4].x && p[4].x > p[5].x).toBe(true);
    // turns drop straight down (column aligned)
    expect(p[2].x).toBe(p[3].x);
    expect(p[5].x).toBe(p[6].x);
  });

  it('wraps a 5-node chain into 3 rows with the last row partial', () => {
    const p = serpentinePositions(5, 2, { x: 0, y: 0 }, cell);
    const rows = new Set(p.map((c) => c.y));
    expect(rows.size).toBe(3);
    expect(p[2].x > p[3].x).toBe(true); // row 1 reversed
  });

  it('sizes the placeholder box to the grid plus turn margins', () => {
    expect(serpentineBlockSize(7, 3, cell, 28)).toEqual({ width: 702, height: 258 });
  });

  it('labels connectors per consecutive pair', () => {
    expect(serpentineConnectors(7, 3)).toEqual(['h-lr', 'h-lr', 'turn-r', 'h-rl', 'h-rl', 'turn-l']);
  });

  it('picks a landscape column count', () => {
    expect(chainColumns(5, cell)).toBe(2);
    expect(chainColumns(7, cell)).toBe(3);
  });
});

describe('buildHyperGraphModel — loop lane', () => {
  // judge→grilly→griller→judge, every step kind:Loop (the exact mkLoop shape).
  const ringEdges = {
    l0: { sources: ['judge'], targets: ['grilly'], kind: 'Loop' },
    l1: { sources: ['grilly'], targets: ['griller'], kind: 'Loop' },
    l2: { sources: ['griller'], targets: ['judge'], kind: 'Loop' },
  };

  it('lays a mkLoop ring flat: forward steps enter the layout, the closing step becomes the arc', () => {
    const m = buildHyperGraphModel(chainDef(ringEdges, ['judge', 'grilly', 'griller']));
    const loops = m.render.edges.filter((e) => e.isLoop);
    expect(loops).toHaveLength(3);
    const arcs = loops.filter((e) => e.loopArc);
    expect(arcs).toHaveLength(1); // exactly one back-edge per ring
    // Forward steps rank in the layout; the arc stays out so dagre is acyclic.
    expect(m.layout.edges).toHaveLength(2);
    expect(m.layout.edges.some((le) => le.from === arcs[0].from && le.to === arcs[0].to)).toBe(false);
  });

  it('orients the lane at the member the start sentinel enters, deterministically', () => {
    // A Control edge from the start sentinel targets judge — the ring entry.
    // The lane starts there and the arc is the closing step griller→judge,
    // regardless of blob iteration order.
    const def = chainDef(
      { ...ringEdges, entry: { sources: ['start-node'], targets: ['judge'], kind: 'Control' } },
      ['judge', 'grilly', 'griller'],
    );
    const m = buildHyperGraphModel(def);
    expect(m.ringLanes).toEqual([
      { members: ['judge', 'grilly', 'griller'], backEdgeId: 'l2' },
    ]);
    expect(m.render.edges.find((e) => e.id === 'l2')?.loopArc).toBe(true);
    expect(m.render.edges.find((e) => e.id === 'l0')?.loopArc).toBe(false);
  });

  it('recognises the PascalCase `"Loop"` the wire carries, on a lone back-edge', () => {
    // Plain forward spine with one kind:Loop back-edge E→A. The back-edge's
    // target (A) already reaches its source (E) through the spine → arc.
    const def = chainDef({ ...spine, back: { sources: ['E'], targets: ['A'], kind: 'Loop' } });
    const m = buildHyperGraphModel(def);
    const back = m.render.edges.find((e) => e.id === 'back');
    expect(back?.isLoop).toBe(true);
    expect(back?.loopArc).toBe(true);
    expect(m.layout.edges).toHaveLength(4); // the spine only
  });

  it('always arcs a self-loop and keeps it out of the layout', () => {
    const def = chainDef({ self: { sources: ['A'], targets: ['A'], kind: 'Loop' } }, ['A']);
    const m = buildHyperGraphModel(def);
    const self = m.render.edges.find((e) => e.id === 'self');
    expect(self?.isLoop).toBe(true);
    expect(self?.loopArc).toBe(true);
    expect(m.layout.edges).toHaveLength(0);
  });

  it('a loop edge still carries its gate (the mkLoop loop_control predicate)', () => {
    const def = chainDef(
      {
        l0: { sources: ['A'], targets: ['B'], kind: 'Loop' },
        l1: {
          sources: ['B'],
          targets: ['A'],
          kind: 'Loop',
          gates: [
            {
              PredicateHolds: {
                routing_var: 'loop_control',
                op: 'Eq',
                value: { String: 'continue' },
                state: 'Idle',
                transitions: [],
              },
            },
          ],
        },
      },
      ['A', 'B'],
    );
    const m = buildHyperGraphModel(def);
    const back = m.render.edges.find((e) => e.id === 'l1');
    expect(back?.gate?.kind).toBe('predicate');
  });
});
