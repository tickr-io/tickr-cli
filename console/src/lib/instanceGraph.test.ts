import { describe, it, expect } from 'vitest';
import { buildInstanceGraphModel } from './instanceGraph';
import type { InstanceSnapshot } from '@/api/client';

function mkDef(id: string, name: string) {
  return {
    id,
    name,
    task_type: 'regular',
    max_attempts: 1,
    timeout_secs: null,
    nix_expression_path: `/nix#${name}`,
    inputs: [],
    outputs: [],
    secrets: [],
    routing_vars: [],
    emits: [],
  };
}

function mkTi(id: string, taskId: string, state: string) {
  return {
    id,
    task_id: taskId,
    name: taskId,
    task_type: 'regular',
    state,
    executor_id: null,
    attempt: 0,
    started_at: null,
    completed_at: null,
    transitions: [],
  };
}

function mkEdge(id: string, from: string, to: string, kind: string) {
  return { id, sources: [from], targets: [to], kind, gates: [] };
}

/** A snapshot whose graph is a single 3-task ring A→B→C→A (every step a loop
 * edge), with the ring edges supplied in a caller-chosen order. */
function ring(edgeOrder: { id: string; from: string; to: string }[]): InstanceSnapshot {
  const ids = ['A', 'B', 'C'];
  return {
    id: 'wi',
    workflow_id: 'wf',
    name: 'ring',
    graph: {
      start: 'n-start',
      end: 'n-end',
      nodes: ids.map((id) => ({ id, kind: 'task', ground: 'pending', grounded_at: null })),
      edges: edgeOrder.map((e) => mkEdge(e.id, e.from, e.to, 'loop')),
    },
    tasks: ids.map((id) => mkDef(id, id)),
    task_instances: [],
    routing_variables: {},
  } as unknown as InstanceSnapshot;
}

describe('buildInstanceGraphModel — single-ring loop layout', () => {
  const closed = [
    { id: 'ab', from: 'A', to: 'B' },
    { id: 'bc', from: 'B', to: 'C' },
    { id: 'ca', from: 'C', to: 'A' },
  ];

  it('yields THE SAME arced back-edge and N−1 forward steps regardless of edge order', () => {
    for (const order of [closed, [...closed].reverse(), [closed[2], closed[0], closed[1]]]) {
      const model = buildInstanceGraphModel(ring(order));
      const loops = model.edges.filter((e) => e.isLoop);
      const arcs = loops.filter((e) => e.loopArc);
      const forward = loops.filter((e) => !e.loopArc);
      // No external entry in this fixture → the lane starts at the smallest
      // member id (A), so the closing step C→A is ALWAYS the arc.
      expect(arcs.map((a) => a.id)).toEqual(['ca']);
      expect(forward).toHaveLength(2); // N − 1
      expect(model.ringLanes).toEqual([{ members: ['A', 'B', 'C'], backEdgeId: 'ca' }]);
      // the forward steps are fed into the layout; the arc is kept out of it.
      expect(model.layout.edges.filter((le) => arcs.some((a) => a.from === le.from && a.to === le.to))).toHaveLength(0);
    }
  });
});

describe('buildInstanceGraphModel — frontier edges', () => {
  it('marks a grounded-success source → not-started target as frontier', () => {
    const snap = {
      id: 'wi',
      workflow_id: 'wf',
      name: 'frontier',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: [
          { id: 'A', kind: 'task', ground: 'success', grounded_at: null },
          { id: 'B', kind: 'task', ground: 'pending', grounded_at: null },
          { id: 'C', kind: 'task', ground: 'pending', grounded_at: null },
        ],
        edges: [mkEdge('ab', 'A', 'B', 'data'), mkEdge('bc', 'B', 'C', 'data')],
      },
      tasks: [mkDef('A', 'A'), mkDef('B', 'B'), mkDef('C', 'C')],
      // A completed; B & C never minted.
      task_instances: [mkTi('ti-a', 'A', 'Completed')],
      routing_variables: {},
    } as unknown as InstanceSnapshot;

    const model = buildInstanceGraphModel(snap);
    const ab = model.edges.find((e) => e.id === 'ab')!;
    const bc = model.edges.find((e) => e.id === 'bc')!;
    expect(ab.frontier).toBe(true); // completed → never-minted
    expect(bc.frontier).toBeUndefined(); // source never started
  });
});

describe('buildInstanceGraphModel — fork arms + barrier join', () => {
  // A fork after anchor A into two arms (F1, F2) rejoining at B renders as an
  // ungated fan-out (A→F1, A→F2 plain edges) plus a barrier-join HyperEdge
  // ({F1,F2}→B) — a multi-source edge the builder renders as a fan-in junction,
  // which is how the join reads legibly on the page.
  it('renders parallel arms and the barrier join as a fan-in junction', () => {
    const snap = {
      id: 'wi',
      workflow_id: 'wf',
      name: 'fork',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: ['A', 'F1', 'F2', 'B'].map((id) => ({
          id,
          kind: 'task',
          ground: 'pending',
          grounded_at: null,
        })),
        edges: [
          mkEdge('a-f1', 'A', 'F1', 'control'),
          mkEdge('a-f2', 'A', 'F2', 'control'),
          // The barrier: both arm tails feed B in one HyperEdge.
          { id: 'join', sources: ['F1', 'F2'], targets: ['B'], kind: 'control', gates: [] },
        ],
      },
      tasks: ['A', 'F1', 'F2', 'B'].map((id) => mkDef(id, id)),
      task_instances: [],
      routing_variables: {},
    } as unknown as InstanceSnapshot;

    const model = buildInstanceGraphModel(snap);

    // The fan-out arms are plain 1→1 edges from the anchor.
    expect(model.edges.some((e) => e.from === 'A' && e.to === 'F1')).toBe(true);
    expect(model.edges.some((e) => e.from === 'A' && e.to === 'F2')).toBe(true);

    // The barrier join renders as a junction fed by BOTH arm tails, converging
    // to the single successor B.
    const junction = model.junctions.find((j) => j.edgeId === 'join');
    expect(junction).toBeDefined();
    const jid = junction!.id;
    expect(model.edges.some((e) => e.from === 'F1' && e.to === jid)).toBe(true);
    expect(model.edges.some((e) => e.from === 'F2' && e.to === jid)).toBe(true);
    expect(model.edges.some((e) => e.from === jid && e.to === 'B')).toBe(true);
  });
});

describe('buildInstanceGraphModel — branch gated arms + selection join', () => {
  // A branch after anchor A into two GATED arms (F1, F2) rejoining at B renders
  // differently from a fork: the fan-out feeders are gated `Data` edges (each
  // carrying a gate badge), and the join is TWO SEPARATE 1→1 edges (F1→B, F2→B)
  // — the selection join — NOT a single fan-in barrier junction. Whichever arm's
  // gate fires grounds B, so each tail feeds B independently.
  function signalGate(name: string) {
    return { kind: 'signal', state: 'Idle', signal_name: name, transitions: [] };
  }
  it('renders gated arm feeders and the selection join as separate 1→1 edges', () => {
    const snap = {
      id: 'wi',
      workflow_id: 'wf',
      name: 'branch',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: ['A', 'F1', 'F2', 'B'].map((id) => ({
          id,
          kind: 'task',
          ground: 'pending',
          grounded_at: null,
        })),
        edges: [
          // Gated fan-out: each arm feeder is a Data edge with a selecting gate.
          { id: 'a-f1', sources: ['A'], targets: ['F1'], kind: 'data', gates: [signalGate('go_1')] },
          { id: 'a-f2', sources: ['A'], targets: ['F2'], kind: 'data', gates: [signalGate('go_2')] },
          // The selection join: two SEPARATE 1→1 edges into B, not one barrier.
          mkEdge('f1-b', 'F1', 'B', 'control'),
          mkEdge('f2-b', 'F2', 'B', 'control'),
        ],
      },
      tasks: ['A', 'F1', 'F2', 'B'].map((id) => mkDef(id, id)),
      task_instances: [],
      routing_variables: {},
    } as unknown as InstanceSnapshot;

    const model = buildInstanceGraphModel(snap);

    // Each arm feeder renders from the anchor and carries its selecting gate.
    const f1 = model.edges.find((e) => e.from === 'A' && e.to === 'F1')!;
    const f2 = model.edges.find((e) => e.from === 'A' && e.to === 'F2')!;
    expect(f1.gates.map((g) => g.kind)).toEqual(['signal']);
    expect(f2.gates.map((g) => g.kind)).toEqual(['signal']);

    // The selection join is two separate 1→1 edges into B — NOT a fan-in
    // junction (that is fork's barrier shape).
    expect(model.edges.some((e) => e.from === 'F1' && e.to === 'B')).toBe(true);
    expect(model.edges.some((e) => e.from === 'F2' && e.to === 'B')).toBe(true);
    expect(model.junctions.some((j) => j.edgeId === 'f1-b' || j.edgeId === 'f2-b')).toBe(false);
  });
});

describe('buildInstanceGraphModel — producer↔gate selection projection', () => {
  function mkPredicateEdge(id: string, from: string, to: string, reads: string) {
    return {
      id,
      sources: [from],
      targets: [to],
      kind: 'data',
      gates: [{ kind: 'predicate', state: 'Idle', routing_var: reads, transitions: [] }],
    };
  }
  // A declares routing var `x`; the C→D edge's predicate gate reads it.
  function snap(taskInstances: ReturnType<typeof mkTi>[]): InstanceSnapshot {
    const ids = ['A', 'B', 'C', 'D'];
    return {
      id: 'wi',
      workflow_id: 'wf',
      name: 'producer',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: ids.map((id) => ({ id, kind: 'task', ground: 'pending', grounded_at: null })),
        edges: [
          mkEdge('ab', 'A', 'B', 'data'),
          mkEdge('bc', 'B', 'C', 'data'),
          mkPredicateEdge('cd', 'C', 'D', 'x'),
        ],
      },
      tasks: ids.map((id) =>
        id === 'A'
          ? { ...mkDef('A', 'A'), routing_vars: [{ name: 'x', var_type: 'int' }] }
          : mkDef(id, id),
      ),
      task_instances: taskInstances,
      routing_variables: {},
    } as unknown as InstanceSnapshot;
  }

  it('links the producing task to the predicate gate edge that reads its routing var', () => {
    const model = buildInstanceGraphModel(snap([]));
    expect(model.selection.taskToGates['A']).toEqual(['cd']);
    expect(model.selection.gateToTask['cd']).toBe('A');
  });

  it('is liveness-invariant — the same adjacency regardless of task-instance state', () => {
    const dormant = buildInstanceGraphModel(snap([])).selection;
    const live = buildInstanceGraphModel(
      snap([mkTi('ti-a', 'A', 'Completed'), mkTi('ti-c', 'C', 'Running')]),
    ).selection;
    expect(live).toEqual(dormant);
  });
});

describe('buildInstanceGraphModel — loop turn counter', () => {
  // A loop turn parks and re-queues the SAME instance, so turns are counted
  // from the transitions into Parked on the latest attempt — never from
  // `attempt` (which only increments on retry).
  function ringWithHistory(parkCounts: Record<string, number>): InstanceSnapshot {
    const base = ring([
      { id: 'ab', from: 'A', to: 'B' },
      { id: 'bc', from: 'B', to: 'C' },
      { id: 'ca', from: 'C', to: 'A' },
    ]) as unknown as { task_instances: unknown[] };
    base.task_instances = Object.entries(parkCounts).map(([taskId, parks]) => ({
      ...mkTi(`ti-${taskId}`, taskId, 'Running'),
      transitions: Array.from({ length: parks }, (_, i) => [
        { from: 'Running', to: 'Parked', at: `2026-07-06T10:0${i}:00Z` },
        { from: 'Parked', to: 'Queued', at: `2026-07-06T10:0${i}:30Z` },
      ]).flat(),
    }));
    return base as unknown as InstanceSnapshot;
  }

  it('carries the max Parked-count of the arc endpoints as turns, on the arc only', () => {
    const model = buildInstanceGraphModel(ringWithHistory({ A: 3, B: 3, C: 2 }));
    const arc = model.edges.find((e) => e.loopArc)!;
    // The arc is C→A (edge `ca`): endpoints have parked 2 and 3 times.
    expect(arc.turns).toBe(3);
    for (const e of model.edges.filter((x) => x.isLoop && !x.loopArc)) {
      expect(e.turns).toBeUndefined();
    }
  });

  it('omits turns before the first turn completes', () => {
    const model = buildInstanceGraphModel(ringWithHistory({ A: 0, B: 0, C: 0 }));
    const arc = model.edges.find((e) => e.loopArc)!;
    expect(arc.turns).toBeUndefined();
  });
});
