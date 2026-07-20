import { describe, it, expect } from 'vitest';
import {
  buildVersionHistory,
  currentVersion,
  graphForVersion,
  computeDelta,
} from './patchHistory';
import type { InstanceSnapshot, SnapshotGraph } from '@/api/client';

function node(id: string) {
  return { code: id.slice(0, 4), id, kind: 'task', ground: 'pending', grounded_at: null };
}
function edge(id: string, sources: string[], targets: string[]) {
  return { code: id.slice(0, 4), id, sources, targets, kind: 'control', gates: [] };
}
function def(id: string, name: string) {
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

// A pristine A→B (edge e-ab), a first insert of X between A and B (v1), and a
// second insert of Y between X and B (v2). Each apply records its lowered ops
// and the reshaped snapshot keyed by the version it produced — exactly what the
// substrate persists.
function snap(): InstanceSnapshot {
  const v1Graph: SnapshotGraph = {
    start: 'n-start',
    end: 'n-end',
    nodes: [node('A'), node('B'), node('X')],
    edges: [edge('e-ax', ['A'], ['X']), edge('e-xb', ['X'], ['B'])],
  } as unknown as SnapshotGraph;
  const liveGraph: SnapshotGraph = {
    start: 'n-start',
    end: 'n-end',
    nodes: [node('A'), node('B'), node('X'), node('Y')],
    edges: [edge('e-ax', ['A'], ['X']), edge('e-xy', ['X'], ['Y']), edge('e-yb', ['Y'], ['B'])],
  } as unknown as SnapshotGraph;
  return {
    id: 'wi',
    workflow_id: 'wf',
    name: 'evo',
    version: 2,
    graph: liveGraph,
    tasks: [def('A', 'alpha'), def('B', 'beta'), def('X', 'commit-doc'), def('Y', 'gamma')],
    task_instances: [],
    routing_variables: {},
    applied_patches: [
      {
        patch_key: 'p1',
        prior_version: 0,
        version: 1,
        reason: 'add commit step',
        provenance: 'external',
        applied_at: '2026-07-09T12:04:00Z',
        ops: [
          { op: 'AddNode', node_id: 'X', sources: [], targets: [] },
          { op: 'RemoveEdge', edge_id: 'e-ab', sources: [], targets: [] },
          { op: 'AddEdge', sources: ['A'], targets: ['X'] },
          { op: 'AddEdge', sources: ['X'], targets: ['B'] },
        ],
      },
      {
        patch_key: 'p2',
        prior_version: 1,
        version: 2,
        reason: null,
        provenance: 'self',
        applied_at: '2026-07-09T12:09:00Z',
        ops: [
          { op: 'AddNode', node_id: 'Y', sources: [], targets: [] },
          { op: 'RemoveEdge', edge_id: 'e-xb', sources: [], targets: [] },
          { op: 'AddEdge', sources: ['X'], targets: ['Y'] },
          { op: 'AddEdge', sources: ['Y'], targets: ['B'] },
        ],
      },
    ],
    version_snapshots: { '1': v1Graph },
  } as unknown as InstanceSnapshot;
}

describe('patchHistory — version list', () => {
  it('lists the pristine baseline then one entry per applied patch, with op/provenance/reason/time', () => {
    const h = buildVersionHistory(snap());
    expect(h.map((e) => e.version)).toEqual([0, 1, 2]);
    expect(h[0]).toMatchObject({ pristine: true, operation: 'pristine', priorVersion: null });
    // The operation label reads the added task's name off its definition.
    expect(h[1]).toMatchObject({
      operation: 'insert "commit-doc"',
      provenance: 'external',
      reason: 'add commit step',
      appliedAt: '2026-07-09T12:04:00Z',
      priorVersion: 0,
      patchKey: 'p1',
    });
    expect(h[2].operation).toBe('insert "gamma"');
    expect(h[2].provenance).toBe('self');
  });

  it('summarises a non-insert primitive patch by its op count', () => {
    const s = snap();
    s.applied_patches![0].ops = [
      { op: 'RemoveEdge', edge_id: 'e-ab', sources: [], targets: [] } as never,
      { op: 'AddEdge', sources: ['A'], targets: ['B'] } as never,
    ];
    expect(buildVersionHistory(s)[1].operation).toBe('2 ops');
  });

  it('a never-patched instance yields a single pristine entry', () => {
    const s = snap();
    s.version = 0;
    s.applied_patches = [];
    s.version_snapshots = {};
    const h = buildVersionHistory(s);
    expect(h).toHaveLength(1);
    expect(h[0].pristine).toBe(true);
  });
});

describe('patchHistory — graph selection', () => {
  it('loads the current version from the live graph and a past version from its stored snapshot', () => {
    const s = snap();
    expect(currentVersion(s)).toBe(2);
    expect(graphForVersion(s, 2)).toBe(s.graph);
    expect(graphForVersion(s, 1)).toBe(s.version_snapshots!['1']);
    // v0 of a patched instance was never retained.
    expect(graphForVersion(s, 0)).toBeUndefined();
  });
});

describe('patchHistory — delta', () => {
  it('reads additions from the ops and ghosts a removed edge via the prior snapshot', () => {
    const d = computeDelta(snap(), 2)!;
    expect([...d.addedNodes]).toEqual(['Y']);
    // AddEdge ops are matched to the minted edge ids by endpoints.
    expect(d.addedEdges).toEqual(new Set(['e-xy', 'e-yb']));
    // The removed e-xb is ghosted between endpoints that survive into v2 (X, B).
    expect(d.ghostEdges).toEqual([{ id: 'e-xb', from: 'X', to: 'B' }]);
    expect(d.removedNodeIds.size).toBe(0);
  });

  it('the first patch lights up additions but cannot ghost removals (v0 not retained)', () => {
    const d = computeDelta(snap(), 1)!;
    expect([...d.addedNodes]).toEqual(['X']);
    expect(d.addedEdges).toEqual(new Set(['e-ax', 'e-xb']));
    // Prior is v0 (not retained) → the removed e-ab has no recoverable geometry.
    expect(d.ghostEdges).toEqual([]);
  });

  it('has no delta for the pristine baseline', () => {
    expect(computeDelta(snap(), 0)).toBeNull();
  });
});
