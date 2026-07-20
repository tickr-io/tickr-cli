import { describe, it, expect } from 'vitest';
import { buildGateRows, incidentGateRows } from './gates';
import type { InstanceSnapshot, GateView } from '@/api/client';

const T0 = '2026-06-12T10:00:00Z';

function snapshotWith(gates: GateView[], routingVariables: Record<string, { kind: string; value: unknown }> = {}): InstanceSnapshot {
  return {
    id: 'wi1',
    workflow_id: 'wf1',
    name: 'fixture',
    workflow_name: 'fixture',
    workflow_version: 1,
    state: 'InProgress',
    scheduled_at: T0,
    triggered_at: null,
    started_at: null,
    completed_at: null,
    transitions: [],
    triggered_by: null,
    tags: {},
    storage: 'live',
    task_count: 2,
    completed_tasks: 0,
    tasks: [
      {
        id: 't-check',
        name: 'check',
        task_type: 'regular',
        max_attempts: 3,
        timeout_secs: null,
        nix_expression_path: '/nix',
        inputs: [],
        outputs: [],
        secrets: [],
        routing_vars: [{ name: 'coverage', var_type: 'int' }],
        emits: [],
      },
      {
        id: 't-ship',
        name: 'ship',
        task_type: 'regular',
        max_attempts: 3,
        timeout_secs: null,
        nix_expression_path: '/nix',
        inputs: [],
        outputs: [],
        secrets: [],
        routing_vars: [],
        emits: [],
      },
    ],
    task_instances: [],
    graph: {
      start: 'n-start',
      end: 'n-end',
      nodes: [
        { code: 'CHCK', id: 't-check', kind: 'task', ground: 'pending', grounded_at: null },
        { code: 'SHIP', id: 't-ship', kind: 'task', ground: 'pending', grounded_at: null },
      ],
      edges: [{ code: 'E001', id: 'e1', sources: ['t-check'], targets: ['t-ship'], kind: 'data', gates }],
    },
    routing_variables: routingVariables,
  } as InstanceSnapshot;
}

function predicateGate(over: Partial<GateView>): GateView {
  return {
    kind: 'predicate',
    state: 'Dispatched',
    signal_id: null,
    signal_name: null,
    predicate: null,
    captures: [],
    routing_var: 'coverage',
    op: '>=',
    value: { kind: 'int', value: 80 },
    timeout_secs: null,
    duration_secs: null,
    transitions: [{ from: 'Idle', to: 'Dispatched', at: T0 }],
    ...over,
  };
}

describe('buildGateRows', () => {
  it('renders sources → targets as task names with the declared expression', () => {
    const rows = buildGateRows(snapshotWith([predicateGate({})]));
    expect(rows).toHaveLength(1);
    expect(rows[0].sources).toEqual(['check']);
    expect(rows[0].targets).toEqual(['ship']);
    expect(rows[0].expression).toBe('coverage >= 80');
  });

  it('names the producer while the routing variable is absent', () => {
    const rows = buildGateRows(snapshotWith([predicateGate({})]));
    expect(rows[0].currentValue).toBeNull();
    expect(rows[0].awaitingProducer).toBe('check');
    expect(rows[0].annotation).toBeNull();
  });

  it('joins the current typed value once produced', () => {
    const rows = buildGateRows(
      snapshotWith([predicateGate({})], { coverage: { kind: 'int', value: 85 } }),
    );
    expect(rows[0].currentValue).toEqual({ kind: 'int', value: 85 });
    expect(rows[0].awaitingProducer).toBeNull();
  });

  it('annotates a doomed Dispatched gate with its reject time when a timeout is armed', () => {
    const rows = buildGateRows(
      snapshotWith([predicateGate({ timeout_secs: 300 })], {
        coverage: { kind: 'int', value: 42 },
      }),
    );
    const expected = new Date(Date.parse(T0) + 300_000);
    expect(rows[0].annotation).toContain('predicate false against current value');
    expect(rows[0].annotation).toContain(expected.toLocaleString());
  });

  it('annotates a hanging Dispatched gate when no timeout is declared', () => {
    const rows = buildGateRows(
      snapshotWith([predicateGate({})], { coverage: { kind: 'int', value: 42 } }),
    );
    expect(rows[0].annotation).toBe(
      'predicate false against current value — no timeout declared',
    );
  });

  it('does not annotate satisfied gates and distinguishes Rejected from Cancelled', () => {
    const rows = buildGateRows(
      snapshotWith(
        [
          predicateGate({ state: 'Satisfied', signal_id: null }),
          predicateGate({ state: 'Rejected' }),
          predicateGate({ state: 'Cancelled' }),
        ],
        { coverage: { kind: 'int', value: 99 } },
      ),
    );
    expect(rows[0].annotation).toBeNull();
    expect(rows[1].stateCopy).toBe('own deadline elapsed');
    expect(rows[2].stateCopy).toBe('abandoned mid-flight');
  });
});

describe('incidentGateRows', () => {
  /** Three tasks, three gated edges: a→b (gate G1), b→c (gate G2), and a
   * multi-role edge b→b' shape where task b appears in both sources and
   * targets (gate G3) — the both-groups case. */
  function multiEdgeSnapshot(): InstanceSnapshot {
    const base = snapshotWith([]);
    return {
      ...base,
      tasks: [
        ...base.tasks,
        {
          id: 't-notify',
          name: 'notify',
          task_type: 'regular',
          max_attempts: 3,
          timeout_secs: null,
          nix_expression_path: '/nix',
          inputs: [],
          outputs: [],
          secrets: [],
          routing_vars: [],
          emits: [],
        },
      ],
      graph: {
        ...base.graph,
        edges: [
          { code: 'E001', id: 'e1', sources: ['t-check'], targets: ['t-ship'], kind: 'data', gates: [predicateGate({})] },
          {
            code: 'E002',
            id: 'e2',
            sources: ['t-ship'],
            targets: ['t-notify'],
            kind: 'data',
            gates: [predicateGate({ routing_var: 'coverage' })],
          },
          {
            code: 'E003',
            id: 'e3',
            sources: ['t-ship', 't-check'],
            targets: ['t-ship', 't-notify'],
            kind: 'data',
            gates: [predicateGate({})],
          },
        ],
      },
    } as InstanceSnapshot;
  }

  it('splits gates into gated-by and gates-downstream for one task', () => {
    const { gatedBy, gatesDownstream } = incidentGateRows(multiEdgeSnapshot(), 't-ship');
    // e1 targets ship; e3 targets include ship.
    expect(gatedBy.map((r) => r.edgeId)).toEqual(['e1', 'e3']);
    // e2 and e3 are fed by ship.
    expect(gatesDownstream.map((r) => r.edgeId)).toEqual(['e2', 'e3']);
  });

  it('a gate incident in both roles appears in both groups — no dedup', () => {
    const { gatedBy, gatesDownstream } = incidentGateRows(multiEdgeSnapshot(), 't-ship');
    expect(gatedBy.some((r) => r.edgeId === 'e3')).toBe(true);
    expect(gatesDownstream.some((r) => r.edgeId === 'e3')).toBe(true);
  });

  it('excludes gates on edges the task is not incident to', () => {
    const { gatedBy, gatesDownstream } = incidentGateRows(multiEdgeSnapshot(), 't-notify');
    expect(gatedBy.map((r) => r.edgeId)).toEqual(['e2', 'e3']);
    expect(gatesDownstream).toEqual([]);
  });

  it('a gateless task yields two empty groups', () => {
    const snap = snapshotWith([]);
    const { gatedBy, gatesDownstream } = incidentGateRows(snap, 't-check');
    expect(gatedBy).toEqual([]);
    expect(gatesDownstream).toEqual([]);
  });
});
