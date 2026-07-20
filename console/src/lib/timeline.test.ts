import { describe, it, expect } from 'vitest';
import { buildTimelineModel } from './timeline';
import { STATE_TOKEN, normalizeState } from '@/api/normalize';
import type { InstanceSnapshot, SnapshotTaskInstance } from '@/api/client';

const T0 = Date.parse('2026-06-12T10:00:00Z');
const iso = (offsetSec: number) => new Date(T0 + offsetSec * 1000).toISOString();

/** A snapshot fixture with a retried task, a gate wait, and a
 * cascade-cancelled never-minted task. */
function fixture(): InstanceSnapshot {
  return {
    id: 'wi1',
    workflow_id: 'wf1',
    name: 'fixture',
    workflow_name: 'fixture',
    workflow_version: 1,
    state: 'Failed',
    scheduled_at: iso(0),
    triggered_at: iso(0),
    started_at: iso(1),
    completed_at: iso(120),
    transitions: [],
    triggered_by: { kind: 'Cron', signal_id: null, name: null },
    tags: {},
    storage: 'live',
    task_count: 3,
    completed_tasks: 1,
    tasks: [
      mkDef('t-extract', 'extract'),
      mkDef('t-load', 'load'),
      mkDef('t-report', 'report'),
    ],
    task_instances: [
      mkTi('ti1', 't-extract', 'extract', 'Failed', 0, iso(10), iso(30)),
      mkTi('ti2', 't-extract', 'extract', 'Completed', 1, iso(35), iso(60)),
      mkTi('ti3', 't-load', 'load', 'Running', 0, iso(70), null),
    ],
    graph: {
      start: 'n-start',
      end: 'n-end',
      nodes: [
        { code: 'EXTR', id: 't-extract', kind: 'task', ground: 'success', grounded_at: iso(60) },
        { code: 'LOAD', id: 't-load', kind: 'task', ground: 'pending', grounded_at: null },
        { code: 'RPRT', id: 't-report', kind: 'task', ground: 'cancelled', grounded_at: iso(90) },
      ],
      edges: [
        {
          code: 'E001',
          id: 'e1',
          sources: ['t-extract'],
          targets: ['t-load'],
          kind: 'data',
          gates: [
            {
              kind: 'signal',
              state: 'Satisfied',
              signal_id: 'sig1',
              signal_name: 'approval',
              predicate: null,
              captures: [],
              routing_var: null,
              op: null,
              value: null,
              timeout_secs: null,
              duration_secs: null,
              transitions: [
                { from: 'Idle', to: 'Dispatched', at: iso(60) },
                { from: 'Dispatched', to: 'Satisfied', at: iso(68) },
              ],
            },
          ],
        },
      ],
    },
    routing_variables: {},
  } as InstanceSnapshot;
}

function mkDef(id: string, name: string) {
  return {
    id,
    name,
    task_type: 'regular',
    max_attempts: 3,
    timeout_secs: null,
    nix_expression_path: '/nix',
    inputs: [],
    outputs: [],
    secrets: [],
    routing_vars: [],
    emits: [],
  };
}

function mkTi(
  id: string,
  taskId: string,
  name: string,
  state: string,
  attempt: number,
  started: string | null,
  completed: string | null,
) {
  return {
    id,
    task_id: taskId,
    name,
    task_type: 'regular',
    state,
    executor_id: null,
    attempt,
    started_at: started,
    completed_at: completed,
    transitions: started ? [{ from: 'Assigned', to: 'Running', at: started }] : [],
  };
}

describe('buildTimelineModel', () => {
  const now = T0 + 100_000;

  it('renders retries as separate spans with their own timings', () => {
    const model = buildTimelineModel(fixture(), now);
    const extracts = model.rows.filter((r) => r.key.startsWith('task:ti1') || r.key.startsWith('task:ti2'));
    expect(extracts).toHaveLength(2);
    expect(extracts[0].outcome).toBe('Failed');
    expect(extracts[0].end).toBe(T0 + 30_000);
    expect(extracts[1].label).toContain('attempt 2');
    expect(extracts[1].outcome).toBe('Completed');
  });

  it('renders gate waits as their own lanes spanning dispatch to terminal', () => {
    const model = buildTimelineModel(fixture(), now);
    const lane = model.rows.find((r) => r.kind === 'gate');
    expect(lane).toBeDefined();
    expect(lane!.start).toBe(T0 + 60_000);
    expect(lane!.end).toBe(T0 + 68_000);
    expect(lane!.outcome).toBe('Satisfied');
  });

  it('places cascade-cancelled never-minted tasks at their grounding time', () => {
    const model = buildTimelineModel(fixture(), now);
    const dead = model.rows.find((r) => r.kind === 'cancelled');
    expect(dead).toBeDefined();
    expect(dead!.label).toContain('report');
    expect(dead!.start).toBe(T0 + 90_000);
    expect(dead!.end).toBe(T0 + 90_000);
  });

  it('leaves a running task open-ended and bounds the axis at now', () => {
    const model = buildTimelineModel(fixture(), now);
    const running = model.rows.find((r) => r.key === 'task:ti3');
    expect(running!.end).toBeNull();
    expect(model.max).toBe(now);
    expect(model.min).toBe(T0 + 10_000);
  });

  it('orders rows stably by start time', () => {
    const model = buildTimelineModel(fixture(), now);
    const starts = model.rows.map((r) => r.start);
    expect([...starts].sort((a, b) => a - b)).toEqual(starts);
    // Same input twice → identical output.
    expect(buildTimelineModel(fixture(), now)).toEqual(model);
  });

  it('handles an empty snapshot without temporal records', () => {
    const empty = { ...fixture(), task_instances: [], graph: { start: '', end: '', nodes: [], edges: [] } };
    const model = buildTimelineModel(empty as InstanceSnapshot, now);
    expect(model.rows).toEqual([]);
    expect(model.min).toBe(model.max);
  });
});

type Tr = { from: string; to: string; at: string };

/** A task instance with an explicit transition history — the loop-body shape:
 * one instance re-queued across turns, so many Running/Parked windows. */
function mkLoopTi(state: string, trans: Tr[], completed: string | null): SnapshotTaskInstance {
  const firstRun = trans.find((t) => t.to === 'Running');
  return {
    id: 'tiL',
    task_id: 't-loop',
    name: 'body',
    task_type: 'regular',
    state,
    executor_id: null,
    attempt: 0,
    started_at: firstRun?.at ?? null,
    completed_at: completed,
    transitions: trans,
  } as SnapshotTaskInstance;
}

/** A snapshot carrying exactly one loop-body task instance and nothing else, so
 * the only rows are the body's. */
function loopSnap(ti: SnapshotTaskInstance): InstanceSnapshot {
  return {
    ...fixture(),
    state: 'Completed',
    task_instances: [ti],
    graph: { start: '', end: '', nodes: [], edges: [] },
  } as InstanceSnapshot;
}

const T = (s: number) => T0 + s * 1000;

describe('buildTimelineModel — loop-body parked spans', () => {
  const now = T0 + 100_000;

  // A loop turning twice then completing.
  const completedLoop: Tr[] = [
    { from: 'Assigned', to: 'Running', at: iso(10) },
    { from: 'Running', to: 'Parked', at: iso(20) },
    { from: 'Parked', to: 'Queued', at: iso(25) },
    { from: 'Queued', to: 'Delivered', at: iso(26) },
    { from: 'Delivered', to: 'Assigned', at: iso(27) },
    { from: 'Assigned', to: 'Running', at: iso(30) },
    { from: 'Running', to: 'Completed', at: iso(40) },
  ];

  it('renders Parked windows as amber spans between Running windows', () => {
    const model = buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now);
    const segs = model.rows.find((r) => r.kind === 'task')!.segments!;
    const parked = segs.filter((s) => s.state === 'Parked');
    expect(parked).toHaveLength(1);
    expect(parked.every((s) => s.token === 'warning')).toBe(true);
    // amber park sits between two Running windows.
    const running = segs.filter((s) => s.state === 'Running');
    expect(running).toHaveLength(2);
    expect(segs.indexOf(running[0])).toBeLessThan(segs.indexOf(parked[0]));
    expect(segs.indexOf(parked[0])).toBeLessThan(segs.indexOf(running[1]));
  });

  it('single ring → one arc-closing window and N−1 forward windows', () => {
    const segs = buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now)
      .rows.find((r) => r.kind === 'task')!.segments!;
    const running = segs.filter((s) => s.state === 'Running');
    const arcClosing = running.filter((s) => s.outcome === 'Completed');
    const forward = running.filter((s) => s.outcome === 'Running');
    expect(arcClosing).toHaveLength(1); // closes the ring, green
    expect(arcClosing[0].token).toBe('success');
    expect(forward).toHaveLength(1); // N−1 forward hand-offs, blue
    expect(forward[0].token).toBe('info');
    // Deterministic: array order is time order, so repeated builds agree.
    expect(buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now)).toEqual(
      buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now),
    );
  });

  it('a Running window closing Failed renders red', () => {
    const trans: Tr[] = [
      { from: 'Assigned', to: 'Running', at: iso(10) },
      { from: 'Running', to: 'Parked', at: iso(20) },
      { from: 'Parked', to: 'Queued', at: iso(25) },
      { from: 'Queued', to: 'Assigned', at: iso(27) },
      { from: 'Assigned', to: 'Running', at: iso(30) },
      { from: 'Running', to: 'Failed', at: iso(40) },
    ];
    const segs = buildTimelineModel(loopSnap(mkLoopTi('Failed', trans, iso(40))), now)
      .rows.find((r) => r.kind === 'task')!.segments!;
    const closing = segs.find((s) => s.outcome === 'Failed')!;
    expect(closing.state).toBe('Running');
    expect(closing.token).toBe('destructive');
  });

  it('keeps Parked amber live-at-now (intermediate), with no terminal marker', () => {
    const trans: Tr[] = [
      { from: 'Assigned', to: 'Running', at: iso(10) },
      { from: 'Running', to: 'Parked', at: iso(20) },
    ];
    const row = buildTimelineModel(loopSnap(mkLoopTi('Parked', trans, null)), now).rows.find(
      (r) => r.kind === 'task',
    )!;
    const last = row.segments!.at(-1)!;
    expect(last.state).toBe('Parked');
    expect(last.token).toBe('warning');
    expect(last.end).toBeNull(); // open at now
    expect(row.terminalMarker).toBeUndefined(); // not terminal
  });

  it('keeps Parked amber when reaped Completed and fires a terminal end-cap', () => {
    const trans: Tr[] = [
      { from: 'Assigned', to: 'Running', at: iso(10) },
      { from: 'Running', to: 'Parked', at: iso(20) },
      { from: 'Parked', to: 'Completed', at: iso(30) },
    ];
    const row = buildTimelineModel(loopSnap(mkLoopTi('Completed', trans, iso(30))), now).rows.find(
      (r) => r.kind === 'task',
    )!;
    const parked = row.segments!.find((s) => s.state === 'Parked')!;
    expect(parked.token).toBe('warning'); // no amber→green flip on teardown
    expect(row.terminalMarker).toBeDefined();
    expect(row.terminalMarker!.token).toBe('success'); // outcome lands on-axis
    expect(row.terminalMarker!.at).toBe(T(30));
  });

  it('fires a red end-cap for an all-parked timeout reap', () => {
    const trans: Tr[] = [
      { from: 'Assigned', to: 'Running', at: iso(10) },
      { from: 'Running', to: 'Parked', at: iso(20) },
      { from: 'Parked', to: 'Failed', at: iso(30) },
    ];
    const row = buildTimelineModel(loopSnap(mkLoopTi('Failed', trans, iso(30))), now).rows.find(
      (r) => r.kind === 'task',
    )!;
    expect(row.segments!.at(-1)!.token).toBe('warning'); // last drawn span is amber Parked
    expect(row.terminalMarker!.token).toBe('destructive');
  });

  it('omits the end-cap on the plain Running→terminal path (outcome already on a bar)', () => {
    const row = buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now)
      .rows.find((r) => r.kind === 'task')!;
    // The final drawn segment is the green arc-closing Running window, so no
    // redundant marker is added.
    expect(row.segments!.at(-1)!.outcome).toBe('Completed');
    expect(row.terminalMarker).toBeUndefined();
  });

  it('labels the bar by total ring involvement (first window start → last window end)', () => {
    const row = buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now)
      .rows.find((r) => r.kind === 'task')!;
    const segs = row.segments!;
    expect(row.start).toBe(segs[0].start); // first window start = bar start
    expect(row.end).toBe(segs.at(-1)!.end); // last window end = bar end (parked time included)
    expect(row.start).toBe(T(10));
    expect(row.end).toBe(T(40));
  });

  it('degrades historical instances with no recorded transitions to a single span', () => {
    const trans: Tr[] = [{ from: 'Assigned', to: 'Running', at: iso(10) }];
    const row = buildTimelineModel(loopSnap(mkLoopTi('Completed', trans, iso(40))), now).rows.find(
      (r) => r.kind === 'task',
    )!;
    expect(row.segments).toBeUndefined(); // legacy single span
    expect(row.end).toBe(T(40));
    expect(row.outcome).toBe('Completed');
  });

  it('resolves the parked-span colour through the shared state palette', () => {
    const segs = buildTimelineModel(loopSnap(mkLoopTi('Completed', completedLoop, iso(40))), now)
      .rows.find((r) => r.kind === 'task')!.segments!;
    const parked = segs.find((s) => s.state === 'Parked')!;
    expect(parked.token).toBe(STATE_TOKEN[normalizeState('Parked')]);
  });
});
