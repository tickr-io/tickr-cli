import { describe, it, expect } from 'vitest';
import {
  findTaskInstance,
  siblingAttempts,
  taskDefFor,
  isTerminalTaskState,
  attemptDurationMs,
  isAbnormalEnd,
  markerLine,
  boundedTail,
  taskCtxSlice,
} from './taskSlice';
import type { CtxEntry, InstanceContext, InstanceSnapshot, SnapshotTaskDef } from '@/api/client';

const T0 = Date.parse('2026-06-12T10:00:00Z');
const iso = (offsetSec: number) => new Date(T0 + offsetSec * 1000).toISOString();

/** A snapshot fixture with a retried task (two Attempts) and a single-Attempt
 * task — the two shapes the attempt-chip derivation must distinguish. */
function fixture(): InstanceSnapshot {
  return {
    id: 'wi1',
    workflow_id: 'wf1',
    workflow_name: 'fixture',
    workflow_version: '1.0.0',
    state: 'InProgress',
    scheduled_at: iso(0),
    triggered_at: iso(0),
    started_at: iso(1),
    completed_at: null,
    transitions: [],
    triggered_by: { kind: 'Cron', signal_id: null, name: null },
    tags: {},
    storage: 'live',
    task_count: 2,
    completed_tasks: 0,
    tasks: [mkDef('t-extract', 'extract'), mkDef('t-load', 'load')],
    task_instances: [
      // Inserted out of attempt order on purpose — the selector sorts.
      mkTi('ti2', 't-extract', 'extract', 'Completed', 1, iso(35), iso(60)),
      mkTi('ti1', 't-extract', 'extract', 'Failed', 0, iso(10), iso(30)),
      mkTi('ti3', 't-load', 'load', 'Running', 0, iso(70), null),
    ],
    graph: { start: 'n-s', end: 'n-e', nodes: [], edges: [] },
    routing_variables: {},
  } as unknown as InstanceSnapshot;
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
    transitions: [],
  };
}

describe('findTaskInstance', () => {
  it('returns the snapshot row for a minted id', () => {
    expect(findTaskInstance(fixture(), 'ti1')?.attempt).toBe(0);
  });

  it('returns null for an unknown id', () => {
    expect(findTaskInstance(fixture(), 'nope')).toBeNull();
  });
});

describe('siblingAttempts', () => {
  it('returns every Attempt of the same task, ordered by attempt', () => {
    const snap = fixture();
    const self = findTaskInstance(snap, 'ti2')!;
    const siblings = siblingAttempts(snap, self);
    expect(siblings.map((s) => s.id)).toEqual(['ti1', 'ti2']);
    expect(siblings.map((s) => s.attempt)).toEqual([0, 1]);
  });

  it('excludes other tasks', () => {
    const snap = fixture();
    const self = findTaskInstance(snap, 'ti1')!;
    expect(siblingAttempts(snap, self).some((s) => s.id === 'ti3')).toBe(false);
  });

  it('a single-Attempt task is its own only sibling', () => {
    const snap = fixture();
    const self = findTaskInstance(snap, 'ti3')!;
    expect(siblingAttempts(snap, self).map((s) => s.id)).toEqual(['ti3']);
  });
});

describe('taskDefFor', () => {
  it('resolves the definition behind the instance', () => {
    const snap = fixture();
    const self = findTaskInstance(snap, 'ti1')!;
    expect(taskDefFor(snap, self)?.max_attempts).toBe(3);
  });
});

describe('isTerminalTaskState', () => {
  it.each(['Completed', 'Failed', 'Cancelled'])('%s is terminal', (s) => {
    expect(isTerminalTaskState(s)).toBe(true);
  });

  it.each(['Pending', 'Queued', 'Delivered', 'Assigned', 'Running'])('%s is not terminal', (s) => {
    expect(isTerminalTaskState(s)).toBe(false);
  });
});

describe('isAbnormalEnd', () => {
  it('terminal + marker absent ⇒ abnormal', () => {
    expect(isAbnormalEnd('Failed', false)).toBe(true);
    expect(isAbnormalEnd('Completed', false)).toBe(true);
  });

  it('a present marker is never abnormal', () => {
    expect(isAbnormalEnd('Failed', true)).toBe(false);
  });

  it('non-terminal states are never abnormal — the stream is still open', () => {
    expect(isAbnormalEnd('Running', false)).toBe(false);
  });

  it('cancelled tasks are excluded — they may never have run', () => {
    expect(isAbnormalEnd('Cancelled', false)).toBe(false);
  });

  it('unknown marker state is never abnormal — no inference from ignorance', () => {
    expect(isAbnormalEnd('Failed', undefined)).toBe(false);
  });
});

describe('markerLine', () => {
  it('renders the exit status', () => {
    expect(markerLine(0, undefined)).toContain('exit 0');
    expect(markerLine(2, undefined)).toContain('exit 2');
  });

  it('appends the reason for non-status exits', () => {
    expect(markerLine(-1, 'terminated without exit status')).toContain(
      'terminated without exit status',
    );
  });
});

describe('boundedTail', () => {
  it('passes small logs through unchanged', () => {
    expect(boundedTail(['a', 'b'], 5)).toEqual({ shown: ['a', 'b'], hidden: 0 });
  });

  it('keeps the last N lines and counts the hidden ones', () => {
    const lines = Array.from({ length: 10 }, (_, i) => `l${i}`);
    const { shown, hidden } = boundedTail(lines, 3);
    expect(shown).toEqual(['l7', 'l8', 'l9']);
    expect(hidden).toBe(7);
  });
});

function ctxEntry(name: string, value: unknown, producer = 'extract'): CtxEntry {
  return {
    name,
    kind: 'json',
    value,
    secret: false,
    present: true,
    producer,
    created_at: iso(5),
  };
}

function ctxFixture(): InstanceContext {
  return {
    storage: 'live',
    run: [ctxEntry('rows', 42), ctxEntry('api_token', null), ctxEntry('region', 'eu-west-1')],
    trigger: [ctxEntry('order_id', 'C-123', 'signal sig-1')],
    gates: [{ signal_id: 'sig-9', entries: [ctxEntry('approver', 'kim', 'wakeup sig-9')] }],
  } as InstanceContext;
}

function defWith(over: Partial<SnapshotTaskDef>): SnapshotTaskDef {
  return {
    id: 't-load',
    name: 'load',
    task_type: 'regular',
    max_attempts: 3,
    timeout_secs: null,
    nix_expression_path: '/nix',
    inputs: [],
    outputs: [],
    secrets: [],
    routing_vars: [],
    emits: [],
    ...over,
  } as SnapshotTaskDef;
}

describe('taskCtxSlice', () => {
  it('resolves each input from the scope its declared source names', () => {
    const def = defWith({
      inputs: [
        { name: 'rows', source: { kind: 'task', task: 'extract' } },
        { name: 'order_id', source: { kind: 'trigger' } },
        { name: 'approver', source: { kind: 'signal', signal_name: 'approval' } },
      ],
    });
    const slice = taskCtxSlice(def, ctxFixture());
    expect(slice.inputs.map((i) => [i.name, i.entry?.value ?? null])).toEqual([
      ['rows', 42],
      ['order_id', 'C-123'],
      ['approver', 'kim'],
    ]);
  });

  it('ambient names walk run, then trigger, then gate scopes', () => {
    const def = defWith({
      inputs: [
        { name: 'region', source: null },
        { name: 'order_id', source: null },
        { name: 'approver', source: null },
      ],
    });
    const slice = taskCtxSlice(def, ctxFixture());
    expect(slice.inputs.map((i) => i.entry?.value ?? null)).toEqual([
      'eu-west-1',
      'C-123',
      'kim',
    ]);
  });

  it('a not-yet-produced input is awaiting (null entry), not an error', () => {
    const def = defWith({ inputs: [{ name: 'missing', source: { kind: 'task', task: 'x' } }] });
    const slice = taskCtxSlice(def, ctxFixture());
    expect(slice.inputs[0].entry).toBeNull();
  });

  it('outputs and routing variables resolve from the run scope', () => {
    const def = defWith({
      outputs: ['rows'],
      routing_vars: [{ name: 'region', var_type: 'string' }],
    });
    const slice = taskCtxSlice(def, ctxFixture());
    expect(slice.outputs).toEqual([
      { name: 'rows', isRoutingVar: false, entry: expect.objectContaining({ value: 42 }) },
      {
        name: 'region',
        isRoutingVar: true,
        entry: expect.objectContaining({ value: 'eu-west-1' }),
      },
    ]);
  });

  it('secrets carry names only — no entry, no value, even when ctx has one', () => {
    const def = defWith({
      inputs: [{ name: 'api_token', source: null }],
      secrets: ['api_token'],
    });
    const slice = taskCtxSlice(def, ctxFixture());
    expect(slice.secretNames).toEqual(['api_token']);
    expect(slice.inputs).toEqual([]);
    expect(JSON.stringify(slice)).not.toContain('"value"');
  });
});

describe('attemptDurationMs', () => {
  it('is completed−started for a terminal Attempt', () => {
    const self = findTaskInstance(fixture(), 'ti1')!;
    expect(attemptDurationMs(self, T0 + 999_000)).toBe(20_000);
  });

  it('ticks against now for a running Attempt', () => {
    const self = findTaskInstance(fixture(), 'ti3')!;
    expect(attemptDurationMs(self, T0 + 100_000)).toBe(30_000);
  });

  it('is null before the first Running transition', () => {
    expect(attemptDurationMs({ started_at: null, completed_at: null }, T0)).toBeNull();
  });
});
