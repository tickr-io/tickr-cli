import { describe, it, expect } from 'vitest';
import {
  describeEvent,
  displayOrdered,
  matchesFilter,
  EVENT_CAT,
  EVENT_STATE_PARITY,
  eventToken,
  relTime,
  shortId,
} from './events';
import { STATE_TOKEN } from '@/api/normalize';

describe('describeEvent', () => {
  it('derives summary and id-chips from the archived payload shape', () => {
    const { summary, chips } = describeEvent({
      event_type: 'TaskCompleted',
      payload: {
        TaskCompleted: {
          task_instance_id: 'aaaaaaaa-1111-2222-3333-444444444444',
          workflow_instance_id: 'bbbbbbbb-1111-2222-3333-444444444444',
          task_id: 'cccccccc-1111-2222-3333-444444444444',
          executor_id: 'dddddddd-1111-2222-3333-444444444444',
          routing_variables: {},
        },
      },
    });
    expect(summary).toBe('task completed');
    const byLabel = Object.fromEntries(chips.map((c) => [c.label, c.value]));
    expect(byLabel.task).toBe('aaaaaaaa');
    expect(byLabel.instance).toBe('bbbbbbbb');
    expect(byLabel.executor).toBe('dddddddd');
    // routing_variables is a nested map, not an id — never a chip.
    expect(chips.find((c) => c.label === 'routing_variables')).toBeUndefined();
  });

  it('surfaces a parked loop turn with the amber waiting category, Task filter, summary, and chips', () => {
    // A parked turn is an unresolved, in-progress loop turn — amber `waiting`
    // styling (the one event to spot), not happy-path neutral and not a
    // terminal success/failure/cancel — and must be reachable under the Task
    // filter rather than rendering as the raw-type fallback.
    expect(EVENT_CAT.TaskParked).toBe('waiting');
    expect(eventToken('TaskParked')).toBe('warning');
    expect(matchesFilter('TaskParked', 'Task')).toBe(true);
    const { summary, chips } = describeEvent({
      event_type: 'TaskParked',
      payload: {
        TaskParked: {
          task_instance_id: 'aaaaaaaa-1111-2222-3333-444444444444',
          workflow_instance_id: 'bbbbbbbb-1111-2222-3333-444444444444',
          task_id: 'cccccccc-1111-2222-3333-444444444444',
          executor_id: 'dddddddd-1111-2222-3333-444444444444',
          routing_variables: {},
        },
      },
    });
    expect(summary).not.toBe('TaskParked');
    expect(summary).toBe('task parked — loop turn');
    const byLabel = Object.fromEntries(chips.map((c) => [c.label, c.value]));
    expect(byLabel.task).toBe('aaaaaaaa');
    expect(byLabel.instance).toBe('bbbbbbbb');
    expect(byLabel.taskdef).toBe('cccccccc');
    expect(byLabel.executor).toBe('dddddddd');
  });

  it('renders enum-with-data fields as their variant name', () => {
    const { chips } = describeEvent({
      event_type: 'CreateWorkflowInstance',
      payload: {
        CreateWorkflowInstance: {
          workflow_id: 'aaaaaaaa-1111-2222-3333-444444444444',
          scheduled_at: null,
          triggered_by: { Manual: { signal_id: 'x' } },
        },
      },
    });
    const byLabel = Object.fromEntries(chips.map((c) => [c.label, c.value]));
    expect(byLabel.triggered_by).toBe('Manual');
  });

  it('falls back to the event_type for unknown types instead of breaking', () => {
    const { summary, chips } = describeEvent({
      event_type: 'SomeFutureEvent',
      payload: { SomeFutureEvent: {} },
    });
    expect(summary).toBe('SomeFutureEvent');
    expect(chips).toEqual([]);
  });
});

describe('filters', () => {
  it('groups the real vocabulary into Workflow / Task / Gate', () => {
    expect(matchesFilter('WorkflowCompleted', 'Workflow')).toBe(true);
    expect(matchesFilter('TaskQueued', 'Task')).toBe(true);
    expect(matchesFilter('GateTimeoutFired', 'Gate')).toBe(true);
    expect(matchesFilter('TaskQueued', 'Workflow')).toBe(false);
    expect(matchesFilter('GateOutcome', 'Task')).toBe(false);
    expect(matchesFilter('anything', 'All')).toBe(true);
  });

  it('covers every categorized event type with some filter beyond All', () => {
    for (const eventType of Object.keys(EVENT_CAT)) {
      const matched = (['Workflow', 'Task', 'Gate'] as const).some((f) =>
        matchesFilter(eventType, f),
      );
      expect(matched, `${eventType} must belong to a filter group`).toBe(true);
    }
  });

  it('has no cluster or timer vocabulary — those never reach the data plane', () => {
    for (const t of ['NodesJoined', 'ArmTimer', 'RemoveTimer', 'OwnershipClaimed']) {
      expect(EVENT_CAT[t]).toBeUndefined();
    }
  });
});

describe('event-type map is pinned to the server tenant-visible set', () => {
  // Twin of the server's `tenant_visible_set_is_pinned` tripwire: the UI map
  // keys must be exactly the server's tenant-visible event types. Reclassifying
  // an event type on the wire (either direction) shows up as a diff here, not
  // as a silently mis-coloured or uncoloured row. `TaskUpdate` is NOT a wire
  // type and must be absent; the three quiet task phases must be present.
  const TENANT_VISIBLE = [
    'CreateWorkflowInstance',
    'CancelWorkflowInstance',
    'TickReceived',
    'WorkflowSubmitted',
    'WorkflowInstanceCreated',
    'WorkflowTriggered',
    'WorkflowCompleted',
    'WorkflowFailed',
    'TaskInstanceCreated',
    'TaskQueued',
    'TaskDelivered',
    'TaskAssigned',
    'TaskStarted',
    'TaskCompleted',
    'TaskFailed',
    'TaskParked',
    'TaskCancelled',
    'GateDispatched',
    'GateOutcome',
    'GateTimeoutFired',
  ];

  it('keys exactly the 20-type tenant-visible set', () => {
    expect(Object.keys(EVENT_CAT).sort()).toEqual([...TENANT_VISIBLE].sort());
  });

  it('drops the invented TaskUpdate key and carries the three real task phases', () => {
    expect(EVENT_CAT.TaskUpdate).toBeUndefined();
    expect(EVENT_CAT.TaskDelivered).toBe('task');
    expect(EVENT_CAT.TaskAssigned).toBe('task');
    expect(EVENT_CAT.TaskStarted).toBe('task');
  });
});

describe('cross-map palette parity', () => {
  // The event-type map and the state→token table key different spaces, so they
  // cannot share a table — but they MUST resolve shared semantics to the same
  // token. This is the guard the issue's deepening turns on: recolouring one
  // map without the other fails CI here.
  it.each(EVENT_STATE_PARITY)('%s agrees with state %s on its token', (eventType, state) => {
    expect(eventToken(eventType)).toBe(STATE_TOKEN[state]);
  });

  it('aligns cancel across both maps (red, not amber)', () => {
    expect(eventToken('TaskCancelled')).toBe('destructive');
    expect(eventToken('TaskCancelled')).toBe(STATE_TOKEN.cancelled);
  });

  it('resolves GateTimeoutFired red under the unified terminal-failure rule', () => {
    expect(eventToken('GateTimeoutFired')).toBe('destructive');
  });

  it('keeps TaskParked off happy-path/neutral colour (amber unresolved)', () => {
    expect(eventToken('TaskParked')).toBe('warning');
    expect(eventToken('TaskParked')).toBe(STATE_TOKEN.parked);
    expect(eventToken('TaskParked')).not.toBe('neutral');
    expect(eventToken('TaskParked')).not.toBe('success');
  });
});

describe('lifecycle colour buckets', () => {
  // The two quiet lifecycle levels read as distinct event-log-local hues, not
  // the shared status tokens: workflow → violet, task → cyan.
  it('resolves workflow-lifecycle events to the violet bucket', () => {
    for (const t of [
      'CreateWorkflowInstance',
      'TickReceived',
      'WorkflowSubmitted',
      'WorkflowInstanceCreated',
      'WorkflowTriggered',
    ]) {
      expect(eventToken(t)).toBe('workflow-lifecycle');
    }
  });

  it('resolves the five quiet task-lifecycle events to the cyan bucket', () => {
    for (const t of [
      'TaskInstanceCreated',
      'TaskQueued',
      'TaskDelivered',
      'TaskAssigned',
      'TaskStarted',
    ]) {
      expect(eventToken(t)).toBe('task-lifecycle');
    }
  });

  it('does NOT reuse in-progress blue (info) for workflow lifecycle', () => {
    expect(eventToken('WorkflowTriggered')).not.toBe('info');
  });

  it('splits the fallback: an unknown/unclassified type is neutral grey, not task', () => {
    // Once task lifecycle is a vivid colour, the old default-to-task fallback
    // would paint every unclassified event as a task — so unknown must be grey.
    expect(eventToken('SomeFutureEvent')).toBe('neutral');
    expect(eventToken('SomeFutureEvent')).not.toBe('task-lifecycle');
    expect(eventToken('')).toBe('neutral');
  });
});

describe('display order (occurrence time, arrival tiebreak)', () => {
  const ev = (seq: number, ts: string) => ({ seq, ts });

  it('sorts newest occurrence time first, tiebreaking on arrival seq descending', () => {
    const out = displayOrdered([
      ev(1, '2026-07-09T10:00:00Z'),
      ev(2, '2026-07-09T10:00:05Z'),
      ev(3, '2026-07-09T10:00:05Z'),
    ]);
    // ts desc: the two 10:00:05 rows lead, tiebroken by seq desc (3 before 2),
    // then the older 10:00:00 row.
    expect(out.map((e) => e.seq)).toEqual([3, 2, 1]);
  });

  it('a late-arriving older event lands in occurrence order within the buffer, not at the top', () => {
    // Buffer as arrival order would hold it (newest arrival first): the late
    // event (seq 4) carries an older ts than the rows already shown.
    const arrivalOrder = [
      ev(4, '2026-07-09T10:00:02Z'), // late arrival, older occurrence
      ev(3, '2026-07-09T10:00:06Z'),
      ev(2, '2026-07-09T10:00:04Z'),
      ev(1, '2026-07-09T10:00:00Z'),
    ];
    const out = displayOrdered(arrivalOrder);
    expect(out.map((e) => e.seq)).toEqual([3, 2, 4, 1]);
    // It is NOT at the top even though it arrived most recently.
    expect(out[0].seq).not.toBe(4);
  });

  it('is a pure derived view — does not mutate the input buffer', () => {
    const buf = [ev(1, '2026-07-09T10:00:00Z'), ev(2, '2026-07-09T10:00:05Z')];
    const snapshot = buf.map((e) => e.seq);
    displayOrdered(buf);
    expect(buf.map((e) => e.seq)).toEqual(snapshot);
  });
});

describe('time rendering', () => {
  it('relTime is coarse and monotonic', () => {
    const now = Date.parse('2026-06-12T12:00:00Z');
    expect(relTime('2026-06-12T11:59:56Z', now)).toBe('4s ago');
    expect(relTime('2026-06-12T11:57:00Z', now)).toBe('3m ago');
    expect(relTime('2026-06-12T09:00:00Z', now)).toBe('3h ago');
    expect(relTime('2026-06-10T09:00:00Z', now)).toBe('2d ago');
  });

  it('shortId truncates uuids and passes other values through', () => {
    expect(shortId('aaaaaaaa-1111-2222-3333-444444444444')).toBe('aaaaaaaa');
    expect(shortId('Cron')).toBe('Cron');
  });
});
