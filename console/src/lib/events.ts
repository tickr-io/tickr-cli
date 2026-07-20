// Event log domain mapping: category per substrate event type, filter
// groups, and the row projection (summary + typed id-chips) derived from the
// event's archived payload. The vocabulary is the real `EventType` set —
// never invent event types (DC-0007).

import type { Event } from '@/api/client';
import { STATE_TOKEN, type SemanticToken } from '@/api/normalize';

/**
 * Display category — a key-space the event log owns (event-type, not state),
 * but every category resolves into the *same* `SemanticToken` vocabulary the
 * state palette uses (see `EVENT_TOKEN`), so the two surfaces speak one colour
 * language. `waiting` is the unresolved-amber bucket (a parked turn, a gate
 * dispatched and awaiting its signal); `cancel` is terminal-red, aligned with
 * the state map's `cancelled`.
 */
export type EventCat =
  | 'workflow'
  | 'task'
  | 'gate'
  | 'success'
  | 'failure'
  | 'cancel'
  | 'waiting';

/**
 * Category per tenant-visible event type. Pinned to the server's exact
 * tenant-visible set (20 types) — a parity test fails if this drifts from the
 * wire allowlist. Cluster and timer internals never reach the data plane, so
 * no cluster/timer categories exist here; an unknown type (a future addition
 * outpacing the UI) falls back to `task` neutral styling rather than breaking
 * the row.
 */
export const EVENT_CAT: Record<string, EventCat> = {
  CreateWorkflowInstance: 'workflow',
  CancelWorkflowInstance: 'cancel',
  TickReceived: 'workflow',
  WorkflowSubmitted: 'workflow',
  WorkflowInstanceCreated: 'workflow',
  WorkflowTriggered: 'workflow',
  WorkflowCompleted: 'success',
  WorkflowFailed: 'failure',
  TaskInstanceCreated: 'task',
  TaskQueued: 'task',
  // The quiet phase transitions of a task's delivery — neutral so they don't
  // drown out the events that carry an outcome.
  TaskDelivered: 'task',
  TaskAssigned: 'task',
  TaskStarted: 'task',
  TaskCompleted: 'success',
  TaskFailed: 'failure',
  // A parked turn is an in-progress, unresolved loop turn — amber `waiting`,
  // the one event you need to spot, not happy-path neutral.
  TaskParked: 'waiting',
  TaskCancelled: 'cancel',
  // A dispatched gate is waiting on its signal — the same unresolved amber.
  GateDispatched: 'waiting',
  GateOutcome: 'gate',
  // A gate expiry kills the edge — terminal-bad, so red under the unified rule.
  GateTimeoutFired: 'cancel',
};

/**
 * The event-log-local colour vocabulary: the five shared `SemanticToken`s PLUS
 * two event-log-only lifecycle hues the shared status palette does not hold —
 * `workflow-lifecycle` (violet/indigo) and `task-lifecycle` (cyan). The event
 * log owns its category key-space, so it widens the token vocabulary *here*
 * without touching the closed shared status union (`SemanticToken`) that the
 * graph node, badge, and timeline all resolve through — those hues would
 * dead-end at the badge variant and pollute the fixed status palette. The
 * outcome and gate buckets still resolve through the shared status tokens, so
 * cross-map parity with the state palette is untouched.
 */
export type EventLogToken = SemanticToken | 'workflow-lifecycle' | 'task-lifecycle';

/**
 * Category → event-log token. The outcome buckets speak the one shared colour
 * language (`success`/`failure`/`cancel` resolve to the same tokens as the
 * `completed`/`failed`/`cancelled` states, `waiting` to `parked`'s amber, gate
 * to `info`) — guarded by the cross-map parity test. The two quiet lifecycle
 * levels get their own event-log-local hues instead: `workflow` → violet,
 * `task` → cyan, so the two levels read as distinct rather than both neutral.
 */
export const EVENT_TOKEN: Record<EventCat, EventLogToken> = {
  workflow: 'workflow-lifecycle',
  task: 'task-lifecycle',
  gate: 'info',
  success: 'success',
  failure: 'destructive',
  cancel: 'destructive',
  waiting: 'warning',
};

/** The event-log token a row resolves into — the single colour source for both
 * the row renderer and the cross-map parity test. Fallback split: an
 * unclassified/unknown type resolves to neutral grey, NOT the task lifecycle
 * colour, so a future unclassified event stays grey rather than being silently
 * painted as a task. */
export function eventToken(eventType: string): EventLogToken {
  const cat = EVENT_CAT[eventType];
  return cat === undefined ? 'neutral' : EVENT_TOKEN[cat];
}

/**
 * Event types whose colour MUST track a canonical state's colour — the shared
 * semantics across the two key-spaces. The cross-map parity test asserts
 * `eventToken(type)` equals `STATE_TOKEN[state]` for each pair (reading the
 * real state table, not a copy), so recolouring one map without the other
 * fails CI.
 */
export const EVENT_STATE_PARITY: ReadonlyArray<[string, keyof typeof STATE_TOKEN]> = [
  ['WorkflowCompleted', 'completed'],
  ['TaskCompleted', 'completed'],
  ['WorkflowFailed', 'failed'],
  ['TaskFailed', 'failed'],
  ['TaskCancelled', 'cancelled'],
  ['TaskParked', 'parked'],
];

/**
 * Filter groups. The kit's Cluster filter is deliberately absent: cluster
 * events never reach the tenant page, by design.
 */
export const FILTERS = ['All', 'Workflow', 'Task', 'Gate'] as const;
export type EventFilter = (typeof FILTERS)[number];

const WORKFLOW_TYPES = new Set([
  'CreateWorkflowInstance',
  'CancelWorkflowInstance',
  'TickReceived',
  'WorkflowSubmitted',
  'WorkflowInstanceCreated',
  'WorkflowTriggered',
  'WorkflowCompleted',
  'WorkflowFailed',
]);
const TASK_TYPES = new Set([
  'TaskInstanceCreated',
  'TaskQueued',
  'TaskDelivered',
  'TaskAssigned',
  'TaskStarted',
  'TaskCompleted',
  'TaskFailed',
  'TaskCancelled',
  'TaskParked',
]);
const GATE_TYPES = new Set(['GateDispatched', 'GateOutcome', 'GateTimeoutFired']);

export function matchesFilter(eventType: string, filter: EventFilter): boolean {
  switch (filter) {
    case 'All':
      return true;
    case 'Workflow':
      return WORKFLOW_TYPES.has(eventType);
    case 'Task':
      return TASK_TYPES.has(eventType);
    case 'Gate':
      return GATE_TYPES.has(eventType);
  }
}

/** Human phrasing per event type (the payload supplies the ids). */
const SUMMARY: Record<string, string> = {
  CreateWorkflowInstance: 'workflow instance requested',
  CancelWorkflowInstance: 'cancel requested',
  TickReceived: 'cron tick received',
  WorkflowSubmitted: 'workflow definition submitted',
  WorkflowInstanceCreated: 'workflow instance created',
  WorkflowTriggered: 'workflow triggered',
  WorkflowCompleted: 'workflow completed',
  WorkflowFailed: 'workflow failed',
  TaskInstanceCreated: 'task instance minted',
  TaskQueued: 'task queued for execution',
  TaskDelivered: 'task delivered to executor',
  TaskAssigned: 'task assigned to executor',
  TaskStarted: 'task execution started',
  TaskCompleted: 'task completed',
  TaskFailed: 'task failed',
  TaskCancelled: 'task cancelled',
  TaskParked: 'task parked — loop turn',
  GateDispatched: 'edge gate dispatched',
  GateOutcome: 'gate outcome received',
  GateTimeoutFired: 'gate expired — no wakeup in time',
};

export interface EventChip {
  /** Short chip label, e.g. `instance`, `task`, `edge`, `signal`. */
  label: string;
  /** Short rendering of the value (uuid head or enum name). */
  value: string;
}

/** Chip label per payload key — anything else uuid-ish falls back to the key
 *  with `_id` stripped. */
const CHIP_LABEL: Record<string, string> = {
  workflow_id: 'workflow',
  workflow_instance_id: 'instance',
  task_instance_id: 'task',
  task_id: 'taskdef',
  executor_id: 'executor',
  edge_id: 'edge',
  signal_id: 'signal',
  originating_signal_id: 'signal',
};

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function shortId(v: string): string {
  return UUID_RE.test(v) ? v.slice(0, 8) : v;
}

/**
 * Project one event row for display: the human summary plus typed id-chips
 * pulled from the payload. The archived payload is the serde shape
 * `{ "<EventType>": { ...fields } }`; fields that are uuid-ish or known
 * enums (trigger provenance, cancel reason) become chips, nested structures
 * are skipped.
 */
export function describeEvent(ev: Pick<Event, 'event_type' | 'payload'>): {
  summary: string;
  chips: EventChip[];
} {
  const summary = SUMMARY[ev.event_type] ?? ev.event_type;
  const inner = (ev.payload as Record<string, unknown>)?.[ev.event_type];
  const chips: EventChip[] = [];
  if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
    for (const [key, raw] of Object.entries(inner as Record<string, unknown>)) {
      if (raw === null || raw === undefined) continue;
      if (typeof raw === 'string') {
        if (key.endsWith('_id') || UUID_RE.test(raw)) {
          chips.push({ label: CHIP_LABEL[key] ?? key.replace(/_id$/, ''), value: shortId(raw) });
        } else if (key === 'reason' || key === 'triggered_by') {
          chips.push({ label: key, value: raw });
        }
      } else if ((key === 'reason' || key === 'triggered_by') && typeof raw === 'object') {
        // Enum-with-data serde shape, e.g. { "Manual": { signal_id: … } } —
        // the variant name is the readable part.
        const variant = Object.keys(raw as Record<string, unknown>)[0];
        if (variant) chips.push({ label: key, value: variant });
      }
    }
  }
  return { summary, chips };
}

/**
 * Display order over the arrival-ordered buffer: newest occurrence time first
 * (`ts` descending), tiebroken by arrival (`seq` descending). A *derived view*
 * only — the poll cursor and buffer retention stay on arrival (`seq`) order, so
 * delivery is unchanged (gap-free, duplicate-free); this client-side sort is
 * what makes the visible rows read in occurrence order while the cursor keeps
 * paging by arrival. Out of scope: an event older than the oldest buffered row
 * sorts to the bottom of the visible window rather than its true global
 * position — this is a live tail, and a reload re-fetches in occurrence order.
 */
export function displayOrdered<T extends Pick<Event, 'ts' | 'seq'>>(events: readonly T[]): T[] {
  return [...events].sort((a, b) => {
    const at = new Date(a.ts).getTime();
    const bt = new Date(b.ts).getTime();
    return bt - at || b.seq - a.seq;
  });
}

/** `14:03:27` wall-clock rendering of an RFC3339 timestamp. */
export function clockTime(ts: string): string {
  const d = new Date(ts);
  const p = (n: number) => String(n).padStart(2, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
}

/** Relative age, coarse on purpose: `4s ago`, `3m ago`, `2h ago`. */
export function relTime(ts: string, nowMs: number): string {
  const s = Math.max(0, Math.round((nowMs - new Date(ts).getTime()) / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}
