/**
 * Timeline layout model — a pure function from the instance snapshot's
 * transition histories to waterfall geometry (one row per task attempt,
 * gate-wait lanes per gated HyperEdge, cascade-cancelled never-minted tasks
 * at their grounding time). Follows the UI-side translator precedent
 * (ADR-0035): rendering components consume this output and hold no timing
 * logic of their own. `nowMs` is injected so the function stays pure.
 */

import type { InstanceSnapshot, GateView, SnapshotTaskInstance } from '@/api/client';
import { normalizeState, STATE_TOKEN, type CanonicalState, type SemanticToken } from '@/api/normalize';
import { gateLabel } from './instanceGraph';

export type TimelineRowKind = 'task' | 'gate' | 'cancelled';

/** One inter-transition window of a task row: a coloured slice of the bar. A
 * loop body re-queues the *same* instance, so its transition history yields
 * many windows — alternating Running execution and amber Parked hand-offs. */
export interface TimelineSegment {
  /** The window's own state word (Running / Parked / Queued / …). */
  state: string;
  /** Window start, epoch ms. */
  start: number;
  /** Window end, epoch ms; null = open (live, no closing transition yet). */
  end: number | null;
  /** Resolved fill, via the one shared state→token palette. */
  token: SemanticToken;
  /** State driving the end-cap predicate and tooltip: the closing terminal for
   * a Running window that reaps terminal, else the window's own state. */
  outcome: string;
}

export interface TimelineRow {
  key: string;
  kind: TimelineRowKind;
  label: string;
  /** Bar start, epoch ms. */
  start: number;
  /** Bar end, epoch ms; null = still open (running / still waiting). */
  end: number | null;
  /** Substrate outcome driving the bar color via the shared state mapping:
   * a TaskState/WorkflowState word for tasks, a GateState word for gates,
   * `Cancelled` for never-minted cascade casualties. */
  outcome: string;
  /** 0-indexed attempt for task rows with retries. */
  attempt?: number;
  /** Per-window slices for a loop body (one instance, many Running/Parked
   * windows). Absent for non-loop tasks and historical instances that lack a
   * recorded terminal transition — they render the single scalar span. */
  segments?: TimelineSegment[];
  /** A terminal point-marker putting loop-body fate on-axis when no segment bar
   * already carries the instance's outcome (all-parked timeout, fast-fail
   * absorb). */
  terminalMarker?: { at: number; token: SemanticToken };
}

export interface TimelineModel {
  rows: TimelineRow[];
  /** Axis bounds, epoch ms. Equal when there is nothing to draw. */
  min: number;
  max: number;
}

function ms(iso: string | null | undefined): number | null {
  if (!iso) return null;
  const t = new Date(iso).getTime();
  return Number.isNaN(t) ? null : t;
}

// Re-dispatch states between a park and the next run. They only draw once a
// park has latched: the very first dispatch (before the body ever ran) is plain
// startup, not loop hand-off, so it draws nothing.
const REDISPATCH = new Set(['Queued', 'Delivered', 'Assigned']);

const TERMINAL_CANON = new Set<CanonicalState>([
  'completed',
  'failed',
  'cancelled',
  'killed',
  'timed_out',
  'skipped',
]);

function isTerminalWord(raw: string): boolean {
  return TERMINAL_CANON.has(normalizeState(raw));
}

/**
 * Walk a task instance's transition history into one coloured segment per
 * inter-transition window. Array order *is* time order by construction (each
 * `at` is stamped from the single clock seam at the state-machine apply point),
 * so no sort. Returns `[]` for the non-loop / historical cases so the caller
 * falls back to the single scalar span — byte-identical to the legacy render.
 */
function deriveSegments(ti: SnapshotTaskInstance): TimelineSegment[] {
  const trans = ti.transitions;
  if (!trans || trans.length === 0) return [];

  // A terminal instance whose closing transition predates transition recording
  // (historical run) degrades to the legacy single span.
  const terminalInstance = isTerminalWord(ti.state) || ti.completed_at != null;
  const recordedTerminal = trans.some((t) => isTerminalWord(t.to));
  if (terminalInstance && !recordedTerminal) return [];

  const segs: TimelineSegment[] = [];
  let parkedSeen = false;
  for (let i = 0; i < trans.length; i++) {
    const state = trans[i].to;
    const start = ms(trans[i].at);
    if (start == null) continue;
    // A terminal transition closes the window before it; it never opens one.
    if (isTerminalWord(state)) continue;
    const close = trans[i + 1];

    let token: SemanticToken;
    let outcome = state;
    if (state === 'Parked') {
      // Parked is amber in every lifecycle position — the Running override
      // below never fires here, so there is no amber→green flip on teardown.
      parkedSeen = true;
      token = STATE_TOKEN[normalizeState(state)];
    } else if (normalizeState(state) === 'in_progress') {
      // A Running window adopts its closing transition's terminal colour
      // (Completed → green / Failed → red); otherwise it stays blue (closes
      // into a park, or is live at now).
      if (close && isTerminalWord(close.to)) {
        token = STATE_TOKEN[normalizeState(close.to)];
        outcome = close.to;
      } else {
        token = STATE_TOKEN[normalizeState(state)];
      }
    } else if (REDISPATCH.has(state)) {
      if (!parkedSeen) continue; // pre-first-run startup draws nothing
      token = 'warning'; // loop hand-off time, amber like the park it latched behind
    } else {
      token = STATE_TOKEN[normalizeState(state)];
    }
    segs.push({ state, start, end: close ? ms(close.at) : null, token, outcome });
  }

  // A single drawn window is just a plain task — emit no segments so it renders
  // identically to the legacy scalar span. Only true loop bodies (many windows)
  // take the segment path.
  return segs.length > 1 ? segs : [];
}

/** The terminal end-cap: fired iff the instance is terminal and the last drawn
 * segment's outcome differs from the instance state — i.e. no segment bar
 * already carries the outcome (all-parked timeout, fast-fail absorb). */
function terminalMarker(
  ti: SnapshotTaskInstance,
  segs: TimelineSegment[],
): { at: number; token: SemanticToken } | undefined {
  if (segs.length === 0 || !isTerminalWord(ti.state)) return undefined;
  const last = segs[segs.length - 1];
  if (normalizeState(last.outcome) === normalizeState(ti.state)) return undefined;
  const at = last.end ?? ms(ti.completed_at);
  if (at == null) return undefined;
  return { at, token: STATE_TOKEN[normalizeState(ti.state)] };
}

function gateLane(gate: GateView, edgeId: string, index: number): TimelineRow | null {
  // A gate lane spans Dispatched → terminal; gates that never dispatched
  // have no temporal extent and draw nothing.
  const dispatched = gate.transitions.find((t) => t.to === 'Dispatched');
  if (!dispatched) return null;
  const start = ms(dispatched.at);
  if (start == null) return null;
  const terminal = gate.transitions.find(
    (t) => t.to === 'Satisfied' || t.to === 'Rejected' || t.to === 'Cancelled',
  );
  const end = terminal ? ms(terminal.at) : null;
  return {
    key: `gate:${edgeId}:${index}`,
    kind: 'gate',
    label: `${gate.kind} · ${gateLabel(gate)}`,
    start,
    end: end ?? (gate.state === 'Dispatched' ? null : start),
    outcome: gate.state,
  };
}

export function buildTimelineModel(snapshot: InstanceSnapshot, nowMs: number): TimelineModel {
  const rows: TimelineRow[] = [];

  // One row per task attempt — retries are separate task instances, so they
  // render as separate spans naturally.
  const mintedTaskIds = new Set<string>();
  for (const ti of snapshot.task_instances) {
    mintedTaskIds.add(ti.task_id);
    const start = ms(ti.started_at) ?? ms(ti.transitions[0]?.at);
    if (start == null) continue; // no temporal record yet (e.g. still queued)
    const end = ms(ti.completed_at);
    // Loop bodies re-queue the same instance, so one instance carries many
    // Running/Parked windows; the scalar span stays the legacy outer extent.
    const segments = deriveSegments(ti);
    const marker = terminalMarker(ti, segments);
    rows.push({
      key: `task:${ti.id}`,
      kind: 'task',
      label: ti.attempt > 0 ? `${ti.name} (attempt ${ti.attempt + 1})` : ti.name,
      start,
      end: end ?? (ti.state === 'Running' ? null : ms(ti.transitions.at(-1)?.at) ?? null),
      outcome: ti.state,
      attempt: ti.attempt,
      ...(segments.length ? { segments } : {}),
      ...(marker ? { terminalMarker: marker } : {}),
    });
  }

  // Gate-wait lanes, per gate on every edge, so "task was slow" and "edge
  // was waiting on a gate" stay distinguishable.
  for (const edge of snapshot.graph.edges) {
    edge.gates.forEach((gate, i) => {
      const lane = gateLane(gate, edge.id, i);
      if (lane) rows.push(lane);
    });
  }

  // Cascade-cancelled tasks that never minted an instance: their grounding
  // timestamp is the only temporal record — a point marker at branch death.
  const defById = new Map(snapshot.tasks.map((t) => [t.id, t]));
  for (const node of snapshot.graph.nodes) {
    if (node.kind !== 'task' || node.ground !== 'cancelled') continue;
    if (mintedTaskIds.has(node.id)) continue;
    const at = ms(node.grounded_at);
    if (at == null) continue;
    rows.push({
      key: `cancelled:${node.id}`,
      kind: 'cancelled',
      label: `${defById.get(node.id)?.name ?? node.id} (never started)`,
      start: at,
      end: at,
      outcome: 'Cancelled',
    });
  }

  // Stable order: by start time, ties by label then attempt — deterministic
  // for identical snapshots regardless of input ordering.
  rows.sort(
    (a, b) =>
      a.start - b.start || a.label.localeCompare(b.label) || (a.attempt ?? 0) - (b.attempt ?? 0),
  );

  if (rows.length === 0) return { rows, min: nowMs, max: nowMs };
  const min = Math.min(...rows.map((r) => r.start));
  const max = Math.max(...rows.map((r) => r.end ?? nowMs), min);
  return { rows, min, max };
}
