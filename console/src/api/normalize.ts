/**
 * The Tickr backend emits workflow-instance / task state in two different casings depending on
 * the endpoint:
 *   - /api/workflows, /api/workflows/instances/{id}, /api/workflows/instances/{id}/tasks
 *     -> Rust `format!("{:?}", state)` → PascalCase: "InProgress", "Scheduled", ...
 *   - /api/dashboard/stats — workflows[].status
 *     -> string literals: "in-progress", "scheduled", "completed", "failed",
 *        "pending", "queued", "skipped", "killed", "timed-out", "unknown"
 *
 * This normalizer collapses both into a small canonical set the UI can switch on.
 */
export type CanonicalState =
  | 'scheduled'
  | 'pending'
  | 'queued'
  | 'in_progress'
  | 'parked'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'killed'
  | 'timed_out'
  | 'skipped'
  | 'unknown';

const MAP: Record<string, CanonicalState> = {
  // PascalCase (WorkflowState / TaskState debug)
  Scheduled: 'scheduled',
  PendingSchedule: 'pending',
  Triggered: 'scheduled',
  Pending: 'pending',
  Queued: 'queued',
  InProgress: 'in_progress',
  Running: 'in_progress',
  Parked: 'parked',
  Completed: 'completed',
  Failed: 'failed',
  Cancelled: 'cancelled',
  Killed: 'killed',
  TimedOut: 'timed_out',
  Skipped: 'skipped',
  // kebab-case (legacy dashboard status strings)
  scheduled: 'scheduled',
  pending: 'pending',
  queued: 'queued',
  'in-progress': 'in_progress',
  parked: 'parked',
  completed: 'completed',
  failed: 'failed',
  cancelled: 'cancelled',
  killed: 'killed',
  'timed-out': 'timed_out',
  skipped: 'skipped',
  unknown: 'unknown',
};

export function normalizeState(raw: string | null | undefined): CanonicalState {
  if (!raw) return 'unknown';
  return MAP[raw] ?? 'unknown';
}

/**
 * The day-clock's four display buckets. Distinct from `normalizeState`, which
 * stays canonical so the workflow detail page can show `Cancelled` verbatim
 * (DC: folding is a presentation decision, not a loss of information). Here the
 * substrate's transient-by-construction states (`PendingSchedule`,
 * `Triggered`) fold into `scheduled`, and the terminal-not-success states
 * (`Cancelled`, `Killed`, `TimedOut`) fold into `failed`, so the four-bucket
 * dial reads honestly without exposing internal-only transitions. States
 * outside the four families (`Pending`, `Queued`, `Skipped`, `Unknown`) are
 * not bucketed — they return `null` and the dial omits them.
 */
export type ClockBucket = 'scheduled' | 'in_progress' | 'completed' | 'failed';

const CLOCK_FOLD: Partial<Record<CanonicalState, ClockBucket>> = {
  scheduled: 'scheduled',
  pending: 'scheduled', // PendingSchedule
  in_progress: 'in_progress',
  completed: 'completed',
  failed: 'failed',
  cancelled: 'failed',
  killed: 'failed',
  timed_out: 'failed',
};

export function clockBucketForState(raw: string | null | undefined): ClockBucket | null {
  return CLOCK_FOLD[normalizeState(raw)] ?? null;
}

export const STATE_LABEL: Record<CanonicalState, string> = {
  scheduled: 'Scheduled',
  pending: 'Pending',
  queued: 'Queued',
  in_progress: 'In progress',
  parked: 'Parked',
  completed: 'Completed',
  failed: 'Failed',
  cancelled: 'Cancelled',
  killed: 'Killed',
  timed_out: 'Timed out',
  skipped: 'Skipped',
  unknown: 'Unknown',
};

/**
 * Reason-driven label for a cancelled task. A cancelled task always wears the
 * terminal-failure token (red) — the colour is the outcome channel — but its
 * LABEL varies with the `CancelReason` so an operator reads the exact cause: a
 * user cancel is "Cancelled", a dependency-cascade cancel reads "Skipped", an
 * external-signal cancel "Cancelled (signal)", and a timeout keeps its
 * established "Timed out" badge even though it is terminally cancelled
 * underneath. An absent / unrecognised reason falls back to the plain
 * "Cancelled" label.
 */
export const CANCEL_REASON_LABEL: Record<string, string> = {
  User: 'Cancelled',
  Dependency: 'Skipped',
  External: 'Cancelled (signal)',
  Executor: 'Cancelled',
  Timeout: 'Timed out',
};

export function cancelledLabel(reason: string | null | undefined): string {
  return (reason ? CANCEL_REASON_LABEL[reason] : undefined) ?? STATE_LABEL.cancelled;
}

/**
 * Human suffix for a cancelled task's kill-confirmation sub-status (distinct
 * from its terminal state). `Confirmed` → "kill confirmed" (the executor acked
 * the process group is gone); `Unconfirmed` → "kill unconfirmed" (the kill was
 * requested but no ack landed — a zombie process may still be alive). `null` /
 * absent → no suffix (no kill was requested for this task). The operator reads
 * this alongside the "Cancelled" label to know whether the work truly stopped.
 */
export function killConfirmationLabel(kc: string | null | undefined): string | null {
  if (kc === 'Confirmed') return 'kill confirmed';
  if (kc === 'Unconfirmed') return 'kill unconfirmed';
  return null;
}

export type BadgeVariant =
  | 'default'
  | 'secondary'
  | 'destructive'
  | 'outline'
  | 'success'
  | 'warning'
  | 'info';

/**
 * The shared semantic-token vocabulary every status surface resolves into.
 * `neutral` is slate (an absence-of-status hue), the others map 1:1 to the
 * `--success` / `--info` / `--warning` / `--destructive` CSS tokens.
 */
export type SemanticToken = 'success' | 'info' | 'warning' | 'destructive' | 'neutral';

/**
 * THE single state→colour source. Every state-coloured surface — the graph
 * node hue (here), the legend, and (per its own slice) the state badge variant
 * — derives from this one table rather than carrying an independent state→colour
 * map, so cross-surface state colour cannot drift. Giving a state a hue means
 * editing exactly this row; a never-minted task is *not* a key here — it renders
 * an explicit neutral absence-of-state in the consumer, so it can never be
 * silently recoloured by a future row.
 */
export const STATE_TOKEN: Record<CanonicalState, SemanticToken> = {
  // amber = unresolved (the run is still in motion / waiting).
  scheduled: 'warning',
  pending: 'neutral',
  queued: 'neutral',
  in_progress: 'info',
  parked: 'warning',
  completed: 'success',
  // red = terminal-failure. `timed_out` is a terminal failure (the day-clock
  // already folds it to `failed`), so it wears red here too — colour is the
  // outcome channel, the badge/label carries the exact `Timed out` type.
  failed: 'destructive',
  cancelled: 'destructive',
  killed: 'destructive',
  timed_out: 'destructive',
  skipped: 'neutral',
  unknown: 'neutral',
};

/** Semantic token → the `hsl(var(--…))` colour it renders as. `neutral` is the
 * muted-foreground slate shared by absence-of-status everywhere. */
export const TOKEN_VAR: Record<SemanticToken, string> = {
  success: 'var(--success)',
  info: 'var(--info)',
  warning: 'var(--warning)',
  destructive: 'var(--destructive)',
  neutral: 'var(--muted-foreground)',
};

/** The four resolved gate states the live graph colours (a gate that is still
 * `Idle` wears its bare kind hue, with no state swatch). A second axis from the
 * task-state palette but resolving into the same token vocabulary. */
export type ResolvedGateState = 'dispatched' | 'satisfied' | 'rejected' | 'cancelled';

export const GATE_STATE_TOKEN: Record<ResolvedGateState, SemanticToken> = {
  // Dispatched = waiting on a signal — amber, the same unresolved hue as a
  // parked task; rendered with a loud pulse by the consumer so a run parked on
  // a gate reads as stuck even when nothing is running.
  dispatched: 'warning',
  satisfied: 'success',
  rejected: 'destructive',
  cancelled: 'destructive',
};

const RESOLVED_GATE_STATES = new Set<string>(['dispatched', 'satisfied', 'rejected', 'cancelled']);

/** Narrow a raw gate state to the resolved set the palette colours, or `null`
 * for `Idle` (kind hue, no state swatch). */
export function resolvedGateState(raw: string): ResolvedGateState | null {
  const st = raw.toLowerCase();
  return RESOLVED_GATE_STATES.has(st) ? (st as ResolvedGateState) : null;
}

/**
 * Semantic token → badge variant. The badge palette resolves through the same
 * token vocabulary as every other status surface; `neutral` (any
 * absence-of-status state) renders as the slate `secondary` fill, matching the
 * muted-foreground neutral used by the graph and timeline.
 */
const TOKEN_BADGE: Record<SemanticToken, BadgeVariant> = {
  success: 'success',
  info: 'info',
  warning: 'warning',
  destructive: 'destructive',
  neutral: 'secondary',
};

/**
 * DC-0001 status colour semantics. NOT an independent state→colour map — it is
 * `STATE_TOKEN` resolved through `TOKEN_BADGE`, so the badge variant and the
 * graph/timeline hue are derived from the one canonical state→token table and
 * cannot drift apart (the cross-axis state-colour agreement is structural, not
 * a test). Giving a state a colour means editing exactly one row of
 * `STATE_TOKEN`; this table follows for free.
 */
export const STATE_BADGE: Record<CanonicalState, BadgeVariant> = Object.fromEntries(
  (Object.keys(STATE_TOKEN) as CanonicalState[]).map((s) => [s, TOKEN_BADGE[STATE_TOKEN[s]]]),
) as Record<CanonicalState, BadgeVariant>;
