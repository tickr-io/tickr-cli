/**
 * Health page logic — the two behaviors the `GET /api/health` endpoint
 * deliberately does NOT own, plus the pure status→hue mapping.
 *
 * The endpoint is **raw-instantaneous**: it reports exactly what each component
 * looked like this request and holds no history. Hysteresis needs history, so
 * the two smoothing behaviors live here in the UI:
 *
 *   1. **API-gate cascade** — the UI reaches the whole system only through the
 *      API, so if the API is unhealthy (in practice: the endpoint did not
 *      answer at all) every row below is reported unhealthy and its card dims.
 *   2. **2-consecutive-read debounce** — the band design guarantees a normal
 *      NATS reconnect blip jitters green↔amber (never green↔red), so requiring
 *      two consecutive reads of a new status before flipping a row keeps a
 *      transient blip from flickering the page. Bands/thresholds stay in the
 *      endpoint; the UI owns only the cascade, the debounce, and status→hue.
 */

import type { BadgeVariant } from '@/api/normalize';
import type { components } from '@/api/types.gen';

/** Health wire types come from the generated OpenAPI contract. */
export type HealthStatus = components['schemas']['ComponentStatus'];
export type ComponentHealth = components['schemas']['ComponentHealth'];
export type DataPlaneSqlHealth = components['schemas']['DataPlaneSqlHealth'];
export type HealthResponse = components['schemas']['HealthResponse'];

/** The three page sections, in the order they mirror the topology. */
export type HealthSection = 'api' | 'data' | 'control';

/** The response fields, one per page row. */
export type RowKey =
  | 'api'
  | 'conductor'
  | 'nats_kv'
  | 'executors'
  | 'data_plane_sql'
  | 'control_plane';

export interface RowSpec {
  key: RowKey;
  section: HealthSection;
  /** Row name (the detail text comes live from the endpoint). */
  name: string;
}

/**
 * Rows in render order: **API** → **Data plane** (Conductor, NATS JetStream KV,
 * Executors, selected SQL repository) → **Control plane** (one rollup). The
 * Executors entry is a single **pool** row — its detail is the endpoint's
 * *"N alive · X/Y slots"*, never per-executor rows.
 */
export const HEALTH_ROWS: readonly RowSpec[] = [
  { key: 'api', section: 'api', name: 'API gateway' },
  { key: 'conductor', section: 'data', name: 'Conductor' },
  { key: 'nats_kv', section: 'data', name: 'NATS JetStream KV' },
  { key: 'executors', section: 'data', name: 'Executors' },
  { key: 'data_plane_sql', section: 'data', name: 'SQL repository' },
  { key: 'control_plane', section: 'control', name: 'Control plane' },
];

/** Section header copy (the "one entry point / per-tenant execution / one
 * rollup" descriptors from the accepted design). */
export const HEALTH_SECTIONS: readonly {
  section: HealthSection;
  title: string;
  caption: string;
}[] = [
  { section: 'api', title: 'API', caption: "the UI's single entry point — gates everything below" },
  { section: 'data', title: 'Data plane', caption: 'per-tenant execution' },
  {
    section: 'control',
    title: 'Control plane',
    caption: 'one rollup — API → Frontend HTTP health check (Control plane)',
  },
];

/** status → semantic badge variant. healthy = green, degraded = amber,
 * unhealthy = red — the DC-0001 success/warning/destructive hues. */
export const HEALTH_BADGE: Record<HealthStatus, BadgeVariant> = {
  healthy: 'success',
  degraded: 'warning',
  unhealthy: 'destructive',
};

/** status → the Tailwind background token for the row's status dot. */
export const HEALTH_DOT_BG: Record<HealthStatus, string> = {
  healthy: 'bg-success',
  degraded: 'bg-warning',
  unhealthy: 'bg-destructive',
};

/** One reading of the endpoint: either a parsed response, or unreachable — the
 * endpoint not answering IS the API-down signal (DC-0013). */
export type HealthReading =
  | { ok: true; response: HealthResponse }
  | { ok: false };

/** A row as displayed. SQL implementation metadata affects only its label. */
export interface DisplayRow {
  status: HealthStatus;
  detail: string;
  implementation?: DataPlaneSqlHealth['implementation'];
}

export type HealthDisplay = Record<RowKey, DisplayRow>;

/**
 * The debounce state. `display` is what the page shows (null until the first
 * reading lands); `pending` remembers, per row, a candidate new status seen on
 * the previous read but not yet confirmed by a second consecutive read.
 */
export interface HealthState {
  display: HealthDisplay | null;
  pending: Partial<Record<RowKey, DisplayRow>>;
}

export function initialHealthState(): HealthState {
  return { display: null, pending: {} };
}

/** Resolve row copy. Backend selection never changes status behavior. */
export function healthRowName(spec: RowSpec, row: DisplayRow): string {
  if (spec.key !== 'data_plane_sql') return spec.name;
  switch (row.implementation) {
    case 'postgres':
      return 'Postgres';
    case 'sqlite':
      return 'SQLite';
    default:
      return spec.name;
  }
}

/**
 * The raw effective rows for one reading, cascade applied but NOT yet debounced.
 * Unreachable ⇒ the API row is unhealthy and every row below cascades to
 * unhealthy (the UI can't see a component except through the API). Reachable ⇒
 * the response's own rows, except that a genuinely-unhealthy API still cascades
 * (DC-0013: API is the gate).
 */
function rawRows(reading: HealthReading): HealthDisplay {
  if (!reading.ok) {
    const down: DisplayRow = { status: 'unhealthy', detail: 'API unreachable — cannot verify' };
    return {
      api: { status: 'unhealthy', detail: 'endpoint unreachable' },
      conductor: down,
      nats_kv: down,
      executors: down,
      data_plane_sql: down,
      control_plane: down,
    };
  }
  const r = reading.response;
  const api: DisplayRow = { status: r.api.status, detail: r.api.detail };
  const cascade = api.status === 'unhealthy';
  const row = (
    component: ComponentHealth,
    implementation?: DataPlaneSqlHealth['implementation'],
  ): DisplayRow =>
    cascade
      ? { status: 'unhealthy', detail: 'API unhealthy — cannot verify', implementation }
      : { status: component.status, detail: component.detail, implementation };
  return {
    api,
    conductor: row(r.conductor),
    nats_kv: row(r.nats_kv),
    executors: row(r.executors),
    data_plane_sql: row(r.data_plane_sql, r.data_plane_sql.implementation),
    control_plane: row(r.control_plane),
  };
}

/**
 * Fold one reading into the display state with the 2-consecutive-read debounce.
 *
 * - First reading ever: nothing to debounce against — adopt it verbatim (so a
 *   page that opens onto a down endpoint cascades immediately, not after a wait).
 * - Same status as displayed: refresh the row (detail may have changed, e.g.
 *   the executor slot count) and clear any pending candidate.
 * - New status: hold the displayed row and remember the candidate; only when a
 *   second consecutive read shows that same new status does the row flip. A
 *   single-read blip therefore never changes a row.
 */
export function reduceHealth(state: HealthState, reading: HealthReading): HealthState {
  const raw = rawRows(reading);
  if (state.display === null) {
    return { display: raw, pending: {} };
  }
  const display = { ...state.display };
  const pending: Partial<Record<RowKey, DisplayRow>> = {};
  for (const { key } of HEALTH_ROWS) {
    const cur = state.display[key];
    const next = raw[key];
    if (next.status === cur.status) {
      display[key] = next; // stable status — refresh detail, drop candidate
    } else {
      const candidate = state.pending[key];
      if (candidate && candidate.status === next.status) {
        display[key] = next; // second consecutive read of the new status — flip
      } else {
        display[key] = cur; // hold the displayed row
        pending[key] = next; // remember the (unconfirmed) candidate
      }
    }
  }
  return { display, pending };
}

/** The data-/control-plane cards dim when the (displayed) API row is unhealthy —
 * the API gates everything, so the UI can't vouch for anything beneath it. */
export function cardsDimmed(display: HealthDisplay | null): boolean {
  return display?.api.status === 'unhealthy';
}
