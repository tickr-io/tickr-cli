import { describe, it, expect } from 'vitest';
import {
  reduceHealth,
  initialHealthState,
  cardsDimmed,
  type HealthReading,
  type HealthResponse,
  type HealthStatus,
  type HealthState,
} from './health';

/** A full response with every row `healthy` unless overridden. */
function response(
  over: Partial<Record<keyof HealthResponse, HealthStatus>> = {},
  implementation: 'postgres' | 'sqlite' = 'postgres',
): HealthResponse {
  const row = (key: string, status: HealthStatus) => ({
    status,
    detail: `${key}:${status}`,
    detection_window: 'instant',
  });
  return {
    checked_at: '2026-07-15T14:23:40Z',
    api: row('api', over.api ?? 'healthy'),
    data_plane_sql: {
      ...row('data_plane_sql', over.data_plane_sql ?? 'healthy'),
      implementation,
    },
    nats_kv: row('nats_kv', over.nats_kv ?? 'healthy'),
    executors: {
      ...row('executors', over.executors ?? 'healthy'),
      capacity_interpretation: 'observation_only',
    },
    conductor: row('conductor', over.conductor ?? 'healthy'),
    control_plane: row('control_plane', over.control_plane ?? 'healthy'),
  };
}

const ok = (
  over?: Partial<Record<keyof HealthResponse, HealthStatus>>,
  implementation?: 'postgres' | 'sqlite',
): HealthReading => ({
  ok: true,
  response: response(over, implementation),
});
const unreachable: HealthReading = { ok: false };

/** Fold a sequence of readings from a fresh state. */
function run(...readings: HealthReading[]): HealthState {
  return readings.reduce(reduceHealth, initialHealthState());
}

describe('reduceHealth — first reading adoption', () => {
  it('adopts the first reading verbatim (nothing to debounce against)', () => {
    const s = run(ok({ executors: 'degraded' }));
    expect(s.display?.executors.status).toBe('degraded');
    expect(s.display?.api.status).toBe('healthy');
  });

  it('cascades immediately when the very first reading is unreachable', () => {
    const s = run(unreachable);
    expect(s.display?.api.status).toBe('unhealthy');
    expect(s.display?.data_plane_sql.status).toBe('unhealthy');
    expect(cardsDimmed(s.display)).toBe(true);
  });

  it('carries either selected SQL implementation through the same status path', () => {
    const postgres = run(ok({ data_plane_sql: 'unhealthy' }, 'postgres'));
    const sqlite = run(ok({ data_plane_sql: 'unhealthy' }, 'sqlite'));
    expect(postgres.display?.data_plane_sql).toMatchObject({
      status: 'unhealthy',
      implementation: 'postgres',
    });
    expect(sqlite.display?.data_plane_sql).toMatchObject({
      status: 'unhealthy',
      implementation: 'sqlite',
    });
  });
});

describe('reduceHealth — 2-consecutive-read debounce', () => {
  it('does not flip a row on a single-read blip', () => {
    // healthy established, one degraded read, then back to healthy.
    const s = run(ok(), ok({ nats_kv: 'degraded' }), ok());
    expect(s.display?.nats_kv.status).toBe('healthy');
  });

  it('flips a row only after two consecutive reads of the new status', () => {
    const s = run(ok(), ok({ nats_kv: 'degraded' }), ok({ nats_kv: 'degraded' }));
    expect(s.display?.nats_kv.status).toBe('degraded');
  });

  it('holds at the first read then flips on the second (step by step)', () => {
    let s = run(ok()); // established healthy
    s = reduceHealth(s, ok({ executors: 'degraded' }));
    expect(s.display?.executors.status).toBe('healthy'); // held
    s = reduceHealth(s, ok({ executors: 'degraded' }));
    expect(s.display?.executors.status).toBe('degraded'); // confirmed
  });

  it('resets the candidate when a differing new status interrupts the streak', () => {
    // healthy → degraded (candidate) → unhealthy (new candidate, not confirmed)
    const s = run(ok(), ok({ nats_kv: 'degraded' }), ok({ nats_kv: 'unhealthy' }));
    expect(s.display?.nats_kv.status).toBe('healthy'); // neither confirmed twice
  });

  it('refreshes detail while the status is stable', () => {
    const first = run(ok({ executors: 'healthy' }));
    // same status, different detail (slot count moved)
    const bumped: HealthReading = {
      ok: true,
      response: {
        ...response(),
        executors: {
          status: 'healthy',
          detail: '4 alive · 2/8 slots',
          detection_window: 'liveness window 2m',
          capacity_interpretation: 'observation_only',
        },
      },
    };
    const s = reduceHealth(first, bumped);
    expect(s.display?.executors.detail).toBe('4 alive · 2/8 slots');
  });
});

describe('reduceHealth — API-gate cascade + debounce interaction', () => {
  it('a single unreachable blip does not cascade an established-healthy page', () => {
    const s = run(ok(), unreachable, ok());
    expect(s.display?.api.status).toBe('healthy');
    expect(s.display?.data_plane_sql.status).toBe('healthy');
    expect(cardsDimmed(s.display)).toBe(false);
  });

  it('two consecutive unreachable reads cascade every row to unhealthy and dim', () => {
    const s = run(ok(), unreachable, unreachable);
    expect(s.display?.api.status).toBe('unhealthy');
    expect(s.display?.conductor.status).toBe('unhealthy');
    expect(s.display?.nats_kv.status).toBe('unhealthy');
    expect(s.display?.executors.status).toBe('unhealthy');
    expect(s.display?.data_plane_sql.status).toBe('unhealthy');
    expect(s.display?.control_plane.status).toBe('unhealthy');
    expect(cardsDimmed(s.display)).toBe(true);
  });

  it('reports every row below unhealthy when a reachable response has an unhealthy API', () => {
    // Reachable but API itself unhealthy — cascade still applies (first read adopts).
    const s = run(ok({ api: 'unhealthy', data_plane_sql: 'healthy' }));
    expect(s.display?.data_plane_sql.status).toBe('unhealthy');
    expect(cardsDimmed(s.display)).toBe(true);
  });
});
