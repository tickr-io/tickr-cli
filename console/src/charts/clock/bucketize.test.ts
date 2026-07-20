import { describe, it, expect } from 'vitest';
import type { ClockInstance } from '@/api/client';
import { bucketsFromInstances, instanceSlot } from './bucketize';

// Build an RFC3339 instant from a local wall-clock time so the test is
// timezone-independent (instanceSlot reads local hours).
function atLocal(hour: number, minute: number): string {
  const d = new Date();
  d.setHours(hour, minute, 0, 0);
  return d.toISOString();
}

function inst(state: string, hour: number, minute: number): ClockInstance {
  return {
    id: `${state}-${hour}:${minute}`,
    workflow_id: '00000000-0000-0000-0000-000000000000',
    workflow_name: 'wf',
    scheduled_at: atLocal(hour, minute),
    state,
  };
}

describe('instanceSlot', () => {
  it('maps a local morning time to the AM ring 144-bucket index', () => {
    expect(instanceSlot(atLocal(9, 30))).toEqual({ half: 'am', index: 9 * 12 + 6 });
  });
  it('maps a local afternoon time to the PM ring', () => {
    expect(instanceSlot(atLocal(14, 0))).toEqual({ half: 'pm', index: 2 * 12 + 0 });
  });
  it('returns null for a missing/invalid time', () => {
    expect(instanceSlot(null)).toBeNull();
    expect(instanceSlot('not-a-date')).toBeNull();
  });
});

describe('bucketsFromInstances', () => {
  it('buckets by local scheduled_at and folds state into the right family', () => {
    const { am, pm } = bucketsFromInstances([
      inst('Completed', 9, 0), // AM, index 108
      inst('Cancelled', 9, 0), // folds to failed, same AM bucket
      inst('InProgress', 15, 30), // PM, index 3*12+6 = 42
    ]);
    expect(am[108].counts.completed).toBe(1);
    expect(am[108].counts.failed).toBe(1); // Cancelled folded in
    expect(pm[42].counts.in_progress).toBe(1);
  });

  it('skips instances whose state is outside the four families', () => {
    const { am, pm } = bucketsFromInstances([inst('Queued', 8, 0), inst('Skipped', 13, 0)]);
    const total = [...am, ...pm].reduce(
      (n, b) => n + b.counts.scheduled + b.counts.in_progress + b.counts.completed + b.counts.failed,
      0,
    );
    expect(total).toBe(0);
  });
});
