import type { ClockInstance } from '@/api/client';
import { clockBucketForState } from '@/api/normalize';
import { TOTAL_BUCKETS, type RadialBucket } from './types';

export type ClockStatus = 'scheduled' | 'in_progress' | 'completed' | 'failed';

const COUNTED_STATUSES: ClockStatus[] = ['scheduled', 'in_progress', 'completed', 'failed'];

function emptyCounts(): Record<ClockStatus, number> {
  return { scheduled: 0, in_progress: 0, completed: 0, failed: 0 };
}

function ringDate(half: 'am' | 'pm', bucketIndex: number, dayAnchor: Date): { start: Date; end: Date } {
  const baseHour = half === 'am' ? 0 : 12;
  const hour = baseHour + Math.floor(bucketIndex / 12);
  const minute = (bucketIndex % 12) * 5;
  const start = new Date(dayAnchor);
  start.setHours(hour, minute, 0, 0);
  const end = new Date(start.getTime() + 5 * 60 * 1000);
  return { start, end };
}

function freshRing(half: 'am' | 'pm', dayAnchor: Date): RadialBucket<ClockStatus>[] {
  return Array.from({ length: TOTAL_BUCKETS }, (_, i) => ({
    index: i,
    counts: emptyCounts(),
    ...ringDate(half, i, dayAnchor),
  }));
}

function finishRings(
  am: RadialBucket<ClockStatus>[],
  pm: RadialBucket<ClockStatus>[],
): { am: RadialBucket<ClockStatus>[]; pm: RadialBucket<ClockStatus>[]; maxValue: number } {
  // p90 clamp — ignore wild outliers when scaling.
  const allCounts: number[] = [];
  for (const ring of [am, pm]) {
    for (const b of ring) {
      for (const k of COUNTED_STATUSES) {
        if (b.counts[k] > 0) allCounts.push(b.counts[k]);
      }
    }
  }
  allCounts.sort((a, b) => a - b);
  const p90 = allCounts.length ? allCounts[Math.floor(allCounts.length * 0.9)] : 1;
  const maxValue = Math.max(1, p90);
  return { am, pm, maxValue };
}

/** The AM/PM ring + bucket index an instance's local `scheduled_at` falls in. */
export function instanceSlot(
  scheduledAt: string | null | undefined,
): { half: 'am' | 'pm'; index: number } | null {
  if (!scheduledAt) return null;
  const d = new Date(scheduledAt);
  if (Number.isNaN(d.getTime())) return null;
  const hour = d.getHours();
  const minute = d.getMinutes();
  const half: 'am' | 'pm' = hour < 12 ? 'am' : 'pm';
  return { half, index: (hour % 12) * 12 + Math.floor(minute / 5) };
}

/**
 * Bucketize day-clock instances into AM/PM 144-bucket rings. Each instance's
 * `scheduled_at` (RFC3339) is read in local time — the dial renders local — and
 * its substrate `state` is folded into one of the four UI buckets via
 * `clockBucketForState`. Instances whose state isn't one of the four families
 * (or with no schedule) are skipped. The geometry (144 buckets) is unchanged
 * for this slice — the per-hour rebucketing lands with the DC-0002 dial port.
 */
export function bucketsFromInstances(
  instances: ReadonlyArray<ClockInstance>,
  dayAnchor: Date = new Date(),
): { am: RadialBucket<ClockStatus>[]; pm: RadialBucket<ClockStatus>[]; maxValue: number } {
  const am = freshRing('am', dayAnchor);
  const pm = freshRing('pm', dayAnchor);

  for (const inst of instances) {
    const slot = instanceSlot(inst.scheduled_at);
    if (!slot) continue;
    const bucket = clockBucketForState(inst.state);
    if (!bucket) continue;
    if (slot.index < 0 || slot.index >= TOTAL_BUCKETS) continue;
    (slot.half === 'am' ? am : pm)[slot.index].counts[bucket] += 1;
  }

  return finishRings(am, pm);
}
