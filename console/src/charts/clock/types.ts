export type SeriesKey = string;

export interface RadialBucket<K extends SeriesKey = SeriesKey> {
  /** 0..143 — 12 hours × 12 buckets/hour at 5-minute resolution. */
  index: number;
  start: Date;
  end: Date;
  counts: Record<K, number>;
}

export interface SeriesDescriptor<K extends SeriesKey = SeriesKey> {
  key: K;
  label: string;
  /** CSS variable name (preferred for theming) OR a hex color. */
  color: `var(--${string})` | `#${string}`;
  pattern?: 'solid' | 'dashed' | 'dotted';
}

export interface RingGeometry {
  innerRadius: number;
  outerRadius: number;
  /** Radian where bucket index 0 sits. Default = -π/2 (12 o'clock, north). */
  startAngle?: number;
  /** Sweep direction. Default 'cw'. */
  direction?: 'cw' | 'ccw';
}

export interface RadialPointerEvent<K extends SeriesKey> {
  bucket: RadialBucket<K>;
  series: K;
  count: number;
  clientX: number;
  clientY: number;
}

export const TOTAL_BUCKETS = 144;
export const DEGREES_PER_BUCKET = 360 / TOTAL_BUCKETS; // 2.5

/** Angle in radians for a bucket index, with bucket 0 at the top, going clockwise. */
export function bucketAngleRad(index: number, geometry: RingGeometry): number {
  const start = geometry.startAngle ?? -Math.PI / 2;
  const sign = geometry.direction === 'ccw' ? -1 : 1;
  return start + sign * (index * DEGREES_PER_BUCKET * Math.PI) / 180;
}
