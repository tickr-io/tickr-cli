import { useMemo } from 'react';
import type { RadialBucket, SeriesDescriptor, SeriesKey } from './types';
import { TOTAL_BUCKETS } from './types';
import { useNow } from '@/hooks/useNow';

/**
 * The Tickr day-clock (DC-0002). An analog status dial: AM is the inner ring,
 * PM is the outer ring, both always rendered — no live-half swap. Each hour is
 * its own raised-cosine area-curve (bold status line + soft fill), strictly
 * one hour wide. Each ring carries a two-tier teal time border — a light base
 * always on plus a solid elapsed arc tracking its own 12-hour window. Hands and
 * a two-tone pivot only: no ticks, no numerals, no digital readout.
 *
 * The bucket data arrives in the 144-slot (5-min) space the data layer
 * produces; the dial collapses each hour's twelve slots into a single per-hour
 * band so the geometry is hourly while the click target (the segment id) stays
 * in 144-space, compatible with the side-sheet's slot filter.
 */
interface ClockProps<K extends SeriesKey> {
  /** AM buckets (00:00–11:59), rendered on the inner ring. */
  am: ReadonlyArray<RadialBucket<K>>;
  /** PM buckets (12:00–23:59), rendered on the outer ring. */
  pm: ReadonlyArray<RadialBucket<K>>;
  series: ReadonlyArray<SeriesDescriptor<K>>;
  size?: number;
  /** If omitted, the dial subscribes to the internal useNow() 1Hz tick. */
  now?: Date;
  showHands?: boolean;
  selectedSegmentId?: string | null;
  onSegmentSelect?: (id: string | null) => void;
  className?: string;
  ariaLabel?: string;
  /** 'live' (default): hands shown, borders track wall clock. 'past'/'future':
   *  hands hidden; borders read fully elapsed / fully ahead respectively. */
  viewMode?: 'live' | 'past' | 'future';
}

const DEG = 360 / TOTAL_BUCKETS; // 2.5° per 5-min slot
const HOURS_PER_HALF = 12;
const SLOTS_PER_HOUR = TOTAL_BUCKETS / HOURS_PER_HALF; // 12

interface Ring {
  outer: number;
  inner: number;
}

function pt(cx: number, cy: number, r: number, a: number) {
  return { x: cx + r * Math.cos(a), y: cy + r * Math.sin(a) };
}
function ang(i: number) {
  return -Math.PI / 2 + (i * DEG * Math.PI) / 180;
}

/** One hour's area-curve: a raised-cosine plateau dipping inward from the ring's
 *  outer edge. Returns the bold top line and the light fill beneath it. */
function bump(cx: number, cy: number, geom: Ring, s: number, e: number, t: number, samples = 26) {
  const base = geom.outer;
  const span = geom.outer - geom.inner;
  const half = (DEG * Math.PI) / 360;
  const a0 = ang(s) - half;
  const a1 = ang(e) + half;
  const shape = (p: number) => {
    const ed = 0.42;
    if (p < ed) return 0.5 - 0.5 * Math.cos((Math.PI * p) / ed);
    if (p > 1 - ed) return 0.5 - 0.5 * Math.cos((Math.PI * (1 - p)) / ed);
    return 1;
  };
  const pts: { x: number; y: number }[] = [];
  for (let i = 0; i <= samples; i++) {
    const p = i / samples;
    const a = a0 + (a1 - a0) * p;
    const r = base - span * Math.max(0.06, t) * shape(p);
    pts.push(pt(cx, cy, r, a));
  }
  let line = `M ${pts[0].x} ${pts[0].y}`;
  for (let i = 1; i < pts.length; i++) line += ` L ${pts[i].x} ${pts[i].y}`;
  const lg = a1 - a0 > Math.PI ? 1 : 0;
  const area = `${line} A ${base} ${base} 0 ${lg} 0 ${pts[0].x} ${pts[0].y} Z`;
  return { line, area };
}

/** Arc by absolute degrees (0 = 12 o'clock, clockwise) — the teal elapsed band. */
function arcDeg(cx: number, cy: number, r: number, d0: number, d1: number) {
  const a0 = ((d0 - 90) * Math.PI) / 180;
  const a1 = ((d1 - 90) * Math.PI) / 180;
  const p0 = pt(cx, cy, r, a0);
  const p1 = pt(cx, cy, r, a1);
  const lg = d1 - d0 > 180 ? 1 : 0;
  return `M ${p0.x} ${p0.y} A ${r} ${r} 0 ${lg} 1 ${p1.x} ${p1.y}`;
}

function seriesColor<K extends SeriesKey>(
  series: ReadonlyArray<SeriesDescriptor<K>>,
  key: K,
): string {
  const c = series.find((s) => s.key === key)?.color ?? 'var(--muted-foreground)';
  return c.startsWith('#') ? c : `hsl(${c})`;
}

interface HourBand<K> {
  status: K;
  /** 144-space inclusive slot range for this hour (start..start+11). */
  start: number;
  end: number;
  t: number;
}

/** Collapse a 144-slot ring into per-hour, per-status bands (count > 0 only). */
function hourBands<K extends SeriesKey>(
  data: ReadonlyArray<RadialBucket<K>>,
  series: ReadonlyArray<SeriesDescriptor<K>>,
  maxValue: number,
): HourBand<K>[] {
  const bands: HourBand<K>[] = [];
  for (let hour = 0; hour < HOURS_PER_HALF; hour++) {
    const start = hour * SLOTS_PER_HOUR;
    const end = start + SLOTS_PER_HOUR - 1;
    for (const s of series) {
      let count = 0;
      for (let i = start; i <= end; i++) count += data[i]?.counts[s.key] ?? 0;
      if (count > 0) {
        bands.push({ status: s.key, start, end, t: count / maxValue });
      }
    }
  }
  return bands;
}

function ringMax<K extends SeriesKey>(
  data: ReadonlyArray<RadialBucket<K>>,
  series: ReadonlyArray<SeriesDescriptor<K>>,
): number {
  let max = 1;
  for (let hour = 0; hour < HOURS_PER_HALF; hour++) {
    const start = hour * SLOTS_PER_HOUR;
    for (const s of series) {
      let count = 0;
      for (let i = start; i < start + SLOTS_PER_HOUR; i++) count += data[i]?.counts[s.key] ?? 0;
      if (count > max) max = count;
    }
  }
  return max;
}

function ClockRing<K extends SeriesKey>({
  cx,
  cy,
  geom,
  bands,
  series,
  ringId,
  dim,
  selectedSegmentId,
  onSegmentSelect,
}: {
  cx: number;
  cy: number;
  geom: Ring;
  bands: HourBand<K>[];
  series: ReadonlyArray<SeriesDescriptor<K>>;
  ringId: string;
  dim?: boolean;
  selectedSegmentId?: string | null;
  onSegmentSelect?: (id: string | null) => void;
}) {
  return (
    <g opacity={dim ? 0.8 : 1}>
      {bands.map((b) => {
        const id = `${ringId}:${b.status}:${b.start}-${b.end}`;
        const isSel = selectedSegmentId === id;
        const color = seriesColor(series, b.status);
        const paths = bump(cx, cy, geom, b.start, b.end, b.t);
        return (
          <g
            key={id}
            role="button"
            aria-label={`${String(b.status)} hour ${Math.floor(b.start / SLOTS_PER_HOUR)}`}
            style={{ cursor: 'pointer' }}
            onClick={() => onSegmentSelect?.(isSel ? null : id)}
          >
            <path
              d={paths.area}
              fill={color}
              fillOpacity={isSel ? 0.26 : 0.1}
              stroke="none"
              style={{ transition: 'fill-opacity .12s' }}
            />
            <path
              d={paths.line}
              fill="none"
              stroke={color}
              strokeWidth={isSel ? 3.25 : 2.5}
              strokeLinejoin="round"
              strokeLinecap="round"
              style={{ transition: 'stroke-width .12s' }}
            />
          </g>
        );
      })}
    </g>
  );
}

export function Clock<K extends SeriesKey>({
  am,
  pm,
  series,
  size = 360,
  now: nowProp,
  showHands = true,
  selectedSegmentId,
  onSegmentSelect,
  className,
  ariaLabel,
  viewMode = 'live',
}: ClockProps<K>) {
  const liveNow = useNow();
  const now = nowProp ?? liveNow;
  const isLive = viewMode === 'live';

  const R = size / 2;
  const cx = R;
  const cy = R;

  // Shared scale across both rings so AM and PM hump heights are comparable.
  const maxValue = useMemo(
    () => Math.max(ringMax(am, series), ringMax(pm, series)),
    [am, pm, series],
  );
  const amBands = useMemo(() => hourBands(am, series, maxValue), [am, series, maxValue]);
  const pmBands = useMemo(() => hourBands(pm, series, maxValue), [pm, series, maxValue]);

  const outerRing: Ring = { outer: R * 0.912, inner: R * 0.72 };
  const innerRing: Ring = { outer: R * 0.571, inner: R * 0.45 };
  const progR = R * 0.93; // PM border radius
  const amBorderR = R * 0.585;
  const amBandW = R * 0.028;
  const pmBandW = R * 0.035;

  // Per-ring elapsed fraction of its own 12-hour window.
  const mod = now.getHours() * 60 + now.getMinutes();
  const liveAm = Math.min(1, mod / 720);
  const livePm = Math.max(0, Math.min(1, (mod - 720) / 720));
  const amFrac = viewMode === 'past' ? 1 : viewMode === 'future' ? 0 : liveAm;
  const pmFrac = viewMode === 'past' ? 1 : viewMode === 'future' ? 0 : livePm;

  // Hands (live only). Plain lines re-rendered each useNow tick — no springs.
  const sec = now.getSeconds();
  const min = now.getMinutes() + sec / 60;
  const hr = (now.getHours() % 12) + min / 60;
  const hand = (deg: number, len: number) => pt(cx, cy, R * len, ((deg - 90) * Math.PI) / 180);
  const hh = hand(hr * 30, 0.34);
  const mm = hand(min * 6, 0.5);
  const ss = hand(sec * 6, 0.58);

  const teal = 'hsl(var(--primary))';

  return (
    <div className={className} style={{ width: size, position: 'relative' }}>
      <svg
        viewBox={`0 0 ${size} ${size}`}
        width={size}
        height={size}
        role="img"
        aria-label={ariaLabel ?? 'Workflow status by time of day'}
      >
        {/* Faint background wash — not a grey baseplate. */}
        <circle cx={cx} cy={cy} r={R * 0.915} fill="hsl(var(--primary) / 0.09)" />

        {/* PM border (outer): light-teal base always on + solid elapsed arc. */}
        <circle cx={cx} cy={cy} r={progR} fill="none" stroke={teal} strokeWidth={pmBandW} strokeOpacity={0.2} />
        {pmFrac >= 0.999 ? (
          <circle cx={cx} cy={cy} r={progR} fill="none" stroke={teal} strokeWidth={pmBandW} />
        ) : (
          pmFrac > 0.002 && (
            <path d={arcDeg(cx, cy, progR, 0, pmFrac * 360)} fill="none" stroke={teal} strokeWidth={pmBandW} strokeLinecap="round" />
          )
        )}

        <ClockRing
          cx={cx}
          cy={cy}
          geom={outerRing}
          bands={pmBands}
          series={series}
          ringId="pm"
          selectedSegmentId={selectedSegmentId}
          onSegmentSelect={onSegmentSelect}
        />

        {/* AM border (inner): light-teal base + solid elapsed arc, thinner. */}
        <circle cx={cx} cy={cy} r={amBorderR} fill="none" stroke={teal} strokeWidth={amBandW} strokeOpacity={0.2} />
        {amFrac >= 0.999 ? (
          <circle cx={cx} cy={cy} r={amBorderR} fill="none" stroke={teal} strokeWidth={amBandW} />
        ) : (
          amFrac > 0.002 && (
            <path d={arcDeg(cx, cy, amBorderR, 0, amFrac * 360)} fill="none" stroke={teal} strokeWidth={amBandW} strokeLinecap="round" />
          )
        )}

        <ClockRing
          cx={cx}
          cy={cy}
          geom={innerRing}
          bands={amBands}
          series={series}
          ringId="am"
          dim
          selectedSegmentId={selectedSegmentId}
          onSegmentSelect={onSegmentSelect}
        />

        {showHands && isLive && (
          <g>
            <line x1={cx} y1={cy} x2={hh.x} y2={hh.y} stroke="hsl(var(--foreground))" strokeWidth={3.5} strokeLinecap="round" />
            <line x1={cx} y1={cy} x2={mm.x} y2={mm.y} stroke="hsl(var(--foreground))" strokeWidth={2.25} strokeLinecap="round" />
            <line x1={cx} y1={cy} x2={ss.x} y2={ss.y} stroke={teal} strokeWidth={1.25} strokeLinecap="round" />
            {/* Two-tone pivot. */}
            <circle cx={cx} cy={cy} r={R * 0.02} fill="hsl(var(--foreground))" />
            <circle cx={cx} cy={cy} r={R * 0.009} fill={teal} />
          </g>
        )}
      </svg>
    </div>
  );
}
