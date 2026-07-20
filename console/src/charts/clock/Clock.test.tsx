import { describe, it, expect } from 'vitest';
import { render } from '@testing-library/react';
import { Clock } from './Clock';
import type { RadialBucket, SeriesDescriptor } from './types';
import type { ClockStatus } from './bucketize';

const SERIES: SeriesDescriptor<ClockStatus>[] = [
  { key: 'completed', label: 'Completed', color: 'var(--success)' },
  { key: 'in_progress', label: 'In progress', color: 'var(--info)' },
  { key: 'scheduled', label: 'Scheduled', color: 'var(--warning)' },
  { key: 'failed', label: 'Failed', color: 'var(--destructive)' },
];

function emptyRing(): RadialBucket<ClockStatus>[] {
  return Array.from({ length: 144 }, (_, i) => ({
    index: i,
    start: new Date(),
    end: new Date(),
    counts: { scheduled: 0, in_progress: 0, completed: 0, failed: 0 },
  }));
}

/** A ring with `count` of `status` placed in `hour` (0..11). */
function ringWith(hour: number, status: ClockStatus, count: number): RadialBucket<ClockStatus>[] {
  const r = emptyRing();
  r[hour * 12].counts[status] = count;
  return r;
}

function renderClock(props: Partial<React.ComponentProps<typeof Clock<ClockStatus>>> = {}) {
  const { container } = render(
    <Clock am={emptyRing()} pm={emptyRing()} series={SERIES} size={200} {...props} />,
  );
  return container.querySelector('svg') as SVGSVGElement;
}

describe('Clock — DC-0002 fidelity', () => {
  it('renders no tick lines and no digital readout (the forbiddens)', () => {
    // Past mode hides the hands, so a clean dial has zero <line> and <text>.
    const svg = renderClock({ viewMode: 'past' });
    expect(svg.querySelectorAll('line')).toHaveLength(0);
    expect(svg.querySelectorAll('text')).toHaveLength(0);
  });

  it('renders the background wash and two-tier teal borders per ring', () => {
    const svg = renderClock({ viewMode: 'past' });
    // Wash circle at the faint primary tint.
    expect(svg.querySelector('circle[fill="hsl(var(--primary) / 0.09)"]')).not.toBeNull();
    // Light-teal base on each ring (strokeOpacity 0.2): inner R*0.028, outer R*0.035.
    // size=200 → R=100 → widths 2.8 and 3.5.
    const bases = svg.querySelectorAll('circle[stroke-opacity="0.2"]');
    const widths = Array.from(bases).map((c) => parseFloat(c.getAttribute('stroke-width') ?? ''));
    const near = (target: number) => widths.some((w) => Math.abs(w - target) < 0.01);
    expect(near(2.8)).toBe(true); // AM (inner) border, R*0.028
    expect(near(3.5)).toBe(true); // PM (outer) border, R*0.035
  });

  it('renders the AM ring dimmed (opacity 0.8)', () => {
    const svg = renderClock({ viewMode: 'past' });
    expect(svg.querySelector('g[opacity="0.8"]')).not.toBeNull();
  });

  it('shows three hands and a two-tone pivot in live mode, still no digital text', () => {
    const svg = renderClock({ viewMode: 'live' });
    expect(svg.querySelectorAll('line')).toHaveLength(3); // hour, minute, second
    expect(svg.querySelectorAll('text')).toHaveLength(0);
  });

  it('thickens the selected segment stroke to 3.25', () => {
    const svg = renderClock({
      am: ringWith(9, 'completed', 2),
      pm: emptyRing(),
      viewMode: 'past',
      selectedSegmentId: 'am:completed:108-119',
    });
    const strokeWidths = Array.from(svg.querySelectorAll('path')).map((p) =>
      p.getAttribute('stroke-width'),
    );
    expect(strokeWidths).toContain('3.25');
  });
});
