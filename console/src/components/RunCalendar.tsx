import { useMemo } from 'react';
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from 'lucide-react';
import type { DayCounts } from '@/api/client';

type Level = 'failed' | 'scheduled' | 'in_progress' | 'completed' | 'none';

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];
const CAL_MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const WEEKDAYS = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];
const cx = (...a: (string | false | null | undefined)[]) => a.filter(Boolean).join(' ');
const pad = (n: number) => (n < 10 ? `0${n}` : `${n}`);

/** Calendar colour tier rule (DC-0009): first matching state wins. */
function levelOf(d: DayCounts | undefined): Level {
  if (!d) return 'none';
  if (d.failed > 0) return 'failed';
  if (d.scheduled > 0) return 'scheduled';
  if (d.in_progress > 0) return 'in_progress';
  if (d.completed > 0) return 'completed';
  return 'none';
}

function label(date: Date, d: DayCounts | undefined, level: Level): string {
  const head = `${CAL_MONTHS[date.getMonth()]} ${date.getDate()}`;
  if (level === 'scheduled') return `${head} · scheduled`;
  if (level === 'in_progress') return `${head} · in progress`;
  if (level === 'failed') return `${head} · ${d?.failed} failed`;
  if (level === 'none') return `${head} · no run`;
  return `${head} · ${d?.completed} run${(d?.completed ?? 0) > 1 ? 's' : ''}`;
}

/**
 * One navigable month of run history (DC-0009 colour tiers), co-located with the
 * run list it filters. Month navigation (‹ ›) is a pure client slice over the
 * days the year-parameterised calendar hook already fetched; the year steppers
 * (« ») — and a month step past the year boundary — raise `onNavigate` with a
 * new year so the parent refetches. Clicking a day filters the run list in
 * place; clicking the active day clears the filter. A future-dated
 * `scheduled >= 1` day comes out amber by the same rule (the "Future cell").
 */
export function RunCalendar({
  days,
  year,
  month,
  selectedDate,
  onDayClick,
  onNavigate,
}: {
  days: DayCounts[];
  year: number;
  /** Displayed month, 0-11. Controlled by the parent so navigation persists. */
  month: number;
  selectedDate?: string | null;
  onDayClick: (date: string | null) => void;
  onNavigate: (next: { year: number; month: number }) => void;
}) {
  const byDate = useMemo(() => {
    const m = new Map<string, DayCounts>();
    for (const d of days) m.set(d.date, d);
    return m;
  }, [days]);

  // The month's day cells, each placed by weekday column; a leading offset of
  // empty cells aligns day 1 under its weekday.
  const { cells, leadOffset } = useMemo(() => {
    const dim = new Date(year, month + 1, 0).getDate();
    const lead = new Date(year, month, 1).getDay();
    const out: { iso: string; date: Date }[] = [];
    for (let day = 1; day <= dim; day++) {
      const date = new Date(year, month, day);
      out.push({ iso: `${year}-${pad(month + 1)}-${pad(day)}`, date });
    }
    return { cells: out, leadOffset: lead };
  }, [year, month]);

  // Step `delta` months, wrapping across the year boundary into a refetch.
  const goMonth = (delta: number) => {
    let m = month + delta;
    let y = year;
    if (m < 0) {
      m = 11;
      y -= 1;
    } else if (m > 11) {
      m = 0;
      y += 1;
    }
    onNavigate({ year: y, month: m });
  };

  // Step `delta` years on the same month — always a refetch.
  const goYear = (delta: number) => onNavigate({ year: year + delta, month });

  return (
    <div className="space-y-3">
      <div className="cal-nav">
        <span className="cal-navgroup">
          <button type="button" className="cal-navbtn" aria-label="Previous year" onClick={() => goYear(-1)}>
            <ChevronsLeft size={16} aria-hidden />
          </button>
          <button type="button" className="cal-navbtn" aria-label="Previous month" onClick={() => goMonth(-1)}>
            <ChevronLeft size={16} aria-hidden />
          </button>
        </span>
        <span className="cal-title">
          {MONTH_NAMES[month]} {year}
        </span>
        <span className="cal-navgroup">
          <button type="button" className="cal-navbtn" aria-label="Next month" onClick={() => goMonth(1)}>
            <ChevronRight size={16} aria-hidden />
          </button>
          <button type="button" className="cal-navbtn" aria-label="Next year" onClick={() => goYear(1)}>
            <ChevronsRight size={16} aria-hidden />
          </button>
        </span>
      </div>

      <div className="cal-legend">
        <span className="cal-leg"><span className="cal-sw l-completed" />All success</span>
        <span className="cal-leg"><span className="cal-sw l-failed" />≥1 failure</span>
        <span className="cal-leg"><span className="cal-sw l-scheduled" />≥1 pending</span>
        <span className="cal-leg"><span className="cal-sw l-in_progress" />In progress</span>
      </div>

      <div className="cal-mgrid" role="grid" aria-label={`${MONTH_NAMES[month]} ${year}`}>
        {WEEKDAYS.map((wd) => (
          <span key={wd} className="cal-wd" aria-hidden>
            {wd}
          </span>
        ))}
        {Array.from({ length: leadOffset }, (_, i) => (
          <span key={`lead-${i}`} className="cal-mcell-empty" aria-hidden />
        ))}
        {cells.map((c) => {
          const d = byDate.get(c.iso);
          const level = levelOf(d);
          const sel = selectedDate === c.iso;
          return (
            <button
              key={c.iso}
              type="button"
              data-level={level}
              aria-label={`${c.iso}: ${label(c.date, d, level)}`}
              title={label(c.date, d, level)}
              className={cx('cal-mcell', `l-${level}`, sel && 'sel')}
              onClick={() => onDayClick(sel ? null : c.iso)}
            >
              {c.date.getDate()}
            </button>
          );
        })}
      </div>
    </div>
  );
}
