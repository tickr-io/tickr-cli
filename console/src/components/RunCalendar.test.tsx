import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { RunCalendar } from './RunCalendar';
import type { DayCounts } from '@/api/client';

function day(date: string, over: Partial<DayCounts> = {}): DayCounts {
  const d = { date, completed: 0, failed: 0, in_progress: 0, scheduled: 0, total: 0, ...over };
  d.total = d.completed + d.failed + d.in_progress + d.scheduled;
  return d;
}

function cellFor(date: string): HTMLElement {
  return screen.getByRole('button', { name: new RegExp(`^${date}:`) });
}

const noop = () => {};

describe('RunCalendar', () => {
  it('applies the colour tier rule via l-{level} (failure ▸ scheduled ▸ in-progress ▸ completed)', () => {
    const days = [
      day('2026-01-02', { completed: 3, failed: 1 }), // any failure → failed
      day('2026-01-05', { completed: 2 }), // all completed
      day('2026-01-06', { in_progress: 1, completed: 5 }), // in-progress
    ];
    render(<RunCalendar days={days} year={2026} month={0} onDayClick={vi.fn()} onNavigate={noop} />);
    expect(cellFor('2026-01-02')).toHaveAttribute('data-level', 'failed');
    expect(cellFor('2026-01-02').className).toContain('l-failed');
    expect(cellFor('2026-01-05')).toHaveAttribute('data-level', 'completed');
    expect(cellFor('2026-01-06')).toHaveAttribute('data-level', 'in_progress');
    expect(cellFor('2026-01-03')).toHaveAttribute('data-level', 'none'); // no data
  });

  it('renders a future-dated scheduled day as the amber Future cell (emergent from the rule)', () => {
    render(
      <RunCalendar
        days={[day('2026-12-31', { scheduled: 1 })]}
        year={2026}
        month={11}
        onDayClick={vi.fn()}
        onNavigate={noop}
      />,
    );
    expect(cellFor('2026-12-31')).toHaveAttribute('data-level', 'scheduled');
    expect(cellFor('2026-12-31').className).toContain('l-scheduled');
  });

  it('shows only the displayed month and titles it by name and year', () => {
    // March days are shown; a neighbouring-month day is not in the grid.
    render(
      <RunCalendar
        days={[day('2026-02-15', { completed: 1 }), day('2026-03-10', { completed: 1 })]}
        year={2026}
        month={2}
        onDayClick={vi.fn()}
        onNavigate={noop}
      />,
    );
    expect(screen.getByText('March 2026')).toBeInTheDocument();
    expect(cellFor('2026-03-10')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /^2026-02-15:/ })).toBeNull();
  });

  it('shows an honest breakdown in the tooltip', () => {
    render(
      <RunCalendar
        days={[day('2026-03-01', { completed: 3 })]}
        year={2026}
        month={2}
        onDayClick={vi.fn()}
        onNavigate={noop}
      />,
    );
    expect(cellFor('2026-03-01')).toHaveAttribute('title', 'Mar 1 · 3 runs');
  });

  it('invokes onDayClick with the date, and with null when the active day is re-clicked', () => {
    const onDayClick = vi.fn();
    const days = [day('2026-03-01', { completed: 1 })];
    const { rerender } = render(
      <RunCalendar
        days={days}
        year={2026}
        month={2}
        selectedDate={null}
        onDayClick={onDayClick}
        onNavigate={noop}
      />,
    );
    fireEvent.click(cellFor('2026-03-01'));
    expect(onDayClick).toHaveBeenCalledWith('2026-03-01');

    rerender(
      <RunCalendar
        days={days}
        year={2026}
        month={2}
        selectedDate="2026-03-01"
        onDayClick={onDayClick}
        onNavigate={noop}
      />,
    );
    fireEvent.click(cellFor('2026-03-01'));
    expect(onDayClick).toHaveBeenLastCalledWith(null);
  });

  it('navigates months as a client slice, wrapping the year boundary into a refetch', () => {
    const onNavigate = vi.fn();
    const { rerender } = render(
      <RunCalendar days={[]} year={2026} month={5} onDayClick={vi.fn()} onNavigate={onNavigate} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Next month' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2026, month: 6 });
    fireEvent.click(screen.getByRole('button', { name: 'Previous month' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2026, month: 4 });

    // Stepping past December rolls into the next year (a refetch).
    rerender(
      <RunCalendar days={[]} year={2026} month={11} onDayClick={vi.fn()} onNavigate={onNavigate} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Next month' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2027, month: 0 });

    // Stepping before January rolls into the previous year.
    rerender(
      <RunCalendar days={[]} year={2026} month={0} onDayClick={vi.fn()} onNavigate={onNavigate} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Previous month' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2025, month: 11 });
  });

  it('navigates years on the same month, always a refetch', () => {
    const onNavigate = vi.fn();
    render(
      <RunCalendar days={[]} year={2026} month={5} onDayClick={vi.fn()} onNavigate={onNavigate} />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'Previous year' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2025, month: 5 });
    fireEvent.click(screen.getByRole('button', { name: 'Next year' }));
    expect(onNavigate).toHaveBeenLastCalledWith({ year: 2027, month: 5 });
  });
});
