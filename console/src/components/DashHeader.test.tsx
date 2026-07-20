import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act, fireEvent } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import type { UpcomingInstance } from '@/api/client';
import { DashHeader } from './DashHeader';

// Drive the strip off a mocked data hook so the component test never touches
// the network — the hook is exercised end-to-end by the API integration tests.
vi.mock('@/api/hooks', () => ({ useUpcoming: vi.fn() }));
import { useUpcoming } from '@/api/hooks';

const mockUpcoming = vi.mocked(useUpcoming);

function setData(data: UpcomingInstance[] | undefined, isLoading = false, isFetching = false) {
  // Only the fields DashHeader reads are needed from the query result.
  mockUpcoming.mockReturnValue({ data, isLoading, isFetching } as ReturnType<typeof useUpcoming>);
}

function renderStrip() {
  return render(
    <MemoryRouter>
      <DashHeader />
    </MemoryRouter>,
  );
}

// Mirrors the real wire shape (openapi.yaml UpcomingInstance): `name` is the
// Run name and is required on the wire.
function rows(n: number, baseOffsetMs = 60_000): UpcomingInstance[] {
  return Array.from({ length: n }, (_, i) => ({
    workflow_instance_id: `inst-${i}`,
    workflow_id: `wf-${i}`,
    workflow_name: `workflow ${i}`,
    name: `run ${i}`,
    next_run_at: new Date(Date.now() + baseOffsetMs * (i + 1)).toISOString(),
  }));
}

describe('DashHeader', () => {
  beforeEach(() => mockUpcoming.mockReset());

  it('renders the terse empty state when nothing is scheduled', () => {
    setData([]);
    renderStrip();
    expect(screen.getByText('Nothing scheduled.')).toBeInTheDocument();
  });

  it('renders the Run name on every chip, countdown on the lead', () => {
    setData(rows(1));
    renderStrip();
    // The chip label is the Run name (DC-0015 primary identity), not the workflow name.
    expect(screen.getByText('run 0')).toBeInTheDocument();
    expect(screen.queryByText('workflow 0')).not.toBeInTheDocument();
    // Lead chip carries an "in …" countdown.
    expect(screen.getByText(/^in /)).toBeInTheDocument();
  });

  it('renders three chips with the countdown only on the lead', () => {
    setData(rows(3));
    renderStrip();
    expect(screen.getByText('run 0')).toBeInTheDocument();
    expect(screen.getByText('run 1')).toBeInTheDocument();
    expect(screen.getByText('run 2')).toBeInTheDocument();
    // Exactly one countdown span — the lead's.
    expect(screen.getAllByText(/^in /)).toHaveLength(1);
    // Each chip deep-links to its parent workflow.
    expect(screen.getByRole('link', { name: /run 0/ })).toHaveAttribute('href', '/workflows/wf-0');
  });

  it('falls back to the workflow name when the wire carries no Run name', () => {
    const legacy = rows(1).map((row) => {
      const withoutName: Partial<UpcomingInstance> = { ...row };
      delete withoutName.name;
      return withoutName;
    }) as UpcomingInstance[];
    setData(legacy);
    renderStrip();
    expect(screen.getByText('workflow 0')).toBeInTheDocument();
  });
});

describe('DashHeader fetch-more', () => {
  beforeEach(() => mockUpcoming.mockReset());

  it('is enabled while a full page came back, and widens the ask on click', () => {
    setData(rows(20)); // full page — the wheel may hold more
    renderStrip();
    const btn = screen.getByRole('button', { name: 'Fetch more' });
    expect(btn).toBeEnabled();
    fireEvent.click(btn);
    // The click widens the limit; the hook is re-invoked with the larger ask.
    expect(mockUpcoming).toHaveBeenLastCalledWith(40);
  });

  it('grays out once the response is smaller than the ask (nothing more)', () => {
    setData(rows(5)); // 5 < 20 — the wheel has nothing further armed
    renderStrip();
    const btn = screen.getByRole('button', { name: 'Fetch more' });
    expect(btn).toBeDisabled();
    expect(btn).toHaveAttribute('title', 'No more scheduled runs');
  });

  it('does not render the button at all on the empty state', () => {
    setData([]);
    renderStrip();
    expect(screen.queryByRole('button', { name: /Fetch/ })).not.toBeInTheDocument();
  });
});

describe('DashHeader lead countdown', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it('ticks down every second against next_run_at', () => {
    setData([
      {
        workflow_instance_id: 'inst-0',
        workflow_id: 'wf-0',
        workflow_name: 'soon',
        name: 'soon (2026-07-06 17:30)',
        next_run_at: new Date(Date.now() + 90_000).toISOString(),
      },
    ]);
    renderStrip();
    expect(screen.getByText('in 1m')).toBeInTheDocument();

    // Advance the wall clock 60s; the useNow tick re-renders the countdown.
    act(() => {
      vi.advanceTimersByTime(60_000);
    });
    expect(screen.getByText('in 30s')).toBeInTheDocument();
  });
});
