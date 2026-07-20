import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import type { ClockInstance, ClockResponse } from '@/api/client';
import { DashboardPage } from './DashboardPage';

vi.mock('@/api/hooks', () => ({
  useDashboardClock: vi.fn(),
  useUpcoming: vi.fn(() => ({ data: [], isLoading: false })),
}));
import { useDashboardClock } from '@/api/hooks';

const mockClock = vi.mocked(useDashboardClock);

function instance(state: string, hour: number): ClockInstance {
  const d = new Date();
  d.setHours(hour, 0, 0, 0);
  return {
    id: `${state}-${hour}`,
    workflow_id: 'wf-1',
    workflow_name: `wf ${state}`,
    scheduled_at: d.toISOString(),
    state,
  };
}

function setClock(data: ClockResponse | undefined, opts: Partial<{ isLoading: boolean }> = {}) {
  mockClock.mockReturnValue({
    data,
    isLoading: opts.isLoading ?? false,
    error: null,
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useDashboardClock>);
}

function renderPage() {
  return render(
    <MemoryRouter>
      <DashboardPage />
    </MemoryRouter>,
  );
}

// The dial's clickable curve segments also expose a button role; scope stat-row
// queries to the labelled stat list so they don't collide.
function statRow(name: RegExp) {
  const list = screen.getByRole('list', { name: 'Status filters' });
  return within(list).getByRole('button', { name });
}

const sampleDay: ClockResponse = {
  instances: [
    instance('Completed', 9),
    instance('Completed', 10),
    instance('InProgress', 11),
    instance('Failed', 14),
    instance('Scheduled', 16),
  ],
  live_data_available: true,
};

describe('DashboardPage stat list', () => {
  beforeEach(() => mockClock.mockReset());

  it('derives the four-row counts from the merged instance list', () => {
    setClock(sampleDay);
    renderPage();
    expect(within(statRow(/In progress/)).getByText('1')).toBeInTheDocument();
    expect(within(statRow(/Scheduled/)).getByText('1')).toBeInTheDocument();
    expect(within(statRow(/Completed/)).getByText('2')).toBeInTheDocument();
    expect(within(statRow(/Failed/)).getByText('1')).toBeInTheDocument();
  });

  it('toggles a status off the dial when its row is clicked', () => {
    setClock(sampleDay);
    renderPage();
    const completed = statRow(/Completed/);
    expect(completed).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(completed);
    expect(completed).toHaveAttribute('aria-pressed', 'false');
  });

  it('blocks toggling the last enabled status off (last-one-on guard)', () => {
    setClock(sampleDay);
    renderPage();
    // Turn off three of the four; the fourth must refuse to turn off.
    fireEvent.click(statRow(/In progress/));
    fireEvent.click(statRow(/Scheduled/));
    fireEvent.click(statRow(/Completed/));
    const failed = statRow(/Failed/);
    expect(failed).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(failed);
    expect(failed).toHaveAttribute('aria-pressed', 'true');
  });

  it('shows a degraded indicator when the live half is unavailable', () => {
    setClock({ instances: [], live_data_available: false });
    renderPage();
    expect(screen.getByText('Live data unavailable')).toBeInTheDocument();
  });
});
