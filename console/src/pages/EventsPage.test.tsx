import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import type { Event } from '@/api/client';
import { EventsPage } from './EventsPage';

vi.mock('@/api/hooks', async () => {
  const actual = await vi.importActual<typeof import('@/api/hooks')>('@/api/hooks');
  return { ...actual, useEventTail: vi.fn() };
});
import { useEventTail } from '@/api/hooks';

const mockTail = vi.mocked(useEventTail);

let seqCounter = 0;
function ev(event_type: string, over: Partial<Event> = {}): Event {
  seqCounter += 1;
  return {
    seq: seqCounter,
    id: `00000000-0000-0000-0000-${String(seqCounter).padStart(12, '0')}`,
    ts: new Date(Date.now() - 5_000).toISOString(),
    event_type,
    payload: { [event_type]: {} },
    ...over,
  };
}

function setTail(events: Event[], over: Partial<ReturnType<typeof useEventTail>> = {}) {
  mockTail.mockReturnValue({
    events,
    newSeqs: new Set<number>(),
    isLoading: false,
    error: null,
    ...over,
  });
}

beforeEach(() => {
  vi.clearAllMocks();
  seqCounter = 0;
});

describe('EventsPage', () => {
  it('renders real event rows newest-first with vocabulary intact', () => {
    setTail([ev('WorkflowCompleted'), ev('TaskQueued'), ev('GateDispatched')]);
    render(<EventsPage />);
    const rows = screen.getAllByTestId('event-row');
    expect(rows).toHaveLength(3);
    expect(within(rows[0]).getByText('WorkflowCompleted')).toBeInTheDocument();
    expect(screen.getByText('workflow completed')).toBeInTheDocument();
  });

  it('category filters isolate the matching slice; Cluster filter does not exist', () => {
    setTail([ev('WorkflowTriggered'), ev('TaskQueued'), ev('GateOutcome')]);
    render(<EventsPage />);
    expect(screen.queryByRole('button', { name: 'Cluster' })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Task' }));
    const rows = screen.getAllByTestId('event-row');
    expect(rows).toHaveLength(1);
    expect(within(rows[0]).getByText('TaskQueued')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'All' }));
    expect(screen.getAllByTestId('event-row')).toHaveLength(3);
  });

  it('pause flips the tail inactive and resume re-activates it', () => {
    setTail([ev('TaskStarted')]);
    render(<EventsPage />);
    expect(mockTail).toHaveBeenLastCalledWith(true);
    expect(screen.getByText(/Live · polling/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Pause' }));
    expect(mockTail).toHaveBeenLastCalledWith(false);
    expect(screen.getByText(/Paused · polling/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Resume' }));
    expect(mockTail).toHaveBeenLastCalledWith(true);
  });

  it('states the liveness story in the footer', () => {
    setTail([ev('TaskStarted')]);
    render(<EventsPage />);
    expect(screen.getByText(/~10–17s of occurring/)).toBeInTheDocument();
    expect(screen.getByText(/history is durable/)).toBeInTheDocument();
    expect(screen.getByText(/Showing 1 of 1 buffered/)).toBeInTheDocument();
  });

  it('shows the error state only when there is nothing buffered to show', () => {
    setTail([], { error: new Error('events projection unavailable') });
    render(<EventsPage />);
    expect(screen.getByText(/events projection unavailable/)).toBeInTheDocument();

    // With a buffer, a transient poll failure degrades silently (stale tail
    // beats an error wall).
    setTail([ev('TaskStarted')], { error: new Error('boom') });
    render(<EventsPage />);
    expect(screen.getAllByTestId('event-row').length).toBeGreaterThan(0);
  });

  it('renders the empty state before any events exist', () => {
    setTail([]);
    render(<EventsPage />);
    expect(screen.getByText('No events yet')).toBeInTheDocument();
  });
});
