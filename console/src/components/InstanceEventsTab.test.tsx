import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, act } from '@testing-library/react';
import type { Event } from '@/api/client';
import { InstanceEventsTab } from './InstanceEventsTab';

// Mock only the transport: keep the real `unwrap` and the real tail/poll
// engine, so this exercises the actual incremental-poll behavior end-to-end.
vi.mock('@/api/client', async () => {
  const actual = await vi.importActual<typeof import('@/api/client')>('@/api/client');
  return { ...actual, api: { GET: vi.fn() } };
});
import { api } from '@/api/client';

const mockGet = vi.mocked(api.GET as unknown as (...args: unknown[]) => Promise<unknown>);

function ev(seq: number, event_type: string): Event {
  return {
    seq,
    id: `00000000-0000-0000-0000-${String(seq).padStart(12, '0')}`,
    ts: new Date(Date.now() - 5_000).toISOString(),
    event_type,
    payload: { [event_type]: {} },
  };
}

function ok(events: Event[]) {
  return { data: events, error: undefined, response: { ok: true, status: 200, statusText: 'OK' } };
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('InstanceEventsTab', () => {
  it('renders the instance events and appends only the new seq rows on the 5s poll', async () => {
    // First load (no cursor) returns the newest-first batch; the poll (after
    // the highest seq seen) returns only a strictly-newer row.
    mockGet.mockImplementation((_path: unknown, opts: unknown) => {
      const after = (opts as { params?: { query?: { after?: number } } })?.params?.query?.after;
      if (after === undefined) {
        return Promise.resolve(ok([ev(3, 'WorkflowCompleted'), ev(2, 'TaskQueued'), ev(1, 'TaskQueued')]));
      }
      if (after === 3) return Promise.resolve(ok([ev(4, 'WorkflowFailed')]));
      return Promise.resolve(ok([])); // no further activity
    });

    vi.useFakeTimers({ shouldAdvanceTime: true });
    render(<InstanceEventsTab instanceId="11111111-1111-1111-1111-111111111111" active />);

    // First load lands three rows.
    await waitFor(() => expect(screen.getAllByTestId('event-row')).toHaveLength(3));
    expect(screen.getByText('WorkflowCompleted')).toBeInTheDocument();

    // Advance past the 5s poll interval; the one new row is appended (now four,
    // no duplicates) and the newer event type is present.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    await waitFor(() => expect(screen.getAllByTestId('event-row')).toHaveLength(4));
    expect(screen.getByText('WorkflowFailed')).toBeInTheDocument();

    // The first call had no cursor; the poll carried the highest seq seen.
    const firstCall = mockGet.mock.calls[0][1] as { params?: { query?: Record<string, unknown> } };
    expect(firstCall.params?.query).toEqual({});
    const pollCall = mockGet.mock.calls.find(
      (c) => (c[1] as { params?: { query?: { after?: number } } })?.params?.query?.after === 3,
    );
    expect(pollCall).toBeTruthy();
  });

  it('shows the empty state when the instance has no events', async () => {
    mockGet.mockResolvedValue(ok([]));
    render(<InstanceEventsTab instanceId="11111111-1111-1111-1111-111111111111" active />);
    await waitFor(() => expect(screen.getByText('No events yet')).toBeInTheDocument());
  });
});
