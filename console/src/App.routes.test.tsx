import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import App from './App';
import { ThemeProvider } from './contexts/ThemeContext';

// Data-fetching pages hit the network on mount; in jsdom we don't want a real
// request, just a deterministic failure so the page still renders its heading.
beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('no network in test'))));
});

function renderRoute(path: string) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <MemoryRouter initialEntries={[path]}>
          <App />
        </MemoryRouter>
      </ThemeProvider>
    </QueryClientProvider>,
  );
}

describe('route smoke tests', () => {
  const ROUTES: Array<{ path: string; heading: string }> = [
    { path: '/workflows', heading: 'Workflows' },
    { path: '/workflows/wf1', heading: 'Workflow' },
    // The instance page's heading is the Run handle (a timestamp); with no
    // data in test it renders the em-dash placeholder.
    { path: '/workflows/wf1/instances/i1', heading: '—' },
    { path: '/workflows/wf1/instances/i1/tasks/t1', heading: 'Task' },
    { path: '/events', heading: 'Event log' },
    { path: '/health', heading: 'Health' },
    { path: '/settings', heading: 'Settings' },
  ];

  it.each(ROUTES)('renders $path without crashing', ({ path, heading }) => {
    renderRoute(path);
    expect(screen.getByRole('heading', { name: heading, level: 1 })).toBeInTheDocument();
  });

  it('renders the dashboard at / with the Up next strip', () => {
    // The dashboard dropped its page-title heading; the DashHeader "Up next"
    // strip is the page's head and renders regardless of fetch outcome.
    renderRoute('/');
    expect(screen.getByText('Up next')).toBeInTheDocument();
  });

  it.each(['/runs', '/runs/r1', '/workflows/wf1/runs/i1'])(
    'retires the old %s route to NotFound',
    (path) => {
      renderRoute(path);
      expect(screen.getByRole('heading', { name: '404' })).toBeInTheDocument();
    },
  );

  it('serves the real Event log on /events — no placeholder remains', () => {
    renderRoute('/events');
    // The live-tail chrome renders even when the first poll fails (fetch is
    // stubbed to reject above); only the stream body shows the error state.
    expect(screen.getByText(/polling \/api\/events every 5s/)).toBeInTheDocument();
    expect(screen.queryByText('GET /api/events?after=<cursor>')).not.toBeInTheDocument();
  });
});
