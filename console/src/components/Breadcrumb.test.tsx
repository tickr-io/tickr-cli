import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { Breadcrumb, buildBreadcrumbs } from './Breadcrumb';

function renderAt(
  path: string,
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } }),
) {
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>{children}</MemoryRouter>
    </QueryClientProvider>
  );
  return render(<Breadcrumb />, { wrapper });
}

describe('buildBreadcrumbs', () => {
  it('returns nothing for flat routes', () => {
    expect(buildBreadcrumbs('/')).toEqual([]);
    expect(buildBreadcrumbs('/settings')).toEqual([]);
    expect(buildBreadcrumbs('/events')).toEqual([]);
  });

  it('roots the bare workflows list at Tickr › Workflows', () => {
    expect(buildBreadcrumbs('/workflows')).toEqual([
      { label: 'Tickr', href: '/' },
      { label: 'Workflows', href: '/workflows' },
    ]);
  });

  it('builds the trail for a workflow detail route', () => {
    expect(buildBreadcrumbs('/workflows/wf1')).toEqual([
      { label: 'Tickr', href: '/' },
      { label: 'Workflows', href: '/workflows' },
      { label: 'wf1', href: '/workflows/wf1' },
    ]);
  });

  it('builds the full drill chain down to a task', () => {
    expect(buildBreadcrumbs('/workflows/wf1/instances/r9/tasks/t3')).toEqual([
      { label: 'Tickr', href: '/' },
      { label: 'Workflows', href: '/workflows' },
      { label: 'wf1', href: '/workflows/wf1' },
      { label: 'Instance r9', href: '/workflows/wf1/instances/r9' },
      { label: 'Task t3', href: '/workflows/wf1/instances/r9/tasks/t3' },
    ]);
  });
});

describe('Breadcrumb', () => {
  it('renders clickable ancestor links with correct hrefs and a non-link tail', () => {
    renderAt('/workflows/wf1/instances/r9');
    expect(screen.getByRole('link', { name: 'Workflows' })).toHaveAttribute('href', '/workflows');
    expect(screen.getByRole('link', { name: 'wf1' })).toHaveAttribute('href', '/workflows/wf1');
    // The current (last) segment is not a link.
    expect(screen.queryByRole('link', { name: 'Instance r9' })).toBeNull();
    expect(screen.getByText('Instance r9')).toHaveAttribute('aria-current', 'page');
  });

  it('labels the instance segment with the Run handle once instance data is cached', () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    client.setQueryData(['workflowInstance', 'r9'], {
      id: 'r9',
      workflow_id: 'wf1',
      state: 'Completed',
      scheduled_at: '2026-06-12T09:30:05Z',
      task_count: 3,
      completed_tasks: 3,
    });
    renderAt('/workflows/wf1/instances/r9', client);
    const tail = screen.getByText(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{2}:\d{2}$/);
    expect(tail).toHaveAttribute('aria-current', 'page');
    expect(screen.queryByText('Instance r9')).toBeNull();
  });

  it('labels the workflow segment with the slug once detail data is cached', () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    });
    client.setQueryData(['workflowDetail', 'wf1', null], {
      workflow_id: 'wf1',
      namespace: 'default',
      slug: 'nightly-rollup',
      version: 1,
      nickel_source: '',
      workflow_definition: {},
      available_versions: [],
      completed_runs: 0,
    });
    renderAt('/workflows/wf1', client);
    expect(screen.getByText('nightly-rollup')).toHaveAttribute('aria-current', 'page');
    expect(screen.queryByText('wf1')).toBeNull();
  });

  it('renders nothing on a flat route', () => {
    const { container } = renderAt('/settings');
    expect(container).toBeEmptyDOMElement();
  });
});
