import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import type { Workflow } from '@/api/client';
import { WorkflowsPage } from './WorkflowsPage';

vi.mock('@/api/hooks', () => ({ useWorkflows: vi.fn() }));
import { useWorkflows } from '@/api/hooks';

const mockUse = vi.mocked(useWorkflows);

function wf(name: string, over: Partial<Workflow> = {}): Workflow {
  return {
    id: `id-${name}`,
    namespace: 'default',
    slug: name,
    name,
    trigger: 'FireNow',
    version: 1,
    build_status: 'Ready',
    build_version: 1,
    latest_run_state: 'Completed',
    completed_runs: 3,
    ...over,
  };
}

function setData(data: Workflow[] | undefined, isLoading = false) {
  mockUse.mockReturnValue({
    data,
    isLoading,
    error: null,
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useWorkflows>);
}

function renderPage() {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <Routes>
        <Route path="/" element={<WorkflowsPage />} />
        <Route path="/workflows/:id" element={<div>detail-page</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

describe('WorkflowsPage', () => {
  beforeEach(() => mockUse.mockReset());

  it('renders the six DC-0014 columns and neither Status nor Instances', () => {
    setData([wf('alpha')]);
    renderPage();
    const headers = screen.getAllByRole('columnheader').map((h) => h.textContent);
    expect(headers).toEqual(['Name', 'Trigger', 'Version', 'Build', 'Latest run', 'Completed runs']);
    expect(headers).not.toContain('Status');
    expect(headers).not.toContain('Instances');
    // No per-row "View instances" link competing for the click target.
    expect(screen.queryByText(/View instances/)).not.toBeInTheDocument();
  });

  it('sorts rows alphabetically by name regardless of payload order', () => {
    setData([wf('charlie'), wf('alpha'), wf('bravo')]);
    renderPage();
    const rows = screen.getAllByRole('row').slice(1); // drop header row
    const names = rows.map((r) => within(r).getAllByRole('cell')[0].textContent);
    expect(names).toEqual([
      'alphadefault.alphaid-alpha',
      'bravodefault.bravoid-bravo',
      'charliedefault.charlieid-charlie',
    ]);
  });

  it('navigates to the workflow detail route when a row is clicked', () => {
    setData([wf('alpha')]);
    renderPage();
    fireEvent.click(screen.getByText('alpha'));
    expect(screen.getByText('detail-page')).toBeInTheDocument();
  });

  it('filters client-side on the name+id substring', () => {
    setData([wf('alpha'), wf('beta')]);
    renderPage();
    fireEvent.change(screen.getByPlaceholderText('Search workflows…'), {
      target: { value: 'bet' },
    });
    expect(screen.getByText('beta')).toBeInTheDocument();
    expect(screen.queryByText('alpha')).not.toBeInTheDocument();
  });

  it('shows the unfiltered empty state when no workflows exist', () => {
    setData([]);
    renderPage();
    expect(screen.getByText('No workflows registered')).toBeInTheDocument();
  });

  it('shows the filtered empty state when a search matches nothing', () => {
    setData([wf('alpha')]);
    renderPage();
    fireEvent.change(screen.getByPlaceholderText('Search workflows…'), {
      target: { value: 'zzz' },
    });
    expect(screen.getByText('No workflows match')).toBeInTheDocument();
  });
});
