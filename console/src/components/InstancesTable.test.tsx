import { describe, it, expect } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { InstancesTable } from './InstancesTable';
import type { WorkflowInstance } from '@/api/client';

function inst(over: Partial<WorkflowInstance>): WorkflowInstance {
  return {
    id: 'i1',
    workflow_id: 'w1',
    name: 'demo run',
    workflow_version: 0,
    state: 'Completed',
    scheduled_at: '2026-01-01T00:00:00Z',
    task_count: 3,
    completed_tasks: 3,
    ...over,
  };
}

function renderTable(instances: WorkflowInstance[], liveOnly = false) {
  return render(
    <MemoryRouter>
      <InstancesTable workflowId="w1" instances={instances} liveOnly={liveOnly} />
    </MemoryRouter>,
  );
}

function dataRows() {
  return screen.getAllByRole('row').slice(1); // drop the header row
}

describe('InstancesTable', () => {
  it('renders a visible Workflow-version cell + a Version column', () => {
    renderTable([inst({ id: 'aaaaaaaa1', workflow_version: 2 })]);
    expect(screen.getByText('v2')).toBeInTheDocument();
    expect(screen.getByRole('columnheader', { name: 'Version' })).toBeInTheDocument();
  });

  it('liveOnly scopes the table to non-terminal runs', () => {
    const data = [inst({ id: 'done', state: 'Completed' }), inst({ id: 'running', state: 'InProgress' })];
    const { unmount } = renderTable(data, false);
    expect(dataRows()).toHaveLength(2);
    unmount();
    renderTable(data, true);
    expect(dataRows()).toHaveLength(1);
    expect(screen.getByText(/running/)).toBeInTheDocument();
  });

  it('tags non-terminal rows as live', () => {
    renderTable([inst({ id: 'live1', state: 'InProgress' })]);
    expect(screen.getByText('live')).toBeInTheDocument();
  });

  it('sorts newest-first by scheduled_at', () => {
    renderTable([
      inst({ id: 'older', scheduled_at: '2026-01-01T00:00:00Z' }),
      inst({ id: 'newer', scheduled_at: '2026-02-01T00:00:00Z' }),
    ]);
    const rows = dataRows();
    expect(within(rows[0]).getByText(/newer/)).toBeInTheDocument();
    expect(within(rows[1]).getByText(/older/)).toBeInTheDocument();
  });
});
