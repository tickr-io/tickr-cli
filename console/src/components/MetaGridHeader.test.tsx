import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MetaGridHeader } from './MetaGridHeader';
import type { WorkflowDetail, AvailableVersion } from '@/api/client';

function detail(over: Partial<WorkflowDetail> = {}): WorkflowDetail {
  const available_versions: AvailableVersion[] = over.available_versions ?? [
    { version: 2, status: 'Submitted', inserted_at: '2026-01-02T00:00:00Z' },
    { version: 1, status: 'Ready', inserted_at: '2026-01-01T00:00:00Z' },
  ];
  return {
    workflow_id: 'wf',
    namespace: 'default',
    slug: 'demo',
    version: 2,
    nickel_source: 'src',
    workflow_definition: { name: 'demo', trigger: { Cron: '0 9 * * *' } },
    available_versions,
    latest_run_state: 'Completed',
    completed_runs: 7,
    ...over,
  };
}

describe('MetaGridHeader', () => {
  it('renders the trigger, version, latest run, and completed-runs cells', () => {
    render(<MetaGridHeader detail={detail()} hasExplicitVersion={false} onVersionChange={vi.fn()} />);
    expect(screen.getByLabelText('cron schedule')).toBeInTheDocument();
    expect(screen.getByText('0 9 * * *')).toBeInTheDocument();
    expect(screen.getByText('v2')).toBeInTheDocument(); // version picker trigger
    expect(screen.getByText('Completed')).toBeInTheDocument(); // latest run badge
    expect(screen.getByText('7')).toBeInTheDocument();
  });

  it('default landing: Build reports the latest registration with a (vX) suffix when it diverges', () => {
    // Default version is 2.0.0 (latest live), but a newer 3.0.0 is mid-build.
    const d = detail({
      version: 2,
      available_versions: [
        { version: 3, status: 'Building', inserted_at: '2026-01-03T00:00:00Z' },
        { version: 2, status: 'Submitted', inserted_at: '2026-01-02T00:00:00Z' },
      ],
    });
    render(<MetaGridHeader detail={d} hasExplicitVersion={false} onVersionChange={vi.fn()} />);
    // Reports the latest registration (3.0.0), suffixed because it differs from
    // the Version cell (2.0.0).
    expect(screen.getByText('Building (v3)')).toBeInTheDocument();
  });

  it('explicit pick: Build reports the picked version with no suffix', () => {
    const d = detail({
      version: 2,
      available_versions: [
        { version: 3, status: 'Building', inserted_at: '2026-01-03T00:00:00Z' },
        { version: 2, status: 'Submitted', inserted_at: '2026-01-02T00:00:00Z' },
      ],
    });
    render(<MetaGridHeader detail={d} hasExplicitVersion={true} onVersionChange={vi.fn()} />);
    // Picked 2.0.0 is Submitted → folds to Ready; no parenthetical.
    expect(screen.getByText('Ready')).toBeInTheDocument();
    expect(screen.queryByText(/Building/)).not.toBeInTheDocument();
  });

  it('renders an em dash for a never-fired workflow', () => {
    render(
      <MetaGridHeader
        detail={detail({ latest_run_state: null })}
        hasExplicitVersion={false}
        onVersionChange={vi.fn()}
      />,
    );
    expect(screen.queryByText('Completed')).not.toBeInTheDocument();
  });
});
