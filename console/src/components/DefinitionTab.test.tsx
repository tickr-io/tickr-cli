import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { DefinitionTab } from './DefinitionTab';
import type { WorkflowDetail } from '@/api/client';

function detail(over: Partial<WorkflowDetail> = {}): WorkflowDetail {
  return {
    workflow_id: 'wf',
    namespace: 'default',
    slug: 'demo',
    version: 0,
    nickel_source: 'let utils = import "lib.ncl" in utils.mkWorkflow { name = "demo" }',
    workflow_definition: {
      name: 'demo',
      status: 'Inactive',
      timeout_secs: 120,
      tags: { team: 'core' },
      trigger: { Cron: '0 9 * * *' },
      tasks: {
        t1: {
          name: 'build',
          nix_expression_path: 'github:org/repo#build',
          task_type: 'RegularTask',
          max_attempts: 3,
          timeout_secs: null,
          inputs: ['digest'],
          outputs: ['artifact'],
          secrets: ['token'],
          emits: ['done'],
        },
      },
    },
    available_versions: [{ version: 0, status: 'Submitted', inserted_at: '2026-01-01T00:00:00Z' }],
    latest_run_state: null,
    completed_runs: 0,
    ...over,
  };
}

describe('DefinitionTab', () => {
  it('renders the trigger summary, meta block, tags, and a per-task card with chips', () => {
    render(<DefinitionTab detail={detail()} />);
    // Trigger cell (compact, in the meta grid): the cron expression, no verbose card.
    expect(screen.getByText('0 9 * * *')).toBeInTheDocument();
    // Meta block: status badge (lowercase), timeout, version.
    expect(screen.getByText('inactive')).toBeInTheDocument();
    expect(screen.getByText('120s')).toBeInTheDocument();
    expect(screen.getByText('v0')).toBeInTheDocument();
    // Tags render key + value as a chip.
    expect(screen.getByText('team')).toBeInTheDocument();
    expect(screen.getByText('core')).toBeInTheDocument();
    // Per-task card: type label, name, nix path, io-chips.
    expect(screen.getByText('Regular')).toBeInTheDocument();
    expect(screen.getByText('build')).toBeInTheDocument();
    expect(screen.getByText('github:org/repo#build')).toBeInTheDocument();
    expect(screen.getByText('digest')).toBeInTheDocument();
    expect(screen.getByText('token')).toBeInTheDocument();
    expect(screen.getByText('done')).toBeInTheDocument();
  });

  it('renders a ShadowTask visibly as its own kind, not the default Regular label', () => {
    const d = detail();
    const tasks = (d.workflow_definition as { tasks: Record<string, { task_type: string }> }).tasks;
    tasks.t1.task_type = 'ShadowTask';
    render(<DefinitionTab detail={d} />);
    expect(screen.getByText('Shadow')).toBeInTheDocument();
    expect(screen.queryByText('Regular')).toBeNull();
  });

  it('View Nickel source toggle reveals the highlighted persisted source', () => {
    render(<DefinitionTab detail={detail()} />);
    expect(document.querySelector('.ncl-src')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'View Nickel source' }));
    const src = document.querySelector('.ncl-src');
    expect(src).not.toBeNull();
    expect(src?.textContent).toContain('mkWorkflow');
    // highlight.js wrapped at least one Nix keyword (e.g. `let`/`import`/`in`).
    expect(src?.querySelector('.hljs-keyword')).not.toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Hide source' }));
    expect(document.querySelector('.ncl-src')).toBeNull();
  });
});
