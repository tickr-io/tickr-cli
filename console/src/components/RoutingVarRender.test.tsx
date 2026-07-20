import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TaskGraphTab } from './TaskGraphTab';
import { DefinitionTab } from './DefinitionTab';
import { routingVarNames } from '@/lib/routingVars';
import fixture from '@/test/fixtures/routing-var-workflow.json';
import type { WorkflowDetail } from '@/api/client';

// Canonical API output for an object-shaped
// (`routing_vars: [{ name, var_type }]`), non-loop, predicate-gated workflow.
// This guards against treating declaration objects as renderable strings.
const definition = fixture as Record<string, unknown>;

function detailFromFixture(): WorkflowDetail {
  return {
    workflow_id: 'wf',
    namespace: (definition.namespace as string) ?? 'default',
    slug: (definition.slug as string) ?? 'demo',
    version: (definition.version as number) ?? 1,
    nickel_source: 'let utils = import "lib.ncl" in utils.mkWorkflow { name = "demo" }',
    workflow_definition: definition,
    available_versions: [{ version: 1, status: 'Submitted', inserted_at: '2026-01-01T00:00:00Z' }],
    latest_run_state: null,
    completed_runs: 0,
  } as WorkflowDetail;
}

// A loop workflow carries the identical object-shaped routing-var decls (the
// crash is loop-independent); this inline definition exercises the loop
// topology (a `kind = "loop"` back-edge) through the same shared projection.
const loopDefinition: Record<string, unknown> = {
  namespace: 'default',
  slug: 'loopy',
  name: 'loopy',
  status: 'Active',
  version: 1,
  trigger: 'FireNow',
  tags: {},
  captures: [],
  timeout_secs: null,
  tasks: {
    a: {
      id: 'a',
      name: 'loop-head',
      nix_expression_path: 'pkgs#a',
      task_type: 'RegularTask',
      routing_vars: [{ name: 'turns', var_type: 'int' }],
      inputs: [],
      outputs: [],
      secrets: [],
      emits: [],
    },
    b: {
      id: 'b',
      name: 'loop-body',
      nix_expression_path: 'pkgs#b',
      task_type: 'RegularTask',
      routing_vars: [],
      inputs: [],
      outputs: [],
      secrets: [],
      emits: [],
    },
  },
  task_graph: {
    edges: {
      fwd: { id: 'fwd', sources: ['a'], targets: ['b'], kind: 'Data', gates: [] },
      back: {
        id: 'back',
        sources: ['b'],
        targets: ['a'],
        kind: 'Loop',
        gates: [{ PredicateHolds: { routing_var: 'turns', op: 'Lt', value: { Int: 3 } } }],
      },
    },
  },
};

describe('routing-var workflows render through the shared projection', () => {
  it('the shared projection reads the fixture decls by name', () => {
    const tasks = definition.tasks as Record<string, { routing_vars?: unknown }>;
    const names = Object.values(tasks).flatMap((t) => routingVarNames(t.routing_vars));
    expect(names).toContain('region');
  });

  it('Task graph tab renders the non-loop fixture and shows the routing var by name', () => {
    render(<TaskGraphTab definition={definition} />);
    expect(screen.getByText('region')).toBeInTheDocument();
  });

  it('Definition tab renders the non-loop fixture and shows the routing var by name', () => {
    render(<DefinitionTab detail={detailFromFixture()} />);
    expect(screen.getByText('region')).toBeInTheDocument();
  });

  it('Task graph tab renders a loop workflow and shows its routing var by name', () => {
    render(<TaskGraphTab definition={loopDefinition} />);
    expect(screen.getByText('turns')).toBeInTheDocument();
  });

  it('Definition tab renders a loop workflow and shows its routing var by name', () => {
    const loopDetail = { ...detailFromFixture(), workflow_definition: loopDefinition };
    render(<DefinitionTab detail={loopDetail} />);
    expect(screen.getByText('turns')).toBeInTheDocument();
  });
});
