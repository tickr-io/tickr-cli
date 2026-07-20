import { describe, it, expect } from 'vitest';
import { render, fireEvent } from '@testing-library/react';
import { MemoryRouter, useLocation } from 'react-router-dom';
import { InstanceGraphTab } from './InstanceGraphTab';
import type { InstanceSnapshot } from '@/api/client';

// The tab navigates on node click, so every render needs a router context.
const renderTab = (snapshot: InstanceSnapshot) =>
  render(<InstanceGraphTab snapshot={snapshot} />, { wrapper: MemoryRouter });

// Surfaces the live router path so a click's navigation is observable.
function LocationProbe() {
  return <div data-testid="loc">{useLocation().pathname}</div>;
}

function mkDef(id: string, name: string) {
  return {
    id,
    name,
    task_type: 'regular',
    max_attempts: 1,
    timeout_secs: null,
    nix_expression_path: `/nix#${name}`,
    inputs: [],
    outputs: [],
    secrets: [],
    routing_vars: [],
    emits: [],
  };
}

function mkTi(id: string, taskId: string, state: string) {
  return {
    id,
    task_id: taskId,
    name: taskId,
    task_type: 'regular',
    state,
    executor_id: null,
    attempt: 0,
    started_at: '2026-06-12T10:00:00Z',
    completed_at: null,
    transitions: [],
  };
}

function mkPredicateGate(routingVar: string) {
  return {
    kind: 'predicate',
    state: 'Idle',
    signal_id: null,
    signal_name: null,
    predicate: `${routingVar} == 1`,
    captures: [],
    routing_var: routingVar,
    op: '==',
    value: { kind: 'int', value: 1 },
    timeout_secs: null,
    duration_secs: null,
    transitions: [],
  };
}

function mkSignalGate(state: string) {
  return {
    kind: 'signal',
    state,
    signal_id: 'sig',
    signal_name: 'approval',
    predicate: null,
    captures: [],
    routing_var: null,
    op: null,
    value: null,
    timeout_secs: null,
    duration_secs: null,
    transitions: [],
  };
}

/**
 * A snapshot spanning the live affordances under test: tasks in distinct states
 * (completed / running / failed / scheduled / never-minted), a Dispatched gate,
 * and a frontier edge (completed → never-minted).
 */
function fixture(): InstanceSnapshot {
  const ids = ['A', 'B', 'C', 'D', 'E'];
  return {
    id: 'wi',
    workflow_id: 'wf',
    name: 'live',
    graph: {
      start: 'n-start',
      end: 'n-end',
      nodes: ids.map((id) => ({ id, kind: 'task', ground: 'pending', grounded_at: null })),
      edges: [
        { id: 'ae', sources: ['A'], targets: ['E'], kind: 'data', gates: [] }, // frontier
        { id: 'ab', sources: ['A'], targets: ['B'], kind: 'data', gates: [] },
        { id: 'bc', sources: ['B'], targets: ['C'], kind: 'data', gates: [mkSignalGate('Dispatched')] },
        { id: 'cd', sources: ['C'], targets: ['D'], kind: 'data', gates: [] },
      ],
    },
    tasks: ids.map((id) => mkDef(id, id)),
    task_instances: [
      mkTi('ti-a', 'A', 'Completed'),
      mkTi('ti-b', 'B', 'Running'),
      mkTi('ti-c', 'C', 'Failed'),
      mkTi('ti-d', 'D', 'Scheduled'),
      // E never minted — no instance.
    ],
    routing_variables: {},
  } as unknown as InstanceSnapshot;
}

const dotBg = (root: HTMLElement, taskId: string) =>
  (root.querySelector(`[data-task="${taskId}"] .hg-dot`) as HTMLElement).style.background;
const legendBg = (root: HTMLElement, state: string) =>
  (root.querySelector(`[data-legend-state="${state}"]`) as HTMLElement).style.background;

describe('InstanceGraphTab', () => {
  it('renders graph nodes and legend swatches with consistent hues per state', () => {
    const { container } = renderTab(fixture());
    // For each sampled state, the node dot and the legend swatch read the same
    // hue — they derive from the one shared state→token table.
    for (const [taskId, state] of [
      ['A', 'completed'],
      ['B', 'in_progress'],
      ['C', 'failed'],
      ['D', 'scheduled'],
    ] as const) {
      expect(dotBg(container, taskId)).toBe(legendBg(container, state));
      expect(dotBg(container, taskId)).not.toBe('');
    }
  });

  it('renders a never-minted task neutral, distinct from any active state', () => {
    const { container } = renderTab(fixture());
    const e = container.querySelector('[data-task="E"]')!;
    expect(e.getAttribute('data-state')).toBe('never-minted');
    const neutral = dotBg(container, 'E');
    expect(neutral).toBe(legendBg(container, 'never-minted'));
    // distinct from the active (coloured) states.
    expect(neutral).not.toBe(dotBg(container, 'A')); // completed
    expect(neutral).not.toBe(dotBg(container, 'B')); // running
    expect(neutral).not.toBe(dotBg(container, 'C')); // failed
  });

  // A ghost node: grounded `success` with a grounded_at stamp but no task
  // instance ever minted (server sets `ghost: true`). Rendered in its
  // ground-kind hue, split out from the neutral never-reached node.
  function ghostFixture(): InstanceSnapshot {
    const ids = ['A', 'G', 'E'];
    return {
      id: 'wi',
      workflow_id: 'wf',
      name: 'ghost',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: [
          { id: 'A', kind: 'task', ground: 'success', grounded_at: '2026-06-12T10:00:00Z', ghost: false },
          { id: 'G', kind: 'task', ground: 'success', grounded_at: '2026-06-12T10:01:00Z', ghost: true },
          { id: 'E', kind: 'task', ground: 'pending', grounded_at: null, ghost: false },
        ],
        edges: [
          { id: 'ag', sources: ['A'], targets: ['G'], kind: 'data', gates: [] },
          { id: 'ge', sources: ['G'], targets: ['E'], kind: 'data', gates: [] },
        ],
      },
      tasks: ids.map((id) => mkDef(id, id)),
      // A ran (has an instance); G is a ghost (none); E never minted.
      task_instances: [mkTi('ti-a', 'A', 'Completed')],
      routing_variables: {},
    } as unknown as InstanceSnapshot;
  }

  it('renders a ghost node in its ground-kind hue, distinct from run and never-reached', () => {
    const { container } = renderTab(ghostFixture());
    const ghost = container.querySelector('[data-task="G"]')!;
    // The client can tell the reaped node from a pending-unreached one.
    expect(ghost.getAttribute('data-ghost')).toBe('true');
    expect(ghost.classList.contains('ghost')).toBe(true);
    // Same ground-kind (success) hue as the node that actually ran…
    expect(dotBg(container, 'G')).toBe(legendBg(container, 'completed'));
    // …but NOT the neutral never-minted hue.
    expect(dotBg(container, 'G')).not.toBe(legendBg(container, 'never-minted'));
    expect(dotBg(container, 'G')).not.toBe(dotBg(container, 'E'));
    // A run-completion and a never-reached node are not ghosts.
    expect(container.querySelector('[data-task="A"]')!.getAttribute('data-ghost')).toBeNull();
    expect(container.querySelector('[data-task="E"]')!.getAttribute('data-ghost')).toBeNull();
    expect(container.querySelector('[data-task="E"]')!.getAttribute('data-state')).toBe('never-minted');
  });

  it('gives a running task a full-bar shimmer', () => {
    const { container } = renderTab(fixture());
    expect(container.querySelector('[data-task="B"]')!.classList.contains('running')).toBe(true);
    expect(container.querySelector('[data-task="A"]')!.classList.contains('running')).toBe(false);
  });

  it('marks a dispatched gate with the loud-pulse class', () => {
    const { container } = renderTab(fixture());
    const gate = container.querySelector('[data-gate-state="Dispatched"]')!;
    expect(gate.classList.contains('hg-gate-dispatched')).toBe(true);
  });

  it('statically brightens the frontier edge (completed → not-started)', () => {
    const { container } = renderTab(fixture());
    expect(container.querySelector('[data-edge="ae"]')!.getAttribute('data-frontier')).toBe('true');
    expect(container.querySelector('[data-edge="ab"]')!.getAttribute('data-frontier')).toBeNull();
  });

  // A producer task (A declares routing var `x`) feeding a downstream predicate
  // gate (on edge `cd`) that reads `x` — the shared producer↔gate adjacency the
  // selection highlight rides.
  function producerFixture(): InstanceSnapshot {
    const ids = ['A', 'B', 'C', 'D'];
    return {
      id: 'wi',
      workflow_id: 'wf',
      name: 'producer',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: ids.map((id) => ({ id, kind: 'task', ground: 'pending', grounded_at: null })),
        edges: [
          { id: 'ab', sources: ['A'], targets: ['B'], kind: 'data', gates: [] },
          { id: 'bc', sources: ['B'], targets: ['C'], kind: 'data', gates: [] },
          { id: 'cd', sources: ['C'], targets: ['D'], kind: 'data', gates: [mkPredicateGate('x')] },
        ],
      },
      tasks: ids.map((id) =>
        id === 'A'
          ? { ...mkDef('A', 'A'), routing_vars: [{ name: 'x', var_type: 'int' }] }
          : mkDef(id, id),
      ),
      task_instances: [],
      routing_variables: {},
    } as unknown as InstanceSnapshot;
  }

  it('previews the reading gate on hover of its producer task, and clears on mouse-leave', () => {
    const { container } = renderTab(producerFixture());
    const gateWrap = () => container.querySelector('[data-gate-edge="cd"]')!;
    const nodeA = container.querySelector('[data-task="A"]') as HTMLButtonElement;

    // Nothing hovered → no highlight on the predicate gate reading A's routing var.
    expect(gateWrap().getAttribute('data-active')).toBeNull();

    // Hovering the producer A highlights gate edge `cd` (which reads `x`).
    fireEvent.mouseEnter(nodeA);
    expect(nodeA.getAttribute('data-hovered')).toBe('true');
    expect(gateWrap().getAttribute('data-active')).toBe('true');
    expect(gateWrap().classList.contains('sel')).toBe(true);
    // An unrelated edge dims while the highlight is active.
    expect(container.querySelector('[data-edge="bc"]')!.getAttribute('data-active')).toBeNull();

    // Leaving the node restores the plain view — the highlight can't get stuck.
    fireEvent.mouseLeave(nodeA);
    expect(nodeA.getAttribute('data-hovered')).toBeNull();
    expect(gateWrap().getAttribute('data-active')).toBeNull();
  });

  it('opens the task instance detail page when a minted node is clicked', () => {
    const { container, getByTestId } = render(
      <MemoryRouter initialEntries={['/start']}>
        <InstanceGraphTab snapshot={fixture()} />
        <LocationProbe />
      </MemoryRouter>,
    );
    // A (ti-a) has an instance, so it is navigable.
    const nodeA = container.querySelector('[data-task="A"]') as HTMLButtonElement;
    expect(nodeA.classList.contains('hg-navigable')).toBe(true);
    fireEvent.click(nodeA);
    expect(getByTestId('loc').textContent).toBe('/workflows/wf/instances/wi/tasks/ti-a');
  });

  it('does not navigate from a never-minted node (nothing to open)', () => {
    const { container, getByTestId } = render(
      <MemoryRouter initialEntries={['/start']}>
        <InstanceGraphTab snapshot={fixture()} />
        <LocationProbe />
      </MemoryRouter>,
    );
    // E never minted — no instance, so it is not navigable and click is inert.
    const nodeE = container.querySelector('[data-task="E"]') as HTMLButtonElement;
    expect(nodeE.classList.contains('hg-navigable')).toBe(false);
    fireEvent.click(nodeE);
    expect(getByTestId('loc').textContent).toBe('/start');
  });

  it('shows a settled time-to-complete on a finished node, and no nix path anywhere', () => {
    const snap = fixture();
    // Give A a terminal time: 10:00:00 → 10:01:04 = 1m 4s.
    (snap.task_instances as unknown as Record<string, unknown>[]).find(
      (ti) => ti.id === 'ti-a',
    )!.completed_at = '2026-06-12T10:01:04Z';
    const { container } = renderTab(snap);

    // The completed node A carries its duration, not a live elapsed tick…
    const durA = container.querySelector('[data-task="A"] [data-duration]');
    expect(durA?.textContent).toBe('1m 4s');
    expect(container.querySelector('[data-task="A"] [data-elapsed]')).toBeNull();
    // …while the running node B still ticks elapsed, not a duration.
    expect(container.querySelector('[data-task="B"] [data-elapsed]')).not.toBeNull();
    expect(container.querySelector('[data-task="B"] [data-duration]')).toBeNull();
    // The `path:` nix-expression subline is gone from the graph entirely.
    expect(container.querySelector('.hg-nix')).toBeNull();
  });

  // A 2-task loop ring (L1⇄L2, every step kind loop) whose members have each
  // parked twice — two completed turns. A turn parks and re-queues the SAME
  // instance, so the turn record is the transitions into Parked.
  function loopFixture(): InstanceSnapshot {
    const parkTwice = [
      { from: 'Running', to: 'Parked', at: '2026-07-06T10:00:00Z' },
      { from: 'Parked', to: 'Queued', at: '2026-07-06T10:00:30Z' },
      { from: 'Running', to: 'Parked', at: '2026-07-06T10:01:00Z' },
      { from: 'Parked', to: 'Queued', at: '2026-07-06T10:01:30Z' },
    ];
    return {
      id: 'wi',
      workflow_id: 'wf',
      name: 'loop',
      graph: {
        start: 'n-start',
        end: 'n-end',
        nodes: [
          { id: 'L1', kind: 'task', ground: 'pending', grounded_at: null },
          { id: 'L2', kind: 'task', ground: 'pending', grounded_at: null },
        ],
        edges: [
          { id: 'fwd', sources: ['L1'], targets: ['L2'], kind: 'loop', gates: [] },
          { id: 'back', sources: ['L2'], targets: ['L1'], kind: 'loop', gates: [] },
        ],
      },
      tasks: [mkDef('L1', 'L1'), mkDef('L2', 'L2')],
      task_instances: [
        { ...mkTi('ti-l1', 'L1', 'Running'), transitions: parkTwice },
        { ...mkTi('ti-l2', 'L2', 'Queued'), transitions: parkTwice },
      ],
      routing_variables: {},
    } as unknown as InstanceSnapshot;
  }

  it('draws the loop as a circle with the turn counter in its center', () => {
    const { container } = renderTab(loopFixture());
    // The dashed circle IS the loop; the ring steps draw no separate paths and
    // there is no over-the-top arc.
    const ring = container.querySelectorAll('[data-loop-ring]');
    expect(ring).toHaveLength(1);
    expect(ring[0].querySelector('circle')).toBeTruthy();
    expect(container.querySelectorAll('path[data-loop-dir]')).toHaveLength(2);
    expect(container.querySelectorAll('path[data-loop="true"]')).toHaveLength(0);
    expect(container.querySelectorAll('path[data-arc-lane]')).toHaveLength(0);
    // The center glyph carries the Parked-derived turn count.
    const chip = container.querySelector('[data-ring-center]')!;
    expect(chip).toBeTruthy();
    expect(chip.getAttribute('data-loop-turns')).toBe('2');
    expect(chip.textContent).toContain('2');
  });
});
