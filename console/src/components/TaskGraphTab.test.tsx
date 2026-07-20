import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { TaskGraphTab } from './TaskGraphTab';

// Plain 1→1, a multi-source hyperedge, a signal-gated edge, and a predicate
// gate reading routing var `rvar` produced by task A. Guards the renderer +
// dagre integration; per-case correctness lives in the model tests. Gate
// payloads use the REAL wire variants (SignalReceived / PredicateHolds).
const definition = {
  tasks: {
    A: { name: 'a', nix_expression_path: 'p#a', routing_vars: ['rvar'] },
    B: { name: 'b', nix_expression_path: 'p#b' },
    C: { name: 'c', nix_expression_path: 'p#c' },
    D: { name: 'd', nix_expression_path: 'p#d' },
  },
  task_graph: {
    edges: {
      e1: { id: 'e1', sources: ['A'], targets: ['B'], gates: [] },
      e2: { id: 'e2', sources: ['B', 'C'], targets: ['D'], gates: [] },
      e3: {
        id: 'e3',
        sources: ['A'],
        targets: ['C'],
        gates: [{ SignalReceived: { signal_name: 's', state: 'Idle', transitions: [] } }],
      },
      e4: {
        id: 'e4',
        sources: ['C'],
        targets: ['D'],
        gates: [
          {
            PredicateHolds: {
              routing_var: 'rvar',
              op: 'Eq',
              value: { String: 'go' },
              state: 'Idle',
              transitions: [],
            },
          },
        ],
      },
    },
  },
};

describe('TaskGraphTab', () => {
  it('renders task nodes, a hyperedge junction, gate chips, and a routing-var chip', () => {
    const { container } = render(<TaskGraphTab definition={definition} />);
    expect(container.querySelectorAll('[data-task]')).toHaveLength(4);
    expect(container.querySelector('[data-junction="e2"]')).toBeTruthy();
    expect(container.querySelector('[data-gate-kind="signal"]')).toBeTruthy();
    expect(container.querySelector('[data-gate-kind="predicate"]')).toBeTruthy();
    expect(screen.getByText('rvar')).toBeInTheDocument(); // routing-var chip
    expect(screen.getByText('p#a')).toBeInTheDocument(); // nix subline
  });

  it('highlights the predicate gate reading a routing var when its producer is selected', () => {
    const { container } = render(<TaskGraphTab definition={definition} />);
    fireEvent.click(container.querySelector('[data-task="A"]')!);
    // e4's predicate reads A's routing var → its edge stays lit; an unrelated
    // hyperedge segment (e2, not touching A) dims.
    const e4 = container.querySelector('[data-edge="e4"]')!;
    const e2seg = container.querySelector('[data-edge="e2:j->D"]')!;
    expect(e4.getAttribute('stroke-opacity')).toBe('1');
    expect(e2seg.getAttribute('stroke-opacity')).toBe('0.3');
  });

  it('shows an empty state when there are no tasks', () => {
    render(<TaskGraphTab definition={{}} />);
    expect(screen.getByText('No task graph to display.')).toBeInTheDocument();
  });

  // The fold paths were previously never executed under any test — a broken
  // fold was indistinguishable from a working one (the ring-fold audit's core
  // finding). These two tests render each fold and assert its output reaches
  // the DOM.

  it('folds a ≥5 serial spine into a serpentine whose connector paths reach the DOM', () => {
    const ids = ['T1', 'T2', 'T3', 'T4', 'T5', 'T6'];
    const chain = {
      tasks: Object.fromEntries(ids.map((id) => [id, { name: id, nix_expression_path: `p#${id}` }])),
      task_graph: {
        edges: Object.fromEntries(
          ids.slice(0, -1).map((id, i) => [
            `e${i}`,
            { id: `e${i}`, sources: [id], targets: [ids[i + 1]], gates: [] },
          ]),
        ),
      },
    };
    const { container } = render(<TaskGraphTab definition={chain} />);
    const serp = [...container.querySelectorAll('path[data-serp-kind]')];
    expect(serp).toHaveLength(5); // every intra-chain connector drawn by the fold
    const kinds = serp.map((p) => p.getAttribute('data-serp-kind'));
    expect(kinds).toContain('turn-r'); // the spine actually wrapped
    expect(kinds).toContain('h-rl'); // and drew a right-to-left row
  });

  it('draws a mkLoop ring as a literal circle: dashed ring, chevrons, members on the rim', () => {
    const loopDef = {
      tasks: {
        judge: { name: 'judge', nix_expression_path: 'p#judge' },
        grilly: { name: 'grilly', nix_expression_path: 'p#grilly' },
        griller: { name: 'griller', nix_expression_path: 'p#griller' },
      },
      task_graph: {
        edges: {
          l0: { id: 'l0', sources: ['judge'], targets: ['grilly'], gates: [], kind: 'Loop' },
          l1: { id: 'l1', sources: ['grilly'], targets: ['griller'], gates: [], kind: 'Loop' },
          l2: { id: 'l2', sources: ['griller'], targets: ['judge'], gates: [], kind: 'Loop' },
        },
      },
    };
    const { container } = render(<TaskGraphTab definition={loopDef} />);
    // The dashed circle IS the loop lane; the steps draw no separate paths and
    // there is no over-the-top arc.
    const ring = container.querySelectorAll('[data-loop-ring]');
    expect(ring).toHaveLength(1);
    expect(ring[0].querySelector('circle')).toBeTruthy();
    expect(container.querySelectorAll('path[data-loop-dir]')).toHaveLength(3);
    expect(container.querySelectorAll('path[data-loop="true"]')).toHaveLength(0);
    expect(container.querySelectorAll('path[data-loop-arc]')).toHaveLength(0);
    // The loop glyph holds the center.
    expect(container.querySelector('[data-ring-center]')).toBeTruthy();
    // Members sit ON the circle, equidistant from its center.
    const circle = ring[0].querySelector('circle')!;
    const cx0 = parseFloat(circle.getAttribute('cx')!);
    const cy0 = parseFloat(circle.getAttribute('cy')!);
    const r = parseFloat(circle.getAttribute('r')!);
    for (const id of ['judge', 'grilly', 'griller']) {
      const el = container.querySelector(`[data-task="${id}"]`) as HTMLElement;
      const d = Math.hypot(parseFloat(el.style.left) - cx0, parseFloat(el.style.top) - cy0);
      expect(d).toBeCloseTo(r, 5);
    }
  });
});
