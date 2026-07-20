import { describe, expect, it } from 'vitest';
import { buildHyperGraphModel } from '@/lib/hyperGraph';
import { normalizeWorkflowDefinition } from './workflowDefinition';

describe('normalizeWorkflowDefinition', () => {
  it('normalizes canonical protobuf arrays, numeric enums, and oneofs', () => {
    const normalized = normalizeWorkflowDefinition({
      status: 1,
      trigger: { kind: { FireNow: {} } },
      tasks: [
        {
          id: 'task-a',
          name: 'sensor',
          task_type: 1,
          emits: [{ emit: { OnFailure: { signal_name: 'failed' } } }],
          routing_vars: [{ name: 'decision', var_type: 'string' }],
        },
        { id: 'task-b', name: 'shadow', task_type: 2 },
      ],
      task_graph: {
        nodes: [],
        edges: [
          {
            id: 'edge-a',
            kind: 2,
            sources: ['task-a'],
            targets: ['task-b'],
            gates: [
              {
                kind: {
                  PredicateHolds: {
                    routing_var: 'decision',
                    op: 0,
                    value: { value: { StringValue: 'continue' } },
                  },
                },
              },
            ],
          },
        ],
      },
    });

    expect(normalized.status).toBe('Active');
    expect(normalized.trigger).toBe('FireNow');
    expect(normalized.tasks).toEqual({
      'task-a': expect.objectContaining({
        id: 'task-a',
        task_type: 'SensorTask',
        emits: [{ signal: 'failed', kind: 'on-failure' }],
      }),
      'task-b': expect.objectContaining({ id: 'task-b', task_type: 'ShadowTask' }),
    });
    expect(normalized.task_graph).toEqual(
      expect.objectContaining({
        edges: {
          'edge-a': expect.objectContaining({
            id: 'edge-a',
            kind: 'Loop',
            gates: [
              {
                PredicateHolds: expect.objectContaining({ routing_var: 'decision' }),
              },
            ],
          }),
        },
      }),
    );

    const model = buildHyperGraphModel(normalized);
    expect(model.render.tasks.map((task) => task.id)).toEqual(['task-a', 'task-b']);
    expect(model.render.edges).toEqual([
      expect.objectContaining({ id: 'edge-a', from: 'task-a', to: 'task-b', isLoop: true }),
    ]);
  });

  it('is idempotent for the retained map and named-enum representation', () => {
    const definition = {
      status: 'Inactive',
      trigger: { Cron: '0 * * * *' },
      tasks: { a: { id: 'a', name: 'task', task_type: 'RegularTask' } },
      task_graph: {
        edges: {
          e: { id: 'e', kind: 'Control', sources: ['a'], targets: ['a'], gates: [] },
        },
      },
    };

    expect(normalizeWorkflowDefinition(definition)).toEqual(definition);
  });
});
