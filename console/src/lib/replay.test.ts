import { describe, it, expect } from 'vitest';
import { replaySummary, canResumeFromTask, doomedLabels } from './replay';
import type { InstanceSnapshot, TriggerProvenanceView } from '@/api/client';

function snapshotWith(nodes: { id: string; code: string; ground: string }[]): InstanceSnapshot {
  return {
    graph: {
      start: 'n-start',
      end: 'n-end',
      nodes: nodes.map((n) => ({ ...n, kind: 'task', grounded_at: null })),
      edges: [],
    },
  } as unknown as InstanceSnapshot;
}

describe('replaySummary', () => {
  it('returns null for a non-replay provenance', () => {
    expect(replaySummary({ kind: 'Cron' } as TriggerProvenanceView)).toBeNull();
    expect(replaySummary(null)).toBeNull();
    expect(replaySummary(undefined)).toBeNull();
  });

  it('renders "from ⟨code⟩" for a singleton resume', () => {
    const p: TriggerProvenanceView = {
      kind: 'Replay',
      source_instance: { id: 'src-uuid', code: 'AB12' },
      resume_from: [{ id: 'node-uuid', code: 'CD34' }],
    };
    const s = replaySummary(p)!;
    expect(s.sourceId).toBe('src-uuid');
    expect(s.sourceCode).toBe('AB12');
    expect(s.suffix).toBe('from CD34');
  });

  it('renders "from N HyperNodes" for a multi-node resume', () => {
    const p: TriggerProvenanceView = {
      kind: 'Replay',
      source_instance: { id: 'src', code: 'AB12' },
      resume_from: [
        { id: 'n1', code: 'CD34' },
        { id: 'n2', code: 'EF56' },
        { id: 'n3', code: 'GH78' },
      ],
    };
    expect(replaySummary(p)!.suffix).toBe('from 3 HyperNodes');
  });
});

describe('canResumeFromTask', () => {
  const snap = snapshotWith([
    { id: 'failed-node', code: 'FA1L', ground: 'failed' },
    { id: 'cancelled-node', code: 'CA2C', ground: 'cancelled' },
    { id: 'success-node', code: 'SU3C', ground: 'success' },
    { id: 'pending-node', code: 'PE4N', ground: 'pending' },
  ]);

  it('enables resume only for a Grounded(Failed) HyperNode', () => {
    expect(canResumeFromTask(snap, 'failed-node')).toBe(true);
  });

  it('excludes a cascade-cancelled HyperNode', () => {
    expect(canResumeFromTask(snap, 'cancelled-node')).toBe(false);
  });

  it('excludes success and pending nodes', () => {
    expect(canResumeFromTask(snap, 'success-node')).toBe(false);
    expect(canResumeFromTask(snap, 'pending-node')).toBe(false);
  });

  it('returns false for a node absent from the graph', () => {
    expect(canResumeFromTask(snap, 'missing')).toBe(false);
  });
});

describe('doomedLabels', () => {
  it('maps doomed ids to their identity codes, falling back to a short id', () => {
    const snap = snapshotWith([{ id: 'blocked-node', code: 'BL0K', ground: 'pending' }]);
    expect(doomedLabels(snap, ['blocked-node', 'abcdef0123456789'])).toEqual([
      'BL0K',
      'abcdef01',
    ]);
  });
});
