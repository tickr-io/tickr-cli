/**
 * Pure helpers for the replay operator surface — the subtitle a replay run
 * renders, whether a failed task can be resumed from, and how doomed HyperNodes
 * are labelled. Kept free of React so they unit-test in isolation.
 */

import type { InstanceSnapshot, TriggerProvenanceView } from '@/api/client';

export interface ReplaySummary {
  /** The source run this replay resumes from — its full id (link target). */
  sourceId: string;
  /** The source run's short identity code, for display. */
  sourceCode: string;
  /** The resume-from suffix: `from ⟨code⟩` for a singleton pick, else
   * `from N HyperNodes`. */
  suffix: string;
}

/**
 * The replay subtitle parts for a Replay-provenance run, or `null` when the run
 * is not a replay. A singleton resume renders "replay of X from ⟨code⟩"; more
 * than one renders "from N HyperNodes" — the source run `X` links back to its
 * own detail page in both.
 */
export function replaySummary(p: TriggerProvenanceView | null | undefined): ReplaySummary | null {
  if (!p || p.kind !== 'Replay' || !p.source_instance) return null;
  const resume = p.resume_from ?? [];
  const suffix =
    resume.length === 1 ? `from ${resume[0].code}` : `from ${resume.length} HyperNodes`;
  return {
    sourceId: p.source_instance.id,
    sourceCode: p.source_instance.code,
    suffix,
  };
}

/**
 * Whether a replay can resume from the given task's HyperNode. Enabled iff that
 * node is `Grounded(Failed)` — the genuine failure the operator is standing on.
 * A cascade-`Cancelled` HyperNode (a sibling the failure cascade grounded) is
 * excluded: it did not fail, so resuming from it is meaningless. Returns `false`
 * when the node is absent from the graph (a stale link).
 */
export function canResumeFromTask(snapshot: InstanceSnapshot, taskNodeId: string): boolean {
  const node = snapshot.graph.nodes.find((n) => n.id === taskNodeId);
  return node?.ground === 'failed';
}

/**
 * Label a doomed HyperNode for the "these HyperNodes stay blocked" confirmation:
 * its short identity code when the node is in the snapshot's graph, else the
 * bare id as a fallback. A doomed node is an interior join a sibling failure's
 * dead subtree leaves permanently unfireable.
 */
export function doomedLabels(snapshot: InstanceSnapshot, doomed: string[]): string[] {
  const codeById = new Map(snapshot.graph.nodes.map((n) => [n.id, n.code]));
  return doomed.map((id) => codeById.get(id) ?? id.slice(0, 8));
}
