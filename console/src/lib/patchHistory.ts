/**
 * Patch-history model — the pure, snapshot-driven backbone of the evolution
 * view. It turns an instance snapshot's applied-patch records and stored
 * version snapshots into (a) an ordered, labelled list of Instance versions the
 * operator navigates, and (b) the graph + delta to render for a selected
 * version. All derivation is off data the snapshot already carries; nothing is
 * replayed.
 *
 * The delta is read from each applied-patch record's *lowered ops* (what the
 * patch actually did to the live graph): AddNode/AddEdge are the additions,
 * RemoveNode/RemoveEdge the removals. Removed structures are ghosted using the
 * prior version's stored snapshot for their geometry — so ghosting is available
 * for every version whose predecessor was retained. Version 0 (the pristine
 * spawn shape) is produced by no apply and is not retained as a snapshot, so the
 * first patch's removals cannot be positioned; its additions still light up.
 */

import type {
  InstanceSnapshot,
  SnapshotGraph,
  AppliedPatchView,
  PatchOpView,
} from '@/api/client';

/** One navigable Instance version in reading order (oldest first). */
export interface VersionEntry {
  version: number;
  /** The version in effect before this one; null for the pristine baseline. */
  priorVersion: number | null;
  /** The pristine v0 baseline — produced by no patch. */
  pristine: boolean;
  /** Joins to the authored source and the code-tab section. */
  patchKey?: string;
  /** Reading label for the operation, e.g. `insert "commit-doc"` or `2 ops`. */
  operation: string;
  /** Patch authorship — `self` or `external` (distinct from trigger provenance). */
  provenance?: string;
  reason?: string | null;
  appliedAt?: string;
}

/** A ghostable removed edge: both endpoints still exist in the rendered version,
 * so it can be drawn as a faint over-the-top arc between current node positions. */
export interface GhostEdge {
  id: string;
  from: string;
  to: string;
}

/** The change that produced a version, projected onto that version's graph. */
export interface VersionDelta {
  /** Node ids the patch added (present in the rendered version → glow). */
  addedNodes: Set<string>;
  /** Edge ids the patch added (present in the rendered version → glow). */
  addedEdges: Set<string>;
  /** Removed edges with recoverable geometry, drawn ghosted. */
  ghostEdges: GhostEdge[];
  /** Removed node ids whose geometry could not be recovered (not drawn) — kept
   * so the caller can note that a removal happened it can't position. */
  removedNodeIds: Set<string>;
}

/** The current Instance version — the snapshot's own `version`, defaulting to 0
 * for a never-patched instance (the field is absent pre-patch). */
export function currentVersion(snapshot: InstanceSnapshot): number {
  return snapshot.version ?? 0;
}

/** A short, double-quoted task name for an `insert`'s operation label, resolved
 * from the added node's definition; falls back to the raw id when the def is
 * absent (a node added by a past patch whose def is no longer in the set). */
function addedTaskLabel(snapshot: InstanceSnapshot, nodeId: string): string {
  const def = snapshot.tasks.find((t) => t.id === nodeId);
  return `"${def?.name ?? nodeId}"`;
}

/** Human label for a patch, inferred from its lowered ops. An `insert` always
 * introduces a node, so a record carrying an AddNode reads as `insert "<task>"`;
 * anything else is summarised by its op count (a raw primitive patch). */
function operationLabel(snapshot: InstanceSnapshot, patch: AppliedPatchView): string {
  const added = patch.ops.find((o) => o.op === 'AddNode' && o.node_id);
  if (added?.node_id) return `insert ${addedTaskLabel(snapshot, added.node_id)}`;
  const n = patch.ops.length;
  return `${n} op${n === 1 ? '' : 's'}`;
}

/**
 * The ordered version list: the pristine baseline first, then one entry per
 * applied patch in the order they landed. Empty applied-patches → a single
 * pristine entry (an instance with nothing to navigate; the caller may choose
 * not to render a one-entry timeline).
 */
export function buildVersionHistory(snapshot: InstanceSnapshot): VersionEntry[] {
  const patches = [...(snapshot.applied_patches ?? [])].sort((a, b) => a.version - b.version);
  const entries: VersionEntry[] = [
    { version: 0, priorVersion: null, pristine: true, operation: 'pristine' },
  ];
  for (const p of patches) {
    entries.push({
      version: p.version,
      priorVersion: p.prior_version,
      pristine: false,
      patchKey: p.patch_key,
      operation: operationLabel(snapshot, p),
      provenance: p.provenance,
      reason: p.reason,
      appliedAt: p.applied_at,
    });
  }
  return entries;
}

/**
 * The stored graph for a version, loaded directly (no replay):
 * - the current version → the live `graph` (its snapshot mirrors it);
 * - a past patched version → its retained `version_snapshots` entry;
 * - the pristine v0 of a *never-patched* instance → the live `graph`;
 * - the pristine v0 of a *patched* instance → undefined (not retained).
 */
export function graphForVersion(
  snapshot: InstanceSnapshot,
  version: number,
): SnapshotGraph | undefined {
  if (version === currentVersion(snapshot)) return snapshot.graph;
  return snapshot.version_snapshots?.[String(version)];
}

/** Match an AddEdge op to the edge it produced in the rendered graph, by exact
 * endpoint sets — the op carries the endpoints but not the minted edge id. */
function matchAddedEdgeId(graph: SnapshotGraph, op: PatchOpView): string | undefined {
  const key = (s: string[], t: string[]) => `${[...s].sort().join(',')}>${[...t].sort().join(',')}`;
  const want = key(op.sources, op.targets);
  return graph.edges.find((e) => key(e.sources, e.targets) === want)?.id;
}

/**
 * The delta that produced `version`, read from that version's applied-patch
 * record. Additions are resolved against the rendered graph (they exist there);
 * removals are positioned against the prior version's stored snapshot, so a
 * removed edge whose endpoints survive into the rendered version is ghosted.
 * Returns null when the version has no producing patch (the pristine baseline).
 */
export function computeDelta(
  snapshot: InstanceSnapshot,
  version: number,
): VersionDelta | null {
  const patch = (snapshot.applied_patches ?? []).find((p) => p.version === version);
  const graph = graphForVersion(snapshot, version);
  if (!patch || !graph) return null;

  const addedNodes = new Set<string>();
  const addedEdges = new Set<string>();
  const removedEdgeIds = new Set<string>();
  const removedNodeIds = new Set<string>();
  for (const op of patch.ops) {
    switch (op.op) {
      case 'AddNode':
        if (op.node_id) addedNodes.add(op.node_id);
        break;
      case 'AddEdge': {
        const id = matchAddedEdgeId(graph, op);
        if (id) addedEdges.add(id);
        break;
      }
      case 'RemoveEdge':
        if (op.edge_id) removedEdgeIds.add(op.edge_id);
        break;
      case 'RemoveNode':
        if (op.node_id) removedNodeIds.add(op.node_id);
        break;
    }
  }

  // Ghost removed edges using the prior snapshot for their endpoints. Keep only
  // edges whose endpoints still exist as nodes in the rendered version (a
  // rewired anchor/successor persists; that is the common interpose removal).
  const prior = graphForVersion(snapshot, patch.prior_version);
  const nodeIds = new Set(graph.nodes.map((n) => n.id));
  const ghostEdges: GhostEdge[] = [];
  if (prior) {
    for (const e of prior.edges) {
      if (!removedEdgeIds.has(e.id)) continue;
      const from = e.sources.find((s) => nodeIds.has(s));
      const to = e.targets.find((t) => nodeIds.has(t));
      if (from && to) ghostEdges.push({ id: e.id, from, to });
    }
  }

  return { addedNodes, addedEdges, ghostEdges, removedNodeIds };
}
