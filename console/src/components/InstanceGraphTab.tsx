import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Radio, Clock, Diamond, Braces, Repeat, Hash, GitCommitHorizontal } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import {
  buildInstanceGraphModel,
  type LiveGateBadge,
  type LiveEdgeView,
} from '@/lib/instanceGraph';
import {
  buildVersionHistory,
  computeDelta,
  currentVersion,
  graphForVersion,
  type VersionDelta,
  type VersionEntry,
} from '@/lib/patchHistory';
import { bezier, computeGraphLayout, gateChipBox, loopArc, type RingLayout } from '@/lib/graphLayout';
import {
  GATE_STATE_TOKEN,
  STATE_LABEL,
  STATE_TOKEN,
  TOKEN_VAR,
  normalizeState,
  resolvedGateState,
  type CanonicalState,
  type ResolvedGateState,
} from '@/api/normalize';
import type { InstanceSnapshot } from '@/api/client';

const cx = (...a: (string | false | null | undefined)[]) => a.filter(Boolean).join(' ');

const hsl = (v: string) => `hsl(${v})`;

// The reserved loop routing variable. DC-0018: a ring draws its control at the
// center node, not on the producer's badge or as a `continue` chip on every arc.
const LOOP_CONTROL = 'loop_control';

/** Task-node hue, derived from the one shared state→token table — never an
 * independent hue map. A never-minted task (no state) renders an explicit
 * neutral absence-of-state, *not* a palette key, so giving a state a hue later
 * cannot silently recolour the neutral case. */
function taskHue(state: string | undefined): string {
  return state ? hsl(TOKEN_VAR[STATE_TOKEN[normalizeState(state)]]) : hsl(TOKEN_VAR.neutral);
}

/** A ghost node's outcome hue comes from its ground kind, not an attempt state
 * (nothing ran). Map the projected `ground` string onto the shared canonical
 * state so the ghost reads in its ground-kind colour (DC-0001) — split out from
 * the neutral never-reached node — while a distinct fill (below) marks it as
 * grounded-without-run rather than a genuine completion. */
const GHOST_GROUND_STATE: Record<string, CanonicalState> = {
  success: 'completed',
  failed: 'failed',
  cancelled: 'cancelled',
};

function ghostCanonicalState(ground: string | undefined): CanonicalState | undefined {
  return ground ? GHOST_GROUND_STATE[ground] : undefined;
}

/** Whether a node carries a state fill. The fill is a **full-node wash**, never a
 * partial extent: hue = state (DC-0001) tells the whole story, and a fractional
 * fill would read as a fake progress bar for a categorical state (DC-0004 rejects
 * "fake %"). Running-ness is carried by motion, not by fill amount. Neutral states
 * (pending/queued/skipped/unknown) and never-minted stay unfilled, on purpose. */
function hasStateFill(state: string | undefined): boolean {
  if (!state) return false;
  switch (normalizeState(state)) {
    case 'scheduled':
    case 'in_progress':
    case 'parked':
    case 'completed':
    case 'failed':
    case 'cancelled':
    case 'killed':
    case 'timed_out':
      return true;
    default:
      return false;
  }
}

// The two legend axes read the same tables as the nodes, so a legend swatch
// can never disagree with the node it explains.
const TASK_LEGEND: CanonicalState[] = ['scheduled', 'in_progress', 'parked', 'completed', 'failed'];
const GATE_LEGEND: { state: ResolvedGateState; label: string }[] = [
  { state: 'dispatched', label: 'Dispatched' },
  { state: 'satisfied', label: 'Satisfied' },
  { state: 'rejected', label: 'Rejected' },
  { state: 'cancelled', label: 'Cancelled' },
];

function fmtElapsed(ms: number): string {
  const sec = Math.max(0, Math.floor(ms / 1000));
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  return m < 60 ? `${m}m ${sec % 60}s` : `${Math.floor(m / 60)}h ${m % 60}m`;
}

/** Gate chip with live state, on the same fill grammar as the nodes: a resolved
 * gate fills its pill from the shared gate-state→token table — solid once
 * satisfied/rejected/cancelled, a lighter partial tint while merely dispatched
 * (all sources grounded-success, run parked on it) plus a loud pulse. An idle
 * gate keeps its bare kind hue with no fill. The label carries the state word so
 * Rejected and Cancelled (same red token) read distinctly. */
function LiveGateChip({ gate }: { gate: LiveGateBadge }) {
  const Icon = gate.kind === 'signal' ? Radio : gate.kind === 'timer' ? Clock : Diamond;
  const st = gate.state.toLowerCase();
  const resolved = resolvedGateState(gate.state);
  const tokVar = resolved ? TOKEN_VAR[GATE_STATE_TOKEN[resolved]] : undefined;
  const solidGate = resolved != null && resolved !== 'dispatched';
  const style = tokVar
    ? {
        borderColor: hsl(tokVar),
        color: hsl(tokVar),
        background: `hsl(${tokVar} / ${solidGate ? '.2' : '.12'})`,
      }
    : undefined;
  return (
    <span
      className={cx('hg-gate', `hg-gate-${gate.kind}`, resolved === 'dispatched' && 'hg-gate-dispatched')}
      data-gate-kind={gate.kind}
      data-gate-state={gate.state}
      style={style}
      title={`${gate.label} — ${gate.state}`}
    >
      <Icon size={13} aria-hidden />
      <span className="hg-gate-label">
        {gate.label}
        {st !== 'idle' ? ` · ${st}` : ''}
      </span>
    </span>
  );
}

/** Compact wall-clock (local HH:MM) for a timeline pill; the full timestamp
 * rides the pill's title so nothing is lost. */
function fmtClock(iso: string | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  return Number.isNaN(d.getTime())
    ? ''
    : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

/**
 * The patch-history timeline — the evolution navigator: a discrete, labelled
 * row of Instance versions (pristine baseline first, then one pill per applied
 * patch). Each pill names the operation, provenance, and time; selecting one
 * drives the graph (its stored snapshot + delta) and the code tab (the shared
 * selected version). A `null` selection means "live" — the current version with
 * no delta wash. Only rendered when there is history to navigate.
 */
function PatchHistoryTimeline({
  history,
  current,
  selected,
  onSelect,
}: {
  history: VersionEntry[];
  current: number;
  selected: number | null;
  onSelect: (v: number | null) => void;
}) {
  return (
    <div className="hg-timeline" role="group" aria-label="Patch history">
      {history.map((e, i) => {
        const active = selected === e.version;
        return (
          <div key={e.version} className="hg-tl-seg">
            {i > 0 && <span className="hg-tl-arrow" aria-hidden>→</span>}
            <button
              type="button"
              data-tl-version={e.version}
              data-active={active ? 'true' : undefined}
              aria-pressed={active}
              // Re-clicking the active pill returns to the plain live view.
              onClick={() => onSelect(active ? null : e.version)}
              className={cx('hg-tl-pill', active && 'is-active', e.pristine && 'is-pristine')}
              title={
                e.pristine
                  ? 'Pristine graph — the shape the instance spawned with'
                  : `v${e.priorVersion} → v${e.version} · ${e.operation}${e.provenance ? ` · ${e.provenance}` : ''}${e.reason ? ` · ${e.reason}` : ''}`
              }
            >
              <span className="hg-tl-ver">
                <GitCommitHorizontal size={12} aria-hidden />v{e.version}
                {e.version === current && <span className="hg-tl-tip">live</span>}
              </span>
              <span className="hg-tl-op">{e.operation}</span>
              {!e.pristine && (
                <span className="hg-tl-meta">
                  {e.provenance && (
                    <span className={cx('hg-tl-prov', `is-${e.provenance}`)}>{e.provenance}</span>
                  )}
                  {e.appliedAt && <span className="hg-tl-time tabular-nums">{fmtClock(e.appliedAt)}</span>}
                </span>
              )}
            </button>
          </div>
        );
      })}
    </div>
  );
}

/**
 * The Graph tab in live mode (DC-0004), driven entirely by the polled instance
 * snapshot. Layout is the shared `@/lib/graphLayout` engine (identical folds to
 * the static definition graph — a long spine serpentines, a loop is a circle);
 * this tab PAINTS it with liveness: each node fills by its latest attempt's
 * state (extent = settledness, hue = the shared state token), running tasks tick
 * an elapsed timer and their fill advances, and gate chips carry live gate
 * state. No transport controls — the 5s poll is the liveness mechanism.
 *
 * When the instance has been patched it also renders the evolution view: a
 * patch-history timeline whose selected version drives which stored snapshot the
 * graph paints (loaded directly, no replay) and washes the delta that produced
 * it — added structures glow, removed ones ghost. The selected version lifts to
 * the page so the code tab focuses the same patch.
 */
export function InstanceGraphTab({
  snapshot,
  selectedVersion = null,
  onSelectVersion,
}: {
  snapshot: InstanceSnapshot;
  selectedVersion?: number | null;
  onSelectVersion?: (v: number | null) => void;
}) {
  const history = useMemo(() => buildVersionHistory(snapshot), [snapshot]);
  const current = currentVersion(snapshot);
  const hasHistory = history.length > 1;
  const selectVersion = onSelectVersion ?? (() => {});

  // Which graph the tab paints: the live graph by default (and for the current
  // version), a past version's retained snapshot when one is selected. The
  // pristine v0 of a patched instance was never retained — `undefined` here, and
  // the tab shows an honest note rather than fabricating a shape by replay.
  const renderGraph =
    selectedVersion == null ? snapshot.graph : graphForVersion(snapshot, selectedVersion);
  const pristineUnavailable = selectedVersion != null && renderGraph === undefined;
  const delta: VersionDelta | null = useMemo(
    () => (selectedVersion == null ? null : computeDelta(snapshot, selectedVersion)),
    [snapshot, selectedVersion],
  );

  // An empty graph keeps the layout hooks below total when the selected version
  // has no retained snapshot; the render branches to the note before drawing.
  const model = useMemo(
    () => buildInstanceGraphModel(snapshot, renderGraph ?? { start: '', end: '', nodes: [], edges: [] }),
    [snapshot, renderGraph],
  );
  const navigate = useNavigate();
  // Which node the pointer (or keyboard focus) is on. Hovering a task previews
  // its neighbourhood (its edges + the gates reading its routing vars light, the
  // rest dims); leaving restores the plain view, so the highlight can't get
  // stuck. Clicking a minted node opens its task instance detail page instead.
  const [hovered, setHovered] = useState<string | null>(null);
  // Identity-code overlay: off by default (the graph reads as topology first),
  // toggled on to read off the short code an operator names when authoring a
  // patch. The codes are the same projection the HTTP view and ctx graph carry.
  const [showCodes, setShowCodes] = useState(false);

  // Elapsed tick for running tasks — anchored on the substrate's derived
  // started_at, so it survives poll refreshes without resetting.
  const anyRunning = model.tasks.some((t) => t.running);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!anyRunning) return;
    const t = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(t);
  }, [anyRunning]);

  const { pos, width, height, internalSeg, arcLanes, rings, edgeRoutes } = useMemo(
    () =>
      computeGraphLayout({
        nodes: model.layout.nodes,
        layoutEdges: model.layout.edges,
        renderEdges: model.edges,
        ringLanes: model.ringLanes,
        chains: model.chains,
        junctionIds: new Set(model.junctions.map((j) => j.id)),
        junctionBox: new Map(
          model.junctions
            .filter((j) => j.gates.length > 0)
            .map((j) => [
              j.id,
              gateChipBox(
                j.gates.map(
                  (g) => g.label + (g.state.toLowerCase() !== 'idle' ? ` · ${g.state.toLowerCase()}` : ''),
                ),
              ),
            ]),
        ),
      }),
    [model],
  );

  // A genuinely empty instance (no tasks, nothing to navigate) is the only case
  // that short-circuits; a patched instance always renders the timeline so the
  // operator can pick another version even when the pristine shape is gone.
  if (model.tasks.length === 0 && !hasHistory) {
    return (
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">
          No instance graph to display.
        </CardContent>
      </Card>
    );
  }

  // Delta wash for a selected version: added structures glow (they exist in the
  // rendered snapshot), removed edges ghost (drawn from the prior snapshot's
  // geometry). Predicates fold the delta down to per-structure lookups.
  const addedNode = (id: string) => delta?.addedNodes.has(id) ?? false;
  const addedEdge = (id: string) => delta?.addedEdges.has(id) ?? false;

  // Hover highlight: hovering a task that produces a routing variable lights the
  // predicate gate(s) reading it (via the shared producer↔gate adjacency) plus
  // the task's own connected edges; everything else dims. The adjacency is
  // definition-derived, so the highlight is identical for any instance.
  const activeEdges = new Set<string>();
  const activeNodes = new Set<string>();
  if (hovered) {
    activeNodes.add(hovered);
    for (const e of model.edges) {
      if (e.from === hovered || e.to === hovered) {
        activeEdges.add(e.id);
        activeNodes.add(e.from);
        activeNodes.add(e.to);
      }
    }
    for (const edgeId of model.selection.taskToGates[hovered] ?? []) {
      // A hyperedge gate rides its junction, whose legs are `${edgeId}:…`.
      activeNodes.add(`junction:${edgeId}`);
      for (const e of model.edges) {
        if (e.id === edgeId || e.id.startsWith(`${edgeId}:`)) {
          activeEdges.add(e.id);
          activeNodes.add(e.from);
          activeNodes.add(e.to);
        }
      }
    }
  }
  const dimNode = (id: string) => hovered != null && !activeNodes.has(id);
  const dimEdge = (id: string) => hovered != null && !activeEdges.has(id);
  const ringActive = (rg: RingLayout) =>
    (hovered != null && rg.members.includes(hovered)) || rg.edgeIds.some((id) => activeEdges.has(id));
  const ringDim = (rg: RingLayout) => hovered != null && !ringActive(rg);

  const bez = (from: string, to: string) => bezier(pos, from, to);
  const arc = (id: string, from: string, to: string) => loopArc(pos, arcLanes, id, from, to);
  // An edge's drawn midpoint, resolved the same way the gate badges are placed:
  // a folded serpentine segment carries its own mid, a loop arc computes one,
  // and a plain edge falls back to its bezier. The identity-code chip rides this
  // point (nudged clear of any gate chip above it).
  const edgeMid = (e: LiveEdgeView): { x: number; y: number } | undefined => {
    const fold = internalSeg.get(e.id);
    if (fold?.mid) return fold.mid;
    if (e.loopArc) return arc(e.id, e.from, e.to)?.mid;
    return (edgeRoutes.get(e.id) ?? bez(e.from, e.to))?.mid;
  };
  const ringTurns = (rg: RingLayout) => model.edges.find((e) => e.id === rg.backEdgeId)?.turns;
  // Ring members carry their loop control in the ring's center node (DC-0018), so
  // `loop_control` is folded out of their per-node routing-var badges.
  const ringMemberIds = new Set(rings.flatMap((rg) => rg.members));

  return (
    <Card>
      <CardHeader className="flex flex-row items-start justify-between space-y-0 gap-4">
        <div className="space-y-1.5">
          <CardTitle className="text-base">Graph</CardTitle>
          <CardDescription>
            The instance hypergraph, live: a node’s fill is its state colour; a running task
            animates and ticks elapsed time, gate chips carry live gate state. Updates ride the 5s
            snapshot poll.
          </CardDescription>
        </div>
        <button
          type="button"
          role="switch"
          aria-checked={showCodes}
          data-state={showCodes ? 'on' : 'off'}
          onClick={() => setShowCodes((v) => !v)}
          title="Overlay each node's and edge's identity code — the short handle you name when authoring a patch"
          className={cx('hg-codetoggle', showCodes && 'is-on')}
        >
          <Hash size={13} aria-hidden />
          Identity codes
        </button>
      </CardHeader>
      <CardContent>
        {/* Evolution navigator — only when the instance has been patched. */}
        {hasHistory && (
          <PatchHistoryTimeline
            history={history}
            current={current}
            selected={selectedVersion}
            onSelect={selectVersion}
          />
        )}
        {pristineUnavailable ? (
          <div className="hg-pristine-note">
            The pristine (v{selectedVersion}) graph was not retained — versions are stored from the
            first patch onward, so there is no earlier snapshot to load directly. Pick v
            {history.find((e) => !e.pristine)?.version ?? 1} or later to see the graph and its delta.
          </div>
        ) : (
          <>
        {/* When a version is selected, a one-line delta key sits above the graph
            so the glow/ghost wash is legible without guessing. */}
        {delta && (
          <div className="hg-delta-legend">
            <span className="hg-delta-title">
              v{selectedVersion} delta{selectedVersion === current ? ' (live)' : ''}
            </span>
            <span className="hg-leg-item">
              <span className="hg-delta-mark is-added" />
              Added
            </span>
            <span className="hg-leg-item">
              <span className="hg-delta-mark is-ghost" />
              Removed
            </span>
            <span className="hg-leg-item">
              <span className="hg-delta-mark is-neutral" />
              Unchanged
            </span>
          </div>
        )}
        {/* Two-axis legend — task-state + gate-state — reading the same shared
            tables as the nodes, so a swatch can never disagree with a node. */}
        <div className="hg-legend">
          {TASK_LEGEND.map((s) => (
            <span key={s} className="hg-leg-item">
              <span
                className="hg-leg-mark hg-leg-dot"
                data-legend-state={s}
                style={{ background: hsl(TOKEN_VAR[STATE_TOKEN[s]]) }}
              />
              {STATE_LABEL[s]}
            </span>
          ))}
          <span className="hg-leg-item">
            <span
              className="hg-leg-mark hg-leg-dot"
              data-legend-state="never-minted"
              style={{ background: hsl(TOKEN_VAR.neutral) }}
            />
            Never minted
          </span>
          <span className="hg-leg-item">
            <span
              className="hg-leg-mark hg-leg-ghost"
              data-legend-state="ghost"
              title="Grounded without running — a reaped or cancel-cascaded node, in its ground-kind hue"
            />
            Ghost (reaped)
          </span>
          {model.tasks.some((t) => t.preGrounded) && (
            <span className="hg-leg-item">
              <span
                className="hg-leg-mark hg-leg-carried"
                data-legend-state="carried"
                title="Carried forward from the source run — inherited grounded, not re-run in this replay"
              />
              Carried forward
            </span>
          )}
          {GATE_LEGEND.map((g) => (
            <span key={g.state} className="hg-leg-item">
              <span
                className={cx('hg-leg-mark hg-leg-gate', g.state === 'dispatched' && 'hg-gate-dispatched')}
                data-legend-gate={g.state}
                style={{ borderColor: hsl(TOKEN_VAR[GATE_STATE_TOKEN[g.state]]) }}
              />
              {g.label}
            </span>
          ))}
        </div>

        <div className="hg-scroll">
          <div className="hg-canvas" style={{ width, height }}>
            <svg className="hg-edges" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
              <defs>
                <marker id="ihg-ah" markerWidth="9" markerHeight="9" refX="6.5" refY="3" orient="auto">
                  <path d="M0 0 L6 3 L0 6 z" fill="hsl(var(--muted-foreground))" />
                </marker>
                <marker id="ihg-ah-a" markerWidth="9" markerHeight="9" refX="6.5" refY="3" orient="auto">
                  <path d="M0 0 L6 3 L0 6 z" fill="hsl(var(--primary))" />
                </marker>
                <marker id="ihg-ah-g" markerWidth="9" markerHeight="9" refX="6.5" refY="3" orient="auto">
                  <path d="M0 0 L6 3 L0 6 z" fill="hsl(var(--muted-foreground) / .5)" />
                </marker>
              </defs>
              {/* Ghosted removed edges (delta): drawn first, under the live
                  structures, from the prior snapshot's geometry between endpoints
                  that survive into the rendered version. Faint and dashed — a
                  structure that WAS here and the selected patch removed. */}
              {delta?.ghostEdges.map((g) => {
                const seg = bez(g.from, g.to);
                if (!seg?.path) return null;
                return (
                  <path
                    key={`ghost-${g.id}`}
                    data-ghost-edge={g.id}
                    className="hg-edge-ghost"
                    d={seg.path}
                    fill="none"
                    stroke="hsl(var(--muted-foreground) / .5)"
                    strokeWidth={1.5}
                    strokeDasharray="2 4"
                    markerEnd="url(#ihg-ah-g)"
                  />
                );
              })}
              {/* loop rings: the dashed circle IS the loop lane; chevrons give
                  the clockwise execution direction */}
              {rings.map((rg, i) => {
                const act = ringActive(rg);
                const stroke = act ? 'hsl(var(--primary))' : 'hsl(var(--warning) / .7)';
                return (
                  <g key={`ring-${i}`} data-loop-ring="true" opacity={ringDim(rg) ? 0.3 : 1}>
                    <circle
                      cx={rg.center.x}
                      cy={rg.center.y}
                      r={rg.r}
                      fill="none"
                      stroke={stroke}
                      strokeWidth={act ? 2 : 1.5}
                      strokeDasharray="4 3"
                    />
                    {rg.chevrons.map((c, j) => (
                      <path
                        key={j}
                        data-loop-dir="true"
                        d="M -3 -4.5 L 3 0 L -3 4.5"
                        fill="none"
                        stroke={stroke}
                        strokeWidth={2}
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        transform={`translate(${c.x} ${c.y}) rotate(${c.rotDeg})`}
                      />
                    ))}
                  </g>
                );
              })}
              {/* plain dependency edges and serpentine connectors. A ring step
                  carries an empty internal path (the circle draws it) and is
                  skipped; a serpentine connector carries a real folded path.
                  Frontier edges (grounded-success source → not-started target)
                  are statically brightened — no animation; honest under the 5s
                  poll. */}
              {model.edges.map((e) => {
                if (e.loopArc) return null;
                const fold = internalSeg.get(e.id);
                const seg = fold ?? edgeRoutes.get(e.id) ?? bez(e.from, e.to);
                if (!seg || !seg.path) return null;
                const act = activeEdges.has(e.id);
                const added = addedEdge(e.id);
                const stroke = act
                  ? 'hsl(var(--primary))'
                  : added
                    ? 'hsl(var(--success))'
                    : e.isLoop
                      ? 'hsl(var(--warning) / .7)'
                      : e.frontier
                        ? 'hsl(var(--foreground) / .85)'
                        : 'hsl(var(--muted-foreground) / .45)';
                return (
                  <path
                    key={e.id}
                    data-edge={e.id}
                    data-serp-kind={fold?.serpKind}
                    data-loop={e.isLoop ? 'true' : undefined}
                    data-frontier={e.frontier ? 'true' : undefined}
                    data-active={act ? 'true' : undefined}
                    data-added={added ? 'true' : undefined}
                    className={cx(added && 'hg-edge-added')}
                    d={seg.path}
                    fill="none"
                    stroke={stroke}
                    strokeWidth={act || added || e.frontier ? 2 : 1.5}
                    strokeOpacity={dimEdge(e.id) ? 0.3 : 1}
                    strokeDasharray={e.isLoop ? '4 3' : undefined}
                    markerEnd={`url(#${act ? 'ihg-ah-a' : 'ihg-ah'})`}
                  />
                );
              })}
              {/* self-loop arcs (multi-member rings draw as circles above) */}
              {model.edges.map((e) => {
                if (!e.loopArc || internalSeg.has(e.id)) return null;
                const seg = arc(e.id, e.from, e.to);
                if (!seg) return null;
                const act = activeEdges.has(e.id);
                return (
                  <path
                    key={e.id}
                    data-edge={e.id}
                    data-loop="true"
                    data-arc-lane={arcLanes.get(e.id) ?? 0}
                    data-active={act ? 'true' : undefined}
                    d={seg.path}
                    fill="none"
                    stroke={act ? 'hsl(var(--primary))' : 'hsl(var(--warning) / .7)'}
                    strokeWidth={act ? 2 : 1.5}
                    strokeOpacity={dimEdge(e.id) ? 0.3 : 1}
                    strokeDasharray="4 3"
                    markerEnd={`url(#${act ? 'ihg-ah-a' : 'ihg-ah'})`}
                  />
                );
              })}
            </svg>

            {/* gate badges on 1→1 edges. A loop back-edge's gate is the reserved
                `loop_control == continue` — continue is the default, so DC-0018
                drops it: the ring turning already says "continue". The one
                explicit loop gate (`== done`) rides the exit junction below. */}
            {model.edges.map((e) => {
              if (e.gates.length === 0 || e.isLoop) return null;
              const anchorMid = internalSeg.get(e.id)?.mid;
              const seg = anchorMid
                ? { mid: anchorMid }
                : e.loopArc
                  ? arc(e.id, e.from, e.to)
                  : edgeRoutes.get(e.id) ?? bez(e.from, e.to);
              if (!seg) return null;
              const act = activeEdges.has(e.id);
              return (
                <div
                  key={`${e.id}-gates`}
                  data-gate-edge={e.id}
                  data-active={act ? 'true' : undefined}
                  className={cx('hg-node hg-badgewrap', act && 'sel', dimEdge(e.id) && 'dim')}
                  style={{ left: seg.mid.x, top: seg.mid.y - 26 }}
                >
                  {e.gates.map((g, i) => (
                    <LiveGateChip key={i} gate={g} />
                  ))}
                </div>
              );
            })}

            {/* the loop-control node holding each ring's center (DC-0018): it
                owns the loop's control — names `loop_control` and carries the
                turn counter (the temporal answer — which turn — derived from the
                Parked history; honest under the 5s poll because it only counts
                up). Continue is implicit in the ring; the exit is the one gate. */}
            {rings.map((rg, i) => {
              const turns = ringTurns(rg);
              return (
                <div
                  key={`ring-center-${i}`}
                  className={cx('hg-node', ringDim(rg) && 'dim')}
                  style={{ left: rg.center.x, top: rg.center.y }}
                >
                  <span
                    className="hg-turns hg-loopctl"
                    data-ring-center
                    data-loop-turns={turns}
                    title={
                      turns
                        ? `loop control — ${turns} turn${turns === 1 ? '' : 's'} completed; cycles clockwise until loop_control == done`
                        : 'loop control — tasks cycle clockwise until loop_control == done'
                    }
                  >
                    <Repeat size={11} aria-hidden />
                    <span className="hg-loopctl-var">{LOOP_CONTROL}</span>
                    {turns ? <span className="hg-loopctl-turns">{turns}</span> : null}
                  </span>
                </div>
              );
            })}
            {/* turn counter riding a self-loop arc */}
            {model.edges.map((e) => {
              if (!e.loopArc || !e.turns || internalSeg.has(e.id)) return null;
              const seg = arc(e.id, e.from, e.to);
              if (!seg) return null;
              return (
                <div
                  key={`${e.id}-turns`}
                  className={cx('hg-node', dimEdge(e.id) && 'dim')}
                  style={{ left: seg.mid.x, top: seg.mid.y }}
                >
                  <span
                    className="hg-turns"
                    data-loop-turns={e.turns}
                    title={`${e.turns} loop turn${e.turns === 1 ? '' : 's'} completed`}
                  >
                    <Repeat size={11} aria-hidden />
                    {e.turns}
                  </span>
                </div>
              );
            })}

            {/* junctions, with the hyperedge's gates */}
            {model.junctions.map((j) => {
              const p = pos[j.id];
              if (!p) return null;
              return (
                <div
                  key={j.id}
                  data-junction={j.edgeId}
                  data-active={activeNodes.has(j.id) ? 'true' : undefined}
                  data-added={addedEdge(j.edgeId) ? 'true' : undefined}
                  className={cx(
                    'hg-node hg-jwrap',
                    activeNodes.has(j.id) && 'sel',
                    addedEdge(j.edgeId) && 'hg-added',
                    dimNode(j.id) && 'dim',
                  )}
                  style={{ left: p.x, top: p.y }}
                >
                  {j.gates.length > 0 ? (
                    j.gates.map((g, i) => <LiveGateChip key={i} gate={g} />)
                  ) : (
                    <span className="hg-jdot" />
                  )}
                </div>
              );
            })}

            {/* task nodes with live-state fill */}
            {model.tasks.map((t) => {
              const p = pos[t.id];
              if (!p) return null;
              // A running task ticks a live elapsed timer; a finished one shows
              // its settled time-to-complete (start → first terminal). One slot,
              // two readings — motion + the `info` hue say "live", the muted
              // duration says "done".
              const elapsed =
                t.running && t.startedAt ? fmtElapsed(now - new Date(t.startedAt).getTime()) : null;
              const duration =
                !t.running && t.startedAt && t.completedAt
                  ? fmtElapsed(new Date(t.completedAt).getTime() - new Date(t.startedAt).getTime())
                  : null;
              const hov = hovered === t.id;
              // A minted node opens its task instance detail page on click; a
              // never-minted node has no instance, so it only previews on hover.
              const openable = t.taskInstanceId != null;
              // A ghost node grounded without running: no attempt state, so its
              // hue comes from its ground kind and it fills (marked distinct by
              // the `.ghost` class below), rather than reading as never-reached.
              const ghostState = t.ghost ? ghostCanonicalState(t.ground) : undefined;
              const filled = ghostState != null || hasStateFill(t.state);
              // --tok is the state's own token (the raw `var(--…)` triple, so
              // `hsl(var(--tok) / a)` in CSS resolves through the shared table);
              // a ghost resolves via its ground kind, a never-minted / neutral
              // node stays unfilled.
              const tokVar = ghostState
                ? TOKEN_VAR[STATE_TOKEN[ghostState]]
                : t.state
                  ? TOKEN_VAR[STATE_TOKEN[normalizeState(t.state)]]
                  : TOKEN_VAR.neutral;
              const dotColor = ghostState ? hsl(TOKEN_VAR[STATE_TOKEN[ghostState]]) : taskHue(t.state);
              return (
                <button
                  key={t.id}
                  type="button"
                  data-task={t.id}
                  data-state={ghostState ?? (t.state ? normalizeState(t.state) : 'never-minted')}
                  data-ghost={t.ghost ? 'true' : undefined}
                  data-carried={t.preGrounded ? 'true' : undefined}
                  data-hovered={hov ? 'true' : undefined}
                  data-added={addedNode(t.id) ? 'true' : undefined}
                  className={cx(
                    'hg-node hg-task',
                    filled && 'touched',
                    t.ghost && 'ghost',
                    // A carried-forward replay node is also a ghost; `.carried`
                    // overrides the reaped-ghost hatch so the two read distinctly.
                    t.preGrounded && 'carried',
                    t.running && 'running',
                    hov && 'sel',
                    openable && 'hg-navigable',
                    addedNode(t.id) && 'hg-added',
                    dimNode(t.id) && 'dim',
                  )}
                  style={{ left: p.x, top: p.y, '--tok': tokVar } as React.CSSProperties}
                  title={openable ? 'Open task instance' : undefined}
                  onMouseEnter={() => setHovered(t.id)}
                  onMouseLeave={() => setHovered((h) => (h === t.id ? null : h))}
                  onFocus={() => setHovered(t.id)}
                  onBlur={() => setHovered((h) => (h === t.id ? null : h))}
                  onClick={
                    openable
                      ? () =>
                          navigate(
                            `/workflows/${snapshot.workflow_id}/instances/${snapshot.id}/tasks/${t.taskInstanceId}`,
                          )
                      : undefined
                  }
                >
                  {filled && <span className="hg-fill" aria-hidden />}
                  {/* The dot keeps the ground-kind hue so the outcome reads; the
                      ghost's dashed outline + hatched fill (the `.ghost` class)
                      carry "grounded but never ran". */}
                  <span className="hg-dot st-dot" style={{ background: dotColor }} />
                  <span className="hg-meta">
                    <span className="hg-name">{t.name}</span>
                    <span className="hg-subline">
                      {elapsed ? (
                        <span className="hg-elapsed" data-elapsed>
                          {elapsed}
                        </span>
                      ) : duration ? (
                        <span className="hg-duration" data-duration title="Time to complete">
                          {duration}
                        </span>
                      ) : null}
                      {t.routingVars
                        .filter((v) => !(ringMemberIds.has(t.id) && v === LOOP_CONTROL))
                        .map((v) => (
                          <span key={v} className="hg-var" title="routing variable">
                            <Braces size={10} aria-hidden />
                            {v}
                          </span>
                        ))}
                    </span>
                  </span>
                </button>
              );
            })}

            {/* Identity-code overlay (toggle). Each HyperNode and HyperEdge is
                labelled with the short code an operator reads off to author a
                patch — a node's code tags its top-left corner, an edge's rides
                its drawn midpoint, and a hyperedge's rides its junction. Painted
                last so the chips sit above the nodes; the chips dim in lockstep
                with the structure they annotate under a selection. */}
            {showCodes &&
              model.tasks.map((t) => {
                const p = pos[t.id];
                if (!p) return null;
                return (
                  <span
                    key={`code-${t.id}`}
                    data-node-code={t.code}
                    className={cx('hg-code hg-code-node', dimNode(t.id) && 'dim')}
                    style={{ left: p.x - 89, top: p.y - 27 }}
                    title={`node ${t.code} · ${t.id}`}
                  >
                    {t.code}
                  </span>
                );
              })}
            {showCodes &&
              model.junctions.map((j) => {
                const p = pos[j.id];
                if (!p) return null;
                return (
                  <span
                    key={`code-${j.id}`}
                    data-edge-code={j.code}
                    className={cx('hg-code hg-code-edge', dimNode(j.id) && 'dim')}
                    style={{ left: p.x, top: p.y - 22 }}
                    title={`edge ${j.code} · ${j.edgeId}`}
                  >
                    {j.code}
                  </span>
                );
              })}
            {showCodes &&
              model.edges.map((e) => {
                // Junction legs carry no code (the whole hyperedge's code rides
                // its junction, above); only a self-standing edge is labelled.
                if (!e.code) return null;
                const mid = edgeMid(e);
                if (!mid) return null;
                return (
                  <span
                    key={`code-${e.id}`}
                    data-edge-code={e.code}
                    className={cx('hg-code hg-code-edge', dimEdge(e.id) && 'dim')}
                    style={{ left: mid.x, top: mid.y + 13 }}
                    title={`edge ${e.code} · ${e.id}`}
                  >
                    {e.code}
                  </span>
                );
              })}
          </div>
        </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}
