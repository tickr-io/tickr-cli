import { useMemo, useState } from 'react';
import { Radio, Clock, Diamond, Braces, Repeat } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { buildHyperGraphModel, type GateKind, type GateBadge } from '@/lib/hyperGraph';
import { bezier, computeGraphLayout, gateChipBox, loopArc, type RingLayout } from '@/lib/graphLayout';

const cx = (...a: (string | false | null | undefined)[]) => a.filter(Boolean).join(' ');

// The reserved loop routing variable. DC-0018: a ring draws its control at the
// center node, not on the producer's badge or as a `continue` chip on every arc.
const LOOP_CONTROL = 'loop_control';

function GateChip({ gate }: { gate: GateBadge }) {
  const Icon = gate.kind === 'signal' ? Radio : gate.kind === 'timer' ? Clock : gate.kind === 'predicate' ? Diamond : null;
  return (
    <span className={cx('hg-gate', `hg-gate-${gate.kind}`)} data-gate-kind={gate.kind}>
      {Icon && <Icon size={13} />}
      <span className="hg-gate-label">{gate.kind}</span>
    </span>
  );
}

const GATE_LEGEND: { kind: GateKind; cls: string; label: string }[] = [
  { kind: 'signal', cls: 'hg-leg-signal', label: 'Signal gate' },
  { kind: 'timer', cls: 'hg-leg-timer', label: 'Timer gate' },
  { kind: 'predicate', cls: 'hg-leg-predicate', label: 'Predicate gate' },
];

/**
 * The Task graph tab — DC-0004 Flow rendering in static mode, matching the
 * kit's hypergraph: HTML task nodes (name + nix subline + routing-var chips)
 * positioned by dagre, curved bezier edges, a junction dot only where a true
 * hyperedge fans, gate chips (Radio/Clock/Diamond + label) riding the edge, and
 * producer↔predicate-gate selection highlighting. No liveness affordances.
 *
 * Layout comes from the shared `@/lib/graphLayout` engine (identical to the live
 * instance graph): a loop ring renders as a literal circle, and long serial
 * spines (≥ CHAIN_MIN) fold into a serpentine. This tab only PAINTS the result
 * statically; the instance tab paints the same layout with live state.
 */
export function TaskGraphTab({ definition }: { definition: Record<string, unknown> }) {
  const model = useMemo(() => buildHyperGraphModel(definition), [definition]);
  const [selected, setSelected] = useState<string | null>(null);

  const { pos, width, height, internalSeg, arcLanes, rings, edgeRoutes } = useMemo(
    () =>
      computeGraphLayout({
        nodes: model.layout.nodes,
        layoutEdges: model.layout.edges,
        renderEdges: model.render.edges,
        ringLanes: model.ringLanes,
        chains: model.chains,
        junctionIds: new Set(model.render.junctions.map((j) => j.id)),
        junctionBox: new Map(
          model.render.junctions
            .filter((j) => j.gate)
            .map((j) => [j.id, gateChipBox([j.gate!.kind])]),
        ),
      }),
    [model],
  );

  if (model.render.tasks.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">No task graph to display.</CardContent>
      </Card>
    );
  }

  // Selection: a picked task lights its connected edges + the predicate gates
  // reading its routing var; everything else dims.
  const activeEdges = new Set<string>();
  const activeNodes = new Set<string>();
  if (selected) {
    activeNodes.add(selected);
    for (const e of model.render.edges) {
      if (e.from === selected || e.to === selected) {
        activeEdges.add(e.id);
        activeNodes.add(e.from);
        activeNodes.add(e.to);
      }
    }
    for (const edgeId of model.selection.taskToGates[selected] ?? []) {
      for (const e of model.render.edges) {
        if (e.id === edgeId || e.id.startsWith(`${edgeId}:`)) {
          activeEdges.add(e.id);
          activeNodes.add(e.from);
          activeNodes.add(e.to);
        }
      }
    }
  }
  const dimNode = (id: string) => selected != null && !activeNodes.has(id);
  const dimEdge = (id: string) => selected != null && !activeEdges.has(id);
  const ringActive = (rg: RingLayout) =>
    (selected != null && rg.members.includes(selected)) || rg.edgeIds.some((id) => activeEdges.has(id));
  const ringDim = (rg: RingLayout) => selected != null && !ringActive(rg);
  // Ring members carry their loop control in the ring's center node (DC-0018), so
  // `loop_control` is folded out of their per-node routing-var badges.
  const ringMemberIds = new Set(rings.flatMap((rg) => rg.members));

  const bez = (from: string, to: string) => bezier(pos, from, to);
  const arc = (id: string, from: string, to: string) => loopArc(pos, arcLanes, id, from, to);
  const edgeSeg = (e: { id: string; from: string; to: string; loopArc?: boolean }) =>
    internalSeg.get(e.id) ??
    (e.loopArc ? arc(e.id, e.from, e.to) : edgeRoutes.get(e.id) ?? bez(e.from, e.to));

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Task graph</CardTitle>
        <CardDescription>
          The workflow’s task graph (topology). Direct arrows for plain dependencies; gates ride on
          the edge; a junction appears only where a true hyperedge fans; a loop’s tasks sit on a
          circle — the dashed ring, clockwise from the top.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="hg-legend">
          <span className="hg-leg-item"><span className="hg-leg-mark hg-leg-task" />Task</span>
          <span className="hg-leg-item"><span className="hg-leg-mark hg-leg-ctrl" />Join (hyperedge)</span>
          <span className="hg-leg-item"><span className="hg-leg-mark hg-leg-loop" />Loop ring</span>
          {GATE_LEGEND.map((g) => (
            <span key={g.kind} className="hg-leg-item"><span className={`hg-leg-mark ${g.cls}`} />{g.label}</span>
          ))}
        </div>

        <div className="hg-scroll">
          <div className="hg-canvas" style={{ width, height }}>
            <svg className="hg-edges" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
              <defs>
                <marker id="hg-ah" markerWidth="9" markerHeight="9" refX="6.5" refY="3" orient="auto">
                  <path d="M0 0 L6 3 L0 6 z" fill="hsl(var(--muted-foreground))" />
                </marker>
                <marker id="hg-ah-a" markerWidth="9" markerHeight="9" refX="6.5" refY="3" orient="auto">
                  <path d="M0 0 L6 3 L0 6 z" fill="hsl(var(--primary))" />
                </marker>
              </defs>
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
              {/* plain dependency edges and serpentine connectors (ring steps
                  have an empty internal path — the circle draws them) */}
              {model.render.edges.map((e) => {
                if (e.loopArc) return null;
                const fold = internalSeg.get(e.id);
                const seg = fold ?? edgeRoutes.get(e.id) ?? bez(e.from, e.to);
                if (!seg || !seg.path) return null;
                const act = activeEdges.has(e.id);
                const stroke = act
                  ? 'hsl(var(--primary))'
                  : e.isLoop
                    ? 'hsl(var(--warning) / .7)'
                    : 'hsl(var(--muted-foreground) / .45)';
                return (
                  <path
                    key={e.id}
                    data-edge={e.id}
                    data-serp-kind={fold?.serpKind}
                    data-loop={e.isLoop ? 'true' : undefined}
                    d={seg.path}
                    fill="none"
                    stroke={stroke}
                    strokeWidth={act ? 2 : 1.5}
                    strokeDasharray={e.isLoop ? '4 3' : undefined}
                    strokeOpacity={dimEdge(e.id) ? 0.3 : 1}
                    markerEnd={`url(#${act ? 'hg-ah-a' : 'hg-ah'})`}
                  />
                );
              })}
              {/* self-loops and non-ring fallback loop edges: over-the-top arc */}
              {model.render.edges.map((e) => {
                if (!e.loopArc || internalSeg.has(e.id)) return null;
                const seg = arc(e.id, e.from, e.to);
                if (!seg) return null;
                const act = activeEdges.has(e.id);
                return (
                  <path
                    key={e.id}
                    data-edge={e.id}
                    data-loop="true"
                    data-loop-arc="true"
                    data-arc-lane={arcLanes.get(e.id) ?? 0}
                    d={seg.path}
                    fill="none"
                    stroke={act ? 'hsl(var(--primary))' : 'hsl(var(--warning) / .7)'}
                    strokeWidth={act ? 2 : 1.5}
                    strokeDasharray="4 3"
                    strokeOpacity={dimEdge(e.id) ? 0.3 : 1}
                    markerEnd={`url(#${act ? 'hg-ah-a' : 'hg-ah'})`}
                  />
                );
              })}
            </svg>

            {/* gate badges on 1→1 edges. A loop back-edge's gate is the reserved
                `loop_control == continue` — continue is the default, so DC-0018
                drops it: the dashed ring already says "continue". The one
                explicit loop gate (`== done`) rides the exit junction below. */}
            {model.render.edges.map((e) => {
              if (!e.gate || e.isLoop) return null;
              const seg = edgeSeg(e);
              if (!seg) return null;
              return (
                <div
                  key={`${e.id}-gate`}
                  className={cx('hg-node hg-badgewrap', dimEdge(e.id) && 'dim')}
                  style={{ left: seg.mid.x, top: seg.mid.y - 26 }}
                >
                  <GateChip gate={e.gate} />
                </div>
              );
            })}

            {/* the loop-control node holding each ring's center (DC-0018): it
                owns the loop's control — names `loop_control`. Continue is
                implicit in the ring; the exit (`== done`) is the one gate. */}
            {rings.map((rg, i) => (
              <div
                key={`ring-center-${i}`}
                className={cx('hg-node', ringDim(rg) && 'dim')}
                style={{ left: rg.center.x, top: rg.center.y }}
              >
                <span
                  className="hg-turns hg-loopctl"
                  data-ring-center
                  title="loop control — tasks cycle clockwise until loop_control == done"
                >
                  <Repeat size={11} aria-hidden />
                  <span className="hg-loopctl-var">{LOOP_CONTROL}</span>
                </span>
              </div>
            ))}

            {/* junction nodes (+ gate chip when the hyperedge is gated) */}
            {model.render.junctions.map((j) => {
              const p = pos[j.id];
              if (!p) return null;
              return (
                <div key={j.id} data-junction={j.edgeId} className="hg-node hg-jwrap" style={{ left: p.x, top: p.y }}>
                  {j.gate ? <GateChip gate={j.gate} /> : <span className="hg-jdot" />}
                </div>
              );
            })}

            {/* task nodes */}
            {model.render.tasks.map((t) => {
              const p = pos[t.id];
              if (!p) return null;
              const sel = selected === t.id;
              return (
                <button
                  key={t.id}
                  data-task={t.id}
                  className={cx('hg-node hg-task', sel && 'sel', dimNode(t.id) && 'dim')}
                  style={{ left: p.x, top: p.y }}
                  onClick={() => setSelected(sel ? null : t.id)}
                >
                  <span className="hg-dot st-dot" />
                  <span className="hg-meta">
                    <span className="hg-name">{t.name}</span>
                    <span className="hg-subline">
                      <span className="hg-nix">{t.nix}</span>
                      {t.routingVars
                        .filter((v) => !(ringMemberIds.has(t.id) && v === LOOP_CONTROL))
                        .map((v) => (
                          <span key={v} className="hg-var" title="routing variable">
                            <Braces size={10} />
                            {v}
                          </span>
                        ))}
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
