import { useEffect, useMemo, useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';
import { buildTimelineModel, type TimelineRow } from '@/lib/timeline';
import { normalizeState, TOKEN_VAR, type CanonicalState, type SemanticToken } from '@/api/normalize';
import type { InstanceSnapshot } from '@/api/client';

/** Resolve a shared semantic token to its `hsl(var(--…))` fill — the same path
 * the graph and legend use, so a parked span agrees with its node. */
function tokenColor(token: SemanticToken): string {
  return `hsl(${TOKEN_VAR[token]})`;
}

/** Bar color by outcome — DC-0001 status hues only, via the shared mapping. */
function barColor(row: TimelineRow): string {
  if (row.kind === 'gate') {
    // Gate lanes: waiting = amber, satisfied = green, rejected/cancelled = red.
    const s = row.outcome.toLowerCase();
    if (s === 'satisfied') return 'hsl(var(--success))';
    if (s === 'rejected') return 'hsl(var(--destructive))';
    if (s === 'cancelled') return 'hsl(var(--destructive) / .55)';
    return 'hsl(var(--warning))';
  }
  const canonical: CanonicalState = normalizeState(row.outcome);
  const map: Partial<Record<CanonicalState, string>> = {
    completed: 'hsl(var(--success))',
    failed: 'hsl(var(--destructive))',
    cancelled: 'hsl(var(--destructive) / .55)',
    in_progress: 'hsl(var(--info))',
  };
  return map[canonical] ?? 'hsl(var(--muted-foreground))';
}

function fmtDur(ms: number): string {
  const sec = Math.max(0, Math.round(ms / 1000));
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  return m < 60 ? `${m}m ${sec % 60}s` : `${Math.floor(m / 60)}h ${m % 60}m`;
}

function fmtTick(epochMs: number): string {
  const d = new Date(epochMs);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

/**
 * The Timeline tab — a browser-devtools-style waterfall rendered entirely
 * from the snapshot's transition histories via the pure Timeline layout
 * model. No timing logic lives here; no separate endpoint exists.
 */
export function InstanceTimelineTab({ snapshot }: { snapshot: InstanceSnapshot }) {
  const live = snapshot.completed_at == null;
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!live) return;
    const t = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(t);
  }, [live]);

  const model = useMemo(() => buildTimelineModel(snapshot, now), [snapshot, now]);

  if (model.rows.length === 0) {
    return (
      <Card>
        <CardContent className="p-6 text-sm text-muted-foreground">
          Nothing has run yet — the waterfall appears once the first task starts.
        </CardContent>
      </Card>
    );
  }

  const span = Math.max(model.max - model.min, 1);
  const pct = (t: number) => ((t - model.min) / span) * 100;
  const ticks = [model.min, model.min + span / 2, model.max];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Timeline</CardTitle>
        <CardDescription>
          One bar per task attempt — width is duration, color is outcome. Gate waits render as
          their own lanes; a diamond marks a branch cancelled before it ever started.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="mb-2 flex justify-between font-mono text-[11px] text-muted-foreground tabular-nums">
          {ticks.map((t, i) => (
            <span key={i}>{fmtTick(t)}</span>
          ))}
        </div>
        <div className="space-y-1.5">
          {model.rows.map((row) => {
            const end = row.end ?? now;
            const left = pct(row.start);
            const width = Math.max(pct(end) - left, 0.5);
            const isPoint = row.kind === 'cancelled';
            return (
              <div key={row.key} className="flex items-center gap-3" data-timeline-row={row.kind}>
                <span
                  className={`w-48 shrink-0 truncate text-xs ${row.kind === 'gate' ? 'text-muted-foreground italic' : ''}`}
                  title={row.label}
                >
                  {row.label}
                </span>
                <div className="relative h-5 min-w-0 flex-1 rounded bg-muted/30">
                  {isPoint ? (
                    <span
                      className="absolute top-1/2 h-2.5 w-2.5 -translate-y-1/2 rotate-45"
                      style={{ left: `${left}%`, background: barColor(row) }}
                      title={`${row.label} — cancelled at ${fmtTick(row.start)}`}
                    />
                  ) : row.segments ? (
                    <>
                      {row.segments.map((seg, i) => {
                        const segEnd = seg.end ?? now;
                        const segLeft = pct(seg.start);
                        const segWidth = Math.max(pct(segEnd) - segLeft, 0.5);
                        return (
                          <span
                            key={i}
                            className="absolute top-1/2 h-3 -translate-y-1/2 rounded-sm"
                            style={{
                              left: `${segLeft}%`,
                              width: `${segWidth}%`,
                              background: tokenColor(seg.token),
                            }}
                            title={`${row.label} — ${seg.outcome}`}
                          />
                        );
                      })}
                      {row.terminalMarker && (
                        // Reuses the cancelled point-marker primitive to put the
                        // loop body's terminal fate on-axis.
                        <span
                          className="absolute top-1/2 h-2.5 w-2.5 -translate-y-1/2 rotate-45"
                          style={{
                            left: `${pct(row.terminalMarker.at)}%`,
                            background: tokenColor(row.terminalMarker.token),
                          }}
                          title={`${row.label} — ${row.outcome}`}
                        />
                      )}
                    </>
                  ) : (
                    <span
                      className={`absolute top-1/2 h-3 -translate-y-1/2 rounded-sm ${row.kind === 'gate' ? 'opacity-70' : ''}`}
                      style={{ left: `${left}%`, width: `${width}%`, background: barColor(row) }}
                      title={`${row.label} — ${row.outcome}`}
                    />
                  )}
                </div>
                <span className="w-16 shrink-0 text-right font-mono text-[11px] text-muted-foreground tabular-nums">
                  {isPoint ? '—' : row.end == null ? `${fmtDur(now - row.start)}…` : fmtDur(row.end - row.start)}
                </span>
              </div>
            );
          })}
        </div>
      </CardContent>
    </Card>
  );
}
