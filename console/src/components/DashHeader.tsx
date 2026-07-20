import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useUpcoming } from '@/api/hooks';
import { useNow } from '@/hooks/useNow';
import { cn } from '@/lib/utils';

/** Server default page; each "Fetch more" widens the ask by this much. */
const UPCOMING_PAGE = 20;

/** Local HH:MM for an RFC3339 instant, or '--:--' when absent/unparseable. */
function fmtClock(iso: string | null | undefined): string {
  if (!iso) return '--:--';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '--:--';
  return `${d.getHours().toString().padStart(2, '0')}:${d.getMinutes().toString().padStart(2, '0')}`;
}

/** Coarse "time remaining" label: `12s` / `43m` / `2h 5m` / `3d`. */
function fmtUntil(ms: number): string {
  if (ms <= 0) return 'now';
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) {
    const rem = mins % 60;
    return rem ? `${hrs}h ${rem}m` : `${hrs}h`;
  }
  return `${Math.floor(hrs / 24)}d`;
}

/**
 * "Up next" strip (DC-0008): the scheduled workflows as time chips, sorted
 * soonest-first, with a live countdown on the lead chip. Each chip renders the
 * instance's **Run name** (always — DC-0015's primary identity) and deep-links
 * to the parent workflow. "Fetch more" widens the ask by a page; it disables —
 * stays visible, grayed — once a fetch comes back smaller than the ask (the
 * honest "nothing more" state; no count is ever guessed). Only cron workflows
 * with a pre-created scheduled instance appear — fire-now and waits-on-signal
 * are honestly excluded by the substrate, never faked here.
 */
export function DashHeader() {
  const [limit, setLimit] = useState(UPCOMING_PAGE);
  const { data, isLoading, isFetching } = useUpcoming(limit);
  // 1Hz tick drives only the lead chip's countdown — the data itself polls at 30s.
  const now = useNow(1000);

  const chips = data ?? [];
  // A response smaller than the ask means the wheel has nothing further armed —
  // derived, not stored, so the 30s poll keeps it honest.
  const exhausted = data != null && chips.length < limit;

  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
      <span className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Up next
      </span>

      {isLoading && chips.length === 0 ? (
        <span className="text-sm text-muted-foreground">Loading…</span>
      ) : chips.length === 0 ? (
        <span className="text-sm text-muted-foreground">Nothing scheduled.</span>
      ) : (
        <div className="flex flex-wrap items-center gap-2">
          {chips.map((c, i) => {
            const lead = i === 0;
            const untilMs = c.next_run_at ? new Date(c.next_run_at).getTime() - now.getTime() : 0;
            return (
              <Link
                key={c.workflow_instance_id}
                to={`/workflows/${c.workflow_id}`}
                className={cn(
                  'inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-sm transition-colors',
                  lead
                    ? 'border-primary/40 bg-primary/10 text-foreground hover:bg-primary/15'
                    : 'border-border bg-card text-foreground hover:bg-accent',
                )}
                title={`${c.name || c.workflow_name} (${c.workflow_name}) — fires at ${fmtClock(c.next_run_at)}`}
              >
                <span className="font-medium tabular-nums text-muted-foreground">
                  {fmtClock(c.next_run_at)}
                </span>
                <span className="max-w-[14rem] truncate">
                  {c.name || c.workflow_name || 'unnamed'}
                </span>
                {lead && c.next_run_at && (
                  <span className="tabular-nums text-xs font-medium text-primary">
                    in {fmtUntil(untilMs)}
                  </span>
                )}
              </Link>
            );
          })}
          <button
            type="button"
            data-upcoming-more
            disabled={exhausted || isFetching}
            onClick={() => setLimit((l) => l + UPCOMING_PAGE)}
            className={cn(
              'inline-flex items-center rounded-full border px-3 py-1.5 text-sm transition-colors',
              exhausted
                ? 'cursor-not-allowed border-border/60 bg-transparent text-muted-foreground/50'
                : 'border-border bg-card text-muted-foreground hover:bg-accent hover:text-foreground',
            )}
            title={exhausted ? 'No more scheduled runs' : 'Fetch more scheduled runs'}
          >
            {isFetching && !isLoading ? 'Fetching…' : 'Fetch more'}
          </button>
        </div>
      )}
    </div>
  );
}
