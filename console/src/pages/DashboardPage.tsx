import { useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { CalendarIcon } from 'lucide-react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from '@/components/ui/sheet';
import { Button } from '@/components/ui/button';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { Calendar } from '@/components/ui/calendar';
import { useDashboardClock } from '@/api/hooks';
import { QueryError, EmptyState } from '@/components/QueryStates';
import { DashHeader } from '@/components/DashHeader';
import { StateBadge } from '@/components/StateBadge';
import { useResizeObserver } from '@/hooks/useResizeObserver';
import {
  Clock,
  bucketsFromInstances,
  instanceSlot,
  type ClockStatus,
  type SeriesDescriptor,
} from '@/charts/clock';
import { clockBucketForState, STATE_LABEL } from '@/api/normalize';
import type { ClockInstance } from '@/api/client';
import { cn } from '@/lib/utils';

// DC-0001 status colors. The dial draws curves in these; the stat list dots
// reuse them. in_progress is info/blue, never the brand teal (--primary).
const STATUS_SERIES: SeriesDescriptor<ClockStatus>[] = [
  { key: 'completed', label: 'Completed', color: 'var(--success)' },
  { key: 'in_progress', label: 'In progress', color: 'var(--info)' },
  { key: 'scheduled', label: 'Scheduled', color: 'var(--warning)' },
  { key: 'failed', label: 'Failed', color: 'var(--destructive)', pattern: 'dashed' },
];

// Stat-list rows, in DC-0008 reading order.
const STAT_ROWS: { key: ClockStatus; label: string; color: string }[] = [
  { key: 'in_progress', label: 'In progress', color: 'var(--info)' },
  { key: 'scheduled', label: 'Scheduled', color: 'var(--warning)' },
  { key: 'completed', label: 'Completed', color: 'var(--success)' },
  { key: 'failed', label: 'Failed', color: 'var(--destructive)' },
];

type ViewMode = 'live' | 'past' | 'future';

function localMidnight(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 0, 0, 0, 0);
}

function classifyDate(selected: Date, today: Date): ViewMode {
  const a = localMidnight(selected).getTime();
  const b = localMidnight(today).getTime();
  if (a === b) return 'live';
  return a < b ? 'past' : 'future';
}

const DATE_LABEL_FMT = new Intl.DateTimeFormat(undefined, {
  weekday: 'short',
  month: 'short',
  day: 'numeric',
  year: 'numeric',
});

interface DecodedSegment {
  half: 'am' | 'pm';
  status: ClockStatus;
  start: number;
  end: number;
}

const CLOCK_STATUSES = new Set<string>(STATUS_SERIES.map((s) => s.key));
function isClockStatus(s: string): s is ClockStatus {
  return CLOCK_STATUSES.has(s);
}

function decodeSegmentId(id: string | null): DecodedSegment | null {
  if (!id) return null;
  const parts = id.split(':');
  if (parts.length !== 3) return null;
  const [half, status, range] = parts;
  if (half !== 'am' && half !== 'pm') return null;
  if (!isClockStatus(status)) return null;
  const dash = range.indexOf('-');
  if (dash <= 0) return null;
  const start = Number(range.slice(0, dash));
  const end = Number(range.slice(dash + 1));
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return { half, status, start, end };
}

/** The instances whose slot and folded status match a clicked dial segment. */
function instancesInSegment(
  instances: ReadonlyArray<ClockInstance>,
  seg: DecodedSegment,
): ClockInstance[] {
  return instances.filter((i) => {
    const slot = instanceSlot(i.scheduled_at);
    if (!slot) return false;
    if (slot.half !== seg.half) return false;
    if (slot.index < seg.start || slot.index > seg.end) return false;
    return clockBucketForState(i.state) === seg.status;
  });
}

function bucketRangeLabel(seg: DecodedSegment): string {
  const baseHour = seg.half === 'am' ? 0 : 12;
  const startHour = baseHour + Math.floor(seg.start / 12);
  const startMin = (seg.start % 12) * 5;
  const endHour = baseHour + Math.floor(seg.end / 12);
  const endMin = (seg.end % 12) * 5 + 5; // half-open
  const fmt = (h: number, m: number) =>
    `${String(h % 24).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
  const carryEndHour = endMin >= 60 ? endHour + 1 : endHour;
  const carryEndMin = endMin >= 60 ? 0 : endMin;
  return `${fmt(startHour, startMin)} – ${fmt(carryEndHour, carryEndMin)}`;
}

/** Local HH:MM for an instance's RFC3339 scheduled_at. */
function localHHMM(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function dotStyle(color: string): React.CSSProperties {
  return color.startsWith('#') ? { backgroundColor: color } : { backgroundColor: `hsl(${color})` };
}

export function DashboardPage() {
  // The clock and the data window are both anchored on the user-selected
  // calendar day (defaults to today, local tz).
  const [selectedDate, setSelectedDate] = useState<Date>(() => localMidnight(new Date()));
  const today = useMemo(() => localMidnight(new Date()), []);
  const viewMode: ViewMode = useMemo(() => classifyDate(selectedDate, today), [selectedDate, today]);
  const isToday = viewMode === 'live';

  // Local-midnight boundaries of the selected day, as unix seconds — the API
  // stays timezone-agnostic, so the browser computes the operator-recognisable
  // calendar day here.
  const { startSeconds, endSeconds } = useMemo(() => {
    const start = Math.floor(localMidnight(selectedDate).getTime() / 1000);
    return { startSeconds: start, endSeconds: start + 24 * 60 * 60 };
  }, [selectedDate]);

  const { data, isLoading, error, refetch } = useDashboardClock({
    startSeconds,
    endSeconds,
    live: isToday,
  });

  const instances = useMemo(() => data?.instances ?? [], [data?.instances]);
  const liveDegraded = !!data && !data.live_data_available;

  // Counts for the four-row stat list, derived from the same merged instance
  // list the dial buckets — so the numbers and the curves never disagree.
  // Toggling a row hides it on the dial but does not change the counts.
  const counts = useMemo(() => {
    const c: Record<ClockStatus, number> = {
      in_progress: 0,
      scheduled: 0,
      completed: 0,
      failed: 0,
    };
    for (const inst of instances) {
      const b = clockBucketForState(inst.state);
      if (b) c[b] += 1;
    }
    return c;
  }, [instances]);

  // Active status filter — clicking a stat row toggles its family on/off the
  // dial. At least one status stays on (the last-one-on guard).
  const [activeStatuses, setActiveStatuses] = useState<Set<ClockStatus>>(
    () => new Set(STATUS_SERIES.map((s) => s.key)),
  );
  const toggleStatus = (key: ClockStatus) => {
    setActiveStatuses((prev) => {
      if (prev.has(key)) {
        if (prev.size === 1) return prev; // last-one-on guard
        const next = new Set(prev);
        next.delete(key);
        return next;
      }
      const next = new Set(prev);
      next.add(key);
      return next;
    });
  };

  const filteredInstances = useMemo(
    () =>
      instances.filter((i) => {
        const b = clockBucketForState(i.state);
        return b !== null && activeStatuses.has(b);
      }),
    [instances, activeStatuses],
  );

  const buckets = useMemo(
    () => bucketsFromInstances(filteredInstances, selectedDate),
    [filteredInstances, selectedDate],
  );

  const clockHostRef = useRef<HTMLDivElement | null>(null);
  const { width: hostWidth, height: hostHeight } = useResizeObserver(clockHostRef);
  const clockSize = Math.max(240, Math.min(560, Math.floor(Math.min(hostWidth, hostHeight) || 0)));

  const [selectedSegmentId, setSelectedSegmentId] = useState<string | null>(null);
  const decoded = useMemo(() => decodeSegmentId(selectedSegmentId), [selectedSegmentId]);
  const matched = useMemo(
    () => (decoded ? instancesInSegment(filteredInstances, decoded) : []),
    [decoded, filteredInstances],
  );

  const [datePopoverOpen, setDatePopoverOpen] = useState(false);
  const dateLabel = isToday ? 'Today' : DATE_LABEL_FMT.format(selectedDate);

  return (
    <div className="space-y-6">
      <DashHeader />

      {error ? (
        <QueryError error={error as Error} onRetry={() => refetch()} />
      ) : (
        <Card className="overflow-hidden">
          <CardHeader className="flex flex-row items-start justify-between gap-3 space-y-0">
            <div>
              <CardTitle className="text-base">{dateLabel}</CardTitle>
              <CardDescription>
                Workflow runs across the day. AM = inner, PM = outer.
              </CardDescription>
            </div>
            <div className="flex items-center gap-2">
              {liveDegraded && (
                <span
                  className="rounded-full border border-warning/40 bg-warning/10 px-2 py-0.5 text-xs text-foreground"
                  title="The live half was unavailable — showing archived runs only."
                >
                  Live data unavailable
                </span>
              )}
              {!isToday && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setSelectedDate(localMidnight(new Date()))}
                >
                  Today
                </Button>
              )}
              <Popover open={datePopoverOpen} onOpenChange={setDatePopoverOpen}>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    size="sm"
                    className="gap-2 font-normal"
                    aria-label="Pick a date"
                  >
                    <CalendarIcon className="size-4" />
                    <span className="tabular-nums">{DATE_LABEL_FMT.format(selectedDate)}</span>
                  </Button>
                </PopoverTrigger>
                <PopoverContent align="end" className="w-auto p-0">
                  <Calendar
                    mode="single"
                    selected={selectedDate}
                    onSelect={(d) => {
                      if (!d) return;
                      setSelectedDate(localMidnight(d));
                      setDatePopoverOpen(false);
                    }}
                    autoFocus
                  />
                </PopoverContent>
              </Popover>
            </div>
          </CardHeader>

          <CardContent>
            {/* today-grid: clock left, stat list right. Collapses to one column below 720px. */}
            <div className="grid grid-cols-1 gap-6 sm:grid-cols-[1fr_minmax(148px,172px)] sm:items-center">
              <div ref={clockHostRef} className="mx-auto aspect-square w-full" style={{ maxWidth: 560 }}>
                {clockSize > 0 && (
                  <Clock
                    am={buckets.am}
                    pm={buckets.pm}
                    series={STATUS_SERIES}
                    size={clockSize}
                    viewMode={viewMode}
                    now={isToday ? undefined : selectedDate}
                    ariaLabel={`Workflow timeline for ${dateLabel}`}
                    selectedSegmentId={selectedSegmentId}
                    onSegmentSelect={setSelectedSegmentId}
                  />
                )}
                {isLoading && (
                  <p className="mt-2 text-center text-xs text-muted-foreground">Loading…</p>
                )}
              </div>

              <ul className="flex flex-col gap-1" aria-label="Status filters">
                {STAT_ROWS.map((row) => {
                  const on = activeStatuses.has(row.key);
                  return (
                    <li key={row.key}>
                      <button
                        type="button"
                        onClick={() => toggleStatus(row.key)}
                        aria-pressed={on}
                        title={`Toggle ${row.label} on the dial`}
                        className={cn(
                          'flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left transition-colors hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                          !on && 'opacity-40',
                        )}
                      >
                        <span
                          className="inline-block size-2.5 shrink-0 rounded-sm"
                          style={dotStyle(row.color)}
                        />
                        <span className={cn('flex-1 text-sm', !on && 'line-through')}>
                          {row.label}
                        </span>
                        <span className="tabular-nums text-sm font-medium">{counts[row.key]}</span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            </div>
          </CardContent>
        </Card>
      )}

      <Sheet open={!!decoded} onOpenChange={(open) => !open && setSelectedSegmentId(null)}>
        <SheetContent className="flex flex-col gap-0 p-0">
          <SheetHeader>
            {decoded ? (
              <>
                <SheetTitle>
                  {STATE_LABEL[decoded.status]} · {bucketRangeLabel(decoded)} (
                  {decoded.half.toUpperCase()})
                </SheetTitle>
                <SheetDescription>
                  {matched.length} run{matched.length === 1 ? '' : 's'} in this slot.
                </SheetDescription>
              </>
            ) : (
              <SheetTitle>Segment</SheetTitle>
            )}
          </SheetHeader>
          <div className="flex-1 overflow-y-auto px-2 py-2">
            {matched.length === 0 ? (
              <div className="px-4 py-8">
                <EmptyState
                  title="No runs match"
                  description="No instances fell in this exact hour-and-status slot."
                />
              </div>
            ) : (
              <ul className="divide-y divide-border">
                {matched.map((inst) => (
                  <li key={inst.id}>
                    <Link
                      to={`/workflows/${inst.workflow_id}`}
                      onClick={() => setSelectedSegmentId(null)}
                      className="flex items-center justify-between gap-3 rounded-md px-4 py-3 hover:bg-accent"
                    >
                      <div className="min-w-0">
                        <div className="truncate font-medium">{inst.workflow_name || 'unnamed'}</div>
                        <div className="truncate font-mono text-xs text-muted-foreground">
                          {inst.id}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="tabular-nums text-xs text-muted-foreground">
                          {localHHMM(inst.scheduled_at)}
                        </span>
                        <StateBadge state={inst.state} />
                      </div>
                    </Link>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </SheetContent>
      </Sheet>
    </div>
  );
}
