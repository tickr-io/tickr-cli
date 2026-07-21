import { format } from 'date-fns';
import { RefreshCw } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from '@/lib/utils';
import { useHealth } from '@/api/hooks';
import {
  HEALTH_BADGE,
  HEALTH_DOT_BG,
  HEALTH_ROWS,
  HEALTH_SECTIONS,
  cardsDimmed,
  healthRowName,
  type DisplayRow,
  type HealthDisplay,
  type HealthSection,
} from '@/lib/health';

/** A row's status dot: 9px circle in the status hue; a healthy dot pulses a
 * green ring (the "alive" cue), amber/red stay static. */
function StatusDot({ status }: { status: DisplayRow['status'] }) {
  return (
    <span
      aria-hidden
      className={cn(
        'inline-block h-2.5 w-2.5 rounded-full',
        HEALTH_DOT_BG[status],
        status === 'healthy' && 'health-dot-pulse',
      )}
    />
  );
}

/** One component row: status dot + name + live detail + status badge. */
function HealthRow({ name, row, rowKey }: { name: string; row: DisplayRow; rowKey: string }) {
  return (
    <div
      data-row={rowKey}
      data-status={row.status}
      className="grid grid-cols-[16px_1fr_auto] items-center gap-3 border-b border-border px-4 py-3 last:border-b-0"
    >
      <StatusDot status={row.status} />
      <span className="text-sm font-medium">
        {name}
        <span className="ml-2 text-xs font-normal text-muted-foreground">{row.detail}</span>
      </span>
      <Badge variant={HEALTH_BADGE[row.status]}>{row.status}</Badge>
    </div>
  );
}

/** One section card (API / Data plane / Control plane). Data- and control-plane
 * cards dim when the API row is unhealthy — the API gates everything below. */
function SectionCard({
  section,
  title,
  caption,
  display,
  dim,
}: {
  section: HealthSection;
  title: string;
  caption: string;
  display: HealthDisplay;
  dim: boolean;
}) {
  const rows = HEALTH_ROWS.filter((r) => r.section === section);
  return (
    <Card
      data-section={section}
      data-dimmed={dim || undefined}
      className={cn('overflow-hidden transition-opacity', dim && 'opacity-50')}
    >
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <span className="text-sm font-semibold">{title}</span>
        <span className="text-xs text-muted-foreground">{caption}</span>
      </div>
      {rows.map((spec) => (
        <HealthRow
          key={spec.key}
          rowKey={spec.key}
          name={healthRowName(spec, display[spec.key])}
          row={display[spec.key]}
        />
      ))}
    </Card>
  );
}

function LoadingCards() {
  return (
    <div className="space-y-3">
      {[0, 1, 2].map((i) => (
        <Skeleton key={i} className="h-24 w-full" />
      ))}
    </div>
  );
}

/**
 * The operator Health surface (DC-0013). Reads the real `GET /api/health` and
 * renders three sections mirroring the topology — API → Data plane → Control
 * plane — each row a status dot + name + detail + badge, with healthy = green
 * (pulsing), degraded = amber, unhealthy = red.
 *
 * Two behaviors the endpoint deliberately does not own live in the hook, not
 * here: the API-gate cascade (endpoint unreachable ⇒ every row below unhealthy,
 * cards dim) and the 2-consecutive-read debounce (a transient NATS blip never
 * flickers the page). The endpoint is raw-instantaneous and holds no history;
 * hysteresis needs history, so it lives in the UI.
 */
export function HealthPage() {
  const { display, checkedAt, reachable, isLoading, recheck } = useHealth();
  const dim = cardsDimmed(display);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold tracking-tight">Health</h1>
        <p className="text-sm text-muted-foreground">
          Component status mirroring how the UI reaches the system: API → Data plane → Control
          plane. The API gates everything — the UI only sees the system through it.
        </p>
      </div>

      <div className="flex items-center gap-3 text-xs text-muted-foreground">
        <Button size="sm" variant="outline" onClick={recheck} disabled={isLoading}>
          <RefreshCw size={13} className="mr-1.5" aria-hidden />
          Recheck
        </Button>
        {checkedAt ? (
          <span>last checked {format(new Date(checkedAt), 'HH:mm:ss')}</span>
        ) : (
          <span>checking…</span>
        )}
        {!reachable && <span className="font-medium text-destructive">· endpoint unreachable</span>}
      </div>

      {display === null ? (
        <LoadingCards />
      ) : (
        <div className="space-y-3">
          {HEALTH_SECTIONS.map((s) => (
            <SectionCard
              key={s.section}
              section={s.section}
              title={s.title}
              caption={s.caption}
              display={display}
              // Only the data-/control-plane cards dim; the API card is the gate itself.
              dim={s.section !== 'api' && dim}
            />
          ))}
        </div>
      )}
    </div>
  );
}
