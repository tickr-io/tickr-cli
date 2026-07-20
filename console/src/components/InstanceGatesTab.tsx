import { useMemo } from 'react';
import { Radio, Clock, Diamond, MoveRight } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Badge, type BadgeProps } from '@/components/ui/badge';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { EmptyState } from '@/components/QueryStates';
import { buildGateRows, formatRoutingValue, type GateRowModel } from '@/lib/gates';
import type { InstanceSnapshot } from '@/api/client';

/** DC-0004 type glyphs: signal/radio, timer/clock, predicate/diamond. */
function KindGlyph({ kind }: { kind: GateRowModel['kind'] }) {
  const Icon = kind === 'signal' ? Radio : kind === 'timer' ? Clock : Diamond;
  return (
    <span className="inline-flex items-center gap-1.5 text-muted-foreground">
      <Icon size={13} aria-hidden />
      {kind}
    </span>
  );
}

const STATE_VARIANT: Record<string, BadgeProps['variant']> = {
  Idle: 'secondary',
  Dispatched: 'warning',
  Satisfied: 'success',
  Rejected: 'destructive',
  Cancelled: 'outline',
};

function StateCell({ row }: { row: GateRowModel }) {
  return (
    <div className="space-y-1">
      <Badge variant={STATE_VARIANT[row.state] ?? 'outline'}>
        {row.state}
        {row.stateCopy ? ` — ${row.stateCopy}` : ''}
      </Badge>
      {row.annotation && (
        <p className="max-w-56 text-xs text-muted-foreground">{row.annotation}</p>
      )}
    </div>
  );
}

/**
 * The gate-rows table — the one rendering of a gate row (declared
 * expression, current value with awaiting-producer copy, will-reject /
 * no-timeout annotations). The instance page's Gates tab and the task
 * page's incident-gate groups both render through this, so the
 * doomed-branch detector reads identically wherever the operator meets it.
 */
export function GateRowsTable({ rows }: { rows: GateRowModel[] }) {
  return (
    <Card>
      <CardContent className="p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Edge</TableHead>
              <TableHead>Type</TableHead>
              <TableHead>Expression</TableHead>
              <TableHead>Current value</TableHead>
              <TableHead>State</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row) => (
              <TableRow key={row.key} data-gate-row>
                <TableCell>
                  <span className="inline-flex flex-wrap items-center gap-1.5 text-sm">
                    {row.sources.join(', ')}
                    <MoveRight size={13} aria-hidden className="text-muted-foreground" />
                    {row.targets.join(', ')}
                  </span>
                </TableCell>
                <TableCell>
                  <KindGlyph kind={row.kind} />
                </TableCell>
                <TableCell className="font-mono text-xs">{row.expression}</TableCell>
                <TableCell className="text-xs">
                  {row.kind !== 'predicate' ? (
                    <span className="text-muted-foreground">—</span>
                  ) : row.currentValue ? (
                    <span className="font-mono">{formatRoutingValue(row.currentValue)}</span>
                  ) : (
                    <span className="text-muted-foreground italic">
                      not yet produced — awaiting{' '}
                      {row.awaitingProducer ? <em>{row.awaitingProducer}</em> : 'its producer'}
                    </span>
                  )}
                </TableCell>
                <TableCell>
                  <StateCell row={row} />
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

/**
 * The Gates tab: every gate on every HyperEdge of the instance, exactly
 * once, with its declared Expression and (for predicate gates) the Current
 * value joined client-side from the snapshot's routing variables. Reads
 * only the polled snapshot.
 */
export function InstanceGatesTab({ snapshot }: { snapshot: InstanceSnapshot }) {
  const rows = useMemo(() => buildGateRows(snapshot), [snapshot]);

  if (rows.length === 0) {
    return (
      <Card>
        <CardContent className="p-6">
          <EmptyState
            title="No gates on this instance"
            description="Every edge fires on source completion alone — nothing is waiting on a signal, timer, or predicate."
          />
        </CardContent>
      </Card>
    );
  }

  return <GateRowsTable rows={rows} />;
}
