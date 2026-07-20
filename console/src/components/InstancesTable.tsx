import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { ChevronRight } from 'lucide-react';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { StateBadge } from '@/components/StateBadge';
import type { WorkflowInstance } from '@/api/client';

/** Terminal run states — everything else is "live" (non-terminal). */
const TERMINAL = new Set(['Completed', 'Failed']);
const pad = (n: number) => (n < 10 ? `0${n}` : `${n}`);

/** Scheduled time as a relative day + clock ("today 14:32" / "3d ago 09:00"). */
function relDay(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const days = Math.floor((Date.now() - d.getTime()) / 864e5);
  const hm = `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  if (days <= 0) return `today ${hm}`;
  if (days === 1) return `yesterday ${hm}`;
  return `${days}d ago ${hm}`;
}

/**
 * The Instances tab's run table — the merged live + archived run list, newest
 * first, with a whole-row drill-in and a visible Workflow-version cell. A
 * non-terminal row carries a `live` tag. (Run number, Trigger provenance and
 * Duration columns await DTO fields a later slice adds — see backlog.)
 */
export function InstancesTable({
  workflowId,
  instances,
  liveOnly,
}: {
  workflowId: string;
  instances: WorkflowInstance[];
  liveOnly: boolean;
}) {
  const navigate = useNavigate();
  const rows = useMemo(() => {
    const sorted = [...instances].sort((a, b) =>
      (b.scheduled_at ?? '').localeCompare(a.scheduled_at ?? ''),
    );
    return liveOnly ? sorted.filter((i) => !TERMINAL.has(i.state)) : sorted;
  }, [instances, liveOnly]);

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Run</TableHead>
          <TableHead>State</TableHead>
          <TableHead>Scheduled</TableHead>
          <TableHead className="text-right">Tasks</TableHead>
          <TableHead>Version</TableHead>
          <TableHead className="text-right" />
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((inst) => {
          const live = !TERMINAL.has(inst.state);
          return (
            <TableRow
              key={inst.id}
              onClick={() => navigate(`/workflows/${workflowId}/instances/${inst.id}`)}
              className="cursor-pointer"
            >
              <TableCell>
                <div className="font-medium">{inst.name}</div>
                <div className="font-mono text-xs text-muted-foreground">
                  {inst.id.slice(0, 8)} {live && <span className="live-tag">live</span>}
                </div>
              </TableCell>
              <TableCell>
                <StateBadge state={inst.state} />
              </TableCell>
              <TableCell className="muted tabular-nums">{relDay(inst.scheduled_at)}</TableCell>
              <TableCell className="text-right tabular-nums">
                {inst.completed_tasks}/{inst.task_count}
              </TableCell>
              <TableCell className="font-mono text-xs text-muted-foreground">
                {inst.workflow_version ? `v${inst.workflow_version}` : '—'}
              </TableCell>
              <TableCell className="text-right">
                <ChevronRight size={15} className="drill-row-chev" />
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}
