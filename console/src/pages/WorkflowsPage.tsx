import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Search } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useWorkflows } from '@/api/hooks';
import { QueryError, TableLoading, EmptyState } from '@/components/QueryStates';
import { StateBadge } from '@/components/StateBadge';
import { TriggerCell } from '@/components/TriggerCell';
import { BuildCell } from '@/components/BuildCell';

export function WorkflowsPage() {
  const { data, isLoading, error, refetch } = useWorkflows();
  const [q, setQ] = useState('');
  const navigate = useNavigate();

  // Client-side over the full payload (workflow definitions are low-cardinality):
  // case-insensitive name+id substring filter, then default alphabetical sort.
  const filtered = useMemo(() => {
    const needle = q.trim().toLowerCase();
    const rows = !needle
      ? (data ?? [])
      : (data ?? []).filter(
          (w) => w.name.toLowerCase().includes(needle) || w.id.toLowerCase().includes(needle),
        );
    return [...rows].sort((a, b) => a.name.localeCompare(b.name));
  }, [data, q]);

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Workflows</h1>
          <p className="text-sm text-muted-foreground">
            All registered workflow definitions ({data?.length ?? 0}).
          </p>
        </div>
        <div className="relative w-72">
          <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search workflows…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            className="pl-9"
          />
        </div>
      </div>

      {error ? (
        <QueryError error={error as Error} onRetry={() => refetch()} />
      ) : (
        <Card>
          <CardContent className="p-0">
            {isLoading ? (
              <div className="p-4">
                <TableLoading rows={8} cols={6} />
              </div>
            ) : filtered.length === 0 ? (
              <div className="p-6">
                <EmptyState
                  title={q ? 'No workflows match' : 'No workflows registered'}
                  description={q ? 'Try a different search term.' : 'Register one to see it here.'}
                />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Trigger</TableHead>
                    <TableHead>Version</TableHead>
                    <TableHead>Build</TableHead>
                    <TableHead>Latest run</TableHead>
                    <TableHead className="text-right">Completed runs</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filtered.map((w) => (
                    <TableRow
                      key={w.id}
                      onClick={() => navigate(`/workflows/${w.id}`)}
                      className="cursor-pointer hover:bg-accent"
                    >
                      <TableCell className="font-medium">
                        {w.name}
                        <div className="font-mono text-xs text-muted-foreground">
                          {w.namespace}.{w.slug}
                        </div>
                        <div className="text-xs text-muted-foreground">{w.id}</div>
                      </TableCell>
                      <TableCell>
                        <TriggerCell trigger={w.trigger} />
                      </TableCell>
                      <TableCell className="font-mono text-sm">
                        {w.version ?? <span className="text-muted-foreground">—</span>}
                      </TableCell>
                      <TableCell>
                        <BuildCell
                          build_status={w.build_status}
                          version={w.version}
                          build_version={w.build_version}
                        />
                      </TableCell>
                      <TableCell>
                        {w.latest_run_state ? (
                          <StateBadge state={w.latest_run_state} />
                        ) : (
                          <span className="text-muted-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">{w.completed_runs}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  );
}
