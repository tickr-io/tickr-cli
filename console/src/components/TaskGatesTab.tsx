import { useMemo } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { EmptyState } from '@/components/QueryStates';
import { GateRowsTable } from '@/components/InstanceGatesTab';
import { incidentGateRows } from '@/lib/gates';
import type { InstanceSnapshot, SnapshotTaskInstance } from '@/api/client';

/**
 * The Gates tab: only the gates incident to this task — its `task_id`
 * appears in the HyperEdge's sources or targets — split into "Gated by"
 * (what it waits on) and "Gates downstream" (what it releases). A gate
 * qualifying for both renders in both; the two groups answer different
 * operator questions. Rows are the instance page's gate rows wholesale,
 * projected client-side from the polled snapshot.
 */
export function TaskGatesTab({
  snapshot,
  taskInstance,
}: {
  snapshot: InstanceSnapshot;
  taskInstance: SnapshotTaskInstance;
}) {
  const { gatedBy, gatesDownstream } = useMemo(
    () => incidentGateRows(snapshot, taskInstance.task_id),
    [snapshot, taskInstance.task_id],
  );

  if (gatedBy.length === 0 && gatesDownstream.length === 0) {
    return (
      <Card>
        <CardContent className="p-6">
          <EmptyState
            title="Plain dependencies"
            description="This task's edges carry no gates — they fire on source completion alone."
          />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Card className="border-0 shadow-none">
          <CardHeader className="p-0 pb-1">
            <CardTitle className="text-base">Gated by</CardTitle>
            <CardDescription>What this task waits on before it can start.</CardDescription>
          </CardHeader>
        </Card>
        {gatedBy.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            Nothing — every inbound edge fires on source completion alone.
          </p>
        ) : (
          <GateRowsTable rows={gatedBy} />
        )}
      </div>

      <div className="space-y-2">
        <Card className="border-0 shadow-none">
          <CardHeader className="p-0 pb-1">
            <CardTitle className="text-base">Gates downstream</CardTitle>
            <CardDescription>What this task's completion releases.</CardDescription>
          </CardHeader>
        </Card>
        {gatesDownstream.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            Nothing — no gated edge is fed by this task.
          </p>
        ) : (
          <GateRowsTable rows={gatesDownstream} />
        )}
      </div>
    </div>
  );
}
