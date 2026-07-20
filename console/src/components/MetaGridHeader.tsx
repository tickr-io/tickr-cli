import type { WorkflowDetail, Trigger } from '@/api/client';
import type { components } from '@/api/types.gen';
import { TriggerCell } from './TriggerCell';
import { BuildCell } from './BuildCell';
import { VersionPicker } from './VersionPicker';
import { StateBadge } from './StateBadge';

type BuildStatus = components['schemas']['Workflow']['build_status'];

function toBuildStatus(raw: string): BuildStatus {
  if (raw === 'Ready' || raw === 'Submitted') return 'Ready';
  if (raw === 'Building') return 'Building';
  return 'BuildFailed';
}

function Cell({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="ov-cell">
      <div className="ov-k">{label}</div>
      <div className="ov-v">{children}</div>
    </div>
  );
}

/**
 * The Workflow detail page's five-cell meta grid (DC-0010), laid out as the
 * kit's `ov-grid.wf-meta`: Trigger · Version · Build · Latest run · Completed runs.
 *
 * Trigger and Version are picker-scoped; Latest run and Completed runs are
 * workflow-aggregate and stay invariant as the picker moves. The Build cell is
 * two-mode (see below).
 */
export function MetaGridHeader({
  detail,
  hasExplicitVersion,
  onVersionChange,
}: {
  detail: WorkflowDetail;
  hasExplicitVersion: boolean;
  onVersionChange: (version: number) => void;
}) {
  const def = detail.workflow_definition as Record<string, unknown>;
  const trigger = def.trigger as Trigger | undefined;

  const latestReg = detail.available_versions[0];
  const reported = hasExplicitVersion
    ? detail.available_versions.find((v) => v.version === detail.version) ?? latestReg
    : latestReg;
  const reportedVersion = reported?.version ?? detail.version;
  const reportedStatus = reported?.status ?? 'Building';

  return (
    <div className="ov-grid wf-meta">
      <Cell label="Trigger">
        {trigger ? <TriggerCell trigger={trigger} /> : <span className="muted">—</span>}
      </Cell>
      <Cell label="Version">
        <VersionPicker
          currentVersion={detail.version}
          availableVersions={detail.available_versions}
          onChange={onVersionChange}
        />
      </Cell>
      <Cell label="Build">
        <BuildCell
          build_status={toBuildStatus(reportedStatus)}
          build_version={reportedVersion}
          showVersion={reportedVersion !== detail.version}
        />
      </Cell>
      <Cell label="Latest run">
        {detail.latest_run_state ? <StateBadge state={detail.latest_run_state} /> : <span className="muted">—</span>}
      </Cell>
      <Cell label="Completed runs">
        <span className="tabular-nums">{detail.completed_runs}</span>
      </Cell>
    </div>
  );
}
