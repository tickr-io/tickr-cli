import type { components } from '@/api/types.gen';

type BuildStatus = components['schemas']['Workflow']['build_status'];

/**
 * Renders a workflow's latest-registration build outcome (DC-0014): a small
 * colored bullet + a Sentence-case label, with a parenthetical version for
 * non-Ready states.
 *   - Ready       → solid --success bullet, "Ready" (no parenthetical)
 *   - Building     → pulsating --info bullet, "Building (vX)"
 *   - BuildFailed  → solid --destructive bullet, "Failed (vX)"
 *
 * The bullet shares the DC-0001 status color tokens with the Latest-run
 * StateBadge; build outcome and run outcome are independent facts, so the same
 * hue appearing on both cells of a row is honest signal, not a conflict.
 *
 * `showVersion` gates the `(vX)` parenthetical. The list page leaves it on (its
 * Build cell can report a different version than the Version column). The detail
 * page's meta-grid header turns it off in explicit-pick mode, where the Build
 * cell always equals the Version cell, so the suffix would be redundant.
 */
export function BuildCell({
  build_status,
  build_version,
  showVersion = true,
}: {
  build_status: BuildStatus;
  /** Live version — accepted for prop-shape parity; not rendered here. */
  version?: number | null;
  build_version?: number | null;
  showVersion?: boolean;
}) {
  const v = build_version ?? '';
  const suffix = showVersion ? ` (v${v})` : '';

  let dotClass: string;
  let label: string;
  switch (build_status) {
    case 'Ready':
      dotClass = 'bg-success';
      label = 'Ready';
      break;
    case 'Building':
      dotClass = 'bg-info animate-pulse';
      label = `Building${suffix}`;
      break;
    case 'BuildFailed':
    default:
      dotClass = 'bg-destructive';
      label = `Failed${suffix}`;
      break;
  }

  return (
    <span className="inline-flex items-center gap-2">
      <span className={`inline-block size-2 rounded-full ${dotClass}`} aria-hidden="true" />
      <span>{label}</span>
    </span>
  );
}
