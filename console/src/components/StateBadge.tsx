import { Badge } from './ui/badge';
import { cancelledLabel, normalizeState, STATE_BADGE, STATE_LABEL } from '@/api/normalize';

/**
 * `reason` is the task's `cancel_reason` (a `CancelReason` string) when the
 * state is `Cancelled`. It only drives the LABEL — the badge variant stays the
 * state's canonical token — so a `Dependency` cancel reads "Skipped" and a
 * `Timeout` cancel reads "Timed out" while both keep the terminal-failure hue.
 */
export function StateBadge({
  state,
  reason,
}: {
  state: string | null | undefined;
  reason?: string | null;
}) {
  const c = normalizeState(state);
  const label = c === 'cancelled' ? cancelledLabel(reason) : STATE_LABEL[c];
  return <Badge variant={STATE_BADGE[c]}>{label}</Badge>;
}
