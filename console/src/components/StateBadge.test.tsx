import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { StateBadge } from './StateBadge';
import {
  STATE_BADGE,
  STATE_LABEL,
  STATE_TOKEN,
  type CanonicalState,
} from '@/api/normalize';

// DC-0001 status color semantics. Each canonical state maps to one badge variant
// (its color token) and one sentence-case label. A representative raw input is
// listed so we exercise StateBadge end-to-end through normalizeState.
const CASES: Array<{
  state: CanonicalState;
  raw: string;
  label: string;
  classFragment: string;
}> = [
  { state: 'scheduled', raw: 'Scheduled', label: 'Scheduled', classFragment: 'bg-warning' },
  { state: 'pending', raw: 'Pending', label: 'Pending', classFragment: 'bg-secondary' },
  { state: 'queued', raw: 'Queued', label: 'Queued', classFragment: 'bg-secondary' },
  { state: 'in_progress', raw: 'InProgress', label: 'In progress', classFragment: 'bg-info' },
  { state: 'completed', raw: 'Completed', label: 'Completed', classFragment: 'bg-success' },
  { state: 'failed', raw: 'Failed', label: 'Failed', classFragment: 'bg-destructive' },
  { state: 'killed', raw: 'Killed', label: 'Killed', classFragment: 'bg-destructive' },
  // timed_out is a terminal failure — red, matching the day-clock fold and the
  // graph/timeline hue (all derive from the one STATE_TOKEN row).
  { state: 'timed_out', raw: 'TimedOut', label: 'Timed out', classFragment: 'bg-destructive' },
  // Neutral (absence-of-status) states resolve to the slate secondary fill.
  { state: 'skipped', raw: 'Skipped', label: 'Skipped', classFragment: 'bg-secondary' },
  { state: 'unknown', raw: 'unknown', label: 'Unknown', classFragment: 'bg-secondary' },
];

describe('DC-0001 status map', () => {
  it('maps in-progress to info (blue), never the brand accent', () => {
    expect(STATE_BADGE.in_progress).toBe('info');
    expect(STATE_BADGE.in_progress).not.toBe('default');
  });

  it('maps scheduled to warning (amber) and completed/failed to their hues', () => {
    expect(STATE_BADGE.scheduled).toBe('warning');
    expect(STATE_BADGE.completed).toBe('success');
    expect(STATE_BADGE.failed).toBe('destructive');
    expect(STATE_BADGE.killed).toBe('destructive');
  });

  it('colours timed_out as a terminal failure (red), matching the day-clock fold', () => {
    expect(STATE_BADGE.timed_out).toBe('destructive');
    expect(STATE_TOKEN.timed_out).toBe('destructive');
  });

  it('derives the badge variant from the one STATE_TOKEN table — no second state→colour map', () => {
    // Every state's badge variant must be its STATE_TOKEN resolved through the
    // token→variant map, so the badge and the graph/timeline hue cannot drift.
    const TOKEN_BADGE = {
      success: 'success',
      info: 'info',
      warning: 'warning',
      destructive: 'destructive',
      neutral: 'secondary',
    } as const;
    for (const state of Object.keys(STATE_TOKEN) as CanonicalState[]) {
      expect(STATE_BADGE[state]).toBe(TOKEN_BADGE[STATE_TOKEN[state]]);
    }
  });
});

describe('StateBadge', () => {
  it.each(CASES)('renders $state with its label and color', ({ raw, label, classFragment }) => {
    const { unmount } = render(<StateBadge state={raw} />);
    const badge = screen.getByText(label);
    expect(badge).toBeInTheDocument();
    expect(badge.className).toContain(classFragment);
    unmount();
  });

  it('falls back to Unknown for an unrecognised state', () => {
    render(<StateBadge state="totally-bogus" />);
    expect(screen.getByText(STATE_LABEL.unknown)).toBeInTheDocument();
  });

  // A cancelled task always wears the terminal-failure token (red); the LABEL
  // varies with the CancelReason so an operator reads the exact cause.
  const CANCEL_CASES: Array<{ reason: string; label: string }> = [
    { reason: 'User', label: 'Cancelled' },
    { reason: 'Dependency', label: 'Skipped' },
    { reason: 'External', label: 'Cancelled (signal)' },
    { reason: 'Executor', label: 'Cancelled' },
    { reason: 'Timeout', label: 'Timed out' },
  ];
  it.each(CANCEL_CASES)(
    'renders a Cancelled task with the $reason-driven label in the terminal-failure hue',
    ({ reason, label }) => {
      const { unmount } = render(<StateBadge state="Cancelled" reason={reason} />);
      const badge = screen.getByText(label);
      expect(badge).toBeInTheDocument();
      expect(badge.className).toContain('bg-destructive');
      unmount();
    },
  );

  it('falls back to the plain Cancelled label when no reason is supplied', () => {
    render(<StateBadge state="Cancelled" />);
    const badge = screen.getByText('Cancelled');
    expect(badge).toBeInTheDocument();
    expect(badge.className).toContain('bg-destructive');
  });
});
