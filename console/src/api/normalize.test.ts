import { describe, it, expect } from 'vitest';
import {
  normalizeState,
  clockBucketForState,
  cancelledLabel,
  killConfirmationLabel,
  STATE_LABEL,
} from './normalize';

describe('normalizeState keeps the substrate vocabulary canonical', () => {
  // The detail page reads verbatim state, so Cancelled must stay Cancelled and
  // not collapse to Failed at the canonical layer (folding is dial-only).
  it('preserves Cancelled / Killed / TimedOut as distinct canonical states', () => {
    expect(normalizeState('Cancelled')).toBe('cancelled');
    expect(normalizeState('Killed')).toBe('killed');
    expect(normalizeState('TimedOut')).toBe('timed_out');
    expect(STATE_LABEL.cancelled).toBe('Cancelled');
  });

  // Parked is the canonical state for a loop turn that succeeded but is still
  // turning — surfaced verbatim, not folded into in_progress or completed.
  it('maps Parked to its own canonical state and label', () => {
    expect(normalizeState('Parked')).toBe('parked');
    expect(normalizeState('parked')).toBe('parked');
    expect(STATE_LABEL.parked).toBe('Parked');
  });
});

describe('cancelledLabel maps a CancelReason to a reason-driven label', () => {
  // The state stays `cancelled` (terminal-failure token); only the label
  // varies with the reason so the cause reads without guessing.
  it('renders the reason-specific label for each CancelReason', () => {
    expect(cancelledLabel('User')).toBe('Cancelled');
    expect(cancelledLabel('Dependency')).toBe('Skipped');
    expect(cancelledLabel('External')).toBe('Cancelled (signal)');
    expect(cancelledLabel('Executor')).toBe('Cancelled');
    expect(cancelledLabel('Timeout')).toBe('Timed out');
  });

  it('falls back to the plain Cancelled label for an absent / unknown reason', () => {
    expect(cancelledLabel(null)).toBe(STATE_LABEL.cancelled);
    expect(cancelledLabel(undefined)).toBe(STATE_LABEL.cancelled);
    expect(cancelledLabel('bogus')).toBe(STATE_LABEL.cancelled);
  });
});

describe('killConfirmationLabel renders the kill-confirmation sub-status', () => {
  // Distinct from the terminal state — an operator reads whether a zombie
  // process might still be alive alongside the "Cancelled" label.
  it('maps Confirmed / Unconfirmed to a human suffix', () => {
    expect(killConfirmationLabel('Confirmed')).toBe('kill confirmed');
    expect(killConfirmationLabel('Unconfirmed')).toBe('kill unconfirmed');
  });

  it('renders nothing when no kill was requested', () => {
    expect(killConfirmationLabel(null)).toBeNull();
    expect(killConfirmationLabel(undefined)).toBeNull();
    expect(killConfirmationLabel('bogus')).toBeNull();
  });
});

describe('clockBucketForState folds into the four UI buckets', () => {
  it('folds transient-by-construction states into scheduled', () => {
    expect(clockBucketForState('Scheduled')).toBe('scheduled');
    expect(clockBucketForState('PendingSchedule')).toBe('scheduled');
    expect(clockBucketForState('Triggered')).toBe('scheduled');
  });

  it('folds terminal-not-success states into failed', () => {
    expect(clockBucketForState('Failed')).toBe('failed');
    expect(clockBucketForState('Cancelled')).toBe('failed');
    expect(clockBucketForState('Killed')).toBe('failed');
    expect(clockBucketForState('TimedOut')).toBe('failed');
  });

  it('maps the plain in-progress and completed states', () => {
    expect(clockBucketForState('InProgress')).toBe('in_progress');
    expect(clockBucketForState('Completed')).toBe('completed');
  });

  it('omits states outside the four families', () => {
    expect(clockBucketForState('Queued')).toBeNull();
    expect(clockBucketForState('Skipped')).toBeNull();
    expect(clockBucketForState('whatever')).toBeNull();
    expect(clockBucketForState(null)).toBeNull();
  });
});
