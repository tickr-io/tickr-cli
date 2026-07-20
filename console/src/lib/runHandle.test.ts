import { describe, it, expect } from 'vitest';
import { formatRunHandle, runHandleSource } from './runHandle';

describe('runHandleSource', () => {
  it('prefers scheduled_at', () => {
    expect(
      runHandleSource({
        scheduled_at: 'A',
        triggered_at: 'B',
        transitions: [{ at: 'C' }],
      }),
    ).toBe('A');
  });

  it('falls back to triggered_at, then the first transition', () => {
    expect(runHandleSource({ scheduled_at: null, triggered_at: 'B' })).toBe('B');
    expect(
      runHandleSource({ scheduled_at: null, triggered_at: null, transitions: [{ at: 'C' }] }),
    ).toBe('C');
    expect(runHandleSource({ scheduled_at: null, triggered_at: null, transitions: [] })).toBeNull();
  });
});

describe('formatRunHandle', () => {
  it('returns null for missing or unparseable input', () => {
    expect(formatRunHandle(null)).toBeNull();
    expect(formatRunHandle(undefined)).toBeNull();
    expect(formatRunHandle('not-a-date')).toBeNull();
  });

  it('formats an ISO timestamp as YYYY-MM-DD HH:MM:SS with a UTC offset', () => {
    const handle = formatRunHandle('2026-06-12T09:30:05Z');
    expect(handle).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{2}:\d{2}$/);
  });

  it('round-trips the instant: parsing the handle back yields the same epoch', () => {
    const iso = '2026-06-12T09:30:05Z';
    const handle = formatRunHandle(iso)!;
    // Reconstruct an ISO-ish string the Date parser accepts.
    const [date, time, offset] = handle.split(' ');
    expect(new Date(`${date}T${time}${offset}`).getTime()).toBe(new Date(iso).getTime());
  });
});
