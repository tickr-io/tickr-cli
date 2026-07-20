import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { TriggerCell } from './TriggerCell';
import type { Trigger } from '@/api/client';

// Trigger-aware cell shared by the list page and the detail header: one
// icon+label per Trigger variant, discriminated on the canonical serde shape.
// Non-cron triggers never render a fabricated clock time.
describe('TriggerCell', () => {
  it('renders cron as the Calendar icon + the expression (tabular-nums)', () => {
    const trigger: Trigger = { Cron: '0 9 * * *' };
    render(<TriggerCell trigger={trigger} />);
    expect(screen.getByLabelText('cron schedule')).toBeInTheDocument();
    const expr = screen.getByText('0 9 * * *');
    expect(expr).toBeInTheDocument();
    expect(expr.className).toContain('tabular-nums');
  });

  it('renders waits-on-signal as the Radio glyph + "on signal" (muted)', () => {
    const trigger: Trigger = { WaitsOnSignal: { signal_name: 'user-paid', predicate: null } };
    render(<TriggerCell trigger={trigger} />);
    expect(screen.getByLabelText('waits on signal')).toBeInTheDocument();
    expect(screen.getByText('on signal')).toBeInTheDocument();
    expect(screen.queryByText(/\d{1,2}:\d{2}/)).not.toBeInTheDocument();
  });

  it('renders fire-now as the Zap icon + "fire-now" (muted)', () => {
    const trigger: Trigger = 'FireNow';
    render(<TriggerCell trigger={trigger} />);
    expect(screen.getByLabelText('fire-now')).toBeInTheDocument();
    expect(screen.getByText('fire-now')).toBeInTheDocument();
  });
});
