import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { NeedsBackend } from './NeedsBackend';

describe('NeedsBackend', () => {
  it('names the surface, the need, and the exact missing endpoint', () => {
    render(
      <NeedsBackend
        surface="Event log"
        need="A cursor-paged stream of system events."
        endpoint="GET /api/events?after=<cursor>"
      />,
    );
    expect(screen.getByText('Event log')).toBeInTheDocument();
    expect(screen.getByText('A cursor-paged stream of system events.')).toBeInTheDocument();
    expect(screen.getByText('GET /api/events?after=<cursor>')).toBeInTheDocument();
  });

  it('renders as a dashed-border placeholder marked for the honesty rule', () => {
    const { container } = render(<NeedsBackend surface="Health" endpoint="GET /api/health" />);
    const card = container.querySelector('[data-needs-backend]');
    expect(card).not.toBeNull();
    expect(card!.className).toContain('border-dashed');
  });

  it('supplies a default need line when none is given', () => {
    render(<NeedsBackend surface="Health" endpoint="GET /api/health" />);
    expect(screen.getByText(/needs backend support/i)).toBeInTheDocument();
  });
});
