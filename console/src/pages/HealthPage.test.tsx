import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import type { HealthTail } from '@/api/hooks';
import type { HealthDisplay, HealthStatus } from '@/lib/health';
import { HealthPage } from './HealthPage';

vi.mock('@/api/hooks', () => ({ useHealth: vi.fn() }));
import { useHealth } from '@/api/hooks';

const mockUse = vi.mocked(useHealth);

function display(
  over: Partial<Record<keyof HealthDisplay, HealthStatus>> = {},
  implementation: 'postgres' | 'sqlite' = 'postgres',
): HealthDisplay {
  const row = (key: keyof HealthDisplay, fallback: HealthStatus, detail: string) => ({
    status: over[key] ?? fallback,
    detail,
  });
  return {
    api: row('api', 'healthy', 'handler reached; API answering'),
    conductor: row('conductor', 'healthy', 'command-plane-responsive'),
    nats_kv: row('nats_kv', 'healthy', 'kv.status() ok'),
    executors: row('executors', 'degraded', '3 alive · 3/4 slots'),
    data_plane_sql: {
      ...row('data_plane_sql', 'healthy', 'repository reachable; schema compatible'),
      implementation,
    },
    control_plane: row('control_plane', 'healthy', 'control plane up'),
  };
}

function setTail(over: Partial<HealthTail> = {}) {
  mockUse.mockReturnValue({
    display: display(),
    checkedAt: '2026-07-15T14:23:40Z',
    reachable: true,
    isLoading: false,
    recheck: vi.fn(),
    ...over,
  });
}

describe('HealthPage', () => {
  beforeEach(() => mockUse.mockReset());

  it('renders the three sections in topology order', () => {
    setTail();
    render(<HealthPage />);
    const order = Array.from(document.querySelectorAll('[data-section]')).map((e) =>
      e.getAttribute('data-section'),
    );
    expect(order).toEqual(['api', 'data', 'control']);
    // and each section carries its designed title ("Control plane" is also the
    // rollup row's name, so it appears twice — the section header + the row).
    expect(screen.getByText('Data plane')).toBeInTheDocument();
    expect(screen.getAllByText('Control plane').length).toBeGreaterThanOrEqual(1);
  });

  it('renders exactly one Executors pool row reading "N alive · X/Y slots"', () => {
    setTail();
    render(<HealthPage />);
    const rows = document.querySelectorAll('[data-row="executors"]');
    expect(rows).toHaveLength(1);
    expect(rows[0].textContent).toContain('3 alive · 3/4 slots');
  });

  it.each([
    ['postgres', 'Postgres', 'healthy', 'bg-success'],
    ['sqlite', 'SQLite', 'unhealthy', 'bg-destructive'],
  ] as const)(
    'renders the %s implementation and status from the backend-neutral row',
    (implementation, label, status, hue) => {
      setTail({ display: display({ data_plane_sql: status }, implementation) });
      render(<HealthPage />);
      const sql = document.querySelector('[data-row="data_plane_sql"]')!;
      expect(within(sql as HTMLElement).getByText(label)).toBeInTheDocument();
      expect(within(sql as HTMLElement).getByText(status).className).toContain(hue);
      expect(document.querySelector('[data-row="postgres"]')).not.toBeInTheDocument();
    },
  );

  it('shows the endpoint checked_at as the last-checked time', () => {
    setTail();
    render(<HealthPage />);
    expect(screen.getByText(/last checked \d{2}:\d{2}:\d{2}/)).toBeInTheDocument();
  });

  it('dims the data-/control-plane cards (not API) when the API is unhealthy', () => {
    // The cascade: every row below API reads unhealthy, and its card dims.
    setTail({
      display: display({
        api: 'unhealthy',
        conductor: 'unhealthy',
        nats_kv: 'unhealthy',
        executors: 'unhealthy',
        data_plane_sql: 'unhealthy',
        control_plane: 'unhealthy',
      }),
      reachable: false,
    });
    render(<HealthPage />);
    expect(document.querySelector('[data-section="api"]')).not.toHaveAttribute('data-dimmed');
    expect(document.querySelector('[data-section="data"]')).toHaveAttribute('data-dimmed');
    expect(document.querySelector('[data-section="control"]')).toHaveAttribute('data-dimmed');
    // and the unreachable note is surfaced next to Recheck
    expect(screen.getByText(/endpoint unreachable/)).toBeInTheDocument();
  });

  it('re-hits the endpoint when Recheck is clicked', () => {
    const recheck = vi.fn();
    setTail({ recheck });
    render(<HealthPage />);
    fireEvent.click(screen.getByRole('button', { name: /Recheck/ }));
    expect(recheck).toHaveBeenCalledOnce();
  });

  it('shows a loading state before the first reading lands', () => {
    setTail({ display: null, checkedAt: null, isLoading: true });
    render(<HealthPage />);
    expect(screen.getByText('checking…')).toBeInTheDocument();
    expect(screen.queryByText('Data plane')).not.toBeInTheDocument();
  });
});
