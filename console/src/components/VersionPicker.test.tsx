import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import { VersionPicker } from './VersionPicker';
import type { AvailableVersion } from '@/api/client';

const versions: AvailableVersion[] = [
  { version: 3, status: 'Building', inserted_at: '2026-01-03T00:00:00Z' },
  { version: 2, status: 'Submitted', inserted_at: '2026-01-02T00:00:00Z' },
  { version: 1, status: 'BuildFailed', inserted_at: '2026-01-01T00:00:00Z' },
];

describe('VersionPicker', () => {
  it('shows the current version on the trigger', () => {
    render(<VersionPicker currentVersion={2} availableVersions={versions} onChange={vi.fn()} />);
    expect(screen.getByLabelText('select version')).toHaveTextContent('v2');
  });

  it('lists every version with its status when opened', () => {
    render(<VersionPicker currentVersion={2} availableVersions={versions} onChange={vi.fn()} />);
    fireEvent.click(screen.getByLabelText('select version'));
    const options = screen.getAllByRole('option');
    expect(options).toHaveLength(3);
    expect(within(options[0]).getByText('v3')).toBeInTheDocument();
    expect(within(options[0]).getByText('Building')).toBeInTheDocument();
    expect(within(options[1]).getByText('Submitted')).toBeInTheDocument();
    expect(within(options[2]).getByText('BuildFailed')).toBeInTheDocument();
    // The current version is marked selected.
    expect(options[1]).toHaveAttribute('aria-selected', 'true');
  });

  it('invokes onChange with the picked version', () => {
    const onChange = vi.fn();
    render(<VersionPicker currentVersion={2} availableVersions={versions} onChange={onChange} />);
    fireEvent.click(screen.getByLabelText('select version'));
    fireEvent.click(within(screen.getAllByRole('option')[0]).getByText('v3'));
    expect(onChange).toHaveBeenCalledWith(3);
  });
});
