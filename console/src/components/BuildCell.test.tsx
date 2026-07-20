import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BuildCell } from './BuildCell';

// DC-0014 Build cell: bullet color token + Sentence-case label, with a
// parenthetical version for non-Ready states. The bullet is the element
// immediately before the label span.
function bullet(container: HTMLElement): HTMLElement {
  return container.querySelector('span > span[aria-hidden="true"]') as HTMLElement;
}

describe('BuildCell', () => {
  it('Ready → success bullet, "Ready", no parenthetical', () => {
    const { container } = render(
      <BuildCell build_status="Ready" version={3} build_version={3} />,
    );
    expect(screen.getByText('Ready')).toBeInTheDocument();
    expect(screen.queryByText(/\(v/)).not.toBeInTheDocument();
    expect(bullet(container).className).toContain('bg-success');
  });

  it('Building → info bullet, pulsating, "Building (v…)"', () => {
    const { container } = render(
      <BuildCell build_status="Building" version={3} build_version={4} />,
    );
    expect(screen.getByText('Building (v4)')).toBeInTheDocument();
    const b = bullet(container);
    expect(b.className).toContain('bg-info');
    expect(b.className).toContain('animate-pulse');
  });

  it('BuildFailed → destructive bullet, "Failed (v…)"', () => {
    const { container } = render(
      <BuildCell build_status="BuildFailed" version={3} build_version={4} />,
    );
    expect(screen.getByText('Failed (v4)')).toBeInTheDocument();
    expect(bullet(container).className).toContain('bg-destructive');
  });

  it('first-ever build in flight → "Building (v…)" with null live version', () => {
    render(<BuildCell build_status="Building" version={null} build_version={1} />);
    expect(screen.getByText('Building (v1)')).toBeInTheDocument();
  });

  it('first-ever build failed → "Failed (v…)" with null live version', () => {
    render(<BuildCell build_status="BuildFailed" version={null} build_version={1} />);
    expect(screen.getByText('Failed (v1)')).toBeInTheDocument();
  });
});
