import { describe, it, expect } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

// Product rule (honest-unbuilt): unbuilt surfaces are shown but clearly marked,
// and every such placeholder routes through the shared NeedsBackend primitive.
// This guards against a page re-introducing a hand-rolled dashed placeholder
// card — the signature of a per-page fake — instead of using the primitive.
const SRC = join(dirname(fileURLToPath(import.meta.url)), '..');

function walk(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) return walk(full);
    return entry.name.endsWith('.tsx') || entry.name.endsWith('.ts') ? [full] : [];
  });
}

describe('honesty rule: no hand-rolled placeholders', () => {
  it('no page inlines a dashed-border placeholder card', () => {
    const offenders = walk(join(SRC, 'pages')).filter((file) =>
      readFileSync(file, 'utf8').includes('border-dashed'),
    );
    expect(offenders).toEqual([]);
  });

  it('the dashed placeholder lives in the NeedsBackend primitive', () => {
    const primitive = readFileSync(join(SRC, 'components', 'NeedsBackend.tsx'), 'utf8');
    expect(primitive).toContain('border-dashed');
    expect(primitive).toContain('data-needs-backend');
  });
});
