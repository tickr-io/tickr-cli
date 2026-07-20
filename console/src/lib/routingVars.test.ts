import { describe, it, expect } from 'vitest';
import { routingVarNames } from './routingVars';

describe('routingVarNames', () => {
  it('projects object-shaped wire decls to their names', () => {
    expect(
      routingVarNames([
        { name: 'region', var_type: 'string' },
        { name: 'turns', var_type: 'int' },
      ]),
    ).toEqual(['region', 'turns']);
  });

  it('tolerates the legacy string-shaped form', () => {
    expect(routingVarNames(['x', 'y'])).toEqual(['x', 'y']);
  });

  it('returns an empty list for missing / non-array input', () => {
    expect(routingVarNames(undefined)).toEqual([]);
    expect(routingVarNames(null)).toEqual([]);
    expect(routingVarNames({})).toEqual([]);
  });

  it('drops malformed entries rather than leaking an object', () => {
    expect(routingVarNames([{ var_type: 'string' }, 42, { name: 'ok' }])).toEqual(['ok']);
  });
});
