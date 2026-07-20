/**
 * The one canonical projection of a task's `mkRoutingVar` declarations to their
 * names. Every surface that lists routing variables — the detail-page task
 * graph, the Definition tab, and the live instance graph — resolves through
 * here, so the wire's object-shaped decls (`{ name, var_type }`) can never be
 * re-projected inline two different ways again. Re-projecting them as bare
 * strings (and rendering a decl object straight as a React child) is what
 * crashed the detail tabs; one shared helper makes that divergence
 * unrepresentable. Tolerant of the legacy string-shaped form so older callers
 * and fixtures keep resolving.
 */
export function routingVarNames(decls: unknown): string[] {
  if (!Array.isArray(decls)) return [];
  const names: string[] = [];
  for (const d of decls) {
    if (typeof d === 'string') {
      names.push(d);
    } else if (d && typeof d === 'object' && typeof (d as { name?: unknown }).name === 'string') {
      names.push((d as { name: string }).name);
    }
  }
  return names;
}
