import type {PluginOptions as DocsOptions} from '@docusaurus/plugin-content-docs';

type DocusaurusVersion = NonNullable<DocsOptions['versions']>[string];

export type ReleaseSupport = 'supported' | 'archived';

export interface ReleaseLine {
  /** Docusaurus version key. Use `current` until the line is snapshotted. */
  version: string;
  /** User-facing compatibility line, never an individual patch tag. */
  label: string;
  support: ReleaseSupport;
  path: string;
}

export const currentRelease: ReleaseLine = {
  version: 'current',
  label: '0.1',
  support: 'supported',
  path: '',
};

/**
 * Keep retired lines buildable at their stable URLs. Docusaurus supplies the
 * persistent warning banner; `noIndex` keeps retired instructions out of
 * external search indexes. Supported lines remain the only versions linked by
 * the primary site navigation.
 */
export function versionConfig(release: ReleaseLine): DocusaurusVersion {
  const archived = release.support === 'archived';

  return {
    label: release.label,
    path: release.path,
    banner: archived ? 'unmaintained' : 'none',
    noIndex: archived,
  };
}
