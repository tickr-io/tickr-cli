import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type * as OpenApiPlugin from 'docusaurus-plugin-openapi-docs';
import {themes as prismThemes} from 'prism-react-renderer';
import {currentRelease, versionConfig} from './release-support';


const config: Config = {
  title: 'Tickr Documentation',
  tagline: 'Author workflows. Operate the data plane. Understand every run.',
  favicon: 'img/favicon.svg',
  url: process.env.DOCS_URL ?? 'https://tickr-io.github.io',
  baseUrl: process.env.DOCS_BASE_URL ?? '/tickr-cli/',
  organizationName: 'tickr-io',
  projectName: 'tickr-cli',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,
  onBrokenLinks: 'throw',
  markdown: {
    mermaid: true,
  },
  themes: [
    '@docusaurus/theme-mermaid',
    'docusaurus-theme-openapi-docs',
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        language: ['en'],
        indexBlog: false,
        indexDocs: true,
        indexPages: true,
        docsRouteBasePath: '/docs',
        searchBarShortcutHint: false,
      },
    ],
  ],
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          docItemComponent: '@theme/ApiItem',
          editUrl: 'https://github.com/tickr-io/tickr-cli/tree/main/docs-site/',
          showLastUpdateAuthor: false,
          showLastUpdateTime: true,
          breadcrumbs: true,
          lastVersion: 'current',
          versions: {
            current: versionConfig(currentRelease),
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],
  plugins: [
    'docusaurus-plugin-sass',
    [
      'docusaurus-plugin-openapi-docs',
      {
        id: 'tickr-api',
        docsPluginId: 'classic',
        config: {
          tickr: {
            specPath: '../console/openapi.yaml',
            outputDir: 'docs/api',
            hideSendButton: true,
            showSchemas: true,
            sidebarOptions: {
              groupPathsBy: 'tag',
              categoryLinkSource: 'info',
            },
          } satisfies OpenApiPlugin.Options,
        },
      },
    ],
  ],
  themeConfig: {
    image: 'img/social-card.svg',
    colorMode: {
      defaultMode: 'light',
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'tickr docs',
      hideOnScroll: false,
      logo: {
        alt: 'Tickr',
        src: 'img/mark.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Guides',
        },
        {
          to: '/docs/api/tickr-api',
          label: 'HTTP API',
          position: 'left',
        },
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownItemsAfter: [
            {
              to: '/docs/reference/release-support',
              label: 'Release support policy',
            },
          ],
        },
        {
          href: 'https://github.com/tickr-io/tickr-cli',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Use Tickr',
          items: [
            {label: 'Get started', to: '/docs/get-started'},
            {label: 'Author workflows', to: '/docs/author'},
            {label: 'Operate Tickr', to: '/docs/operate'},
          ],
        },
        {
          title: 'Reference',
          items: [
            {label: 'Core DSL', to: '/docs/reference/core-dsl'},
            {label: 'HTTP API', to: '/docs/api/tickr-api'},
            {label: 'CLI', to: '/docs/reference/cli'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/tickr-io/tickr-cli'},
            {label: 'Security', href: 'https://github.com/tickr-io/tickr-cli/blob/main/SECURITY.md'},
            {label: 'Apache-2.0', href: 'https://github.com/tickr-io/tickr-cli/blob/main/LICENSE'},
          ],
        },
      ],
      copyright: `Tickr CLI ${currentRelease.label} documentation · Apache-2.0`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'json', 'nix'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
