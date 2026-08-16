import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import CodeBlock from '@theme/CodeBlock';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

const platforms = [
  {
    value: 'linux-x86-64',
    label: 'Linux x86-64',
    target: 'x86_64-unknown-linux-gnu',
    checksum: 'sha256sum --check "${ARCHIVE}.sha256"',
  },
  {
    value: 'linux-arm64',
    label: 'Linux ARM64',
    target: 'aarch64-unknown-linux-gnu',
    checksum: 'sha256sum --check "${ARCHIVE}.sha256"',
  },
  {
    value: 'macos-apple-silicon',
    label: 'macOS Apple silicon',
    target: 'aarch64-apple-darwin',
    checksum: 'shasum -a 256 -c "${ARCHIVE}.sha256"',
  },
] as const;

function installCommands(version: string, target: string, checksum: string): string {
  return `VERSION=${version}
TARGET=${target}
ARCHIVE="tickr-lite-v\${VERSION}-\${TARGET}.tar.gz"

wget "https://github.com/tickr-io/tickr-cli/releases/download/v\${VERSION}/\${ARCHIVE}"
wget "https://github.com/tickr-io/tickr-cli/releases/download/v\${VERSION}/\${ARCHIVE}.sha256"
${checksum}
tar -xzf "\${ARCHIVE}"
cd "\${ARCHIVE%.tar.gz}"`;
}

export default function TickrLiteInstall(): React.JSX.Element {
  const {siteConfig} = useDocusaurusContext();
  const releaseVersion = siteConfig.customFields?.releaseVersion;
  if (typeof releaseVersion !== 'string') {
    throw new Error('Docusaurus releaseVersion custom field must be a string');
  }

  return (
    <Tabs
      groupId="tickr-lite-platform"
      queryString="platform"
      defaultValue="linux-x86-64"
    >
      {platforms.map((platform) => (
        <TabItem key={platform.value} value={platform.value} label={platform.label}>
          <CodeBlock language="bash">
            {installCommands(releaseVersion, platform.target, platform.checksum)}
          </CodeBlock>
        </TabItem>
      ))}
    </Tabs>
  );
}
