---
title: Get started with Tickr Lite
description: Install Tickr Lite and run your first local workflow.
sidebar_position: 1
hide_title: true
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import TickrLiteInstall from '@site/src/components/TickrLiteInstall';

<h1 className="getting-started-title">Get started with Tickr Lite</h1>

## Prerequisites

- An `invitation.json` file from your Tickr operator.
- Linux x86-64, Linux ARM64, or Apple silicon.
- `curl`, `wget`, and `tar`.

## Install Nix

<Tabs groupId="operating-system" queryString="os">
  <TabItem value="linux" label="Linux" default>

```bash
curl -fsSL https://install.determinate.systems/nix |
  sh -s -- install --determinate --no-confirm
```

  </TabItem>
  <TabItem value="macos" label="macOS">

```bash
curl -Lo Determinate.pkg \
  https://install.determinate.systems/determinate-pkg/stable/Universal
sudo installer -pkg ./Determinate.pkg -target /
```

  </TabItem>
</Tabs>

<details>
<summary>Check the installation</summary>

Open a new terminal, then run:

```bash
nix --version
nix flake --help >/dev/null
```

</details>

## Install Nickel

```bash
nix profile add nixpkgs#nickel
export PATH="$HOME/.nix-profile/bin:$PATH"
nickel --version
```

Tickr Lite supports Nickel 1.16.0 and 1.17.0. Add
`$HOME/.nix-profile/bin` to your shell startup file if a new terminal cannot
find `nickel`.

## Install Tickr Lite

Choose your platform. The commands use the Tickr version documented by this
release of the site.

<TickrLiteInstall />

Keep the extracted directory intact; it contains the executables, Core DSL,
Console, examples, and notices for the same release.

## Set up Tickr Lite

```bash
./tickr-cli setup --from invitation.json
```

Setup validates the invitation, creates the private
`profile/config.json`, defaults durable state to `data/` inside this extracted
release, and applies the Tickr Lite SQLite migrations. Each extracted release
therefore owns its Tenant profile and durable state without shell configuration.

## Explore Tickr in the Console

```bash
./tickr-cli examples run hello-world runtime-patch polyglot
```

The command starts `tickr-lite` when needed, opens the embedded Console at
[http://127.0.0.1:3000/](http://127.0.0.1:3000/), and registers and triggers
all three workflows as their builds become ready. The polyglot workflow runs
Python, JavaScript, Go, and Rust Tasks with one predefined greeting.

After the initial Runs settle, the interactive session remains open:

```text
Commands: run <example>, list, open, help, quit
tickr › run <example>
```

Use `list` to see packaged examples, `run <example>` to trigger another Run,
and `open` to reopen the Console. Press `Ctrl-C` or enter `quit` when finished.
The command stops only the `tickr-lite` process it started.

## Start Tickr Lite later

```bash
./tickr-lite
```

The standalone runtime loads the saved setup profile and keeps the API and
Console available until interrupted.
