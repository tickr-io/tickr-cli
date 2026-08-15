---
title: Install Tickr Lite
description: Verify, configure, migrate, and start a release archive.
sidebar_position: 2
---

# Install Tickr Lite

## Install Nix

Use the
[Determinate Nix Installer](https://install.determinate.systems/).

On Linux:

```bash
curl -fsSL https://install.determinate.systems/nix |
  sh -s -- install --determinate --no-confirm
```

On macOS:

```bash
curl -Lo Determinate.pkg \
  https://install.determinate.systems/determinate-pkg/stable/Universal
sudo installer -pkg ./Determinate.pkg -target /
```

Open a new terminal, then verify Nix and Flakes:

```bash
nix --version
nix flake --help >/dev/null
```

## Install Nickel

```bash
nix profile add nixpkgs#nickel
export PATH="$HOME/.nix-profile/bin:$PATH"
command -v nickel
nickel --version
```

Persist the same `PATH` entry in the shell's startup configuration if a new
terminal does not retain it. Tickr 0.1.4 accepts Nickel 1.16.0 and 1.17.0;
Nixpkgs currently installs Nickel 1.17.0.

## Verify and extract Tickr Lite

Use the archive published for your platform on
[GitHub Releases](https://github.com/tickr-io/tickr-cli/releases). Download the
adjacent `.sha256` file from the same release.

On Linux:

```bash
sha256sum --check tickr-lite-v*.tar.gz.sha256
tar -xzf tickr-lite-v*.tar.gz
cd tickr-lite-v*-*
```

On macOS:

```bash
shasum -a 256 -c tickr-lite-v*.tar.gz.sha256
tar -xzf tickr-lite-v*.tar.gz
cd tickr-lite-v*-*
```

Keep the extracted directory intact. It contains version-matched executables,
Core DSL, Console assets, examples, and release notices.

```bash
export TICKR_HOME="$(pwd -P)"
```

## Configure Tickr Lite

Run setup from the extracted release directory:

```bash
./tickr setup
```

Setup verifies Nix Flakes and a supported Nickel version, then prompts for the
Tenant slug,
the Tenant credential with terminal echo disabled, and a data directory. Press
Enter to accept the recommended data directory:

```text
$HOME/.local/share/tickr-lite
```

The private-beta Control-plane endpoints are selected automatically. Setup
stores the profile at `$HOME/.config/tickr/config.json` with mode `0600`,
creates the data directory with mode `0700`, and applies the Tickr Lite SQLite
migrations. Later Tickr Lite commands load the profile automatically; no
environment file needs to be sourced.
Tickr prepends the extracted release directory to its inherited `PATH`, making
the packaged `tickr-ctx` executable available to runtime-Patch Tasks while
retaining the Nix profile entries.

For non-interactive setup, use a credential file rather than a command-line
token:

```bash
./tickr setup \
  --tenant-slug acme-demo \
  --token-file /secure/path/tickr-tenant-token \
  --data-dir "$HOME/.local/share/tickr-lite"
```

## Run the first example

Continue with the bundled Hello workflow:

```bash
./tickr examples run hello-world
```

The command starts Tickr Lite when necessary and waits for every asynchronous
transition through terminal Task output.

## Start Tickr Lite for ongoing use

After the guided example, keep Tickr Lite running with:

```bash
./tickr tickr-lite
```

The saved profile is loaded automatically. Open the embedded Console at
[http://127.0.0.1:6000/](http://127.0.0.1:6000/).
