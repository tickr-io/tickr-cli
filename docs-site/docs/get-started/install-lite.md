---
title: Install Tickr Lite
description: Verify, configure, migrate, and start a release archive.
sidebar_position: 2
---

# Install Tickr Lite

Use the archive published for your platform on [GitHub Releases](https://github.com/tickr-io/tickr-cli/releases). Download the adjacent `.sha256` file from the same release.

## Verify and extract

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

Keep the extracted directory intact. It contains version-matched executables, Core DSL, Console assets, examples, and release notices.

```bash
export TICKR_HOME="$(pwd -P)"
```

## Install Nickel

Tickr release line 0.1 validates workflow and runtime-Patch source with Nickel 1.16.0. Follow the platform-specific download and checksum values in the archive's `INSTALL.md`, place the executable at `$TICKR_HOME/nickel`, then confirm:

```bash
chmod 755 ./nickel
./nickel --version
```

The output must name Nickel 1.16.0.

## Create the private environment

Replace the three `REPLACE_ME` values with the values from your operator:

```bash
cat > tickr-lite.env <<EOF
export TICKR_HOME="$TICKR_HOME"
export TICKR_STATE_DIR="$HOME/.local/share/tickr-lite"
export TICKR_TENANT_SLUG="REPLACE_ME"
export TICKR_COORDINATOR_HTTP_URL="REPLACE_ME"
export TICKR_COORDINATOR_RELAY_URL="REPLACE_ME"
export TICKR_SQL_BACKEND="sqlite"
export TICKR_SQL_TOPOLOGY="single-node"
export TICKR_CONDUCTOR_SQLITE_URL="sqlite://$HOME/.local/share/tickr-lite/tickr.db"
export TICKR_API_BIND_ADDR="127.0.0.1:6000"
export TICKR_API_URL="http://127.0.0.1:6000"
export TICKR_DSL_PATHS="$TICKR_HOME/dsl"
export PATH="$TICKR_HOME:\$PATH"
EOF
chmod 600 tickr-lite.env
```

Load and validate it:

```bash
. ./tickr-lite.env
case "$TICKR_TENANT_SLUG:$TICKR_COORDINATOR_HTTP_URL:$TICKR_COORDINATOR_RELAY_URL" in
  *REPLACE_ME*|::*|:*:|:*) echo "Complete tickr-lite.env first" >&2; exit 2 ;;
esac
mkdir -p "$TICKR_STATE_DIR"
chmod 700 "$TICKR_STATE_DIR"
```

Every terminal or agent operating this installation must first change to `$TICKR_HOME` and source this file.

## Initialize local state

```bash
./tickr migrate --formation tickr-lite
```

The migration is idempotent. A successful first migration prints:

```text
conductor sqlite migrations applied and verified.
```

## Start Tickr Lite

Keep this process in the foreground:

```bash
./tickr tickr-lite
```

In a second terminal, source the same environment and inspect health once:

```bash
cd "$TICKR_HOME"
. ./tickr-lite.env
curl -fsS "$TICKR_API_URL/api/health" |
  jq '{api, data_plane_sql, control_plane, formation, readiness}'
```

Continue only when `readiness.ready` is `true` and the Control-plane component is healthy. Open the embedded Console at [http://127.0.0.1:6000/](http://127.0.0.1:6000/).

Next: [Run the Hello workflow](./first-run.md).
