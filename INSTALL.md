# Install and operate Tickr Lite

This guide takes a released Tickr Lite archive from checksum verification to two
successful runs against an existing compatible Tickr Control plane:

1. `hello-world`, a single Task that prints `hello from Tickr`;
2. `runtime-patch`, which deterministically grows its live graph into two
   sequential arms from a supplied integer seed.

The commands support Linux x86-64, Linux ARM64, and Apple silicon. Run them from
an ordinary unprivileged account. Tickr Lite keeps its API and embedded Console
on loopback by default.

## What you need

Obtain the Tenant slug and bearer credential from the operator of the matched
Tickr 0.1.4 Control plane. Setup selects the private-beta HTTP and relay
endpoints.

Install Nix with the `nix-command` and `flakes` features enabled. The guided
Hello run needs no other shell tools. The advanced runtime-Patch walkthrough
below also uses `curl` and `jq`.

The release archive contains the `tickr` and `tickr-ctx` executables, the exact
Core DSL for that release, and the runnable examples. Nickel is installed below
at the version Tickr validates in CI.

## 1. Install Nix and Nickel

Install Nix with the
[Determinate Nix Installer](https://install.determinate.systems/).

On Linux:

```sh
curl -fsSL https://install.determinate.systems/nix |
  sh -s -- install --determinate --no-confirm
```

On macOS:

```sh
curl -Lo Determinate.pkg \
  https://install.determinate.systems/determinate-pkg/stable/Universal
sudo installer -pkg ./Determinate.pkg -target /
```

Open a new terminal after installation, then verify Nix and Flakes:

```sh
nix --version
nix flake --help >/dev/null
```

Install Nickel from Nixpkgs and put the user profile on the current shell's
search path:

```sh
nix profile add nixpkgs#nickel
export PATH="$HOME/.nix-profile/bin:$PATH"
command -v nickel
nickel --version
```

Persist the same `PATH` entry in the shell's startup configuration if a new
terminal does not retain it. Tickr 0.1.4 accepts Nickel 1.16.0 and 1.17.0;
Nixpkgs currently installs Nickel 1.17.0.

## 2. Verify and extract the archive

Download one archive and its adjacent `.sha256` file from the same GitHub
release. On Linux:

```sh
sha256sum --check tickr-lite-v*.tar.gz.sha256
tar -xzf tickr-lite-v*.tar.gz
cd tickr-lite-v*-*
```

On macOS:

```sh
shasum -a 256 -c tickr-lite-v*.tar.gz.sha256
tar -xzf tickr-lite-v*.tar.gz
cd tickr-lite-v*-*
```

Keep the extracted directory intact. The examples use paths relative to this
directory, and the bundled DSL must stay matched to the executable.

```sh
export TICKR_HOME="$(pwd -P)"
```

## 3. Configure Tickr Lite

Run the setup command from the extracted release directory:

```sh
./tickr setup
```

Setup verifies Nix Flakes and a supported Nickel version, then asks for:

- the Tenant slug;
- the Tenant credential, with terminal echo disabled;
- the Tickr data directory, recommending
  `$HOME/.local/share/tickr-lite`.

For this private-beta release, setup selects the matched Control-plane endpoints
`https://ctrl.tickr.works` and `https://relay.tickr.works`. It keeps the local
API and embedded Console on `127.0.0.1:6000`.

Setup writes the credential-bearing profile to
`$HOME/.config/tickr/config.json` with mode `0600`, creates the selected data
directory with mode `0700`, and applies the Tickr Lite SQLite migrations. Later
`tickr-lite`, `migrate --formation tickr-lite`, and `examples` commands load
this profile automatically. Environment variables remain explicit deployment
overrides.
When one of those commands starts Tickr Lite, it prepends the extracted release
directory to the inherited `PATH`. The packaged `tickr-ctx` executable is
therefore available to runtime-Patch Tasks while the Nix profile remains
available for `nix` and `nickel`.

For a non-interactive installation, keep the credential out of shell history:

```sh
./tickr setup \
  --tenant-slug acme-demo \
  --token-file /secure/path/tickr-tenant-token \
  --data-dir "$HOME/.local/share/tickr-lite"
```

The credential must be the canonical unpadded base64url encoding of exactly 32
random bytes: 43 ASCII characters matching `[A-Za-z0-9_-]{43}`. Setup never
puts it in a URL or command-line argument.

Setup is idempotent. A later run reuses the stored Tenant and credential unless
an explicit flag or environment override supplies a replacement.

## 4. Run the bundled Hello workflow

```sh
./tickr examples run hello-world
```

If the local API is not running, this command verifies the SQLite migration,
starts Tickr Lite for the example, waits for formation readiness, and stops its
owned process afterward. If Tickr Lite is already running, it uses that process
and leaves it running.

The command registers the bundled Nickel source without copying or JSON-escaping
it, waits for the definition to become `Ready`, triggers it, resolves the
resulting Signal to a Run, waits for the `hello` Task, and reads its log. A
successful run ends with:

```text
Output:
hello from Tickr
```

## 5. Start Tickr Lite for ongoing use

Keep this foreground process running:

```sh
./tickr tickr-lite
```

The saved setup profile is loaded automatically, and Tickr changes to the
version-matched release directory before starting local roles. Open the
embedded Console at <http://127.0.0.1:6000/>.

For detailed health:

```sh
curl -fsS http://127.0.0.1:6000/api/health |
  jq '{api, data_plane_sql, control_plane, formation, readiness}'
```

Continue only when `readiness.ready` is `true` and the Control-plane component
reports healthy.

## 6. Register the runtime-Patch workflow
The remaining runtime-Patch walkthrough uses the HTTP API directly:

```sh
export TICKR_API_URL=http://127.0.0.1:6000
```


The second example starts as a three-Task chain:

```text
choose-counts -> patch-two-arms -> summarize-join
```

For a required integer seed, `choose-counts` hashes `<seed>:left` and
`<seed>:right`, maps each digest into `1..10`, and captures both counts. The
patching Task submits one `mkFork` Patch whose left and right arms contain those
numbers of sequential Tasks. It waits for the Patch to become `Applied`. The
arms run concurrently, each Task prints its position and pauses for one second,
and `summarize-join` runs only after the fork barrier grounds.

Register it:

```sh
jq -n --rawfile source "$TICKR_HOME/examples/runtime-patch.ncl" \
  '{namespace:"default", nickel_source:$source}' \
  > patch-register-request.json

curl -fsS -X POST "$TICKR_API_URL/api/workflows/register" \
  -H 'Content-Type: application/json' \
  -d @patch-register-request.json |
  tee patch-register-response.json

patch_workflow_id="$(jq -er '.workflow_id' patch-register-response.json)"
```

Fetch its build state once and continue only when it is `Ready`:

```sh
curl -fsS "$TICKR_API_URL/api/workflows" |
  jq --arg id "$patch_workflow_id" \
    '.[] | select(.id == $id) | {id, slug, version, build_status}'
```

## 7. Trigger and inspect the runtime Patch

Use any integer seed. The same seed always produces the same two arm lengths:

```sh
curl -fsS -X POST \
  "$TICKR_API_URL/api/workflows/$patch_workflow_id/trigger" \
  -H 'Content-Type: application/json' \
  -d '{"name":"seeded Patch: 42","inputs":{"seed":42}}' |
  tee patch-trigger-response.json

patch_signal_id="$(jq -er '.signal_id' patch-trigger-response.json)"
```

Resolve the Signal once:

```sh
curl -fsS "$TICKR_API_URL/api/signals/$patch_signal_id" |
  tee patch-signal-status.json |
  jq '{status, workflow_instance_id}'
```

Once materialized:

```sh
patch_run_id="$(jq -er '.workflow_instance_id' patch-signal-status.json)"
curl -fsS "$TICKR_API_URL/api/workflows/instances/$patch_run_id/tasks" |
  jq '.[] | {id, name, state, attempt}'
```

The task list grows after `patch-two-arms` reports that its Patch is `Applied`.
The added names are `left-step-01` through `left-step-N` and `right-step-01`
through `right-step-M`. The two arms each remain sequential, execute in
parallel with one another, and rejoin before `summarize-join`.

## Operational boundaries

- The setup profile contains the Tenant credential. Keep
  `$HOME/.config/tickr/config.json` private and never print or commit it.
- Manage only the foreground `./tickr tickr-lite` process belonging to this
  installation. Do not attempt to reconfigure the external Control plane.
- Registration and triggering are asynchronous. Use the returned Workflow and
  Signal identities; never guess identifiers from a prior run.
- Stop on a non-success response, `BuildFailed`, `Rejected`, or a failed Task.
  Preserve the response and report the exact failing identifier and state.
- Stop Tickr Lite with its foreground process's normal interrupt. Do not use a
  broad process-name kill.

## Stop Tickr Lite

Return to the terminal running Tickr Lite and press `Ctrl-C`. A normal shutdown
waits for the Lite supervisor and its critical children to stop.

## Troubleshooting

- **`failed to execute nickel export`** — confirm `$HOME/.nix-profile/bin` is
  on `PATH` and `nickel --version` reports 1.16.0 or 1.17.0.
- **`import lib.ncl` fails** — keep the extracted release intact and rerun
  `./tickr setup` from that directory.
- **a Task cannot find `tickr-ctx`** — confirm `tickr-ctx` remains executable
  beside `tickr`.
- **Nix rejects `path:./examples#...`** — confirm Flakes remain enabled; Tickr
  automatically runs local roles from the version-matched release directory.
- **registration remains `Building`** — fetch its build state once and inspect
  the diagnostic rather than triggering it.
- **the Signal remains `pending`** — fetch the Signal once later; instance
  materialization is asynchronous.
