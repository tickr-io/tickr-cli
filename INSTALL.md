# Install and operate Tickr Lite

This guide takes a released Tickr Lite archive from checksum verification to
three successful Runs against an existing compatible Tickr Control plane:

1. `hello-world`, a single Task that prints `hello from Tickr`;
2. `runtime-patch`, which deterministically grows its live graph into two
   sequential arms from a supplied integer seed;
3. `polyglot`, a four-Task Python, JavaScript, Go, and Rust chain that reads one
   predefined greeting through `tickr-ctx`.

The commands support Linux x86-64, Linux ARM64, and Apple silicon. Run them from
an ordinary unprivileged account. Tickr Lite keeps its API and embedded Console
on loopback by default.

## What you need

Obtain the Tenant slug and bearer credential from the operator of the matched
Tickr 0.1.5 Control plane. Setup selects the private-beta HTTP and relay
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
terminal does not retain it. Tickr 0.1.5 accepts Nickel 1.16.0 and 1.17.0;
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

Run setup with the invitation supplied by your Tickr operator:

```sh
./tickr-cli setup --from invitation.json
```

The invitation supplies the Tenant identity, credential, matched Control-plane
endpoints, compatible Tickr Lite version, and expiry. Setup rejects an expired
invitation or one issued for another Tickr Lite version. Each extracted release
owns one installation-local profile. To configure another Tenant, use another
extracted release directory; explicit `TICKR_CONFIG_PATH` and `--data-dir`
overrides remain available for managed installations.

Without an invitation, `./tickr-cli setup` retains the interactive fallback and
asks for the Tenant slug, Tenant credential, and data directory. Tickr Lite
keeps the local API and embedded Console on `127.0.0.1:3000`, matching the
Console entrypoint used by other formations.

Setup reads the credential from the invitation file; it never places the
credential in a URL or command-line argument. For a fresh installation, setup
writes the generated profile to `profile/config.json` and defaults durable
state to `data/`, both inside the extracted release directory.
`TICKR_CONFIG_PATH=<absolute-path>` and `--data-dir <path>` select other
locations explicitly. Setup creates the profile directory and data directory
with mode `0700`, writes the profile with mode `0600`, and applies the Tickr
Lite SQLite migrations. Later `tickr-lite`, `tickr-cli migrate --formation
tickr-lite`, and `tickr-cli examples` commands resolve this installation-local
profile automatically, even when invoked from another working directory.
Environment variables remain explicit deployment overrides.
When one of those commands starts Tickr Lite, it prepends the extracted release
directory to the inherited `PATH`. The packaged `tickr-ctx` executable is
therefore available to runtime-Patch Tasks while the Nix profile remains
available for `nix` and `nickel`.


Setup is idempotent. A later run with an invitation for the same Tenant reuses
the stored data directory while refreshing its credential and Control-plane
endpoints.

## 4. Explore Tickr in the Console

```sh
./tickr-cli examples run hello-world runtime-patch polyglot
```

If the local API is not running, this command verifies the SQLite migration,
starts the standalone `tickr-lite` executable, waits for formation readiness,
and opens the embedded Console at <http://127.0.0.1:3000/>. If Tickr Lite is
already running, the command uses that process and never assumes ownership of
it.

The three definitions register concurrently and each triggers with predefined
onboarding inputs as soon as its build becomes `Ready`. Python, JavaScript, Go,
and Rust packages come from the pinned Nix flake and are built during polyglot
registration; the Tasks do not compile source after the Run begins.

After the initial Runs settle, the terminal becomes an interactive example
session:

```text
Commands: run <example>, list, open, help, quit
tickr › run <example>
```

Use `list` to inspect packaged examples, `run <example>` to trigger another
Run, and `open` to reopen the Console. Ghost text suggests the next untried
example; press Right Arrow to accept it and Tab to complete names. `Ctrl-C` or
`quit` ends the session and stops only a `tickr-lite` process the command
started.

## 5. Start Tickr Lite later

Run the standalone runtime:

```sh
./tickr-lite
```

The saved setup profile is loaded automatically, and Tickr Lite changes to the
version-matched release directory before starting local roles. Open the
embedded Console at <http://127.0.0.1:3000/>.

For detailed health:

```sh
curl -fsS http://127.0.0.1:3000/api/health |
  jq '{api, data_plane_sql, control_plane, formation, readiness}'
```

Continue only when `readiness.ready` is `true` and the Control-plane component
reports healthy.

## 6. Register the runtime-Patch workflow
The remaining runtime-Patch walkthrough uses the HTTP API directly:

```sh
export TICKR_API_URL=http://127.0.0.1:3000
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

- The invitation and generated setup profile contain the Tenant credential.
  Keep `invitation.json` and `profile/config.json` private; never print or
  commit either file.
- Manage only the foreground `./tickr-lite` process belonging to this
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
  `./tickr-cli setup` from that directory.
- **a Task cannot find `tickr-ctx`** — confirm `tickr-ctx` remains executable
  beside `tickr-lite`.
- **Nix rejects `path:./examples#...`** — confirm Flakes remain enabled; Tickr
  automatically runs local roles from the version-matched release directory.
- **registration remains `Building`** — fetch its build state once and inspect
  the diagnostic rather than triggering it.
- **the Signal remains `pending`** — fetch the Signal once later; instance
  materialization is asynchronous.
