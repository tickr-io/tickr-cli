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

Obtain these values from the operator of the Control plane:

- the tenant slug;
- the Control-plane HTTP subquery channel URL;
- the Control-plane Conductor relay URL.

Install these host tools:

- Nix with the `nix-command` and `flakes` features enabled;
- `curl`;
- `jq`.

The release archive contains the `tickr` and `tickr-ctx` executables, the exact
Core DSL for that release, and the runnable examples. Nickel is installed below
at the version Tickr validates in CI.

## 1. Verify and extract the archive

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

## 2. Install the pinned Nickel evaluator

Tickr invokes `nickel export` when it registers a workflow or parses a runtime
Patch. Install Nickel 1.16.0 into the extracted bundle so every foreground
Tickr process and Task sees the same executable.

```sh
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)
    nickel_asset=nickel-x86_64-linux
    nickel_sha256=05d926d6cfdd3743731a65c08a558c2ae5edd55759f3cee57f5096acb2595816
    ;;
  Linux-aarch64|Linux-arm64)
    nickel_asset=nickel-arm64-linux
    nickel_sha256=1ee39d7c9791d2b1ded7ec656c4226ce20e4fad519808c36c90df55c3b2e1d27
    ;;
  Darwin-arm64)
    nickel_asset=nickel-arm64-macos
    nickel_sha256=6855a4197a8df9067af6c84eaed129715a78194d97987a5f4e46bead96e616ad
    ;;
  *)
    echo "Unsupported platform: $(uname -s)-$(uname -m)" >&2
    exit 2
    ;;
esac

curl -fL \
  "https://github.com/nickel-lang/nickel/releases/download/1.16.0/$nickel_asset" \
  -o nickel
```

Verify the downloaded executable. On Linux:

```sh
printf '%s  nickel\n' "$nickel_sha256" | sha256sum --check -
```

On macOS:

```sh
printf '%s  nickel\n' "$nickel_sha256" | shasum -a 256 --check
```

Then:

```sh
chmod 755 nickel
./nickel --version
```

The version output must name Nickel 1.16.0.

## 3. Create the private environment file

Choose a durable state directory outside the extracted release. Replace the
three values marked `REPLACE_ME`, then write `tickr-lite.env`:

```sh
cat > tickr-lite.env <<EOF
export TICKR_HOME="$TICKR_HOME"
export TICKR_STATE_DIR="$HOME/.local/share/tickr-lite"
export TICKR_TENANT_SLUG="REPLACE_ME"
export TICKR_CTRL_HTTP_URL="REPLACE_ME"
export TICKR_CTRL_RELAY_URL="REPLACE_ME"
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

Reject blank values before continuing:

```sh
. ./tickr-lite.env
case "$TICKR_TENANT_SLUG:$TICKR_CTRL_HTTP_URL:$TICKR_CTRL_RELAY_URL" in
  *REPLACE_ME*|::*|:*:|:*) echo "Complete tickr-lite.env first" >&2; exit 2 ;;
esac
mkdir -p "$TICKR_STATE_DIR"
chmod 700 "$TICKR_STATE_DIR"
```
The obsolete `TICKR_COORDINATOR_HTTP_URL` and `TICKR_COORDINATOR_RELAY_URL`
variables are unsupported and ignored. When either new variable is absent, Tickr
uses its existing loopback default.

Every terminal or agent process operating this installation must first run:

```sh
cd "$TICKR_HOME"
. ./tickr-lite.env
```

Do not mix environment files between tenants or releases.

## 4. Initialize local state

```sh
./tickr migrate --formation tickr-lite
```

A successful first migration prints:

```text
conductor sqlite migrations applied and verified.
```

The same command is safe to run before later starts; it verifies and applies
only the migrations belonging to this binary.

## 5. Start Tickr Lite

Keep this foreground process running:

```sh
./tickr tickr-lite
```

Use a second terminal for the remaining commands. Source the same environment
file there:

```sh
cd /absolute/path/to/the/extracted/tickr-lite-directory
. ./tickr-lite.env
```

Check the local API, SQLite repository, formation, and Control-plane connection:

```sh
curl -fsS "$TICKR_API_URL/api/health" |
  jq '{api, data_plane_sql, control_plane, formation, readiness}'
```

Continue only when `readiness.ready` is `true` and the Control-plane component
reports healthy. Open the embedded Console at <http://127.0.0.1:6000/>.

## 6. Register the Hello workflow

Build the request as a file so the Nickel source is transmitted exactly:

```sh
jq -n --rawfile source "$TICKR_HOME/examples/hello-world.ncl" \
  '{namespace:"default", nickel_source:$source}' \
  > hello-register-request.json

curl -fsS -X POST "$TICKR_API_URL/api/workflows/register" \
  -H 'Content-Type: application/json' \
  -d @hello-register-request.json |
  tee hello-register-response.json

hello_workflow_id="$(jq -er '.workflow_id' hello-register-response.json)"
printf 'hello workflow_id=%s\n' "$hello_workflow_id"
```

Registration is asynchronous. Fetch its build state once:

```sh
curl -fsS "$TICKR_API_URL/api/workflows" |
  jq --arg id "$hello_workflow_id" \
    '.[] | select(.id == $id) | {id, slug, version, build_status}'
```

`Ready` means the workflow can run. If it is still `Building`, run that single
status command again later. Stop on `BuildFailed` and inspect the returned build
diagnostic; do not trigger a failed definition.

## 7. Trigger and inspect Hello

```sh
curl -fsS -X POST \
  "$TICKR_API_URL/api/workflows/$hello_workflow_id/trigger" \
  -H 'Content-Type: application/json' \
  -d '{"name":"my first Tickr run"}' |
  tee hello-trigger-response.json

hello_signal_id="$(jq -er '.signal_id' hello-trigger-response.json)"
printf 'hello signal_id=%s\n' "$hello_signal_id"
```

Resolve the asynchronous Signal once:

```sh
curl -fsS "$TICKR_API_URL/api/signals/$hello_signal_id" |
  tee hello-signal-status.json |
  jq '{status, workflow_instance_id}'
```

When the Signal is `materialized`, record the Run id and inspect its Tasks:

```sh
hello_run_id="$(jq -er '.workflow_instance_id' hello-signal-status.json)"
curl -fsS "$TICKR_API_URL/api/workflows/instances/$hello_run_id/tasks" |
  jq '.[] | {id, name, state, attempt}'
```

After `hello` reaches `Completed`, read its terminal log through the Console or
the API Task-log endpoint. The log contains:

```text
hello from Tickr
```

## 8. Register the runtime-Patch workflow

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

## 9. Trigger and inspect the runtime Patch

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

## Agent operating contract

An agent following this file must obey these rules:

1. Work only in the extracted directory identified by `TICKR_HOME`.
2. Source `tickr-lite.env` before every Tickr or API command; never print the
   complete file in a report.
3. Manage only the foreground `./tickr tickr-lite` process belonging to this
   installation. Do not attempt to start, stop, repair, or reconfigure the
   external Control plane.
4. Register the exact checked-in `.ncl` source through a JSON request file.
   Record every returned `workflow_id`, `signal_id`, and
   `workflow_instance_id`; never guess identifiers from a prior run.
5. Registration and triggering are asynchronous. Fetch status once and report
   the observed state. Do not run an unbounded poll or promise a later update.
6. Trigger only a workflow whose observed `build_status` is `Ready`.
7. For progress, fetch Task state once. Read logs only when explicitly asked.
   Use a bounded live-log query for a non-terminal Task and the query-free log
   endpoint for a terminal Task.
8. Stop on a non-success response, `BuildFailed`, `Rejected`, or a failed Task.
   Preserve the response and report the exact failing identifier and state.
9. Stop Tickr Lite with the foreground process's normal interrupt. Do not use a
   broad process-name kill.
10. Fail closed if `./tickr`, `./tickr-ctx`, `./INSTALL.md`, `./dsl/lib.ncl`,
    `./examples/hello-world.ncl`, or `./tickr-lite.env` is absent.

## Stop Tickr Lite

Return to the terminal running Tickr Lite and press `Ctrl-C`. A normal shutdown
waits for the Lite supervisor and its critical children to stop.

## Troubleshooting

- **`failed to execute nickel export`** — confirm `nickel --version` reports
  1.16.0 and `$TICKR_HOME` is at the front of `PATH`.
- **`import lib.ncl` fails** — confirm
  `TICKR_DSL_PATHS="$TICKR_HOME/dsl"` and `dsl/lib.ncl` exists.
- **a Task cannot find `tickr-ctx`** — confirm `tickr-ctx` exists in
  `$TICKR_HOME`, is executable, and `$TICKR_HOME` is on the Tickr process PATH.
- **Nix rejects `path:./examples#...`** — start Tickr Lite from `$TICKR_HOME`
  and confirm flakes are enabled.
- **registration remains `Building`** — fetch its build state once and inspect
  the diagnostic rather than triggering it.
- **the Signal remains `pending`** — fetch the Signal once later; instance
  materialization is asynchronous.
