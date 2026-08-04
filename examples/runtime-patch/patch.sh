set -euo pipefail

command -v tickr-ctx >/dev/null 2>&1 || {
  echo "patch: tickr-ctx is not on PATH" >&2
  exit 2
}

api="${TICKR_API_URL:-http://127.0.0.1:6000}"
run_id="${TICKR_RUN_ID:?patch: TICKR_RUN_ID is required}"
task_instance_id="${TICKR_TASK_ID:?patch: TICKR_TASK_ID is required}"
left_count="$(tickr-ctx get left_count --default '')"
right_count="$(tickr-ctx get right_count --default '')"

for value in "$left_count" "$right_count"; do
  case "$value" in
    ''|*[!0-9]*) echo "patch: captured counts must be integers" >&2; exit 2 ;;
  esac
  if [ "$value" -lt 1 ] || [ "$value" -gt 10 ]; then
    echo "patch: captured counts must be between 1 and 10" >&2
    exit 2
  fi
done

anchor="$(
  curl -fsS "$api/api/workflows/instances/$run_id/tasks" |
    jq -r --arg id "$task_instance_id" '.[] | select(.id == $id) | .task_id' |
    head -n1
)"
[ -n "$anchor" ] || { echo "patch: could not resolve the live Patch anchor" >&2; exit 2; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
source_file="$work/runtime-patch.ncl"
payload_file="$work/runtime-patch.json"

emit_steps() {
  local arm="$1" total="$2" index handle
  index=1
  while [ "$index" -le "$total" ]; do
    handle="$(printf 'step-%02d' "$index")"
    cat <<EOF
        { handle = "$handle", task = tickr.mkTask {
            name = "$arm-$handle",
            args = [ "$arm", "$index", "$total" ],
            nix_expression_path = "path:./examples#echoPause",
            outputs = [],
            timeout = "30s",
            max_attempts = 1,
        } },
EOF
    index=$((index + 1))
  done
}

{
  cat <<EOF
let tickr = import "lib.ncl" in
tickr.mkFork {
  anchor = "$anchor",
  arms = [
    { handle = "left", steps = [
EOF
  emit_steps left "$left_count"
  cat <<'EOF'
    ] },
    { handle = "right", steps = [
EOF
  emit_steps right "$right_count"
  cat <<'EOF'
    ] },
  ],
  reason = "seeded onboarding example: two sequential arms",
}
EOF
} >"$source_file"

jq -Rs '{nickel_source: .}' "$source_file" >"$payload_file"
response="$(
  curl -fsS -X POST "$api/api/workflows/instances/$run_id/patch" \
    -H 'Content-Type: application/json' \
    -d @"$payload_file"
)"
patch_id="$(printf '%s' "$response" | jq -r '.patch_id // empty')"
[ -n "$patch_id" ] || { echo "patch: response did not contain patch_id: $response" >&2; exit 2; }
printf 'runtime-patch: patch_id=%s submitted; waiting for Applied\n' "$patch_id"

attempt=1
while [ "$attempt" -le 120 ]; do
  status="$(curl -fsS "$api/api/patches/$patch_id" | jq -r '.status // empty')"
  case "$status" in
    Applied)
      printf 'runtime-patch: patch_id=%s Applied\n' "$patch_id"
      exit 0
      ;;
    Rejected|BuildFailed)
      printf 'runtime-patch: patch_id=%s %s\n' "$patch_id" "$status" >&2
      exit 1
      ;;
  esac
  sleep 1
  attempt=$((attempt + 1))
done

echo "patch: timed out waiting for patch $patch_id" >&2
exit 1
