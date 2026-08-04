set -euo pipefail

arm="${1:?arm is required}"
step="${2:?step is required}"
total="${3:?total is required}"
printf 'runtime-patch: %s step %s/%s started\n' "$arm" "$step" "$total"
sleep 1
printf 'runtime-patch: %s step %s/%s completed\n' "$arm" "$step" "$total"
