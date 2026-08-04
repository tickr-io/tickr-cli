set -euo pipefail

command -v tickr-ctx >/dev/null 2>&1 || {
  echo "choose: tickr-ctx is not on PATH" >&2
  exit 2
}

seed="$(tickr-ctx get seed --signal --default '')"
case "$seed" in
  ''|'-'|*[!0-9-]*)
    echo "choose: trigger input seed must be an integer" >&2
    exit 2
    ;;
esac

count_for() {
  local label="$1" digest
  digest="$(printf '%s:%s' "$seed" "$label" | sha256sum | cut -c1-8)"
  printf '%s\n' "$((16#$digest % 10 + 1))"
}

left_count="$(count_for left)"
right_count="$(count_for right)"

tickr-ctx capture left_count --int "$left_count" --allow-undeclared
tickr-ctx capture right_count --int "$right_count" --allow-undeclared
printf 'runtime-patch: seed=%s left_count=%s right_count=%s\n' \
  "$seed" "$left_count" "$right_count"
