set -euo pipefail

left_count="$(tickr-ctx get left_count --default '?')"
right_count="$(tickr-ctx get right_count --default '?')"
printf 'runtime-patch: both arms joined; left_count=%s right_count=%s\n' \
  "$left_count" "$right_count"
