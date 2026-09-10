#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
policy="$root/.github/scripts/docker-tags.sh"
sha=0123456789abcdef0123456789abcdef01234567

assert_tags() {
  local name=$1
  local ref=$2
  local is_default_branch=$3
  local expected=$4
  local actual

  actual=$($policy "$ref" "$sha" "$is_default_branch")

  if [[ $actual != "$expected" ]]; then
    printf '%s: unexpected tags\nexpected:\n%s\nactual:\n%s\n' \
      "$name" "$expected" "$actual" >&2
    return 1
  fi

  printf '%s: %s\n' "$name" "$(printf '%s\n' "$actual" | tr '\n' ' ' | sed 's/ $//')"
}

assert_tags \
  main \
  refs/heads/main \
  true \
  $'latest\nsha-0123456'

assert_tags \
  alpha \
  refs/tags/v2.0.0-alpha.0 \
  false \
  $'v2.0.0-alpha.0\nsha-0123456'

assert_tags \
  rc \
  refs/tags/v2.0.0-rc.0 \
  false \
  $'v2.0.0-rc.0\nsha-0123456'

assert_tags \
  stable \
  refs/tags/v2.0.0 \
  false \
  $'v2.0.0\nstable\nv2\nv2.0\nsha-0123456'

assert_tags \
  non-semver-v-tag \
  refs/tags/vnightly \
  false \
  'sha-0123456'
