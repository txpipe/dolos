#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <git-ref> <git-sha> <is-default-branch>" >&2
  exit 2
}

[[ $# -eq 3 ]] || usage

ref=$1
sha=$2
is_default_branch=$3

[[ $sha =~ ^[0-9a-fA-F]{7,}$ ]] || {
  echo "invalid Git SHA: $sha" >&2
  exit 2
}

case "$is_default_branch" in
  true|false) ;;
  *)
    echo "is-default-branch must be true or false" >&2
    exit 2
    ;;
esac

if [[ $is_default_branch == true ]]; then
  echo latest
fi

# A release ref must be valid enough to identify all three numeric SemVer
# components. Prerelease identifiers preserve the exact version tag, but only
# an unsuffixed release gets the stable, major and minor aliases.
if [[ $ref =~ ^refs/tags/v([0-9]+)\.([0-9]+)\.([0-9]+)(-([0-9A-Za-z]+([.-][0-9A-Za-z]+)*))?$ ]]; then
  version=${ref#refs/tags/}
  echo "$version"

  if [[ -z ${BASH_REMATCH[4]} ]]; then
    echo stable
    echo "v${BASH_REMATCH[1]}"
    echo "v${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
  fi
fi

# Match docker/metadata-action's default short-SHA spelling.
echo "sha-${sha:0:7}"
