#!/bin/sh
# Based on Deno installer: Copyright 2019 the Deno authors. All rights reserved. MIT license.

set -e

main() {
	os=$(uname -s)
	arch=$(uname -m)
	version=${1:-latest}

	root_dir="${DOLOS_INSTALL:-/usr/local/dolos}"

	if [ "$version" == "latest" ]; then
		echo "using latest version"
		download_uri="https://github.com/txpipe/dolos/releases/latest/download/dolos-${os}-${arch}"
	else
		echo "using version ${version}"
		download_uri="https://github.com/txpipe/dolos/releases/download/${version}/dolos-${os}-${arch}"
	fi

	echo "downloading binary from ${download_uri}"

	bin_dir="$root_dir/bin"
	tmp_dir="$root_dir/tmp"
	exe="$bin_dir/dolos"
	simexe="/usr/local/bin/dolos"

	mkdir -p "$bin_dir"
	mkdir -p "$tmp_dir"

	curl -q --fail --location --progress-bar --output "$tmp_dir/dolos" "$download_uri"
	chmod +x "$tmp_dir/dolos"
	
	# atomically rename into place:
	mv "$tmp_dir/dolos" "$exe"

	ln -sf $exe $simexe

	# print version
	"$exe" --version
	
	echo "Dolos was installed successfully to $exe"
	echo "Run 'dolos --help' to get started."
}

main "$1"