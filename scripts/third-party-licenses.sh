#!/bin/sh
# Copy the LICENSE/COPYING/NOTICE files of every Go module linked into the
# bufka binary into third_party_licenses/<module path>/ for release archives.
# Fails if a linked module ships no license file.
set -e
out=third_party_licenses
rm -rf "$out"
missing=0
CGO_ENABLED=1 go list -deps -f '{{with .Module}}{{if not .Main}}{{.Path}} {{.Dir}}{{end}}{{end}}' . |
	sort -u >"${TMPDIR:-/tmp}/bufka-mods.$$"
while read -r mod dir; do
	files=$(find "$dir" -maxdepth 1 -type f \( -iname 'LICEN[CS]E*' -o -iname 'COPYING*' -o -iname 'NOTICE*' \))
	if [ -z "$files" ]; then
		echo "no license file in $mod ($dir)" >&2
		missing=1
		continue
	fi
	mkdir -p "$out/$mod"
	for f in $files; do cp "$f" "$out/$mod/"; done
done <"${TMPDIR:-/tmp}/bufka-mods.$$"
rm -f "${TMPDIR:-/tmp}/bufka-mods.$$"
exit $missing
