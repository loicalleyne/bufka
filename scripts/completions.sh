#!/bin/sh
# Generate shell completions for release archives.
set -e
rm -rf completions
mkdir completions
for sh in bash zsh fish; do
	CGO_ENABLED=1 go run . completion "$sh" >"completions/bufka.$sh"
done
