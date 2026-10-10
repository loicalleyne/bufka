# Agent notes

- TUI / schema-detection reference: `docs/agents/tui-reference.md`.
  When changing `internal/adapters/primary/tui/`, `internal/core/services/topicsample/`,
  or `internal/adapters/secondary/topicsfile/`, update that file in the same change.
- Topic schema overrides: `topics.toml` entries with `message_override = true` were chosen by a
  user (TUI schema picker, `m`). Anything that writes re-detected entries must merge through
  `topicsfile.KeepOverride(prev, next)` so the override isn't clobbered.
- Build/test with `CGO_ENABLED=1` (e.g. `CGO_ENABLED=1 go test ./...`); format Go with `gofumpt`.
