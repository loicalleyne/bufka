# TUI reference (for agents)

> **Keep this current.** Any change to `internal/adapters/primary/tui/`,
> `internal/core/services/topicsample/`, or `internal/adapters/secondary/topicsfile/`
> must update this file in the same commit. Line numbers are deliberately
> omitted — use symbol names and grep.
> Last verified: 2026-10-08.

Stack: Bubble Tea v2 (`charm.land/bubbletea/v2`), bubbles v2, lipgloss v2.
Build/test: `CGO_ENABLED=1 go test ./internal/adapters/primary/tui/...`; format with `gofumpt`.

## Files (`internal/adapters/primary/tui/`)
| File | Purpose |
|---|---|
| deps.go | `Deps` (KafkaConf, DB, TopicsFile/TopicsFilePath, Messages, Advisor, SampleCount/Timeout, SyncManager) |
| model.go | Top-level `Model`, `page` enum, `Update` routing, `updatePage`, per-page `updateX`, `View`, `statusMsg`/`errMsg` |
| menu.go | Main menu (kafka: topics/groups/cache/settings; protobuf: messages/settings) → `menuSelectedMsg` |
| splash.go | Splash (min 2s + topics loaded) |
| topiclist.go | Topics `table.Model`; filter, S/E/A batch, refresh; `loadTopicsCmd` merges topics.toml format + estimates |
| batch.go | Bulk sample/estimate (`runSampleForTopic`, `runEstimateForTopic`) |
| detail.go | Per-topic tabs: Metadata / Sample data / Fetch / Live tail / Size estimate |
| groups.go | Consumer groups + group detail |
| cachelist.go / cacheadd.go / cachedetail.go | Kafka→DuckDB cache list / add wizard / detail |
| syncmanager.go | Background cache sync jobs |
| kafkasettings.go / protosettings.go | Settings editors (persisted in DuckDB) |
| protomessages.go | All message FQNs + topics resolved to each (custom cursor+viewport list) |
| schemapicker.go | Per-topic message-schema override: left = `topicsample.Rank` candidates, right = samples decoded with highlighted one; enter → `applySchemaOverrideCmd` |
| protoschema.go | Schema/source view for one FQN (`protoSchemaBody`, `renderProtoSchema`) |
| styles.go | Shared styles, `humanBytes` |

## Screen architecture
- `Model.page` (consts in model.go: Splash, Menu, List, Detail, Groups, GroupDetail, KafkaCache, CacheAdd, CacheDetail, KafkaSettings, ProtoMessages, ProtoSettings, ProtoSchema, SchemaPicker).
- `Model.pickerReturn` remembers the page the schema picker was opened from (List or Detail); esc returns there.
- Sub-models are value types: `update(...) (subModel, tea.Cmd)`, `resize(w,h)`, `View() string`, optional `filtering()`/`capturingInput()`.
- `Model.Update` handles global msgs (resize all, ctrl+c, menuSelectedMsg, protoMessageSelectedMsg, schemaOverriddenMsg (rebuilds protoMessages, reloads list + detail sample), batch/topics msgs, settings-applied msgs), else `updatePage` switch.
- Per-page handlers intercept esc/enter navigation only when the sub-model isn't filtering/editing, then delegate.
- Opening a page: construct fresh `newXModel(deps, …)`, `resize(m.width, m.height-2)` (detail uses `-3`), return its `Init()`.
- `View`: title bar + body + status/error bar, AltScreen.
- Components: `bubbles/table` (topics, cache, groups), `textinput` filters, `viewport`, tab bar (`renderTabs` in detail.go), custom cursor list (protomessages.go `ensureCursorVisible`; schemapicker.go `refresh`). Split pane: schemapicker.go only (`lipgloss.JoinHorizontal` of two viewports, `paneWidths` 2/5 : rest). No `list.Model`.

## Topic → protobuf message resolution
- `topicsample.Detect(topic, values, messages, advisor) TopicResult` (topicsample.go).
  - `matchProtobuf`: messages decoding *all* payloads with no unknown fields; ranked by topic mention → populated-field score → Confluent index-path match → `nameScore` → name length → alpha. Retries with Confluent header cut.
  - Best → `MessageName`; next up to `maxCandidates` (5) → `Candidates`.
  - `Rank(topic, values, messages) []Candidate` (rank.go): same ordering as Detect, uncapped, with the header-cut retry; used by the schema picker.
  - `DecodeWith(values, info, cut) (rows, matched)` (rank.go): decode with a chosen type, never rejects (failed rows have `DecodeOK=false`).
  - Empty topic: `documentedMessages` (comments mentioning topic).
  - `reviewTie`: Advisor (TinyDecide) annotates `ReviewPick/ReviewConfidence/ReviewAgrees`; never changes `MessageName`.
- Decoding: `dynamicpb` + `proto.Unmarshal` + `protojson.Marshal` → `Row.Decoded`.
- Stored in `topicsfile.Topic` (incl. `Candidates`), written by `detail.go runSampleAction` and `batch.go runSampleForTopic` (+ CLI `kafka sample-messages`, `show-schema`).
- **User override:** `topicsfile.Topic.MessageOverride` (`message_override`). Set by the schema picker (also sets Format=protobuf, MessageName, ProtoFile, ImportRoot, CutConfluencePrefix, Sample/MatchedCount, and rewrites the `_sample` table). Every re-sample writer wraps its entry in `topicsfile.KeepOverride(prev, next)` so the override survives: `detail.go runSampleAction`, `batch.go runSampleForTopic`, `cmd/kafka_sample.go`, `cmd/kafka_show_schema.go`. **Any new writer of topics.toml entries must do the same.** Clear an override by deleting `message_override` from topics.toml (no TUI key yet).
- **Gaps:** TUI never displays `ReviewPick`. Fetch/Tail re-run `Detect` rather than using the saved mapping/override.

## Sampling / preview (detail.go)
- `loadSampleCmd`: stored `_sample` table, else `kafka.SampleTopic` (3s) + `Detect`.
- `fetchCmd`: `kafka.FetchTopic` (30s) + `decodeRecords`.
- `startTailCmd`/`waitTailRecordCmd`: `kafka.TailTopic`, ring cap `tailRingCap`=500.
- Renderers: `renderSample`, `renderFetch`, `renderTail`, `writeDecodedRow`, `prettyJSON`.

## Persistence
- `topics.toml` via `topicsfile.File` (`Load/Get/CopyTopics/Merge/Save` atomic); path `--topics-file` (default `topics.toml`), loaded in `cmd/tui.go runTUI`.
- DuckDB: Kafka settings, proto import paths, cache configs (copy `MessageName` at add time).

## Keybindings
- Global: ctrl+c quit; menu: q quit, ↑/k ↓/j enter.
- Topic list: / filter, esc (cancel batch → clear filter → back), S/E/A, r/R refresh, enter detail, m schema picker, g groups.
- Detail: tab/l, shift+tab/h, t tail toggle, p pause, f fetch, s sample, e estimate, r reload sample, m schema picker, esc back.
- Schema picker: ↑/k ↓/j select, J/K pgup/pgdn ctrl+u/ctrl+d scroll preview, enter apply override, esc back to opener.
- Proto messages: / filter, r resolved-only, ↑/k ↓/j, enter schema, esc; schema: esc back.
- Cache list: a add, enter, /, r, d/x drop (y/n), p/space. Cache add: /, 1/2/3, enter/esc.
- Settings: ↑/k ↓/j, e/enter edit, s save, u, d (kafka) / a,d,x (proto).

## Tests
White-box (`package tui`). Pattern: `m := newXModel(deps); m,_ = m.resize(80,20); m,_ = m.update(tea.KeyPressMsg{Code: r, Text: string(r)})` or `{Code: tea.KeyEsc}`; assert private fields. Routing: `New(nil, deps)`, set `.page`, `Update`, `.(Model)`.
Helpers: `loadTestMessages` (protoschema_test.go, inline .proto), inline `&topicsfile.File{...}`, `ksTestDeps`/`ksEdit`, `typeText`, `containsAll`, `pickerFixture`/`loadedPicker` (schemapicker_test.go: feeds `schemaPickerLoadedMsg` directly, no Kafka).
No tests for detail.go, batch.go, groups.go, cache*.go.
