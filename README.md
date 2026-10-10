
# bufka

bufka consumes protobuf messages from Kafka (SASL auth), transcodes
them into Apache Arrow RecordBatches with
[bufarrowlib](https://github.com/loicalleyne/bufarrowlib), and ingests them
into DuckDB via [couac](https://github.com/loicalleyne/couac) (ADBC). It
exposes a health check and a reverse-proxied DuckDB UI over HTTP, and shuts
down gracefully on Ctrl+C / SIGTERM.

Scaffolded with [HexaGo](https://github.com/padiazg/hexago) - A hexagonal architecture scaffolding CLI tool.

## Overview

This project follows the **Hexagonal Architecture** (also known as Ports & Adapters) pattern, which promotes:
- Clear separation of concerns
- Framework independence in the core domain
- Testability through dependency inversion
- Maintainability and flexibility

## Pipeline

```
Kafka (SASL, franz-go) -> kafkaingest.Ingestor (bufarrowlib transcoders,
batched by size/time) -> kafkaingest.RunSink -> ArrowTableWriter -> DuckDB
(couac/ADBC)
```

Each configured Kafka topic maps to one DuckDB table (name derived from the
topic, see `internal/core/services/kafkaingest/tablename.go`). On shutdown,
the consumer stops polling, any partially-filled batches are flushed, and
the sink drains fully before the process exits.

## Project Structure

```
bufka/
├── cmd/                          # Application commands (Cobra CLI)
│   ├── root.go                  # Root command and configuration
│   └── run.go                   # Server startup with graceful shutdown
├── internal/
│   ├── core/                    # CORE - Business logic (no external dependencies)
│   │   ├── domain/              # Domain entities and value objects
│   │   └── services/      # Application business logic
│   ├── adapters/                # ADAPTERS - External interfaces
│   │   ├── primary/    # Inbound adapters (HTTP, gRPC, CLI)
│   │   └── secondary/   # Outbound adapters (database, external APIs)
│   └── config/                  # Configuration management
├── pkg/                         # Public reusable packages
│   └── logger/                  # Logger implementation
├── main.go                      # Application entry point
├── Makefile                     # Common development tasks
└── README.md                    # This file
```

## Architecture Principles

### Dependency Rule
Dependencies flow inward:
```
Adapters → Core (services) → Domain
```

- **Core Domain** never depends on adapters or infrastructure
- **Services** orchestrate domain logic and define interfaces (ports)
- **Adapters** implement the interfaces defined by the core

### Layers

1. **Domain** (`internal/core/domain/`)
   - Pure business entities
   - Value objects
   - Domain logic
   - No external dependencies

2. **Services** (`internal/core/services/`)
   - Application business logic
   - Orchestrates domain objects
   - Defines port interfaces
   - Framework-agnostic

3. **Adapters** (`internal/adapters/`)
   - **Primary**: Handle incoming requests (HTTP handlers, gRPC, CLI)
   - **Secondary**: Handle outgoing calls (databases, external APIs)

4. **Infrastructure** (`internal/config/`, `pkg/`)
   - Cross-cutting concerns
   - Configuration
   - Logging
   - Utilities

## Getting Started

### Prerequisites

- Go 1.27+ and a C toolchain (bufka needs CGO: `CGO_ENABLED=1`)
- Make (optional, for convenience commands)

### Installation

```bash
# Install dependencies
go mod download

# Build the application
make build
# or
CGO_ENABLED=1 go build -o bufka main.go

# Install the DuckDB driver bufka loads (once per machine/user)
./bufka setup
```

bufka talks to DuckDB through an ADBC driver that is installed separately.
`bufka setup` downloads the version this bufka is tested with
(`--driver-version` to choose another, `--force` to reinstall) from the
[dbc](https://columnar.tech/dbc) driver registry into your user driver
directory (macOS: `~/Library/Application Support/ADBC/Drivers`, Linux:
`~/.config/adbc/drivers`), verifies its signature, and checks that it loads.
It does nothing when a working driver is already installed, e.g. by
`dbc install duckdb`. Like the dbc CLI, registry requests carry dbc's
anonymous machine and install identifiers.

### Running

```bash
# Settings come from the environment; for development keep them in a .env
# file and pass it explicitly (bufka never reads ./.env on its own)
cp .env.example .env

./bufka --env-file .env run
go run main.go --env-file .env run
make run   # uses .env when present
```

The HTTP server starts on port 8080 by default (`SERVER_PORT`). Health check
is at `GET /ping`. When `DUCKDB_UI_ENABLED=true` (default off), DuckDB's
built-in UI (started via `CALL start_ui_server();` on the port in
`DUCKDB_UI_ADDR`) is reverse-proxied at `/duckdb-ui/`.

If no `KAFKA__TOPICS__N__*` variables are set, the Kafka ingest pipeline is
skipped and only the HTTP server runs.

### Configuration

Settings come from three places, highest precedence first:

1. **Saved in the database**: what you edit in the TUI settings screens
   (Kafka connection, schema sources, DuckDB UI).
2. **Environment variables.**
3. **Built-in defaults.**

`bufka config` prints every effective setting and its source (secrets are
never shown). No `.env` file is read implicitly, since an installed command
must not change behaviour with the directory it is run from: pass
`--env-file path` (or set `BUFKA_ENV_FILE`) to load one, or use
[direnv](https://direnv.net) (`.envrc` with `dotenv`) for per-directory
settings. Variables already in the environment win over the file. Keep
secrets (SASL credentials) in the environment; the database only stores
the *names* of the variables holding them. See
[`.env.example`](./.env.example) for the full list of variables, including:

- `SERVER_*` — HTTP server host/port/timeouts
- `DUCKDB_PATH` — the DuckDB database holding every saved setting, sample and
  cached topic. Default: `$XDG_DATA_HOME/bufka/bufka.duckdb`
  (`~/.local/share/bufka/bufka.duckdb`); `~/` is expanded; `:memory:` gives a
  throwaway in-memory database. `bufka paths` shows what is in use
- `DUCKDB_UI_ENABLED` (default `false`), `DUCKDB_UI_ADDR` (default
  `http://localhost:4213`; its port is the UI's port) — DuckDB UI. `run` uses
  only these; `tui` prefers settings saved in settings > duckdb ui
- `KAFKA_SEEDS`, `KAFKA_SASL_USER`, `KAFKA_SASL_PASSWORD`, `KAFKA_TLS_ENABLED`
  — Kafka connection/auth
- `KAFKA_CONSUMER_GROUP`, `KAFKA_CLIENT_COUNT`, `KAFKA_MSG_CHAN_CAP` —
  consumer tuning
- `KAFKA_ARROW_BATCH_SIZE`, `KAFKA_ARROW_FLUSH_INTERVAL` — Arrow batching
- `KAFKA__TOPICS__N__TOPIC`, `_PROTO_FILE`, `_MESSAGE_NAME`,
  `_IMPORT_PATHS`, `_CUT_CONFLUENCE_PREFIX` — indexed topic -> protobuf
  message mapping. `_CUT_CONFLUENCE_PREFIX` strips the full Confluent
  Schema Registry header (magic byte, schema ID and message-index path, 6+
  bytes); values without a valid header are left unchanged.
- `PROTO_IMPORT_PATHS` — semicolon-separated `.proto` root directories used
  by `proto list-messages` and `kafka sample-messages`; every message under
  them is listed and tried during schema detection
- `PROTO_INCLUDE_PATHS` — semicolon-separated import-only directories (like
  `protoc -I`): searched for imports after `PROTO_IMPORT_PATHS` (and, for
  `run`, after each topic's `_IMPORT_PATHS`), but their messages are never
  listed or used for detection. Use it for vendored dependencies.

  You do not need either for Google's common types: the `google/protobuf/*`
  well-known types and the `google/type/*` common types (`Money`, `Date`,
  `DateTime`, `LatLng`, ...) are built into bufka. A local copy on any
  configured path takes precedence over the built-in one.
- `ASYNCAPI_PATHS` — semicolon-separated directories/files with AsyncAPI
  specs (2.x/3.x) that document JSON-wire-format topics

Use `--env-file path/to/file` to load a non-default env file.

Use `--timeout <seconds>` to automatically trigger the same graceful
shutdown sequence after a fixed number of seconds (useful for scripted runs
and smoke tests). Omit it (or pass `0`) to run until a signal is received.

### Utility commands

```bash
bufka setup                                # install the DuckDB driver (once)
bufka paths                                # where the database and history are; checks the DuckDB driver
bufka config                               # effective settings and their source (db, env, default)
```bash
bufka kafka list-topics                    # list cluster topics
bufka proto list-messages [dir]            # list message FQNs under dir, or PROTO_IMPORT_PATHS
bufka kafka sample-messages <topic>|all    # sample topics and detect their schema
bufka kafka show-schema <topic>|all        # show detected schema from stored samples (re-samples only empty topics)
bufka kafka estimate-size <topic>|all      # estimate total topic size from message count × sample size
bufka kafka show-sizes                     # print stored size estimates (no sampling)
```

`proto list-messages` compiles each `.proto` file separately (all roots act
as import paths), reports files that fail on stderr, and prints
`<fqn>\t<root>/<file>`. `--import-path` adds import-only directories.

`kafka sample-messages` reads the newest `--count` (default 50) messages
across all partitions of a topic, without joining a consumer group, and
replaces the DuckDB table `<topic table>_sample`
with one row per message: `topic`, `partition`, `offset`, `timestamp`, raw
`key`/`value` bytes, `headers` (`STRUCT(key, value)[]`), the detection
verdict, the parsed Confluent header (`confluent_header_len`,
`confluent_schema_id`, `confluent_message_indexes`), `decode_ok` and
`decoded_json`.

JSON topics documented by AsyncAPI: when `ASYNCAPI_PATHS` is set, a topic
whose samples are JSON is looked up in the loaded specs (2.x and 3.x; the
channel key, or the 3.x `address`, is the topic name; a `topic/eventType`
channel key is split into topic and event type). The matching message is
attached to the detection result (`resolved by: asyncapi`), the samples are
checked against it (required fields missing, keys the spec does not define)
and the findings are stored as warnings. When a topic has several event types
the message is chosen from single-value `Type`/`Domain`-style enum fields,
falling back to field-name overlap. Protobuf detection is never overridden by
a spec. Field definitions (path, type, required, description, enum, example)
from the spec, or from `.proto` field comments, are stored in the DuckDB table
`kafka_topic_fields` and shown in the UI.

Detection tries every message compiled from `PROTO_IMPORT_PATHS`, on the raw
value and, when every value has one, after stripping the Confluent header.
A message matches only if every non-empty sample decodes with no unknown
fields and at least one populated field. Matching messages are ranked by
where the exact topic name appears in the `.proto` source (the message's own
comments first, then anywhere in its file, e.g. `// Topic name: <topic>`),
then by most populated
fields, then by the message's position in its `.proto` file matching the
Confluent message-index path, then by the number of words the topic name
shares with the message name (split on punctuation and CamelCase, plurals
folded; a 2–4 letter topic word also matches as an abbreviation of
consecutive CamelCase words, so `charge-requests.aol` favours
`AmericaOnLineChargeRequest`), then by
shortest name; runners-up are kept as
`candidates`. Otherwise values are checked as JSON (raw or after a 5-byte
Confluent header), else reported as `unknown`. Topics with nothing to decode
(empty, or only tombstones) are reported as `empty`; if a `.proto` file names
the topic, `message_name` is filled from the documented message (comments on
the message itself first, else the file's top-level messages) without having
been verified against data.

Results are stored in the DuckDB table `kafka_topic_schemas`, one row per
topic; other topics are kept. A schema chosen by hand in the TUI (`m` on a
topic) is kept as an override when the topic is sampled again.

`kafka show-schema <topic>|all` prints the same per-topic summary as
`sample-messages` but reads what is already stored instead of re-sampling.
For each topic it checks the DuckDB `<topic table>_sample` table
: a topic whose table already holds rows is shown
from that stored sample with no Kafka access. A topic that was empty at its
last sampling (no stored rows) is re-sampled once, with a short timeout of
`1.0s`, and its result is stored. These
re-samples run concurrently, bounded by `--concurrency` (default 4). Run
`kafka sample-messages` first to populate the stored samples.

### Topic size estimation

`kafka estimate-size <topic>|all` estimates a topic's total on-the-wire size as
**(number of messages in the topic) × (average size of a sample of messages)**.
The message count is computed from each partition's start/end offsets (no
records are scanned, so it is an upper bound — log compaction and transaction
markers mean some offsets have no live record). The average message size is
measured from the newest `--count` (default 50) messages, where each message's
size is `len(key) + len(value) + Σ len(header key + value)`. `--timeout`
(default 30s) caps the sampling time per topic.

Each estimate is printed and stored (replacing the
table on every run) in the DuckDB table `kafka_topic_size_estimates`, one row
per topic: `topic`, `estimated_at`, `partitions`, `total_messages`,
`sample_count`, `avg_message_bytes`, `stddev_bytes`, `min_message_bytes`,
`max_message_bytes`, `estimated_bytes`, `low_bytes` and `high_bytes`. The
`low`/`high` bounds apply the smallest and largest sampled message size to the
whole topic, and `stddev_bytes` indicates how rough the estimate is (a large
value relative to the average means widely varying message sizes).

`kafka show-sizes` reprints what is already stored in
`kafka_topic_size_estimates` **without re-sampling Kafka** — fast and offline
(run `estimate-size` first to populate the table).
Rows are shown largest estimated size first, in a column-aligned table with
sizes rendered in human-readable binary units (KiB/MiB/GiB…). When writing to a
terminal, long topic names are wrapped (breaking on `.`/`-`/`_` where possible)
so each row fits the terminal width; when the output is piped or redirected,
topic names are left intact.

```
TOPIC               MESSAGES  AVG    ESTIMATED   LOW         HIGH       SAMPLE  ESTIMATED AT
a2a.requests        1250000   512 B  610.35 MiB  305.18 MiB  1.19 GiB   50      2026-10-06T12:40:00Z
clicks.raw          80000     128 B  9.77 MiB    4.88 MiB    19.53 MiB  50      2026-10-06T12:40:01Z
```

### Terminal UI

```bash
bufka       # open the terminal UI (the default when run in a terminal)
bufka tui   # same, explicitly
```

Running `bufka` with no command opens the TUI and accepts the same flags
(`--count`, `--timeout`, `--tinydecide-model`). When stdin/stdout are not a
terminal (pipes, CI, cron) it prints help instead, so scripts never hang on a
full-screen UI.

`bufka tui` is a [Bubble Tea](https://github.com/charmbracelet/bubbletea)
terminal UI over the same adapters the commands above use, so its results
always match them. It opens on a filterable **topic list**
(`/` to filter, `↑`/`↓` to navigate, `enter` to open a topic, `g` for consumer
groups, `q` to quit), merging the live topic names with the stored
detected format and any stored `kafka_topic_size_estimates` row.

Opening a topic (`enter`) shows a **detail view** with tabs cycled by
`tab`/`h`/`l`:

- **Metadata** — partition count, replication factor, per-partition
  leader/replicas/start/end offsets and message count, and the topic's
  broker configuration.
- **Sample data** — the stored `_sample` table when present, else a quick
  live sample, decoded the same way as `kafka sample-messages`/`show-schema`,
  with the detected `format`/`message_name`/`candidates` and (if reviewed) the
  TinyDecide verdict shown above the records.
- **Fetch** (`f`) — an on-demand, one-shot read of the newest `--count`
  (default 50) messages from **every** partition, independent of what is
  stored, grouped by partition.
- **Live tail** (`t` start/stop, `p` pause/resume) — a group-less stream of
  new records as they arrive, capped at the last 500 in memory.
- **Size estimate** — the stored `kafka_topic_size_estimates` row for the
  topic, formatted like `kafka estimate-size`'s output.
- **Schema** (second tab) — how the schema was resolved (`override`,
  `asyncapi`, `proto-comment` or `detected`), the source file and message, the
  AsyncAPI summary/description, key meaning and cleanup policy or the `.proto`
  message comments, any conformance warnings, and a scrollable field table
  (path, type, required, description, enum/example). It works before any
  sampling when the topic is documented in an AsyncAPI spec.

From any tab: `s` samples the topic (same as `kafka sample-messages`, storing into
`kafka_topic_schemas`/`_sample`), `e` estimates its size (same as `kafka
estimate-size`, storing into `kafka_topic_size_estimates`), `r` refreshes the
Sample data tab. `esc` returns to the topic list (stopping any running tail).

`g` from the topic list opens **consumer groups**: a table of group
name/state/member count, `enter` opens a group's per-topic-partition
committed offset, end offset and lag (end − committed) against its member
assignments. When `KAFKA_CONSUMER_GROUP_PREFIX` is set, the groups view is
restricted to names carrying that prefix, matching the cluster's ACLs.

The main menu has four sections:

- **kafka** — topics, consumer groups, cache.
  The *cache* syncs topics into DuckDB tables (`a` add, `p`/space
  pause/resume, `d` drop). A sync runs indefinitely, for a duration
  (e.g. `2h`, `1d`) or for a number of messages. **Resuming a limited sync
  re-runs it**: a sync that starts from *now* consumes a fresh window of the
  configured duration/count from the topic's current end (skipping messages
  produced while paused); a sync that starts from *beginning* continues from
  the last cached offsets with a fresh duration/count budget. Unlimited syncs
  continue from the last cached offsets.
- **sql** — *repl*: a DuckDB-CLI-style SQL prompt on bufka's database (see
  [SQL REPL](#sql-repl) below).
- **schemas** — *catalog*: every loaded protobuf message and AsyncAPI message
  (`tab` cycles all/proto/asyncapi; `enter` opens the field table, `f` toggles
  source/fields for protobuf); *topic mapping*: one row per topic with format,
  schema, how it was resolved and a status (`ok`, `warnings (N)`, `no schema`,
  `not sampled`), filterable (`tab`), sortable (`s`), `enter` opens the topic.
- **settings** — *kafka connection*, and *schema sources*: the protobuf root
  paths, the protobuf include (import-only) paths and the AsyncAPI paths
  (`tab`/`shift+tab` switch list, `a`/`e`/`d` edit, `=` copies between the
  proto and AsyncAPI lists, `s` applies and saves, `r` reloads). Saved paths
  take precedence over `PROTO_IMPORT_PATHS` / `PROTO_INCLUDE_PATHS` /
  `ASYNCAPI_PATHS`.

`kafka show-schema` also prints `RESOLVED-BY`, `SPEC`, warning and field
counts, and per-topic warning and field tables.

`--tinydecide-model`, `--count` and `--timeout` mirror the same-named flags of
`kafka sample-messages`/`estimate-size`. `PROTO_IMPORT_PATHS` and
`ASYNCAPI_PATHS` are optional: without them the UI still works, but
protobuf-based detection and AsyncAPI documentation are unavailable.

#### SQL REPL

*sql > repl* is a SQL prompt modelled on the DuckDB
CLI. It runs on its own connection to bufka's database, so `SET`, `USE`,
temporary tables and transactions stay private to it, and it keeps its
session, output and settings when you go back to the menu (`esc`).
Cached topic tables carry `_kafka_partition`, `_kafka_offset`,
`_kafka_timestamp` and `_kafka_key` columns.

| Key | Action |
| --- | --- |
| `enter` | run, if the input ends with `;` (outside strings/comments) or is a `.command`; otherwise a new line |
| `alt+enter` | new line (`shift+enter` too in terminals with keyboard enhancements, e.g. kitty, Ghostty) |
| `ctrl+enter` | run now, adding a missing `;` (where supported); the footer shows which keys work |
| `↑`/`↓`, `ctrl+r` | history (`~/.local/state/bufka/sql_history`), reverse search |
| `tab` | complete tables, columns, functions and keywords (DuckDB's `sql_auto_complete`), `.commands`, file paths and topic names |
| `pgup`/`pgdn`, wheel | scroll the output |
| `ctrl+c` | stop waiting for a query, clear the input, or (twice on an empty input) leave |

A startup script, saved in the database, runs when the REPL opens: `.sqlrc`
shows it, `.sqlrc add .mode box` appends a line, `.sqlrc set`, `.sqlrc clear`,
`.sqlrc import FILE` and `.sqlrc run` manage it. An older `~/.bufka/sqlrc`
is imported once (and `~/.bufka/sql_history` moved).
Results default to the `duckbox` mode, showing the first and last rows of
`.maxrows` (40).

DuckDB (1.5, through ADBC) cannot interrupt a running statement: `ctrl+c`
stops waiting and discards the result, and the next input runs once the
statement has really finished.

Dot-commands (`.help [COMMAND]` lists them in the REPL):

| Group | Commands |
| --- | --- |
| output | `.mode duckbox\|box\|table\|csv\|tsv\|json\|jsonlines\|line\|markdown\|list`, `.headers on\|off`, `.maxrows N`, `.maxwidth N`, `.nullvalue STR`, `.separator COL [ROW]`, `.timer on\|off`, `.width N…`, `.changes on\|off` |
| schema | `.tables [PATTERN]`, `.schema [PATTERN]`, `.describe TABLE`, `.summarize TABLE`, `.databases`, `.indexes [TABLE]`, `.settings [PATTERN]`, `.extensions` |
| files & shell | `.read FILE`, `.output [FILE]`, `.once [-x] [FILE]` (`-x` opens the result as csv in the default app), `.export FILE` (last result, full, as `.csv`/`.tsv`/`.parquet`/`.json`/`.jsonl`), `.shell CMD` / `.system CMD`, `.cd DIR`, `.open`/`.attach FILE [ALIAS] [--readonly]`, `.detach ALIAS` |
| bufka | `.topics` (cached topics: rows, latest offset/timestamp, sync status), `.sync TOPIC`, `.tail TOPIC [N]` (newest N cached messages), `.ui` (start and open the DuckDB UI), `.explain [analyze] QUERY`, `.compact` (asks first), `.browse` (last result in a scrollable table) |
| repl | `.show`, `.sqlrc …`, `.help [CMD]`, `.bail on\|off`, `.echo on\|off`, `.clear`, `.exit` / `.quit` / `.q` |

## Development

### Available Commands

```bash
make help          # Show all available commands
make build         # Build the application
make run           # Run the application
make test          # Run tests
make test-coverage # Run tests with coverage report
make clean         # Clean build artifacts
make fmt           # Format code
make lint          # Run linter
```

### Testing

```bash
# Run all tests
go test ./...

# Run tests with coverage
go test -v -race -coverprofile=coverage.out ./...

# View coverage report
go tool cover -html=coverage.out
```



## API Endpoints

- `GET /ping` - Health check endpoint
- `/duckdb-ui/*` - Reverse-proxied DuckDB UI (when `DUCKDB_UI_ENABLED=true`)

## Adding New Features

When adding new features, follow the hexagonal architecture:

1. **Add domain entities** in `internal/core/domain/`
2. **Add business logic** in `internal/core/services/`
3. **Add adapters** as needed:
   - HTTP handlers in `internal/adapters/primary/http/`
   - Database repositories in `internal/adapters/secondary/database/`

## Learn More

- [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)
- [Ports & Adapters Pattern](https://herbertograca.com/2017/11/16/explicit-architecture-01-ddd-hexagonal-onion-clean-cqrs-how-i-put-it-all-together/)
- [Clean Architecture](https://blog.cleancoder.com/uncle-bob/2012/08/13/the-clean-architecture.html)

## License

[Your License Here]

## Generated by HexaGo

This project was scaffolded using HexaGo, an opinionated CLI tool for generating hexagonal architecture projects.

Learn more: https://github.com/padiazg/hexago
