/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/loicalleyne/bufka/internal/adapters/primary/tui"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	tinydecideadapter "github.com/loicalleyne/bufka/internal/adapters/secondary/tinydecide"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/couac"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	// legacyTopicsFile is the retired per-directory topics.toml, imported
	// into DuckDB once if found.
	legacyTopicsFile = "topics.toml"
	tuiTinyDecideDir string
	tuiSampleCount   int
	tuiSampleTimeout time.Duration
)

// tuiCmd opens a full-screen terminal UI for exploring the configured Kafka
// cluster: a filterable topic list, per-topic detail tabs (metadata, sample
// data, on-demand fetch, live tail, size estimate), and a consumer-groups
// view with per-partition lag.
var tuiCmd = &cobra.Command{
	Use:   "tui",
	Short: "Open a terminal UI for exploring Kafka topics",
	Long: `Open a full-screen terminal UI for browsing the configured Kafka cluster. It
opens to a main menu with "kafka" (topics, consumer groups, cache), "schemas" (catalog,
topic mapping) and "settings" (kafka connection, schema sources) sections: a filterable list of
topics, a per-topic detail view with tabs for topic metadata, decoded sample
data, an on-demand fetch of recent messages across all partitions, a live
tail of the topic, and the size estimate, plus a consumer-groups view with
per-partition lag.

It reuses the exact code paths behind "kafka list-topics", "kafka
sample-messages", "kafka estimate-size" and "kafka show-sizes", so its results
match those commands. Everything is saved in bufka's DuckDB database (see
"bufka paths"; DUCKDB_PATH overrides its location). PROTO_IMPORT_PATHS and
ASYNCAPI_PATHS are optional but enable protobuf schema detection and AsyncAPI
documentation of JSON topics (saved AsyncAPI paths take precedence
over ASYNCAPI_PATHS).`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runTUI(ctx)
	},
}

// runTUIFunc is the TUI entry point used by bare "bufka"; replaced in tests.
var runTUIFunc = runTUI

func runTUI(ctx context.Context) error {
	cfg := GetConfig()
	var (
		db  *couac.DB
		err error
	)
	if notice := config.LegacyDBNotice(cfg); notice != "" {
		log.Print(notice)
	}
	if cfg.DuckDBPath != "" {
		db, err = database.InitDB(cfg)
		if err != nil {
			return err
		}
		defer func() { _ = db.Close() }()

		// Ensure kafka_cache_configs and kafka_topic_schemas tables are created
		_ = database.EnsureCacheConfigTable(ctx, db)
		_ = database.EnsureTopicSchemaTable(ctx, db)
		_ = database.EnsureTopicFieldsTable(ctx, db)

		// One-time import of a legacy topics.toml (retired to .bak).
		if n, err := database.MigrateTopicsToml(ctx, db, legacyTopicsFile); err == nil && n > 0 {
			log.Printf("migrated %d topic schemas from %s into DuckDB and retired to %s.bak", n, legacyTopicsFile, legacyTopicsFile)
		}
	}

	// Kafka settings saved from the TUI (settings > kafka connection) take precedence;
	// any field left empty falls back to the environment. SASL user/password
	// are resolved from the environment variables the saved settings name.
	var savedKafka kafka.SettingsInput
	kafkaCfg := *cfg
	if db != nil {
		kv, err := database.ReadKafkaSettings(ctx, db)
		if err != nil {
			log.Printf("could not read saved Kafka settings, using environment: %v", err)
		} else if saved := kafka.SettingsInputFromMap(kv); !saved.IsZero() {
			if resolved, err := kafka.ResolveSettings(saved, cfg, os.Getenv); err != nil {
				log.Printf("saved Kafka settings are invalid, using environment: %v", err)
			} else {
				kafka.OverrideConfig(&kafkaCfg, resolved)
				savedKafka = saved
				log.Printf("using Kafka settings saved in the database")
				for _, w := range resolved.Warnings {
					log.Printf("warning: %s", w)
				}
			}
		}
	}
	kafkaConf, err := kafka.NewKafkaConfigFromConfig(&kafkaCfg)
	if err != nil {
		return err
	}

	// Protobuf import paths saved from the TUI (settings > schema sources) take
	// precedence; PROTO_IMPORT_PATHS is the fallback when none were saved
	// or the saved list is empty.
	importPaths := cfg.ProtoImportPaths
	if db != nil {
		saved, found, err := database.ReadProtoImportPaths(ctx, db)
		switch {
		case err != nil:
			log.Printf("could not read saved protobuf import paths, using PROTO_IMPORT_PATHS: %v", err)
		case found && len(saved) > 0:
			importPaths = saved
			log.Printf("using protobuf import paths saved in the database (%d)", len(saved))
		}
	}

	// Include (import-only) paths saved in the database take precedence the
	// same way; PROTO_INCLUDE_PATHS is the fallback.
	includePaths := cfg.ProtoIncludePaths
	if db != nil {
		saved, found, err := database.ReadProtoIncludePaths(ctx, db)
		switch {
		case err != nil:
			log.Printf("could not read saved protobuf include paths, using PROTO_INCLUDE_PATHS: %v", err)
		case found && len(saved) > 0:
			includePaths = saved
			log.Printf("using protobuf include paths saved in the database (%d)", len(saved))
		}
	}

	// AsyncAPI spec paths saved in the database take precedence the same
	// way; ASYNCAPI_PATHS is the fallback when none were saved or the
	// saved list is empty.
	asyncAPIPaths := cfg.AsyncAPIPaths
	if db != nil {
		saved, found, err := database.ReadAsyncAPIPaths(ctx, db)
		switch {
		case err != nil:
			log.Printf("could not read saved AsyncAPI paths, using ASYNCAPI_PATHS: %v", err)
		case found && len(saved) > 0:
			asyncAPIPaths = saved
			log.Printf("using AsyncAPI paths saved in the database (%d)", len(saved))
		}
	}

	// DuckDB UI: a setting saved in settings > duckdb ui wins over the
	// environment (DUCKDB_UI_ENABLED, default off). A failure to start is
	// shown on that screen rather than aborting the TUI.
	duckUI := startDuckDBUI(ctx, cfg, db, true)
	var duckUIErr string
	if duckUI.Err != nil {
		duckUIErr = duckUI.Err.Error()
	}

	var messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]
	if len(importPaths) > 0 {
		var failures []protoinspect.FileError
		messages, failures, err = protoinspect.LoadMessages(importPaths, includePaths...)
		if err != nil {
			return err
		}
		for _, f := range failures {
			log.Printf("skipped %v", f)
		}
		log.Printf("loaded %d protobuf messages from %s", messages.Len(), strings.Join(importPaths, ";"))
	}

	var specs *asyncapi.Index
	if len(asyncAPIPaths) > 0 {
		var loadErrs []asyncapi.LoadError
		specs, loadErrs = asyncapi.Load(asyncAPIPaths)
		for _, e := range loadErrs {
			log.Printf("skipped AsyncAPI spec %v", e)
		}
	}

	var advisor topicsample.Advisor
	if tuiTinyDecideDir != "" {
		a, err := tinydecideadapter.Load(tuiTinyDecideDir)
		if err != nil {
			return err
		}
		advisor = a
	}

	deps := tui.Deps{
		KafkaConf:         kafkaConf,
		KafkaSettings:     savedKafka,
		EnvConfig:         cfg,
		DB:                db,
		Messages:          messages,
		ProtoImportPaths:  importPaths,
		ProtoIncludePaths: includePaths,
		AsyncAPIPaths:     asyncAPIPaths,
		AsyncAPI:          specs,
		Advisor:           advisor,
		SampleCount:       tuiSampleCount,
		SampleTimeout:     tuiSampleTimeout,
		DuckDBUI:          duckUI.Settings,
		DuckDBUIRunning:   duckUI.Running,
		DuckDBUIErr:       duckUIErr,
	}
	deps.SyncManager = tui.NewSyncManager(deps)
	defer deps.SyncManager.StopAll()
	go deps.SyncManager.StartActiveJobs(ctx)

	p := tea.NewProgram(tui.New(ctx, deps))
	final, err := p.Run()
	if fm, ok := final.(tui.Model); ok {
		fm.Close()
	}
	return err
}

// tuiHelpCmd prints a cheat-sheet of TUI navigation and key bindings without
// opening the terminal UI, for users who want a reminder before launching it.
var tuiHelpCmd = &cobra.Command{
	Use:   "help",
	Short: "Show TUI navigation and key bindings",
	Long:  `Print a summary of how to navigate the "bufka tui" screens and their key bindings.`,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Print(tuiHelpText)
	},
}

const tuiHelpText = `bufka tui navigation

Global
  ctrl+c               quit from anywhere (in the SQL REPL: stop waiting / clear input)

Main menu (starting screen, after the loading splash)
  ↑/↓                  navigate kafka/sql/schemas/settings sections
  enter                open selected section
  q                    quit

  kafka > topics            the topic list below
  kafka > consumer groups   the consumer groups view below
  sql > repl                DuckDB-CLI-style SQL prompt (see below)
  schemas > catalog         resolved message FQNs and the Kafka topic(s)
                             each one matched
  schemas > topic mapping   (not implemented yet)
  settings > kafka connection  view/edit Kafka connection settings
  settings > schema sources    view/edit the protobuf import paths

SQL REPL (sql > repl)
  enter                run, when the input ends with ; (or is a .command);
                       otherwise a new line
  alt+enter            new line (shift+enter too, where the terminal supports it)
  ctrl+enter           run now, adding a missing ; (where supported)
  ↑/↓  ctrl+r          history (~/.local/state/bufka/sql_history) / search it
  tab                  complete tables, columns, keywords, .commands, paths
  pgup/pgdn            scroll output
  ctrl+c               stop waiting for a query / clear input / twice: leave
  esc                  back to main menu (the session is kept)
  .help                list dot-commands; .sqlrc edits the startup script
                       (saved in the database)

Topic list (kafka > topics)
  ↑/↓                  navigate
  /                    filter topics
  esc / enter          (while filtering) cancel / apply filter
  enter                open selected topic
  S                    sample & schema-detect all (or filtered) topics
  E                    estimate size for all (or filtered) topics
  A                    sample & estimate all (or filtered) topics
  g                    open consumer groups view
  esc                  back to main menu

Topic detail
  tab, l               next tab
  h                    previous tab
  s                    run sample
  e                    run size estimate
  r                    refresh schema / reload sample
  f                    fetch on demand (Fetch tab: newest 50 per partition)
  t                    start/stop live tail (Tail tab)
  p                    pause/resume tail (Tail tab)
  esc                  back to topic list

Cache list (kafka > cache)
  ↑/↓                  navigate
  /                    filter
  a                    add a topic to the cache
  p, space             pause/resume sync. Resuming a duration- or
                       count-limited sync that starts from "now" re-runs
                       it: a fresh window of the configured duration/count
                       from the topic's current end. Other syncs continue
                       from the last cached offsets ("beginning" syncs get
                       a fresh duration/count budget).
  d, x                 drop the cached topic
  enter                open cache details
  r                    refresh
  esc                  back to main menu

Consumer groups list (kafka > consumer groups)
  ↑/↓                  navigate
  enter                open group detail
  esc                  back to main menu

Consumer group detail
  ↑/↓                  navigate
  esc                  back to groups list

Protobuf messages (protobuf > messages)
  ↑/↓                  navigate
  /                    filter by message FQN or resolved topic name
  esc / enter          (while filtering) cancel / apply filter
  r                    toggle hiding messages with no resolved topic
  enter                open message schema (comments + source definition)
  esc                  back to main menu

Protobuf message schema
  ↑/↓                  scroll
  esc                  back to protobuf messages

Schema sources (settings > schema sources)
  Three lists: proto root paths (messages listed and detected), proto include
  paths (imports only, like protoc -I; google/protobuf and google/type are
  built in) and AsyncAPI paths.
  tab / shift+tab      next / previous list
  ↑/↓                  select a path (crosses into the neighbouring list)
  a                    add a path
  e / enter            edit the selected path (enter saves, esc cancels)
  d                    delete the selected path
  s                    apply: reload messages, use the new paths, and save them
                       to the database
  u                    undo unapplied edits
  esc                  back to main menu (discards unapplied edits)
  Saved paths take precedence over PROTO_IMPORT_PATHS on the next launch;
  the env var is the fallback when nothing (or an empty list) has been
  saved. With DUCKDB_PATH=:memory: edits last for the session only.
  Include paths (PROTO_INCLUDE_PATHS) and AsyncAPI spec paths
  (ASYNCAPI_PATHS) follow the same rule: paths saved in the database take
  precedence over the env var.
  =                    copy the list's paths to the other of proto/asyncapi

DuckDB UI (settings > duckdb ui)
  ↑/↓                  select a setting
  space / enter        toggle Enabled
  e / enter            edit Port (1024-65535; enter saves, esc cancels)
  s                    apply: start, restart on the new port, or stop the UI
                       server, and save the settings to the database
  o                    open the running UI in your default browser (also
                       works on the duckdb ui item of the main menu)
  u                    undo unapplied edits
  esc                  back to main menu (discards unapplied edits)
  The UI listens on http://localhost:<port>/ only and stops when the TUI
  exits. Saved settings take precedence on the next launch; otherwise
  DUCKDB_UI_ENABLED (default false) and the port in DUCKDB_UI_ADDR (default
  4213) are used. The run command ignores saved settings and uses only the
  environment.

Kafka connection (settings > kafka connection)
  ↑/↓                  select a setting
  e / enter            edit it (enter saves, esc cancels; invalid input is rejected)
  d                    clear it (use the environment default)
  s                    apply live and save to the database
  u                    undo unapplied edits
  esc                  back to main menu (discards unapplied edits)
  Seeds: semicolon- or comma-separated host:port list. Msg chan capacity:
  integer >= 1. TLS enabled: t, f, true or false (any case).
  Consumer group prefix overrides Consumer group: with a prefix set, the group
  used is the prefix plus a random suffix. The "Group in effect" row shows the
  real group (read-only).
  SASL user / SASL password: enter the NAME of an environment variable that
  holds the value (shown with a leading $), not the value itself. Empty uses
  KAFKA_SASL_USER / KAFKA_SASL_PASSWORD.
  Saved settings take precedence over the environment on the next launch; empty
  fields fall back to it. Applying takes effect immediately; operations already
  running keep the settings they started with.

Each screen also shows its own key bindings in a help bar at the bottom.
`

func init() {
	// The same flags work on bare "bufka", which opens the TUI.
	for _, fs := range []*pflag.FlagSet{tuiCmd.Flags(), rootCmd.Flags()} {
		fs.StringVar(&tuiTinyDecideDir, "tinydecide-model", "", "directory with meta.json/model.bin; when set, TinyDecide reviews structural schema-match ties (advisory only)")
		fs.IntVar(&tuiSampleCount, "count", 50, "number of newest messages sampled/fetched per topic (per partition for Fetch)")
		fs.DurationVar(&tuiSampleTimeout, "timeout", 30*time.Second, "maximum time spent sampling a topic for the sample/estimate actions")
	}
	tuiCmd.AddCommand(tuiHelpCmd)
	rootCmd.AddCommand(tuiCmd)
}
