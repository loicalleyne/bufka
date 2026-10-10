package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/couac"
	"github.com/spf13/cobra"
)

// configCmd prints every effective setting and where it came from.
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show the effective settings and where each comes from (db, env or default)",
	Long: `Print each setting bufka uses and its source, in precedence order:

  db       saved in bufka's database (edited in the TUI settings screens)
  env      an environment variable (or --env-file / $BUFKA_ENV_FILE)
  default  built in

Secrets are never printed: SASL credentials show only whether they are set
and which variable holds them. No .env file is read unless --env-file or
$BUFKA_ENV_FILE names one.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg := GetConfig()
		var db *couac.DB
		var dbErr error
		if cfg.DuckDBPath != "" {
			if _, err := os.Stat(cfg.DuckDBPath); err == nil {
				if db, dbErr = database.InitDB(cfg); dbErr == nil {
					defer func() { _ = db.Close() }()
				}
			}
		}
		w := cmd.OutOrStdout()
		if err := runConfig(cmd.Context(), w, cfg, db, envFile, os.Getenv); err != nil {
			return err
		}
		if dbErr != nil {
			// Typically another bufka (the TUI) holds the database lock.
			_, _ = fmt.Fprintf(w, "\nnote: could not open the database, so saved (db) settings are not shown: %v\n", dbErr)
		}
		return nil
	},
}

// setting is one row of "bufka config".
type setting struct {
	name, value, source, detail string
}

// envSource is "env" when name is set in the environment, else "default".
func envSource(getenv func(string) string, name string) string {
	if strings.TrimSpace(getenv(name)) != "" {
		return "env"
	}
	return "default"
}

func runConfig(ctx context.Context, w io.Writer, cfg *config.Config, db *couac.DB, envFileFlag string, getenv func(string) string) error {
	var rows []setting
	add := func(name, value, source, detail string) {
		if value == "" {
			value = "(unset)"
		}
		rows = append(rows, setting{name, value, source, detail})
	}

	// Env file.
	switch {
	case envFileFlag != "":
		add("env file", envFileFlag, "flag", "--env-file")
	case getenv(config.EnvFileVar) != "":
		add("env file", getenv(config.EnvFileVar), "env", config.EnvFileVar)
	default:
		add("env file", "none", "default", "pass --env-file to load one")
	}

	// Database.
	dbVal, dbSrc := cfg.DuckDBPath, "env"
	if cfg.DuckDBPathDefaulted {
		dbSrc = "default"
	}
	if dbVal == "" {
		dbVal = config.MemoryDB
	}
	add("database", dbVal, dbSrc, "DUCKDB_PATH")

	// Kafka connection: saved values win field by field.
	var saved kafka.SettingsInput
	if db != nil {
		if kv, err := database.ReadKafkaSettings(ctx, db); err == nil {
			saved = kafka.SettingsInputFromMap(kv)
		}
	}
	pick := func(name, savedV, envVar, envV, def string) {
		switch {
		case strings.TrimSpace(savedV) != "":
			add(name, savedV, "db", "settings > kafka connection")
		case strings.TrimSpace(getenv(envVar)) != "":
			add(name, envV, "env", envVar)
		default:
			add(name, def, "default", envVar)
		}
	}
	pick("kafka seeds", saved.Seeds, "KAFKA_SEEDS", cfg.KafkaSeeds, "")
	pick("consumer group", saved.ConsumerGroup, "KAFKA_CONSUMER_GROUP", cfg.KafkaConsumerGroup, "")
	pick("group prefix", saved.ConsumerGroupPrefix, "KAFKA_CONSUMER_GROUP_PREFIX", cfg.KafkaConsumerGroupPrefix, "")
	pick("instance prefix", saved.InstancePrefix, "KAFKA_INSTANCE_PREFIX", cfg.KafkaInstancePrefix, "")
	pick("msg chan cap", saved.MsgChanCap, "KAFKA_MSG_CHAN_CAP", cfg.KafkaMsgChanCap, cfg.KafkaMsgChanCap)
	pick("tls", saved.TLS, "KAFKA_TLS_ENABLED", strconv.FormatBool(cfg.KafkaTLSEnabled), "false")
	secret := func(name, savedEnv, defEnv string) {
		varName := defEnv
		src := "env"
		if n := kafka.NormalizeEnvName(savedEnv); n != "" {
			varName, src = n, "db"
		}
		state := "not set"
		if getenv(varName) != "" {
			state = "set"
		} else if src == "env" {
			src = "default"
		}
		add(name, "$"+varName+" ("+state+")", src, "value hidden")
	}
	secret("sasl user", saved.UserEnv, "KAFKA_SASL_USER")
	secret("sasl password", saved.PasswordEnv, "KAFKA_SASL_PASSWORD")

	// Schema sources: a saved non-empty list wins over the env var.
	paths := func(name string, read func(context.Context, *couac.DB) ([]string, bool, error), envVar string, envV []string) {
		if db != nil {
			if p, found, err := read(ctx, db); err == nil && found && len(p) > 0 {
				add(name, strings.Join(p, ";"), "db", "settings > schema sources")
				return
			}
		}
		add(name, strings.Join(envV, ";"), envSource(getenv, envVar), envVar)
	}
	paths("proto paths", database.ReadProtoImportPaths, "PROTO_IMPORT_PATHS", cfg.ProtoImportPaths)
	paths("include paths", database.ReadProtoIncludePaths, "PROTO_INCLUDE_PATHS", cfg.ProtoIncludePaths)
	paths("asyncapi paths", database.ReadAsyncAPIPaths, "ASYNCAPI_PATHS", cfg.AsyncAPIPaths)

	// DuckDB UI.
	ui, uiSrc := database.DuckDBUIFromConfig(cfg), "env"
	if envSource(getenv, "DUCKDB_UI_ENABLED") == "default" && envSource(getenv, "DUCKDB_UI_ADDR") == "default" {
		uiSrc = "default"
	}
	if db != nil {
		if s, found, err := database.ReadDuckDBUISettings(ctx, db); err == nil && found {
			ui, uiSrc = s, "db"
		}
	}
	state := "off"
	if ui.Enabled {
		state = "on"
	}
	add("duckdb ui", fmt.Sprintf("%s, port %d", state, ui.Port), uiSrc, "settings > duckdb ui")

	add("log level", cfg.LogLevel, envSource(getenv, "LOG_LEVEL"), "LOG_LEVEL")

	nameW, valW := 0, 0
	for _, r := range rows {
		nameW = max(nameW, len(r.name))
		valW = max(valW, len(r.value))
	}
	valW = min(valW, 60)
	for _, r := range rows {
		if _, err := fmt.Fprintf(w, "%-*s  %-*s  %-7s  %s\n", nameW, r.name, valW, r.value, r.source, r.detail); err != nil {
			return err
		}
	}
	if _, err := os.Stat(cfg.DuckDBPath); db == nil && cfg.DuckDBPath != "" && os.IsNotExist(err) {
		_, _ = fmt.Fprintf(w, "\n(database not created yet: nothing saved)\n")
	}
	return nil
}

func init() {
	rootCmd.AddCommand(configCmd)
}
