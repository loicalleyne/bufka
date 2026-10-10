/*
Copyright © 2026
*/
// Package config loads bufka configuration from the process environment,
// plus an env file only when one is named explicitly (--env-file or
// $BUFKA_ENV_FILE); godotenv only fills variables that are not already set.
// Settings saved in the database take precedence over the environment; see
// "bufka config".
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all application configuration.
type Config struct {
	LogLevel  string
	LogFormat string

	Server ServerConfig

	// DuckDBPath is the local DuckDB database file to open: DUCKDB_PATH,
	// else DefaultDBPath (~/.local/share/bufka/bufka.duckdb). Empty means
	// in-memory (DUCKDB_PATH=:memory:).
	DuckDBPath string
	// DuckDBPathDefaulted is true when DUCKDB_PATH was unset and DuckDBPath
	// is the default location.
	DuckDBPathDefaulted bool
	// DuckDBUIEnabled starts DuckDB's built-in UI server (CALL
	// start_ui_server()). Off by default; run also proxies it at /duckdb-ui/.
	// In the TUI a setting saved in settings > duckdb ui takes precedence.
	DuckDBUIEnabled bool
	// DuckDBUIAddr is the address DuckDB's UI server listens on; its port is
	// used as the server's ui_local_port.
	DuckDBUIAddr string

	// KafkaEnabled is true when at least one Kafka topic is configured.
	KafkaEnabled bool
	KafkaSeeds   string
	// KafkaConsumerGroup is the Kafka consumer group name.
	KafkaConsumerGroup string
	// KafkaConsumerGroupPrefix, when set, overrides KafkaConsumerGroup with
	// prefix + random suffix, useful for avoiding rebalances across restarts.
	KafkaConsumerGroupPrefix string
	KafkaInstancePrefix      string
	KafkaClientCount         string
	KafkaMsgChanCap          string
	KafkaArrowBatchSize      string
	KafkaArrowFlushInterval  string
	KafkaTLSEnabled          bool
	KafkaSASLUser            string
	KafkaSASLPassword        string
	// KafkaCutConfluencePrefix, when true, strips the 6-byte Confluent Schema
	// Registry prefix from every consumed topic that has no per-topic
	// CutConfluencePrefix override in KafkaTopics.
	KafkaCutConfluencePrefix bool
	KafkaTopics              []KafkaTopicConfig

	// ProtoImportPaths are the .proto root directories from the
	// semicolon-separated PROTO_IMPORT_PATHS variable.
	ProtoImportPaths []string

	// ProtoIncludePaths are import-only directories from the
	// semicolon-separated PROTO_INCLUDE_PATHS variable (like protoc -I):
	// searched, after ProtoImportPaths, for imports, but their own messages
	// are never listed or used for schema detection.
	ProtoIncludePaths []string

	// AsyncAPIPaths are the directories and/or files holding AsyncAPI
	// specs (2.x/3.x) that document JSON-wire-format topics, from the
	// semicolon-separated ASYNCAPI_PATHS variable.
	AsyncAPIPaths []string
}

// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Port            int
	Host            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// KafkaTopicConfig describes a protobuf message consumed from one Kafka topic.
type KafkaTopicConfig struct {
	Topic       string
	ProtoFile   string
	MessageName string
	ImportPaths []string
	// CutConfluencePrefix, when true, strips the 6-byte Confluent Schema
	// Registry prefix from messages on this topic before protobuf decoding.
	CutConfluencePrefix bool
}

// EnvFileVar names an env file to load when --env-file is not given.
const EnvFileVar = "BUFKA_ENV_FILE"

// Load reads the process environment into a Config. No .env file is read
// implicitly: an installed CLI must not change behaviour with the current
// directory. envFile (--env-file), else $BUFKA_ENV_FILE, names one to load
// first; it must exist. Variables already set in the environment win over
// the file.
func Load(envFile string) (*Config, error) {
	if envFile == "" {
		envFile = strings.TrimSpace(os.Getenv(EnvFileVar))
	}
	if envFile != "" {
		if err := godotenv.Load(envFile); err != nil {
			return nil, fmt.Errorf("loading env file %s: %w", envFile, err)
		}
	}

	cfg := &Config{
		LogLevel:  getEnvDefault("LOG_LEVEL", "info"),
		LogFormat: getEnvDefault("LOG_FORMAT", "json"),
		Server: ServerConfig{
			Port:            getEnvInt("SERVER_PORT", 8080),
			Host:            getEnvDefault("SERVER_HOST", "0.0.0.0"),
			ReadTimeout:     getEnvDuration("SERVER_READ_TIMEOUT", 15*time.Second),
			WriteTimeout:    getEnvDuration("SERVER_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:     getEnvDuration("SERVER_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: getEnvDuration("SERVER_SHUTDOWN_TIMEOUT", 30*time.Second),
		},

		DuckDBUIEnabled: getEnvBool("DUCKDB_UI_ENABLED", false),
		DuckDBUIAddr:    getEnvDefault("DUCKDB_UI_ADDR", "http://localhost:4213"),

		KafkaSeeds:               strings.TrimSpace(os.Getenv("KAFKA_SEEDS")),
		KafkaConsumerGroup:       os.Getenv("KAFKA_CONSUMER_GROUP"),
		KafkaConsumerGroupPrefix: os.Getenv("KAFKA_CONSUMER_GROUP_PREFIX"),
		KafkaInstancePrefix:      os.Getenv("KAFKA_INSTANCE_PREFIX"),
		KafkaClientCount:         getEnvDefault("KAFKA_CLIENT_COUNT", "1"),
		KafkaMsgChanCap:          getEnvDefault("KAFKA_MSG_CHAN_CAP", "122880"),
		KafkaArrowBatchSize:      getEnvDefault("KAFKA_ARROW_BATCH_SIZE", "1000"),
		KafkaArrowFlushInterval:  getEnvDefault("KAFKA_ARROW_FLUSH_INTERVAL", "5s"),
		KafkaTLSEnabled:          getEnvBool("KAFKA_TLS_ENABLED", false),
		KafkaSASLUser:            os.Getenv("KAFKA_SASL_USER"),
		KafkaSASLPassword:        os.Getenv("KAFKA_SASL_PASSWORD"),
		KafkaCutConfluencePrefix: getEnvBool("KAFKA_CUT_CONFLUENCE_PREFIX", false),

		ProtoImportPaths:  splitList(os.Getenv("PROTO_IMPORT_PATHS"), ";"),
		ProtoIncludePaths: splitList(os.Getenv("PROTO_INCLUDE_PATHS"), ";"),
		AsyncAPIPaths:     splitList(os.Getenv("ASYNCAPI_PATHS"), ";"),
	}

	topics, err := loadKafkaTopicsFromEnv()
	if err != nil {
		return nil, err
	}
	cfg.KafkaTopics = topics
	cfg.KafkaEnabled = len(topics) > 0

	if cfg.DuckDBPath, cfg.DuckDBPathDefaulted, err = resolveDBPath(os.Getenv("DUCKDB_PATH")); err != nil {
		return nil, err
	}

	if cfg.KafkaEnabled {
		if cfg.KafkaSeeds == "" {
			return nil, fmt.Errorf("KAFKA_SEEDS is required when Kafka topics are configured")
		}
		if (cfg.KafkaSASLUser == "") != (cfg.KafkaSASLPassword == "") {
			return nil, fmt.Errorf("KAFKA_SASL_USER and KAFKA_SASL_PASSWORD must both be set or both be empty")
		}
	}

	return cfg, nil
}

// loadKafkaTopicsFromEnv loads indexed Kafka topic definitions from the
// environment, e.g.:
//
//	KAFKA__TOPICS__0__TOPIC=my-topic
//	KAFKA__TOPICS__0__PROTO_FILE=schema/my.proto
//	KAFKA__TOPICS__0__MESSAGE_NAME=my.package.MyMessage
//	KAFKA__TOPICS__0__IMPORT_PATHS=./schema,./vendor/proto
//	KAFKA__TOPICS__0__CUT_CONFLUENCE_PREFIX=true
func loadKafkaTopicsFromEnv() ([]KafkaTopicConfig, error) {
	var topics []KafkaTopicConfig
	seen := make(map[string]bool)
	for i := 0; ; i++ {
		prefix := fmt.Sprintf("KAFKA__TOPICS__%d__", i)
		topic := strings.TrimSpace(os.Getenv(prefix + "TOPIC"))
		protoFile := strings.TrimSpace(os.Getenv(prefix + "PROTO_FILE"))
		messageName := strings.TrimSpace(os.Getenv(prefix + "MESSAGE_NAME"))
		if topic == "" && protoFile == "" && messageName == "" {
			// No more indexed topics; stop scanning once we hit a fully
			// empty slot to avoid scanning indefinitely.
			break
		}
		if topic == "" {
			return nil, fmt.Errorf("%sTOPIC is required", prefix)
		}
		if protoFile == "" {
			return nil, fmt.Errorf("%sPROTO_FILE is required", prefix)
		}
		if messageName == "" {
			return nil, fmt.Errorf("%sMESSAGE_NAME is required", prefix)
		}
		if seen[topic] {
			return nil, fmt.Errorf("duplicate Kafka topic %q", topic)
		}
		seen[topic] = true

		var importPaths []string
		if raw := strings.TrimSpace(os.Getenv(prefix + "IMPORT_PATHS")); raw != "" {
			for _, p := range strings.Split(raw, ",") {
				if p = strings.TrimSpace(p); p != "" {
					importPaths = append(importPaths, p)
				}
			}
		}
		cutConfluencePrefix := false
		if raw := strings.TrimSpace(os.Getenv(prefix + "CUT_CONFLUENCE_PREFIX")); raw != "" {
			parsed, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, fmt.Errorf("%sCUT_CONFLUENCE_PREFIX must be a boolean: %w", prefix, err)
			}
			cutConfluencePrefix = parsed
		}

		topics = append(topics, KafkaTopicConfig{
			Topic:               topic,
			ProtoFile:           protoFile,
			MessageName:         messageName,
			ImportPaths:         importPaths,
			CutConfluencePrefix: cutConfluencePrefix,
		})
	}
	return topics, nil
}

// splitList splits value on sep, trimming entries and dropping empty ones.
func splitList(value, sep string) []string {
	var out []string
	for _, p := range strings.Split(value, sep) {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getEnvDefault(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func getEnvInt(name string, def int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return def
}

func getEnvBool(name string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := strconv.ParseBool(v); err == nil {
			return parsed
		}
	}
	return def
}

func getEnvDuration(name string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if parsed, err := time.ParseDuration(v); err == nil {
			return parsed
		}
	}
	return def
}
