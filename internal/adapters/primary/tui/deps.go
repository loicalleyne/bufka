// Package tui is a PRIMARY adapter (inbound) exposing a full-screen Bubble
// Tea terminal UI for exploring the configured Kafka cluster. It depends only
// on existing secondary adapters and core services; it duplicates no
// business logic, so its results match the equivalent "kafka" subcommands.
package tui

import (
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/pkg/asyncapi"
	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/couac"
)

// Deps are the dependencies the TUI needs, built once by cmd/tui.go.
type Deps struct {
	// KafkaConf is the Kafka client configuration used for every adapter call.
	KafkaConf *kafka.KafkaClientConf
	// KafkaSettings is the raw, user-edited Kafka settings loaded from the
	// database (zero when nothing was saved); empty fields use EnvConfig.
	KafkaSettings kafka.SettingsInput
	// EnvConfig is the configuration loaded from the environment, the
	// fallback for any Kafka setting left empty in settings > kafka connection.
	EnvConfig *config.Config
	// DB is the DuckDB handle, nil when it could not be opened.
	DB *couac.DB
	// Messages are the compiled protobuf messages, nil when
	// PROTO_IMPORT_PATHS is not set.
	Messages *protoinspect.OrderedMap[string, protoinspect.MessageInfo]
	// ProtoImportPaths are the configured .proto root directories, shown
	// edited in the settings > schema sources screen.
	ProtoImportPaths []string
	// ProtoIncludePaths are import-only directories (like protoc -I):
	// searched after ProtoImportPaths when resolving imports, but never
	// listed or used for detection. Saved paths when present, otherwise
	// PROTO_INCLUDE_PATHS.
	ProtoIncludePaths []string
	// AsyncAPIPaths are the configured AsyncAPI spec directories/files
	// (2.x/3.x) documenting JSON-wire-format topics: saved paths when
	// present, otherwise ASYNCAPI_PATHS.
	AsyncAPIPaths []string
	// AsyncAPI is the index loaded from AsyncAPIPaths, nil when none.
	AsyncAPI *asyncapi.Index
	// Advisor is consulted on structural schema-detection ties, nil when no
	// TinyDecide model directory was configured.
	Advisor topicsample.Advisor
	// SampleCount is the default per-topic sample size for the "s" action
	// and the Fetch tab's per-partition retrieval, mirroring "kafka
	// sample-messages --count" and "kafka estimate-size --count".
	SampleCount int
	// SampleTimeout bounds a Kafka read for sampling/fetching one topic.
	SampleTimeout time.Duration
	// DuckDBUI is the DuckDB UI setting in effect (saved, else environment,
	// else off on the default port).
	DuckDBUI database.DuckDBUISettings
	// DuckDBUIRunning reports whether the UI server was started at launch.
	DuckDBUIRunning bool
	// DuckDBUIErr is why the UI server failed to start at launch, if it did.
	DuckDBUIErr string
	// SyncManager coordinates background caching of topic messages into DuckDB.
	SyncManager *SyncManager
}
