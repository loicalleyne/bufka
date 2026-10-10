package cmd

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestRunConfigShowsSources(t *testing.T) {
	ctx := context.Background()
	db, err := database.InitDB(&config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	saved := kafka.SettingsInput{Seeds: "db-broker:9092", PasswordEnv: "MY_PASS"}
	if err := database.WriteKafkaSettings(ctx, db, saved.ToMap()); err != nil {
		t.Fatal(err)
	}
	if err := database.WriteProtoImportPaths(ctx, db, []string{"/db/protos"}); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"KAFKA_SEEDS":          "env-broker:9092",
		"KAFKA_CONSUMER_GROUP": "env-group",
		"KAFKA_SASL_USER":      "alice",
		"MY_PASS":              "s3cret",
		"ASYNCAPI_PATHS":       "/env/specs",
	}
	getenv := func(k string) string { return env[k] }
	cfg := &config.Config{
		KafkaSeeds: env["KAFKA_SEEDS"], KafkaConsumerGroup: env["KAFKA_CONSUMER_GROUP"],
		KafkaSASLUser: "alice", AsyncAPIPaths: []string{"/env/specs"},
		KafkaMsgChanCap: "122880", LogLevel: "info",
		DuckDBPath: "/home/x/.local/share/bufka/bufka.duckdb", DuckDBPathDefaulted: true,
	}

	var out bytes.Buffer
	if err := runConfig(ctx, &out, cfg, db, "", getenv); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	line := func(name string) string {
		l := configRow(s, name)
		if l == "" {
			t.Fatalf("no %q row in:\n%s", name, s)
		}
		return l
	}
	for name, want := range map[string]string{
		"env file":       "env file none default",
		"database":       "default DUCKDB_PATH",
		"kafka seeds":    "db-broker:9092 db",
		"consumer group": "env-group env KAFKA_CONSUMER_GROUP",
		"sasl user":      "$KAFKA_SASL_USER (set) env",
		"sasl password":  "$MY_PASS (set) db",
		"proto paths":    "/db/protos db",
		"asyncapi paths": "/env/specs env ASYNCAPI_PATHS",
		"duckdb ui":      "off, port 4213 default",
	} {
		if l := line(name); !strings.Contains(l, want) {
			t.Errorf("%s: %q lacks %q", name, l, want)
		}
	}
	if strings.Contains(s, "s3cret") || strings.Contains(s, "alice") {
		t.Fatalf("secrets leaked:\n%s", s)
	}

	out.Reset()
	if err := runConfig(ctx, &out, cfg, db, "dev.env", getenv); err != nil {
		t.Fatal(err)
	}
	if l := configRow(out.String(), "env file"); !strings.Contains(l, "dev.env flag --env-file") {
		t.Fatalf("env file flag: %q", l)
	}
}

// configRow returns the "bufka config" row for name, whitespace-collapsed.
func configRow(s, name string) string {
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(l, name+" ") {
			return strings.Join(strings.Fields(l), " ")
		}
	}
	return ""
}
