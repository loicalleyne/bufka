package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestLoadProtoImportPaths(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "unset", value: "", want: nil},
		{name: "single", value: "proto", want: []string{"proto"}},
		{name: "multiple with blanks", value: " proto ; ;vendor/proto;", want: []string{"proto", "vendor/proto"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PROTO_IMPORT_PATHS", tt.value)
			cfg, err := Load("")
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !slices.Equal(cfg.ProtoImportPaths, tt.want) {
				t.Errorf("ProtoImportPaths = %q, want %q", cfg.ProtoImportPaths, tt.want)
			}
		})
	}
}

func TestLoadAsyncAPIPaths(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{name: "unset", value: "", want: nil},
		{name: "single", value: "asyncapi", want: []string{"asyncapi"}},
		{name: "multiple with blanks", value: " asyncapi ; ;specs/api.yaml;", want: []string{"asyncapi", "specs/api.yaml"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ASYNCAPI_PATHS", tt.value)
			cfg, err := Load("")
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if !slices.Equal(cfg.AsyncAPIPaths, tt.want) {
				t.Errorf("AsyncAPIPaths = %q, want %q", cfg.AsyncAPIPaths, tt.want)
			}
		})
	}
}

func TestLoadDuckDBUIEnabledDefaultsOff(t *testing.T) {
	for value, want := range map[string]bool{"": false, "true": true, "false": false, "nope": false} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("DUCKDB_UI_ENABLED", value)
			cfg, err := Load("")
			if err != nil {
				t.Fatal(err)
			}
			if cfg.DuckDBUIEnabled != want {
				t.Fatalf("DUCKDB_UI_ENABLED=%q: got %v want %v", value, cfg.DuckDBUIEnabled, want)
			}
		})
	}
}

func TestLoadProtoIncludePaths(t *testing.T) {
	t.Setenv("PROTO_INCLUDE_PATHS", " vendor/googleapis ; ;third_party;")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := []string{"vendor/googleapis", "third_party"}; !slices.Equal(cfg.ProtoIncludePaths, want) {
		t.Errorf("ProtoIncludePaths = %q, want %q", cfg.ProtoIncludePaths, want)
	}
}

func TestLoadEnvFileIsExplicit(t *testing.T) {
	dir := t.TempDir()
	wd, _ := os.Getwd()
	t.Cleanup(func() { _ = os.Chdir(wd) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BUFKA_ENV_FILE", "")
	t.Setenv("KAFKA_SEEDS", "")
	_ = os.Unsetenv("KAFKA_SEEDS")
	if err := os.WriteFile(".env", []byte("KAFKA_SEEDS=from-cwd:9092\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.KafkaSeeds != "" {
		t.Fatal("./.env must not be loaded implicitly")
	}

	other := filepath.Join(dir, "dev.env")
	if err := os.WriteFile(other, []byte("KAFKA_SEEDS=from-file:9092\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BUFKA_ENV_FILE", other)
	t.Cleanup(func() { _ = os.Unsetenv("KAFKA_SEEDS") })
	cfg, err = Load("")
	if err != nil || cfg.KafkaSeeds != "from-file:9092" {
		t.Fatalf("BUFKA_ENV_FILE: %q %v", cfg.KafkaSeeds, err)
	}
	_ = os.Unsetenv("KAFKA_SEEDS")

	if _, err := Load(filepath.Join(dir, "missing.env")); err == nil {
		t.Fatal("a named env file that does not exist must be an error")
	}
}
