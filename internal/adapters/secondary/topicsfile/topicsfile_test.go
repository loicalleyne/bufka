package topicsfile_test

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/topicsfile"
)

func TestLoad_Missing(t *testing.T) {
	f, err := topicsfile.Load(filepath.Join(t.TempDir(), "topics.toml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(f.Topics) != 0 {
		t.Errorf("Topics = %v, want empty", f.Topics)
	}
}

func TestLoad_Legacy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.toml")
	if err := os.WriteFile(path, []byte("[topics]\naudit-logs.ingestion\n\n# comment\nlogistics.warehouses\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := topicsfile.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := map[string]topicsfile.Topic{"audit-logs.ingestion": {}, "logistics.warehouses": {}}
	if !reflect.DeepEqual(f.Topics, want) {
		t.Errorf("Topics = %v, want %v", f.Topics, want)
	}
}

func TestLoad_Invalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.toml")
	if err := os.WriteFile(path, []byte("[other]\nfoo bar\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := topicsfile.Load(path); err == nil {
		t.Fatal("expected error for invalid file")
	}
}

func TestMergeSaveRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topics.toml")
	if err := os.WriteFile(path, []byte("[topics]\nb.topic\na.topic\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := topicsfile.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	sampled := topicsfile.Topic{
		Table:                   "kafka_a_topic",
		Format:                  "protobuf",
		MessageName:             "pkg.Screen",
		ProtoFile:               "dir/screen.proto",
		ImportRoot:              "proto",
		CutConfluencePrefix:     true,
		ConfluentSchemaIDs:      []int32{123},
		ConfluentMessageIndexes: [][]int32{{1, 0}},
		SampleCount:             50,
		MatchedCount:            50,
		Candidates:              []string{"pkg.Other"},
		SampledAt:               "2026-10-05T12:00:00Z",
	}
	f.Merge(map[string]topicsfile.Topic{"a.topic": sampled})
	if err := f.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	data, _ := os.ReadFile(path)
	if a, b := strings.Index(string(data), "a.topic"), strings.Index(string(data), "b.topic"); a < 0 || b < 0 || a > b {
		t.Errorf("topics not sorted:\n%s", data)
	}

	got, err := topicsfile.Load(path)
	if err != nil {
		t.Fatalf("reload: %v\n%s", err, data)
	}
	want := map[string]topicsfile.Topic{"a.topic": sampled, "b.topic": {}}
	if !reflect.DeepEqual(got.Topics, want) {
		t.Errorf("Topics = %+v\nwant %+v\nfile:\n%s", got.Topics, want, data)
	}
}
