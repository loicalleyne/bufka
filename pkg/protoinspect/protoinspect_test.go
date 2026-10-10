package protoinspect_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

// writeProtoFile writes content to name under dir, creating dir if needed.
func writeProtoFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}

func TestListMessageFullNames(t *testing.T) {
	dir := t.TempDir()

	writeProtoFile(t, dir, "base.proto", `syntax = "proto3";
package sample;

message Address {
  string city = 1;
}
`)

	writeProtoFile(t, dir, "order.proto", `syntax = "proto3";
package sample;

import "base.proto";

message Order {
  message Item {
    string sku = 1;
  }
  Address ship_to = 1;
  repeated Item items = 2;
  map<string, string> labels = 3;
}
`)

	got, err := protoinspect.ListMessageFullNames(dir)
	if err != nil {
		t.Fatalf("ListMessageFullNames: %v", err)
	}

	wantKeys := []string{"sample.Address", "sample.Order", "sample.Order.Item"}
	if gotKeys := got.Keys(); !equalStrings(gotKeys, wantKeys) {
		t.Fatalf("Keys() = %v, want %v", gotKeys, wantKeys)
	}

	wantFiles := map[string]string{
		"sample.Address":    "base.proto",
		"sample.Order":      "order.proto",
		"sample.Order.Item": "order.proto",
	}
	for fqn, wantFile := range wantFiles {
		file, ok := got.Get(fqn)
		if !ok {
			t.Fatalf("Get(%q) missing", fqn)
		}
		if file != wantFile {
			t.Errorf("Get(%q) = %q, want %q", fqn, file, wantFile)
		}
	}

	if _, ok := got.Get("sample.LabelsEntry"); ok {
		t.Error("synthetic map-entry message should be excluded")
	}
	if got.Len() != len(wantKeys) {
		t.Errorf("Len() = %d, want %d", got.Len(), len(wantKeys))
	}
}

// TestListMessageFullNames_RealProtoDir is a smoke test against the repo's
// real proto/ directory. Imports within it (e.g.
// shop/v1/*) are rooted at proto/, not at any
// subdirectory, so the full tree must be compiled together.
func TestListMessageFullNames_RealProtoDir(t *testing.T) {
	root := filepath.Join("..", "..", "proto")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("proto dir not found: %v", err)
	}

	got, err := protoinspect.ListMessageFullNames(root)
	if err != nil {
		t.Fatalf("ListMessageFullNames(%q): %v", root, err)
	}
	if got.Len() == 0 {
		t.Error("expected at least one message under proto/")
	}
}

// TestListMessageFullNames_ExtraImportPaths verifies that imports not found
// under root are resolved from extraImportPaths.
func TestListMessageFullNames_ExtraImportPaths(t *testing.T) {
	root := t.TempDir()
	vendorDir := t.TempDir()

	writeProtoFile(t, vendorDir, "vendor/shared.proto", `syntax = "proto3";
package vendor;

message Shared {
  string id = 1;
}
`)

	writeProtoFile(t, root, "main.proto", `syntax = "proto3";
package sample;

import "vendor/shared.proto";

message Main {
  vendor.Shared shared = 1;
}
`)

	if _, err := protoinspect.ListMessageFullNames(root); err == nil {
		t.Fatal("expected error without extraImportPaths, got nil")
	}

	got, err := protoinspect.ListMessageFullNames(root, vendorDir)
	if err != nil {
		t.Fatalf("ListMessageFullNames with extraImportPaths: %v", err)
	}

	wantKeys := []string{"sample.Main"}
	if gotKeys := got.Keys(); !equalStrings(gotKeys, wantKeys) {
		t.Fatalf("Keys() = %v, want %v", gotKeys, wantKeys)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
