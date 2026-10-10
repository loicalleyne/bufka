package protoinspect_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

// TestLoadMessages_MultipleRoots checks cross-root imports, skipping of
// broken files and first-wins on duplicate fully-qualified names.
func TestLoadMessages_MultipleRoots(t *testing.T) {
	rootA := t.TempDir()
	rootB := t.TempDir()

	writeProtoFile(t, rootA, "a/order.proto", `syntax = "proto3";
package sample;
import "b/shared.proto";
message Order {
  message Line { string sku = 1; }
  sample.shared.Shared shared = 1;
  map<string, string> labels = 2;
}
`)
	writeProtoFile(t, rootA, "a/broken.proto", `syntax = "proto3";
package sample;
message Broken { NotAType x = 1; }
`)
	writeProtoFile(t, rootB, "b/shared.proto", `syntax = "proto3";
package sample.shared;
message Shared { string id = 1; }
`)
	writeProtoFile(t, rootB, "b/dup.proto", `syntax = "proto3";
package sample;
message Order { int64 other = 1; }
`)

	got, failures, err := protoinspect.LoadMessages([]string{rootA, rootB})
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	if len(failures) != 1 || failures[0].File != "a/broken.proto" {
		t.Fatalf("failures = %v, want only a/broken.proto", failures)
	}

	wantKeys := []string{"sample.Order", "sample.Order.Line", "sample.shared.Shared"}
	if gotKeys := got.Keys(); !equalStrings(gotKeys, wantKeys) {
		t.Fatalf("Keys() = %v, want %v", gotKeys, wantKeys)
	}
	order, _ := got.Get("sample.Order")
	if order.Root != rootA || order.File != "a/order.proto" {
		t.Errorf("sample.Order from %s/%s, want first-wins %s/a/order.proto", order.Root, order.File, rootA)
	}
	if order.Descriptor.Fields().ByName("shared") == nil {
		t.Error("sample.Order descriptor missing field shared")
	}
	shared, _ := got.Get("sample.shared.Shared")
	if shared.Root != rootB {
		t.Errorf("sample.shared.Shared root = %s, want %s", shared.Root, rootB)
	}
}

func TestLoadMessages_NoRoots(t *testing.T) {
	if _, _, err := protoinspect.LoadMessages(nil); err == nil {
		t.Fatal("expected error for no roots")
	}
}

func TestLoadMessages_CommentsAndSource(t *testing.T) {
	root := t.TempDir()
	src := `syntax = "proto3";
package c;

// Topic name: c.jobs
message Job { string id = 1; }
message Other { string id = 1; }
`
	writeProtoFile(t, root, "jobs.proto", src)
	got, _, err := protoinspect.LoadMessages([]string{root})
	if err != nil {
		t.Fatalf("LoadMessages: %v", err)
	}
	job, _ := got.Get("c.Job")
	if job.Comments != " Topic name: c.jobs\n" {
		t.Errorf("Job.Comments = %q", job.Comments)
	}
	other, _ := got.Get("c.Other")
	if other.Comments != "" {
		t.Errorf("Other.Comments = %q, want empty", other.Comments)
	}
	if job.FileSource != src || other.FileSource != src {
		t.Error("FileSource does not hold the file text")
	}
}

func ExampleLoadMessages() {
	root, _ := os.MkdirTemp("", "protos")
	defer os.RemoveAll(root)
	_ = os.WriteFile(filepath.Join(root, "ping.proto"), []byte(`syntax = "proto3";
package demo;
message Ping { string id = 1; }
`), 0o644)

	msgs, _, _ := protoinspect.LoadMessages([]string{root})
	msgs.Range(func(fqn string, info protoinspect.MessageInfo) bool {
		fmt.Println(fqn, info.File)
		return true
	})
	// Output: demo.Ping ping.proto
}
