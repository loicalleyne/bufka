package protoinspect_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/loicalleyne/bufarrowlib"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

const usesGoogleTypes = `syntax = "proto3";
package acme;
import "google/protobuf/timestamp.proto";
import "google/type/money.proto";
import "google/type/datetime.proto";
message Order {
  google.type.Money total = 1;
  google.type.DateTime placed = 2;
  google.protobuf.Timestamp seen = 3;
}
`

func writeRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "order.proto"), []byte(usesGoogleTypes), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestEmbeddedGoogleTypesResolve(t *testing.T) {
	root := writeRoot(t)
	msgs, failures, err := protoinspect.LoadMessages([]string{root})
	if err != nil || len(failures) > 0 {
		t.Fatalf("LoadMessages: err=%v failures=%v", err, failures)
	}
	if _, ok := msgs.Get("acme.Order"); !ok {
		t.Fatal("acme.Order not loaded")
	}
	if _, ok := msgs.Get("google.type.Money"); ok {
		t.Fatal("embedded imports must not appear in the catalog")
	}
	if _, err := protoinspect.ListMessageFullNames(root); err != nil {
		t.Fatalf("ListMessageFullNames: %v", err)
	}
}

func TestEmbeddedImportDirForBufarrowlib(t *testing.T) {
	root := writeRoot(t)
	if _, err := bufarrowlib.CompileProtoToFileDescriptor("order.proto", protoinspect.WithEmbeddedImportDir([]string{root})); err != nil {
		t.Fatalf("compile with embedded dir: %v", err)
	}
	if len(protoinspect.EmbeddedImports()) != 17 {
		t.Fatalf("expected 17 embedded files, got %v", protoinspect.EmbeddedImports())
	}
}

func TestLocalCopyShadowsEmbedded(t *testing.T) {
	root := writeRoot(t)
	inc := t.TempDir()
	dir := filepath.Join(inc, "google", "type")
	_ = os.MkdirAll(dir, 0o755)
	// A local money.proto without units/nanos: if it is used, compiling a
	// file that references Money still works but the field set differs.
	_ = os.WriteFile(filepath.Join(dir, "money.proto"), []byte("syntax = \"proto3\";\npackage google.type;\nmessage Money { string local = 1; }\n"), 0o644)
	msgs, failures, err := protoinspect.LoadMessages([]string{root}, inc)
	if err != nil || len(failures) > 0 {
		t.Fatalf("err=%v failures=%v", err, failures)
	}
	info, _ := msgs.Get("acme.Order")
	money := info.Descriptor.Fields().ByName("total").Message()
	if money.Fields().ByName("local") == nil {
		t.Fatal("local copy should shadow the embedded file")
	}
}
