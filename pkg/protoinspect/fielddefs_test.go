package protoinspect_test

import (
	"reflect"
	"testing"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
	"github.com/loicalleyne/bufka/pkg/schemafield"
)

func TestMessageInfo_FieldDefs(t *testing.T) {
	root := t.TempDir()
	writeProtoFile(t, root, "f/doc.proto", `syntax = "proto3";
package f;
import "google/protobuf/timestamp.proto";

// A document.
message Doc {
  // The id.
  string id = 1; // primary key
  int64 count = 2;
  message Inner {
    bool flag = 1; // inner flag
  }
  Inner inner = 3;
  repeated Inner items = 4;
  map<string, Inner> by_name = 5;
  map<string, int32> counts = 6;
  // Kind of doc.
  Kind kind = 7;
  oneof choice {
    string a = 8;
    bytes b = 9;
  }
  optional string note = 10;
  Doc parent = 11;
  google.protobuf.Timestamp created = 12;
}

enum Kind {
  KIND_UNSPECIFIED = 0; // unset
  KIND_BIG = 1;
}
`)
	got, _, err := protoinspect.LoadMessages([]string{root})
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := got.Get("f.Doc")
	defs := doc.FieldDefs()
	byPath := map[string]schemafield.FieldDef{}
	var paths []string
	for _, d := range defs {
		byPath[d.Path] = d
		paths = append(paths, d.Path)
	}
	wantPaths := []string{
		"id", "count", "inner", "inner.flag", "items", "items[].flag",
		"by_name", "by_name{}.flag", "counts", "kind", "a", "b", "note", "parent", "created",
	}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("paths = %v, want %v", paths, wantPaths)
	}
	check := func(p string, want schemafield.FieldDef) {
		t.Helper()
		want.Path = p
		want.Source = schemafield.SourceProto
		if !reflect.DeepEqual(byPath[p], want) {
			t.Errorf("%s = %+v, want %+v", p, byPath[p], want)
		}
	}
	check("id", schemafield.FieldDef{Type: "string", Number: 1, Description: "The id. primary key"})
	check("inner.flag", schemafield.FieldDef{Type: "bool", Number: 1, Description: "inner flag"})
	check("items", schemafield.FieldDef{Type: "f.Doc.Inner", Number: 4, Repeated: true})
	check("by_name", schemafield.FieldDef{Type: "map<string,f.Doc.Inner>", Number: 5})
	check("counts", schemafield.FieldDef{Type: "map<string,int32>", Number: 6})
	check("kind", schemafield.FieldDef{
		Type: "f.Kind", Number: 7, Description: "Kind of doc.",
		Enum: []string{"KIND_UNSPECIFIED", "KIND_BIG"},
	})
	check("b", schemafield.FieldDef{Type: "bytes", Number: 9, Oneof: "choice"})
	check("note", schemafield.FieldDef{Type: "string", Number: 10, Nullable: true})
	check("parent", schemafield.FieldDef{Type: "f.Doc", Number: 11})
	check("created", schemafield.FieldDef{Type: "google.protobuf.Timestamp", Number: 12})
}

func TestMessageInfo_FieldDefs_Proto2(t *testing.T) {
	root := t.TempDir()
	writeProtoFile(t, root, "g/p2.proto", `syntax = "proto2";
package g;
message M {
  required string name = 1;
  optional int32 size = 2 [default = 7];
}
`)
	got, _, err := protoinspect.LoadMessages([]string{root})
	if err != nil {
		t.Fatal(err)
	}
	m, _ := got.Get("g.M")
	d := m.FieldDefs()
	if len(d) != 2 || !d[0].Required || d[1].Default != "7" || d[1].Required {
		t.Errorf("unexpected defs %+v", d)
	}
}
