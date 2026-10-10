// Package protoinspect walks a directory of .proto files and compiles them
// at runtime (via protocompile, no protoc required) to enumerate the
// fully-qualified names of every message they declare.
package protoinspect

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ListMessageFullNames walks root for .proto files, compiles them all in a
// single pass (so files that import each other link correctly), and returns
// an OrderedMap from each message's fully-qualified name to the relative
// .proto file path it is declared in. Synthetic map-entry messages are
// excluded. Iteration order follows the sorted discovery order of the
// .proto files, then declaration order within each file.
//
// extraImportPaths are searched, in order, after root for any import not
// found under root; imports still unresolved after that fall back to
// protocompile's bundled google/protobuf/* well-known types and then the
// embedded google/type common types (see NewResolver). Use this for
// imports (e.g. vendored googleapis types) that live outside root.
func ListMessageFullNames(root string, extraImportPaths ...string) (*OrderedMap[string, string], error) {
	relPaths, err := findProtoFiles(root)
	if err != nil {
		return nil, fmt.Errorf("protoinspect: find proto files under %q: %w", root, err)
	}

	importPaths := append([]string{root}, extraImportPaths...)
	compiler := protocompile.Compiler{
		Resolver: NewResolver(importPaths),
	}
	files, err := compiler.Compile(context.Background(), relPaths...)
	if err != nil {
		return nil, fmt.Errorf("protoinspect: compile proto files under %q: %w", root, err)
	}

	out := NewOrderedMap[string, string]()
	for _, fd := range files {
		collectFileMessages(fd, out)
	}
	return out, nil
}

// findProtoFiles returns all .proto file paths under root, relative to root
// and slash-separated, sorted for deterministic compile order.
func findProtoFiles(root string) ([]string, error) {
	var relPaths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".proto" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relPaths = append(relPaths, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(relPaths)
	return relPaths, nil
}

// collectFileMessages records the fully-qualified name of every non-map-entry
// message declared in fd, including nested messages, recursively.
func collectFileMessages(fd protoreflect.FileDescriptor, out *OrderedMap[string, string]) {
	msgs := fd.Messages()
	for i := 0; i < msgs.Len(); i++ {
		collectMessage(msgs.Get(i), fd.Path(), out)
	}
}

func collectMessage(md protoreflect.MessageDescriptor, file string, out *OrderedMap[string, string]) {
	if md.IsMapEntry() {
		return
	}
	out.Set(string(md.FullName()), file)

	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		collectMessage(nested.Get(i), file, out)
	}
}
