package protoinspect

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// MessageInfo locates a compiled message descriptor.
type MessageInfo struct {
	// Root is the directory File was found under.
	Root string
	// File is the .proto path relative to Root, slash-separated.
	File       string
	Descriptor protoreflect.MessageDescriptor
	// Comments are the message's leading, detached and trailing comments.
	Comments string
	// FileSource is the full text of File, shared by its messages.
	FileSource string
}

// FileError records a .proto file that failed to compile.
type FileError struct {
	Root string
	File string
	Err  error
}

func (e FileError) Error() string {
	return fmt.Sprintf("%s/%s: %v", e.Root, e.File, e.Err)
}

// LoadMessages walks every root for .proto files and compiles each one
// separately, with all roots plus extraImportPaths as import paths, so that
// one broken or conflicting file does not prevent the rest from loading.
// Files that fail are returned as FileErrors rather than aborting. When the
// same relative path or fully-qualified message name appears more than once,
// the first one found (in root order) wins. Synthetic map-entry messages are
// excluded.
func LoadMessages(roots []string, extraImportPaths ...string) (*OrderedMap[string, MessageInfo], []FileError, error) {
	if len(roots) == 0 {
		return nil, nil, fmt.Errorf("protoinspect: no proto roots given")
	}
	importPaths := append(append([]string{}, roots...), extraImportPaths...)
	out := NewOrderedMap[string, MessageInfo]()
	var failures []FileError
	seenFiles := make(map[string]bool)

	for _, root := range roots {
		relPaths, err := findProtoFiles(root)
		if err != nil {
			return nil, nil, fmt.Errorf("protoinspect: find proto files under %q: %w", root, err)
		}
		for _, rel := range relPaths {
			if seenFiles[rel] {
				continue
			}
			seenFiles[rel] = true

			compiler := protocompile.Compiler{
				Resolver:       NewResolver(importPaths),
				SourceInfoMode: protocompile.SourceInfoStandard,
			}
			files, err := compiler.Compile(context.Background(), rel)
			if err != nil {
				failures = append(failures, FileError{Root: root, File: rel, Err: err})
				continue
			}
			source, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
			if err != nil {
				failures = append(failures, FileError{Root: root, File: rel, Err: err})
				continue
			}
			fileSource := string(source)
			locations := files[0].SourceLocations()
			msgs := files[0].Messages()
			for i := 0; i < msgs.Len(); i++ {
				collectDescriptors(msgs.Get(i), func(md protoreflect.MessageDescriptor) {
					if _, exists := out.Get(string(md.FullName())); !exists {
						out.Set(string(md.FullName()), MessageInfo{
							Root:       root,
							File:       rel,
							Descriptor: md,
							Comments:   messageComments(locations.ByDescriptor(md)),
							FileSource: fileSource,
						})
					}
				})
			}
		}
	}
	return out, failures, nil
}

func messageComments(loc protoreflect.SourceLocation) string {
	var b strings.Builder
	for _, c := range loc.LeadingDetachedComments {
		b.WriteString(c)
	}
	b.WriteString(loc.LeadingComments)
	b.WriteString(loc.TrailingComments)
	return b.String()
}

func collectDescriptors(md protoreflect.MessageDescriptor, fn func(protoreflect.MessageDescriptor)) {
	if md.IsMapEntry() {
		return
	}
	fn(md)
	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		collectDescriptors(nested.Get(i), fn)
	}
}
