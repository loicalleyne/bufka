// Package asyncapi loads AsyncAPI (2.x and 3.x) specifications that document
// Kafka topics carrying JSON messages, and indexes them by topic.
//
// Specs are parsed into a generic, order-preserving tree rather than typed
// AsyncAPI structs, so that the loader tolerates the many slightly different
// shapes found in hand-written specs. Message payload schemas are flattened
// into source-neutral schemafield.FieldDef values.
package asyncapi

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/loicalleyne/bufka/pkg/schemafield"
)

// ErrExternalRef is wrapped by LoadError values reporting a $ref that points
// outside the spec document. Such refs are never fetched; the affected field
// keeps the ref string as its Type.
var ErrExternalRef = errors.New("external $ref not resolved")

// LoadError reports a problem with a single spec file.
type LoadError struct {
	File string
	Err  error
}

func (e LoadError) Error() string { return e.File + ": " + e.Err.Error() }

func (e LoadError) Unwrap() error { return e.Err }

// MessageSpec describes one message that may appear on a topic.
type MessageSpec struct {
	Name           string
	Title          string
	Summary        string
	Description    string
	ContentType    string
	EventType      string // from a "topic/eventType" channel key, if any
	KeyDescription string // headers.properties.key.description
	Fields         []schemafield.FieldDef
	Required       []string // top-level required payload properties
}

// TopicSpec describes a Kafka topic documented by an AsyncAPI spec.
type TopicSpec struct {
	Topic           string
	SpecFile        string
	SpecTitle       string
	SpecVersion     string // info.version
	AsyncAPIVersion string
	Summary         string
	Description     string
	CleanupPolicy   string
	Messages        []MessageSpec
}

// Index holds the topics found by Load.
type Index struct {
	topics map[string]*TopicSpec
	files  []string
}

// Topics returns all topics sorted by name.
func (ix *Index) Topics() []TopicSpec {
	out := make([]TopicSpec, 0, len(ix.topics))
	for _, t := range ix.topics {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out
}

// Lookup returns the spec for a topic.
func (ix *Index) Lookup(topic string) (TopicSpec, bool) {
	t, ok := ix.topics[topic]
	if !ok {
		return TopicSpec{}, false
	}
	return *t, true
}

// Files returns the AsyncAPI spec files that were loaded, sorted.
func (ix *Index) Files() []string {
	return append([]string(nil), ix.files...)
}

// Load reads AsyncAPI specs from the given files or directories (walked
// recursively). Only *.yml, *.yaml and *.json files with a top-level
// "asyncapi" key are considered; other files are skipped silently. Errors and
// warnings are reported per file and never abort loading of other files.
func Load(paths []string) (*Index, []LoadError) {
	ix := &Index{topics: map[string]*TopicSpec{}}
	var errs []LoadError
	seen := map[string]bool{}
	for _, p := range paths {
		st, err := os.Stat(p)
		if err != nil {
			errs = append(errs, LoadError{File: p, Err: err})
			continue
		}
		if !st.IsDir() {
			if !seen[p] {
				seen[p] = true
				errs = append(errs, ix.loadFile(p, true)...)
			}
			continue
		}
		werr := filepath.WalkDir(p, func(fp string, d fs.DirEntry, err error) error {
			if err != nil {
				errs = append(errs, LoadError{File: fp, Err: err})
				return nil
			}
			if d.IsDir() || seen[fp] || !isSpecExt(fp) {
				return nil
			}
			seen[fp] = true
			errs = append(errs, ix.loadFile(fp, false)...)
			return nil
		})
		if werr != nil {
			errs = append(errs, LoadError{File: p, Err: werr})
		}
	}
	sort.Strings(ix.files)
	return ix, errs
}

func isSpecExt(p string) bool {
	switch strings.ToLower(filepath.Ext(p)) {
	case ".yml", ".yaml", ".json":
		return true
	}
	return false
}

// loadFile parses one file. Parse errors are reported only when the file was
// named explicitly or looks like an AsyncAPI document.
func (ix *Index) loadFile(path string, explicit bool) []LoadError {
	data, err := os.ReadFile(path)
	if err != nil {
		return []LoadError{{File: path, Err: err}}
	}
	root, err := parse(data)
	if err != nil {
		if explicit || strings.Contains(string(data), "asyncapi") {
			return []LoadError{{File: path, Err: fmt.Errorf("parse: %w", err)}}
		}
		return nil
	}
	doc, ok := root.(*obj)
	if !ok || !doc.has("asyncapi") {
		return nil
	}
	ix.files = append(ix.files, path)
	l := &loader{file: path, root: doc}
	for _, t := range l.topics() {
		if ex, ok := ix.topics[t.Topic]; ok {
			ex.Messages = append(ex.Messages, t.Messages...)
			if ex.Summary == "" {
				ex.Summary = t.Summary
			}
			if ex.Description == "" {
				ex.Description = t.Description
			}
			if ex.CleanupPolicy == "" {
				ex.CleanupPolicy = t.CleanupPolicy
			}
			continue
		}
		ix.topics[t.Topic] = t
	}
	return l.errs
}
