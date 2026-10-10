// Package topicsfile is a SECONDARY adapter that reads and writes topics.toml,
// the per-topic record of detected message schemas.
package topicsfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	toml "github.com/pelletier/go-toml/v2"
)

// Topic is the detected schema of one Kafka topic.
type Topic struct {
	Table                   string    `toml:"table,omitempty"`
	Format                  string    `toml:"format,omitempty"`
	MessageName             string    `toml:"message_name,omitempty"`
	ProtoFile               string    `toml:"proto_file,omitempty"`
	ImportRoot              string    `toml:"import_root,omitempty"`
	CutConfluencePrefix     bool      `toml:"cut_confluence_prefix,omitempty"`
	ConfluentSchemaIDs      []int32   `toml:"confluent_schema_ids,omitempty"`
	ConfluentMessageIndexes [][]int32 `toml:"confluent_message_indexes,omitempty"`
	SampleCount             int       `toml:"sample_count,omitempty"`
	MatchedCount            int       `toml:"matched_count,omitempty"`
	Candidates              []string  `toml:"candidates,omitempty"`
	// Reviewed is true when the match was a structural tie and a
	// topicsample.Advisor (e.g. TinyDecide) was consulted.
	Reviewed bool `toml:"reviewed,omitempty"`
	// ReviewPick is the advisor's choice among MessageName and Candidates,
	// empty if it had no opinion. Never applied automatically.
	ReviewPick string `toml:"review_pick,omitempty"`
	// ReviewConfidence is the advisor's confidence in ReviewPick.
	ReviewConfidence float64 `toml:"review_confidence,omitempty"`
	// ReviewAgrees is true when ReviewPick equals MessageName.
	ReviewAgrees bool `toml:"review_agrees,omitempty"`
	// SampledAt is RFC 3339; go-toml's omitempty mishandles time.Time.
	SampledAt string `toml:"sampled_at,omitempty"`
	// MessageOverride is true when MessageName was chosen by a user rather
	// than detected; re-sampling keeps it (see KeepOverride).
	MessageOverride bool `toml:"message_override,omitempty"`
}

// KeepOverride returns next, a freshly detected entry, with the
// user-chosen schema fields of prev carried over when prev is an override.
func KeepOverride(prev, next Topic) Topic {
	if !prev.MessageOverride {
		return next
	}
	next.Format = prev.Format
	next.MessageName = prev.MessageName
	next.ProtoFile = prev.ProtoFile
	next.ImportRoot = prev.ImportRoot
	next.CutConfluencePrefix = prev.CutConfluencePrefix
	next.MessageOverride = true
	return next
}

// File is the content of topics.toml, keyed by topic name.
type File struct {
	mu     sync.RWMutex
	Topics map[string]Topic `toml:"topics"`
}

// Get returns the topic entry for name, if present.
func (f *File) Get(name string) (Topic, bool) {
	if f == nil {
		return Topic{}, false
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.Topics[name]
	return t, ok
}

// CopyTopics returns a copy of the topics map.
func (f *File) CopyTopics() map[string]Topic {
	if f == nil {
		return nil
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	cp := make(map[string]Topic, len(f.Topics))
	for k, v := range f.Topics {
		cp[k] = v
	}
	return cp
}

// Load reads path. A missing file yields an empty File. A file in the legacy
// format (bare topic names under [topics]) is converted to empty entries.
func Load(path string) (*File, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return &File{Topics: map[string]Topic{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var f File
	if err := toml.Unmarshal(data, &f); err != nil {
		legacy, ok := parseLegacy(data)
		if !ok {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		return legacy, nil
	}
	if f.Topics == nil {
		f.Topics = map[string]Topic{}
	}
	return &f, nil
}

// parseLegacy treats every non-blank, non-comment line after [topics] as a topic name.
func parseLegacy(data []byte) (*File, bool) {
	f := &File{Topics: map[string]Topic{}}
	inTopics := false
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
		case line == "[topics]":
			inTopics = true
		case strings.HasPrefix(line, "["):
			return nil, false
		case inTopics && !strings.ContainsAny(line, "= \t"):
			f.Topics[line] = Topic{}
		default:
			return nil, false
		}
	}
	return f, sc.Err() == nil && inTopics
}

// Merge sets the entries in updates, keeping all other topics.
func (f *File) Merge(updates map[string]Topic) {
	if f == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Topics == nil {
		f.Topics = map[string]Topic{}
	}
	for name, t := range updates {
		f.Topics[name] = t
	}
}

// Save atomically writes f to path, with topics sorted by name.
func (f *File) Save(path string) error {
	f.mu.RLock()
	data, err := toml.Marshal(f)
	f.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("encode topics: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp*")
	if err != nil {
		return fmt.Errorf("create temp file for %s: %w", path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	return nil
}
