package sqlrepl

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
)

// DefaultHistoryLimit bounds the saved history.
const DefaultHistoryLimit = 1000

// History is the REPL input history, persisted one entry per record. An
// entry may span lines; records are separated by a NUL byte so multi-line
// statements survive a restart.
type History struct {
	Path    string
	Limit   int
	entries []string
}

// LoadHistory reads path; a missing file is an empty history.
func LoadHistory(path string) (*History, error) {
	h := &History{Path: path, Limit: DefaultHistoryLimit}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return h, nil
		}
		return h, err
	}
	for _, e := range strings.Split(string(data), "\x00") {
		if strings.TrimSpace(e) != "" {
			h.entries = append(h.entries, strings.TrimSpace(e))
		}
	}
	h.trim()
	return h, nil
}

// Entries returns the history, oldest first.
func (h *History) Entries() []string { return h.entries }

// Len is the number of entries.
func (h *History) Len() int { return len(h.entries) }

// At returns entry i (0 = oldest).
func (h *History) At(i int) string { return h.entries[i] }

// Add appends an entry (skipping blanks and immediate repeats) and saves.
func (h *History) Add(entry string) error {
	entry = strings.TrimSpace(entry)
	if entry == "" || (len(h.entries) > 0 && h.entries[len(h.entries)-1] == entry) {
		return nil
	}
	h.entries = append(h.entries, entry)
	h.trim()
	return h.save()
}

func (h *History) trim() {
	if h.Limit > 0 && len(h.entries) > h.Limit {
		h.entries = append([]string(nil), h.entries[len(h.entries)-h.Limit:]...)
	}
}

func (h *History) save() error {
	if h.Path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(h.Path), 0o700); err != nil {
		return err
	}
	tmp := h.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range h.entries {
		_, _ = w.WriteString(e)
		_ = w.WriteByte(0)
		_ = w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, h.Path)
}

// Search returns the index of the newest entry before `before` containing
// query (case-insensitive), or -1. Use before = Len() to search from the
// newest entry.
func (h *History) Search(query string, before int) int {
	q := strings.ToLower(query)
	for i := min(before, len(h.entries)) - 1; i >= 0; i-- {
		if strings.Contains(strings.ToLower(h.entries[i]), q) {
			return i
		}
	}
	return -1
}
