package sqlrepl

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Suggestion is one completion candidate: replace input[Start:cursor] with
// Text.
type Suggestion struct {
	Text  string
	Start int
	// Kind is DuckDB's suggestion_type (keyword, table, column, function,
	// …), or "command" / "file" for local completions.
	Kind string
}

// maxSuggestions bounds what Complete returns.
const maxSuggestions = 50

// Complete returns completions for input with the cursor at byte offset
// cursor. Dot-command names and file arguments are completed locally; SQL
// uses DuckDB's sql_auto_complete on the session's completion connection,
// falling back to keywords and table names when it is unavailable.
func (s *Session) Complete(ctx context.Context, input string, cursor int) []Suggestion {
	cursor = min(max(cursor, 0), len(input))
	before := input[:cursor]

	// A dot-command line: the current line starts with "." and no SQL is
	// pending before it.
	lineStart := strings.LastIndexByte(before, '\n') + 1
	line := before[lineStart:]
	if strings.HasPrefix(strings.TrimLeft(line, " \t"), ".") && onlyNoise(sqlBefore(before[:lineStart])) {
		return s.completeDot(line, lineStart)
	}

	sugg, err := s.completeSQL(ctx, before)
	if err == nil {
		return preferPrefix(sugg, before)
	}
	return s.completeFallback(ctx, before)
}

// preferPrefix keeps the suggestions that start with what was typed
// (case-insensitive), in DuckDB's order. sql_auto_complete ranks fuzzily,
// so when nothing matches the prefix its ranking is kept as is.
func preferPrefix(sugg []Suggestion, before string) []Suggestion {
	var out []Suggestion
	for _, x := range sugg {
		if x.Start > len(before) {
			continue
		}
		typed := strings.ToLower(before[x.Start:])
		if strings.HasPrefix(strings.ToLower(strings.TrimLeft(x.Text, `"`)), strings.TrimLeft(typed, `"`)) {
			out = append(out, x)
		}
	}
	if len(out) == 0 {
		return sugg
	}
	return out
}

// sqlBefore returns the trailing incomplete SQL of s.
func sqlBefore(s string) string {
	_, rest := Split(s)
	return rest
}

func (s *Session) completeDot(line string, lineStart int) []Suggestion {
	trimmed := strings.TrimLeft(line, " \t")
	offset := lineStart + len(line) - len(trimmed)
	fields := strings.Fields(trimmed)
	if len(fields) == 0 || (len(fields) == 1 && !strings.HasSuffix(trimmed, " ")) {
		prefix := ""
		if len(fields) == 1 {
			prefix = fields[0]
		}
		var out []Suggestion
		for _, n := range DotNames() {
			if strings.HasPrefix(n, strings.ToLower(prefix)) {
				out = append(out, Suggestion{Text: n, Start: offset, Kind: "command"})
			}
		}
		return out
	}
	cmd := fields[0]
	partial := ""
	if !strings.HasSuffix(trimmed, " ") {
		partial = fields[len(fields)-1]
	}
	start := lineStart + len(line) - len(partial)
	switch strings.ToLower(strings.TrimPrefix(cmd, ".")) {
	case "mode":
		return prefixed(modeList(), partial, start, "mode")
	case "headers", "header", "timer", "changes", "bail", "echo":
		return prefixed([]string{"on", "off"}, partial, start, "keyword")
	case "help":
		var names []string
		for _, n := range DotNames() {
			names = append(names, strings.TrimPrefix(n, "."))
		}
		return prefixed(names, partial, start, "command")
	case "tail", "sync":
		if s.Deps.Topics != nil {
			if topics, err := s.Deps.Topics(context.Background()); err == nil {
				var names []string
				for _, t := range topics {
					names = append(names, t.Topic)
				}
				return prefixed(names, partial, start, "topic")
			}
		}
	case "describe", "summarize", "indexes", "indices", "tables", "schema":
		return prefixed(s.tableNames(context.Background()), partial, start, "table")
	}
	if DotTakesPath(cmd) {
		return s.completePath(partial, start, strings.EqualFold(strings.TrimPrefix(cmd, "."), "cd"))
	}
	return nil
}

func modeList() []string { return strings.Split(modeNames(), "|") }

func prefixed(cands []string, partial string, start int, kind string) []Suggestion {
	var out []Suggestion
	lp := strings.ToLower(partial)
	for _, c := range cands {
		if strings.HasPrefix(strings.ToLower(c), lp) {
			out = append(out, Suggestion{Text: c, Start: start, Kind: kind})
		}
		if len(out) >= maxSuggestions {
			break
		}
	}
	return out
}

// completePath lists directory entries matching partial; directories get a
// trailing slash.
func (s *Session) completePath(partial string, start int, dirsOnly bool) []Suggestion {
	dir, base := filepath.Split(partial)
	lookIn := dir
	if lookIn == "" {
		lookIn = "."
	}
	entries, err := os.ReadDir(s.path(lookIn))
	if err != nil {
		return nil
	}
	var out []Suggestion
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, base) || (strings.HasPrefix(name, ".") && !strings.HasPrefix(base, ".")) {
			continue
		}
		isDir := e.IsDir()
		if !isDir && e.Type()&os.ModeSymlink != 0 {
			if st, err := os.Stat(filepath.Join(s.path(lookIn), name)); err == nil {
				isDir = st.IsDir()
			}
		}
		if dirsOnly && !isDir {
			continue
		}
		text := dir + name
		if isDir {
			text += "/"
		}
		out = append(out, Suggestion{Text: text, Start: start, Kind: "file"})
		if len(out) >= maxSuggestions {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Text < out[j].Text })
	return out
}

// completeSQL asks DuckDB. sql_auto_complete returns suggestion_start as a
// byte offset into the text it was given.
func (s *Session) completeSQL(ctx context.Context, before string) ([]Suggestion, error) {
	s.completeMu.Lock()
	defer s.completeMu.Unlock()
	sql := `SELECT suggestion, suggestion_start, suggestion_type FROM sql_auto_complete(` + quoteString(before) + `) LIMIT ` + strconv.Itoa(maxSuggestions)
	res, err := s.complete.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var out []Suggestion
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		text := rec.Column(0).(*array.String)
		start := rec.Column(1).(*array.Int32)
		kind := rec.Column(2).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			st := int(start.Value(i))
			bst := len(before)
			if st >= 0 && st <= len(before) {
				bst = st
			}
			out = append(out, Suggestion{
				Text:  strings.Clone(text.Value(i)),
				Start: bst,
				Kind:  strings.ToLower(strings.Clone(kind.Value(i))),
			})
		}
	}
	return out, res.Reader.Err()
}

// completeFallback completes the word before the cursor from keywords and
// table names.
func (s *Session) completeFallback(ctx context.Context, before string) []Suggestion {
	start := len(before)
	for start > 0 && isWordByte(before[start-1]) {
		start--
	}
	word := before[start:]
	if word == "" {
		return nil
	}
	cands := append(s.tableNames(ctx), Keywords()...)
	sort.Strings(cands)
	out := prefixed(cands, word, start, "keyword")
	for i := range out {
		if IsKeyword(out[i].Text) {
			continue
		}
		out[i].Kind = "table"
	}
	return out
}

func isWordByte(c byte) bool {
	return c == '_' || c >= 0x80 || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// tableNames lists table and view names, via the completion connection.
func (s *Session) tableNames(ctx context.Context) []string {
	s.completeMu.Lock()
	defer s.completeMu.Unlock()
	if s.complete == nil {
		return nil
	}
	res, err := s.complete.Query(ctx, `SELECT table_name FROM duckdb_tables() WHERE NOT internal UNION SELECT view_name FROM duckdb_views() WHERE NOT internal ORDER BY 1`)
	if err != nil {
		return nil
	}
	defer res.Close()
	var out []string
	for res.Reader.Next() {
		col := res.Reader.RecordBatch().Column(0).(*array.String)
		for i := 0; i < col.Len(); i++ {
			out = append(out, strings.Clone(col.Value(i)))
		}
	}
	return out
}

// CommonPrefix is the longest prefix shared by every suggestion's text
// (case-sensitive), for Tab's first press.
func CommonPrefix(sugg []Suggestion) string {
	if len(sugg) == 0 {
		return ""
	}
	p := sugg[0].Text
	for _, s := range sugg[1:] {
		for !strings.HasPrefix(s.Text, p) {
			p = p[:len(p)-1]
		}
	}
	return p
}
