// Package sqlrepl is the core of bufka's DuckDB-CLI-style SQL REPL: input
// tokenizing and statement splitting, query execution against a dedicated
// DuckDB connection, result formatting, dot-commands, history and
// completion. It has no TUI dependency; the TUI screen drives it.
package sqlrepl

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// TokenKind classifies a lexed span of SQL.
type TokenKind int

const (
	TokSpace TokenKind = iota
	TokComment
	TokString // '..', E'..', $tag$..$tag$
	TokIdent
	TokQuotedIdent // ".."
	TokKeyword
	TokNumber
	TokOperator
	TokSemicolon
)

// Token is one lexed span; Start/End are byte offsets into the input.
type Token struct {
	Kind       TokenKind
	Start, End int
	// Unterminated is set on a string, quoted identifier or block comment
	// that runs to the end of the input.
	Unterminated bool
}

// Text returns the token's source text.
func (t Token) Text(src string) string { return src[t.Start:t.End] }

// Lex tokenizes src following DuckDB's lexical rules (as in the DuckDB 2.0
// PEG grammar): '..' strings with doubled-quote escapes, E'..' strings with
// backslash escapes, ".." identifiers, $tag$..$tag$ strings, -- line comments
// and nested /* */ block comments. It never fails: unterminated constructs
// are reported with Token.Unterminated.
func Lex(src string) []Token {
	var toks []Token
	i := 0
	for i < len(src) {
		start := i
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			for i < len(src) && strings.IndexByte(" \t\n\r\f", src[i]) >= 0 {
				i++
			}
			toks = append(toks, Token{Kind: TokSpace, Start: start, End: i})
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
			toks = append(toks, Token{Kind: TokComment, Start: start, End: i})
		case c == '/' && i+1 < len(src) && src[i+1] == '*':
			end, ok := scanBlockComment(src, i)
			i = end
			toks = append(toks, Token{Kind: TokComment, Start: start, End: i, Unterminated: !ok})
		case c == '\'':
			end, ok := scanQuoted(src, i+1, '\'', false)
			i = end
			toks = append(toks, Token{Kind: TokString, Start: start, End: i, Unterminated: !ok})
		case (c == 'e' || c == 'E') && i+1 < len(src) && src[i+1] == '\'':
			end, ok := scanQuoted(src, i+2, '\'', true)
			i = end
			toks = append(toks, Token{Kind: TokString, Start: start, End: i, Unterminated: !ok})
		case c == '"':
			end, ok := scanQuoted(src, i+1, '"', false)
			i = end
			toks = append(toks, Token{Kind: TokQuotedIdent, Start: start, End: i, Unterminated: !ok})
		case c == '$':
			if tag, ok := dollarTag(src, i); ok {
				body := i + len(tag)
				if j := strings.Index(src[body:], tag); j >= 0 {
					i = body + j + len(tag)
					toks = append(toks, Token{Kind: TokString, Start: start, End: i})
				} else {
					i = len(src)
					toks = append(toks, Token{Kind: TokString, Start: start, End: i, Unterminated: true})
				}
				break
			}
			// $1 positional parameter or a lone $.
			i++
			for i < len(src) && isDigit(src[i]) {
				i++
			}
			toks = append(toks, Token{Kind: TokOperator, Start: start, End: i})
		case c == ';':
			i++
			toks = append(toks, Token{Kind: TokSemicolon, Start: start, End: i})
		case isDigit(c) || (c == '.' && i+1 < len(src) && isDigit(src[i+1])):
			i = scanNumber(src, i)
			toks = append(toks, Token{Kind: TokNumber, Start: start, End: i})
		case isIdentStart(src, i):
			for i < len(src) && isIdentPart(src, i) {
				_, w := utf8.DecodeRuneInString(src[i:])
				i += w
			}
			kind := TokIdent
			if IsKeyword(src[start:i]) {
				kind = TokKeyword
			}
			toks = append(toks, Token{Kind: kind, Start: start, End: i})
		default:
			_, w := utf8.DecodeRuneInString(src[i:])
			i += w
			toks = append(toks, Token{Kind: TokOperator, Start: start, End: i})
		}
	}
	return toks
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(s string, i int) bool {
	r, _ := utf8.DecodeRuneInString(s[i:])
	return r == '_' || unicode.IsLetter(r)
}

func isIdentPart(s string, i int) bool {
	r, _ := utf8.DecodeRuneInString(s[i:])
	return r == '_' || r == '$' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// scanQuoted scans past the closing quote q starting at i (just after the
// opening quote). A doubled quote is an escaped quote; with backslash set, a
// backslash escapes the next byte.
func scanQuoted(s string, i int, q byte, backslash bool) (int, bool) {
	for i < len(s) {
		switch s[i] {
		case '\\':
			if backslash {
				i += 2
				continue
			}
		case q:
			if i+1 < len(s) && s[i+1] == q {
				i += 2
				continue
			}
			return i + 1, true
		}
		i++
	}
	return len(s), false
}

// scanBlockComment scans a possibly nested /* */ comment starting at i.
func scanBlockComment(s string, i int) (int, bool) {
	depth := 0
	for i < len(s) {
		switch {
		case strings.HasPrefix(s[i:], "/*"):
			depth++
			i += 2
		case strings.HasPrefix(s[i:], "*/"):
			depth--
			i += 2
			if depth == 0 {
				return i, true
			}
		default:
			i++
		}
	}
	return len(s), false
}

// dollarTag returns the opening $tag$ (or $$) at i, if any.
func dollarTag(s string, i int) (string, bool) {
	j := i + 1
	if j < len(s) && s[j] == '$' {
		return "$$", true
	}
	if j >= len(s) || !isIdentStart(s, j) {
		return "", false
	}
	for j < len(s) && s[j] != '$' {
		if !isIdentPart(s, j) || s[j] == '$' {
			return "", false
		}
		_, w := utf8.DecodeRuneInString(s[j:])
		j += w
	}
	if j >= len(s) {
		return "", false
	}
	return s[i : j+1], true
}

func scanNumber(s string, i int) int {
	if strings.HasPrefix(s[i:], "0x") || strings.HasPrefix(s[i:], "0X") {
		i += 2
		for i < len(s) && strings.IndexByte("0123456789abcdefABCDEF_", s[i]) >= 0 {
			i++
		}
		return i
	}
	for i < len(s) && (isDigit(s[i]) || s[i] == '_') {
		i++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && (isDigit(s[i]) || s[i] == '_') {
			i++
		}
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		j := i + 1
		if j < len(s) && (s[j] == '+' || s[j] == '-') {
			j++
		}
		if j < len(s) && isDigit(s[j]) {
			i = j
			for i < len(s) && isDigit(s[i]) {
				i++
			}
		}
	}
	return i
}

// Unit is one runnable piece of REPL input: a SQL statement (including its
// terminating semicolon) or a dot-command line.
type Unit struct {
	Text string
	Dot  bool
}

// Split cuts input into complete units. A dot-command is a line starting
// with "." while no SQL is pending; it ends at the newline (or end of
// input). A SQL statement ends at a semicolon outside any string, quoted
// identifier or comment. rest is the trailing incomplete SQL, empty
// (ignoring whitespace and comments) when the whole input was consumed.
func Split(input string) (units []Unit, rest string) {
	pos := 0
	for pos < len(input) {
		// Skip blank space between units.
		trimmed := strings.TrimLeft(input[pos:], " \t\r\n")
		pos = len(input) - len(trimmed)
		if pos >= len(input) {
			break
		}
		if trimmed[0] == '.' {
			end := strings.IndexByte(trimmed, '\n')
			if end < 0 {
				end = len(trimmed)
			}
			line := strings.TrimSpace(trimmed[:end])
			units = append(units, Unit{Text: line, Dot: true})
			pos += end
			continue
		}
		stmtEnd := -1
		for _, t := range Lex(trimmed) {
			if t.Kind == TokSemicolon {
				stmtEnd = t.End
				break
			}
		}
		if stmtEnd < 0 {
			return units, trimmed
		}
		stmt := strings.TrimSpace(trimmed[:stmtEnd])
		if !onlyNoise(stmt[:len(stmt)-1]) {
			units = append(units, Unit{Text: stmt})
		}
		pos += stmtEnd
	}
	return units, ""
}

// Complete reports whether input has nothing pending: every statement is
// terminated (or the input holds only dot-commands, whitespace and
// comments). The TUI runs the input on enter only when it is complete.
func Complete(input string) bool {
	units, rest := Split(input)
	return onlyNoise(rest) && len(units) > 0
}

// onlyNoise reports whether s holds nothing but whitespace and complete
// comments.
func onlyNoise(s string) bool {
	for _, t := range Lex(s) {
		if t.Kind == TokComment && !t.Unterminated {
			continue
		}
		if t.Kind != TokSpace {
			return false
		}
	}
	return true
}

// keywords are DuckDB keywords used for highlighting and the completion
// fallback (a subset of duckdb_keywords()).
var keywords = func() map[string]bool {
	m := map[string]bool{}
	for _, k := range strings.Fields(`
ALL ALTER ANALYZE AND ANTI ANY ARRAY AS ASC ASOF ATTACH BEGIN BETWEEN BY CALL CASE CAST CHECK CHECKPOINT
COLLATE COLUMN COLUMNS COMMIT CONSTRAINT COPY CREATE CROSS CTE CURRENT_DATE CURRENT_TIMESTAMP DEFAULT
DELETE DESC DESCRIBE DETACH DISTINCT DO DROP ELSE END EXCEPT EXCLUDE EXISTS EXPLAIN EXPORT FALSE FETCH FILTER
FIRST FOR FOREIGN FROM FULL FUNCTION GLOB GROUP GROUPING HAVING IF ILIKE IMPORT IN INDEX INNER INSERT INSTALL
INTERSECT INTERVAL INTO IS JOIN KEY LAST LATERAL LEFT LIKE LIMIT LOAD MACRO MATERIALIZED NATURAL NOT NOTHING
NULL NULLS OFFSET ON OR ORDER OUTER OVER PARTITION PIVOT POSITIONAL PRAGMA PRIMARY QUALIFY RANGE RECURSIVE
REFERENCES RENAME REPLACE RESET RETURNING RIGHT ROLLBACK ROW ROWS SAMPLE SCHEMA SECRET SELECT SEMI SEQUENCE
SET SHOW SIMILAR SOME SUMMARIZE TABLE TEMP TEMPORARY THEN TO TRANSACTION TRUE TYPE UNION UNIQUE UNNEST
UNPIVOT UPDATE USE USING VACUUM VALUES VIEW WHEN WHERE WINDOW WITH WITHIN
`) {
		m[k] = true
	}
	return m
}()

// IsKeyword reports whether word is a DuckDB keyword (case-insensitive).
func IsKeyword(word string) bool { return keywords[strings.ToUpper(word)] }

// Keywords returns the keyword list (upper case, unsorted).
func Keywords() []string {
	out := make([]string, 0, len(keywords))
	for k := range keywords {
		out = append(out, k)
	}
	return out
}
