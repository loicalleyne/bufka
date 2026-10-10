package sqlrepl

import (
	"reflect"
	"testing"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		units []Unit
		rest  string
	}{
		{"single", "SELECT 1;", []Unit{{Text: "SELECT 1;"}}, ""},
		{"no semicolon", "SELECT 1", nil, "SELECT 1"},
		{"two", "SELECT 1; SELECT 2;", []Unit{{Text: "SELECT 1;"}, {Text: "SELECT 2;"}}, ""},
		{"trailing partial", "SELECT 1; SELECT", []Unit{{Text: "SELECT 1;"}}, "SELECT"},
		{"semicolon in string", "SELECT 'a;b'", nil, "SELECT 'a;b'"},
		{"doubled quote", "SELECT 'it''s;';", []Unit{{Text: "SELECT 'it''s;';"}}, ""},
		{"unterminated string", "SELECT 'a;", nil, "SELECT 'a;"},
		{"escape string", `SELECT E'a\';b';`, []Unit{{Text: `SELECT E'a\';b';`}}, ""},
		{"quoted ident", `SELECT "a;b" FROM t;`, []Unit{{Text: `SELECT "a;b" FROM t;`}}, ""},
		{"dollar", "SELECT $$a;b$$;", []Unit{{Text: "SELECT $$a;b$$;"}}, ""},
		{"dollar tag", "SELECT $x$a;$$b$x$;", []Unit{{Text: "SELECT $x$a;$$b$x$;"}}, ""},
		{"param", "SELECT $1;", []Unit{{Text: "SELECT $1;"}}, ""},
		{"line comment", "SELECT 1 -- x;\n", nil, "SELECT 1 -- x;\n"},
		{"line comment then end", "SELECT 1 -- x;\n;", []Unit{{Text: "SELECT 1 -- x;\n;"}}, ""},
		{"nested block", "SELECT /* a /* b; */ c; */ 1;", []Unit{{Text: "SELECT /* a /* b; */ c; */ 1;"}}, ""},
		{"empty statements", ";; SELECT 1;", []Unit{{Text: "SELECT 1;"}}, ""},
		{"dot", ".tables", []Unit{{Text: ".tables", Dot: true}}, ""},
		{"dot then sql", ".mode csv\nSELECT 1;", []Unit{{Text: ".mode csv", Dot: true}, {Text: "SELECT 1;"}}, ""},
		{"dot inside pending sql is sql", "SELECT\n.5;", []Unit{{Text: "SELECT\n.5;"}}, ""},
		{"unicode", "SELECT 'héllo;' AS ü;", []Unit{{Text: "SELECT 'héllo;' AS ü;"}}, ""},
		{"multiline", "SELECT *\nFROM t\nWHERE x = 1;", []Unit{{Text: "SELECT *\nFROM t\nWHERE x = 1;"}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			units, rest := Split(tt.in)
			if !reflect.DeepEqual(units, tt.units) || rest != tt.rest {
				t.Fatalf("Split(%q) = %#v, %q; want %#v, %q", tt.in, units, rest, tt.units, tt.rest)
			}
		})
	}
}

func TestCompleteStatement(t *testing.T) {
	for in, want := range map[string]bool{
		"":                    false,
		"   ":                 false,
		"SELECT 1":            false,
		"SELECT 1;":           true,
		"SELECT 1; -- done":   true,
		"SELECT 1; /* open":   false,
		"SELECT ';'":          false,
		".tables":             true,
		"SELECT 1;\nSELECT 2": false,
	} {
		if got := Complete(in); got != want {
			t.Errorf("Complete(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestLexKinds(t *testing.T) {
	src := "select x, 'a' /* c */ FROM \"T\" -- z\nWHERE n >= 1.5e3;"
	var kinds []TokenKind
	for _, tok := range Lex(src) {
		if tok.Kind != TokSpace {
			kinds = append(kinds, tok.Kind)
		}
	}
	want := []TokenKind{
		TokKeyword, TokIdent, TokOperator, TokString, TokComment, TokKeyword, TokQuotedIdent,
		TokComment, TokKeyword, TokIdent, TokOperator, TokOperator, TokNumber, TokSemicolon,
	}
	if !reflect.DeepEqual(kinds, want) {
		t.Fatalf("kinds = %v, want %v", kinds, want)
	}
}
