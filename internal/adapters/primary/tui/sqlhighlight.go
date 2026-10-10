package tui

import (
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl"
)

var (
	sqlKeywordStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("63")).Bold(true)
	sqlStringStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	sqlNumberStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("179"))
	sqlCommentStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Italic(true)
	sqlIdentStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("117"))
	sqlDotStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("170")).Bold(true)
	sqlBadStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("197"))
)

// highlightSQL colours SQL (and dot-command lines) with the REPL
// tokenizer, for the echo of executed input in the scrollback.
func highlightSQL(src string) string {
	if strings.HasPrefix(strings.TrimSpace(src), ".") && !strings.Contains(strings.TrimSpace(src), "\n") {
		cmd, rest, _ := strings.Cut(src, " ")
		if rest != "" {
			return sqlDotStyle.Render(cmd) + " " + rest
		}
		return sqlDotStyle.Render(cmd)
	}
	var b strings.Builder
	for _, t := range sqlrepl.Lex(src) {
		text := t.Text(src)
		var st *lipgloss.Style
		switch {
		case t.Unterminated:
			st = &sqlBadStyle
		case t.Kind == sqlrepl.TokKeyword:
			st = &sqlKeywordStyle
		case t.Kind == sqlrepl.TokString:
			st = &sqlStringStyle
		case t.Kind == sqlrepl.TokNumber:
			st = &sqlNumberStyle
		case t.Kind == sqlrepl.TokComment:
			st = &sqlCommentStyle
		case t.Kind == sqlrepl.TokQuotedIdent:
			st = &sqlIdentStyle
		}
		if st == nil {
			b.WriteString(text)
			continue
		}
		// Render per line so styles do not span newlines.
		lines := strings.Split(text, "\n")
		for i, l := range lines {
			if i > 0 {
				b.WriteString("\n")
			}
			if l != "" {
				b.WriteString(st.Render(l))
			}
		}
	}
	return b.String()
}
