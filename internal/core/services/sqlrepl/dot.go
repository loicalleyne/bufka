package sqlrepl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// dotCommand is one dot-command.
type dotCommand struct {
	name    string
	aliases []string
	usage   string
	help    string
	group   string
	run     func(ctx context.Context, s *Session, args []string, depth int) []Block
}

// Dot-command groups, in .help order.
const (
	groupOutput = "output"
	groupSchema = "schema"
	groupIO     = "files & shell"
	groupBufka  = "bufka"
	groupREPL   = "repl"
)

var helpGroups = []string{groupOutput, groupSchema, groupIO, groupBufka, groupREPL}

var dotCommands []*dotCommand

func register(c *dotCommand) { dotCommands = append(dotCommands, c) }

func lookupDot(name string) *dotCommand {
	name = strings.ToLower(name)
	for _, c := range dotCommands {
		if c.name == name {
			return c
		}
		for _, a := range c.aliases {
			if a == name {
				return c
			}
		}
	}
	return nil
}

// DotNames lists every dot-command name and alias, with the leading dot,
// sorted.
func DotNames() []string {
	var out []string
	for _, c := range dotCommands {
		out = append(out, "."+c.name)
		for _, a := range c.aliases {
			out = append(out, "."+a)
		}
	}
	sort.Strings(out)
	return out
}

// DotTakesPath reports whether the named dot-command's first argument is a
// file path (for completion).
func DotTakesPath(name string) bool {
	switch strings.TrimPrefix(strings.ToLower(name), ".") {
	case "read", "output", "once", "export", "open", "attach", "cd":
		return true
	}
	return false
}

// runDot runs one dot-command line.
func (s *Session) runDot(ctx context.Context, line string, depth int) []Block {
	args, err := splitArgs(strings.TrimPrefix(line, "."))
	if err != nil {
		return []Block{{Input: line, Err: err}}
	}
	if len(args) == 0 {
		return []Block{{Input: line, Err: errors.New("empty dot-command: try .help")}}
	}
	c := lookupDot(args[0])
	if c == nil {
		return []Block{{Input: line, Err: fmt.Errorf("unknown command .%s: try .help", args[0])}}
	}
	blocks := c.run(ctx, s, args[1:], depth)
	for i := range blocks {
		if blocks[i].Input == "" {
			blocks[i].Input = line
		}
	}
	return blocks
}

// usageErr is the standard error for bad arguments.
func usageErr(c string) []Block {
	cmd := lookupDot(c)
	return []Block{{Err: fmt.Errorf("usage: %s", cmd.usage)}}
}

func info(format string, args ...any) []Block {
	return []Block{{Text: fmt.Sprintf(format, args...)}}
}

func fail(err error) []Block { return []Block{{Err: err}} }

// splitArgs splits a dot-command line shell-style: whitespace separated,
// with '..' and ".." quoting and backslash escapes outside single quotes.
func splitArgs(line string) ([]string, error) {
	var (
		args  []string
		cur   strings.Builder
		inArg bool
		quote rune
	)
	rs := []rune(line)
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
				continue
			}
			if r == '\\' && quote == '"' && i+1 < len(rs) {
				i++
				r = rs[i]
			}
			cur.WriteRune(r)
		case r == '\'' || r == '"':
			quote = r
			inArg = true
		case r == '\\' && i+1 < len(rs):
			i++
			cur.WriteRune(rs[i])
			inArg = true
		case r == ' ' || r == '\t':
			if inArg {
				args = append(args, cur.String())
				cur.Reset()
				inArg = false
			}
		default:
			cur.WriteRune(r)
			inArg = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated %c quote", quote)
	}
	if inArg {
		args = append(args, cur.String())
	}
	return args, nil
}

// restOf returns the raw text after the command name (for .shell and
// .explain, which take free text).
func restOf(line string) string {
	line = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "."))
	if i := strings.IndexAny(line, " \t"); i >= 0 {
		return strings.TrimSpace(line[i:])
	}
	return ""
}

func parseOnOff(args []string) (bool, error) {
	if len(args) != 1 {
		return false, errors.New("expected on or off")
	}
	switch strings.ToLower(args[0]) {
	case "on", "true", "yes", "1":
		return true, nil
	case "off", "false", "no", "0":
		return false, nil
	}
	return false, fmt.Errorf("expected on or off, got %q", args[0])
}

// quoteIdent quotes a (possibly schema-qualified) table reference unless
// it is already a plain or quoted identifier path.
func quoteIdent(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		if strings.HasPrefix(p, `"`) && strings.HasSuffix(p, `"`) && len(p) >= 2 {
			continue
		}
		parts[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(parts, ".")
}

// quoteString is a SQL string literal.
func quoteString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func init() {
	register(&dotCommand{
		name: "help", usage: ".help [COMMAND]", group: groupREPL,
		help: "Show help for every command, or one command",
		run: func(_ context.Context, _ *Session, args []string, _ int) []Block {
			return []Block{{Text: helpText(args)}}
		},
	})
}

func helpText(args []string) string {
	if len(args) > 0 {
		c := lookupDot(strings.TrimPrefix(args[0], "."))
		if c == nil {
			return fmt.Sprintf("unknown command %s\n", args[0])
		}
		s := c.usage + "\n    " + c.help + "\n"
		if len(c.aliases) > 0 {
			s += "    aliases: ." + strings.Join(c.aliases, ", .") + "\n"
		}
		return s
	}
	width := 0
	for _, c := range dotCommands {
		width = max(width, len(c.usage))
	}
	var b strings.Builder
	for _, g := range helpGroups {
		fmt.Fprintf(&b, "%s:\n", g)
		for _, c := range dotCommands {
			if c.group == g {
				fmt.Fprintf(&b, "  %-*s  %s\n", width, c.usage, c.help)
			}
		}
		b.WriteString("\n")
	}
	b.WriteString("keys: enter runs a statement ending in ; · alt+enter (or shift+enter) new line · ctrl+enter run now\n")
	b.WriteString("      ↑/↓ history · ctrl+r search history · tab complete · pgup/pgdn scroll · ctrl+c cancel/clear · esc back\n")
	return b.String()
}
