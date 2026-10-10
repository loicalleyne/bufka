package sqlrepl

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/loicalleyne/bufka/internal/core/services/sqlrepl/format"
)

func init() {
	register(&dotCommand{
		name: "mode", usage: ".mode [MODE]", group: groupOutput,
		help: "Set the output mode: " + modeNames() + " (no argument: show it)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			if len(args) == 0 {
				return info("current output mode: %s\n", s.Settings.Format.Mode)
			}
			if len(args) > 1 {
				return usageErr("mode")
			}
			m, err := format.ParseMode(args[0])
			if err != nil {
				return fail(err)
			}
			s.Settings.Format.Mode = m
			return nil
		},
	})
	register(&dotCommand{
		name: "headers", aliases: []string{"header"}, usage: ".headers on|off", group: groupOutput,
		help: "Show or hide column headers",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			v, err := parseOnOff(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Format.Headers = v
			return nil
		},
	})
	register(&dotCommand{
		name: "maxrows", usage: ".maxrows N", group: groupOutput,
		help: "Rows shown in duckbox mode before the middle is elided (0 = all)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			n, err := oneInt(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.MaxRows = n
			return nil
		},
	})
	register(&dotCommand{
		name: "maxwidth", usage: ".maxwidth N", group: groupOutput,
		help: "Line width for duckbox/box/table; columns that do not fit are hidden (0 = screen width)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			n, err := oneInt(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Format.MaxWidth = n
			s.maxWidthSet = n > 0
			return nil
		},
	})
	register(&dotCommand{
		name: "nullvalue", usage: ".nullvalue STRING", group: groupOutput,
		help: "Text shown for NULL values",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			if len(args) != 1 {
				return usageErr("nullvalue")
			}
			s.Settings.Format.NullValue = args[0]
			return nil
		},
	})
	register(&dotCommand{
		name: "separator", usage: ".separator COL [ROW]", group: groupOutput,
		help: "Column (and row) separators for list and csv modes; \\t and \\n are understood",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			if len(args) < 1 || len(args) > 2 {
				return usageErr("separator")
			}
			s.Settings.Format.ColSep = unescape(args[0])
			if len(args) == 2 {
				s.Settings.Format.RowSep = unescape(args[1])
			}
			return nil
		},
	})
	register(&dotCommand{
		name: "timer", usage: ".timer on|off", group: groupOutput,
		help: "Show how long each statement took",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			v, err := parseOnOff(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Timer = v
			return nil
		},
	})
	register(&dotCommand{
		name: "width", usage: ".width [N ...]", group: groupOutput,
		help: "Fixed column widths for box modes, in column order (0 = automatic; no argument resets)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			widths := make([]int, len(args))
			for i, a := range args {
				n, err := strconv.Atoi(a)
				if err != nil || n < 0 {
					return fail(fmt.Errorf("width %q is not a non-negative number", a))
				}
				widths[i] = n
			}
			s.Settings.Format.Widths = widths
			return nil
		},
	})
	register(&dotCommand{
		name: "changes", usage: ".changes on|off", group: groupOutput,
		help: "Show the number of rows changed by INSERT/UPDATE/DELETE",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			v, err := parseOnOff(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Changes = v
			return nil
		},
	})
	register(&dotCommand{
		name: "show", usage: ".show", group: groupREPL,
		help: "Show the current REPL settings",
		run: func(_ context.Context, s *Session, _ []string, _ int) []Block {
			st := s.Settings
			onOff := func(b bool) string {
				if b {
					return "on"
				}
				return "off"
			}
			width := "screen"
			if s.maxWidthSet {
				width = strconv.Itoa(st.Format.MaxWidth)
			}
			out := "stdout (screen)"
			if s.output != nil {
				out = s.outPath
				if s.onceOnly {
					out += " (once)"
				}
			}
			lines := [][2]string{
				{"mode", string(st.Format.Mode)},
				{"headers", onOff(st.Format.Headers)},
				{"maxrows", strconv.Itoa(st.MaxRows)},
				{"maxwidth", width},
				{"nullvalue", strconv.Quote(st.Format.NullValue)},
				{"colseparator", strconv.Quote(st.Format.ColSep)},
				{"rowseparator", strconv.Quote(st.Format.RowSep)},
				{"width", fmt.Sprint(st.Format.Widths)},
				{"timer", onOff(st.Timer)},
				{"changes", onOff(st.Changes)},
				{"bail", onOff(st.Bail)},
				{"echo", onOff(st.Echo)},
				{"output", out},
				{"cwd", s.cwd()},
			}
			var b strings.Builder
			for _, l := range lines {
				fmt.Fprintf(&b, "%12s: %s\n", l[0], l[1])
			}
			return []Block{{Text: b.String()}}
		},
	})
	register(&dotCommand{
		name: "bail", usage: ".bail on|off", group: groupREPL,
		help: "Stop running the rest of the input after an error",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			v, err := parseOnOff(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Bail = v
			return nil
		},
	})
	register(&dotCommand{
		name: "echo", usage: ".echo on|off", group: groupREPL,
		help: "Echo each statement before its output (also for .read files)",
		run: func(_ context.Context, s *Session, args []string, _ int) []Block {
			v, err := parseOnOff(args)
			if err != nil {
				return fail(err)
			}
			s.Settings.Echo = v
			return nil
		},
	})
	register(&dotCommand{
		name: "clear", usage: ".clear", group: groupREPL,
		help: "Clear the screen",
		run: func(_ context.Context, _ *Session, _ []string, _ int) []Block {
			return []Block{{Action: ActionClear}}
		},
	})
	register(&dotCommand{
		name: "exit", aliases: []string{"quit", "q"}, usage: ".exit", group: groupREPL,
		help: "Leave the REPL, back to the menu (.q, .quit; esc also works)",
		run: func(_ context.Context, _ *Session, _ []string, _ int) []Block {
			return []Block{{Action: ActionExit}}
		},
	})
}

func modeNames() string {
	names := make([]string, len(format.Modes))
	for i, m := range format.Modes {
		names[i] = string(m)
	}
	return strings.Join(names, "|")
}

func oneInt(args []string) (int, error) {
	if len(args) != 1 {
		return 0, fmt.Errorf("expected one number")
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a non-negative number", args[0])
	}
	return n, nil
}

func unescape(s string) string {
	return strings.NewReplacer(`\t`, "\t", `\n`, "\n", `\r`, "\r", `\\`, `\`).Replace(s)
}
