package format

import (
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/x/ansi"
)

type boxStyle int

const (
	boxDuck  boxStyle = iota // DuckDB duckbox: types, elision, row count footer
	boxPlain                 // unicode box, no types
	boxASCII                 // +---+ table
)

type glyphs struct {
	h, v                         string
	tl, tm, tr, ml, mm, mr       string
	bl, bm, br, footL, footM, ff string
}

var (
	unicodeGlyphs = glyphs{
		h: "─", v: "│",
		tl: "┌", tm: "┬", tr: "┐",
		ml: "├", mm: "┼", mr: "┤",
		bl: "└", bm: "┴", br: "┘",
		footL: "├", footM: "┴", ff: "┤",
	}
	asciiGlyphs = glyphs{
		h: "-", v: "|",
		tl: "+", tm: "+", tr: "+",
		ml: "+", mm: "+", mr: "+",
		bl: "+", bm: "+", br: "+",
		footL: "+", footM: "+", ff: "+",
	}
)

func cellText(o Options, c Cell) string {
	s := o.text(c)
	// One cell is one line.
	s = strings.NewReplacer("\r\n", "\\n", "\n", "\\n", "\r", "\\r", "\t", " ").Replace(s)
	return s
}

func fit(s string, w int) string {
	if ansi.StringWidth(s) <= w {
		return s
	}
	return ansi.Truncate(s, w, "…")
}

func pad(s string, w int, right bool) string {
	gap := w - ansi.StringWidth(s)
	if gap <= 0 {
		return s
	}
	if right {
		return strings.Repeat(" ", gap) + s
	}
	return s + strings.Repeat(" ", gap)
}

func center(s string, w int) string {
	gap := w - ansi.StringWidth(s)
	if gap <= 0 {
		return s
	}
	l := gap / 2
	return strings.Repeat(" ", l) + s + strings.Repeat(" ", gap-l)
}

func writeBox(w io.Writer, t *Table, o Options, style boxStyle) error {
	g := unicodeGlyphs
	if style == boxASCII {
		g = asciiGlyphs
	}
	rows := t.Rows()
	ncol := len(t.Columns)
	if ncol == 0 {
		return nil
	}

	// Natural column widths.
	widths := make([]int, ncol)
	for i, c := range t.Columns {
		if o.Headers {
			widths[i] = ansi.StringWidth(c.Name)
		}
		if style == boxDuck {
			widths[i] = max(widths[i], ansi.StringWidth(strings.ToLower(c.Type)))
		}
	}
	cells := make([][]string, len(rows))
	for r, row := range rows {
		cells[r] = make([]string, ncol)
		for i := range ncol {
			s := cellText(o, row[i])
			cells[r][i] = s
			widths[i] = max(widths[i], ansi.StringWidth(s))
		}
	}
	// Columns take their natural width (or the .width setting); only
	// MaxWidth below makes them narrower.
	for i := range widths {
		if i < len(o.Widths) && o.Widths[i] > 0 {
			widths[i] = o.Widths[i]
		}
		widths[i] = max(widths[i], 1)
	}

	// Fit within MaxWidth: first shrink the widest columns (down to
	// minShrink), then hide trailing columns that still do not fit.
	lineWidth := func(ws []int) int {
		n := 1
		for _, wd := range ws {
			n += wd + 3
		}
		return n
	}
	const minShrink = 8
	if o.MaxWidth > 0 {
		for lineWidth(widths) > o.MaxWidth {
			wi := -1
			for i, wd := range widths {
				if wd > minShrink && (wi < 0 || wd > widths[wi]) {
					wi = i
				}
			}
			if wi < 0 {
				break
			}
			widths[wi]--
		}
	}
	shown := ncol
	if o.MaxWidth > 0 {
		shown = 0
		for shown < ncol && (shown == 0 || lineWidth(widths[:shown+1]) <= o.MaxWidth) {
			shown++
		}
	}
	widths = widths[:shown]

	// The duckbox footer must fit: widen the last column if needed.
	var foot []string
	if style == boxDuck {
		rowWord := "rows"
		if t.Total == 1 {
			rowWord = "row"
		}
		if t.Truncated() {
			foot = append(foot, fmt.Sprintf("%d %s (%d shown)", t.Total, rowWord, t.Shown()))
		} else {
			foot = append(foot, fmt.Sprintf("%d %s", t.Total, rowWord))
		}
		if shown < ncol {
			foot = append(foot, fmt.Sprintf("%d columns (%d shown)", ncol, shown))
		}
		for _, f := range foot {
			if need := ansi.StringWidth(f) + 4 - lineWidth(widths); need > 0 {
				widths[shown-1] += need
			}
		}
	}

	rule := func(l, m, r string) string {
		parts := make([]string, shown)
		for i, wd := range widths {
			parts[i] = strings.Repeat(g.h, wd+2)
		}
		return l + strings.Join(parts, m) + r + "\n"
	}
	line := func(vals []string, align func(i int, s string) string) string {
		var b strings.Builder
		b.WriteString(g.v)
		for i := range shown {
			b.WriteString(" ")
			b.WriteString(align(i, fit(vals[i], widths[i])))
			b.WriteString(" ")
			b.WriteString(g.v)
		}
		b.WriteString("\n")
		return b.String()
	}

	var b strings.Builder
	b.WriteString(rule(g.tl, g.tm, g.tr))
	if o.Headers {
		names := make([]string, ncol)
		types := make([]string, ncol)
		for i, c := range t.Columns {
			names[i] = c.Name
			types[i] = strings.ToLower(c.Type)
		}
		hdrAlign := func(i int, s string) string { return pad(s, widths[i], false) }
		if style == boxDuck {
			hdrAlign = func(i int, s string) string { return center(s, widths[i]) }
		}
		b.WriteString(line(names, hdrAlign))
		if style == boxDuck {
			b.WriteString(line(types, hdrAlign))
		}
		b.WriteString(rule(g.ml, g.mm, g.mr))
	}
	cellAlign := func(i int, s string) string { return pad(s, widths[i], t.Columns[i].Right) }
	for r := range t.Head {
		b.WriteString(line(cells[r], cellAlign))
	}
	if style == boxDuck && t.Truncated() {
		dots := make([]string, ncol)
		for i := range dots {
			dots[i] = "·"
		}
		for range 3 {
			b.WriteString(line(dots, func(i int, s string) string { return center(s, widths[i]) }))
		}
	}
	for r := range t.Tail {
		b.WriteString(line(cells[len(t.Head)+r], cellAlign))
	}

	if style != boxDuck {
		b.WriteString(rule(g.bl, g.bm, g.br))
		if shown < ncol {
			fmt.Fprintf(&b, "(%d of %d columns shown)\n", shown, ncol)
		}
		_, err := io.WriteString(w, b.String())
		return err
	}

	// duckbox footer: row count, and hidden columns.
	inner := lineWidth(widths) - 2
	b.WriteString(rule(g.footL, g.footM, g.ff))
	for _, f := range foot {
		b.WriteString(g.v + " " + pad(fit(f, inner-2), inner-2, false) + " " + g.v + "\n")
	}
	b.WriteString(g.bl + strings.Repeat(g.h, inner) + g.br + "\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func writeMarkdown(w io.Writer, t *Table, o Options) error {
	esc := func(s string) string { return strings.ReplaceAll(s, "|", `\|`) }
	ncol := len(t.Columns)
	rows := t.Rows()
	widths := make([]int, ncol)
	for i, c := range t.Columns {
		widths[i] = max(ansi.StringWidth(esc(c.Name)), 3)
	}
	cells := make([][]string, len(rows))
	for r, row := range rows {
		cells[r] = make([]string, ncol)
		for i := range ncol {
			s := esc(cellText(o, row[i]))
			cells[r][i] = s
			widths[i] = max(widths[i], ansi.StringWidth(s))
		}
	}
	var b strings.Builder
	writeRow := func(vals []string, right func(i int) bool) {
		b.WriteString("|")
		for i := range ncol {
			b.WriteString(" " + pad(vals[i], widths[i], right(i)) + " |")
		}
		b.WriteString("\n")
	}
	if o.Headers {
		names := make([]string, ncol)
		for i, c := range t.Columns {
			names[i] = esc(c.Name)
		}
		writeRow(names, func(int) bool { return false })
		b.WriteString("|")
		for i, c := range t.Columns {
			dash := strings.Repeat("-", widths[i])
			if c.Right {
				dash = dash[:len(dash)-1] + ":"
			}
			b.WriteString(" " + dash + " |")
		}
		b.WriteString("\n")
	}
	for r := range rows {
		writeRow(cells[r], func(i int) bool { return t.Columns[i].Right })
	}
	_, err := io.WriteString(w, b.String())
	return err
}
