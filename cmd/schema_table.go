/*
Copyright © 2026
*/
package cmd

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"golang.org/x/term"
)

// schemaRow is one topic's schema-detection summary as rendered by
// sample-messages and show-schema.
type schemaRow struct {
	Topic       string
	Format      string
	MessageName string
	Cut         bool
	Indexes     [][]int32
	Matched     int
	Sample      int
	// ResolvedBy, Spec ("file:message") and Warnings come from the persisted
	// kafka_topic_schemas row; Fields counts its kafka_topic_fields rows.
	ResolvedBy string
	Spec       string
	Warnings   int
	Fields     int
}

// printSchemaTable writes rows as a column-aligned table with a header, sizes
// rendered like show-sizes. Long topic names are wrapped so each row fits the
// terminal width.
func printSchemaTable(rows []schemaRow) error { return printSchemaTableDocs(rows, false) }

// printSchemaTableDocs is printSchemaTable; docs adds the RESOLVED-BY, SPEC,
// WARNINGS and FIELDS columns (used by show-schema only, so sample-messages
// output stays unchanged).
func printSchemaTableDocs(rows []schemaRow, docs bool) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer func() { _ = tw.Flush() }()

	topicWidth := schemaTopicColWidth(rows, docs)
	header := "TOPIC\tFORMAT\tMESSAGE\tCUT\tINDEXES\tMATCHED/SAMPLE"
	if docs {
		header += "\tRESOLVED-BY\tSPEC\tWARNINGS\tFIELDS"
	}
	fmt.Fprintln(tw, header)
	for _, r := range rows {
		lines := wrapText(r.Topic, topicWidth)
		line := fmt.Sprintf("%s\t%s\t%s\tcut=%v\tindexes=%v\t%d/%d",
			lines[0], r.Format, r.MessageName, r.Cut, r.Indexes, r.Matched, r.Sample)
		pad := "\t\t\t\t\t"
		if docs {
			line += fmt.Sprintf("\t%s\t%s\t%d\t%d", dashIfEmpty(r.ResolvedBy), dashIfEmpty(r.Spec), r.Warnings, r.Fields)
			pad += "\t\t\t\t"
		}
		fmt.Fprintln(tw, line)
		for _, cont := range lines[1:] {
			fmt.Fprintf(tw, "%s%s\n", cont, pad)
		}
	}
	return tw.Flush()
}

// schemaTopicColWidth returns the width to wrap the TOPIC column to so each row
// fits the terminal, mirroring show-sizes' topicColWidth.
func schemaTopicColWidth(rows []schemaRow, docs bool) int {
	widest := len("TOPIC")
	for _, r := range rows {
		if len(r.Topic) > widest {
			widest = len(r.Topic)
		}
	}

	termWidth, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || termWidth <= 0 {
		return widest // not a terminal (piped/redirected): don't wrap.
	}

	fixed := schemaFixedColsWidth(rows, docs) + 2*5
	if docs {
		fixed += 2 * 4
	}
	avail := termWidth - fixed
	if avail < minTopicColWidth {
		avail = minTopicColWidth
	}
	if widest < avail {
		return widest
	}
	return avail
}

// schemaFixedColsWidth sums the rendered widths of every column except TOPIC,
// each taken as the wider of its header and its widest value.
func schemaFixedColsWidth(rows []schemaRow, docs bool) int {
	w := []int{len("FORMAT"), len("MESSAGE"), len("CUT"), len("INDEXES"), len("MATCHED/SAMPLE")}
	if docs {
		w = append(w, len("RESOLVED-BY"), len("SPEC"), len("WARNINGS"), len("FIELDS"))
	}
	for _, r := range rows {
		cells := []string{
			r.Format,
			r.MessageName,
			fmt.Sprintf("cut=%v", r.Cut),
			fmt.Sprintf("indexes=%v", r.Indexes),
			fmt.Sprintf("%d/%d", r.Matched, r.Sample),
		}
		if docs {
			cells = append(cells, dashIfEmpty(r.ResolvedBy), dashIfEmpty(r.Spec), fmt.Sprint(r.Warnings), fmt.Sprint(r.Fields))
		}
		for i, c := range cells {
			if len(c) > w[i] {
				w[i] = len(c)
			}
		}
	}
	total := 0
	for _, x := range w {
		total += x
	}
	return total
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// schemaDetail is the persisted documentation of one topic: how it was
// resolved, its warnings and its documented fields.
type schemaDetail struct {
	Topic    string
	Warnings []string
	Fields   []database.TopicFieldRow
}

// printSchemaDetails writes, after the summary table, a WARNINGS table and a
// FIELDS table (TOPIC PATH TYPE REQ DESCRIPTION) for topics that have any.
// Nothing is printed when no topic has warnings or fields.
func printSchemaDetails(w io.Writer, details []schemaDetail) error {
	var warns, fields bool
	for _, d := range details {
		warns = warns || len(d.Warnings) > 0
		fields = fields || len(d.Fields) > 0
	}
	if warns {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "\nTOPIC\tWARNING")
		for _, d := range details {
			for _, x := range d.Warnings {
				fmt.Fprintf(tw, "%s\t%s\n", d.Topic, truncateRunes(x, 100))
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if fields {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "\nTOPIC\tPATH\tTYPE\tREQUIRED\tDESCRIPTION")
		for _, d := range details {
			for _, f := range d.Fields {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%v\t%s\n", d.Topic, f.FieldPath, f.Type, f.Required,
					truncateRunes(strings.Join(strings.Fields(f.Description), " "), 60))
			}
		}
		return tw.Flush()
	}
	return nil
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}
