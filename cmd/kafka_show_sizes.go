/*
Copyright © 2026
*/
package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// minTopicColWidth is the narrowest the wrapped TOPIC column is allowed to get
// before we stop trying to fit rows to the terminal and just let them be wide.
const minTopicColWidth = 12

// kafkaShowSizesCmd prints the previously computed topic size estimates from
// the DuckDB table without re-sampling Kafka.
var kafkaShowSizesCmd = &cobra.Command{
	Use:   "show-sizes",
	Short: "Show topic size estimates already stored by estimate-size (no sampling)",
	Long: `Print the contents of the DuckDB table kafka_topic_size_estimates, produced by
"kafka estimate-size". No Kafka sampling is performed: this only reads what is
already stored, so it is fast and offline. Run "kafka estimate-size" first to
populate or refresh the table.

Rows are shown in a column-aligned table, largest estimated size first, with
sizes rendered in human-readable binary units. Long topic names are wrapped so
each row fits the terminal width.`,
	Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
		defer stop()
		return runShowSizes(ctx)
	},
}

func runShowSizes(ctx context.Context) error {
	cfg := GetConfig()
	if cfg.DuckDBPath == "" {
		return fmt.Errorf("show-sizes needs a database file (DUCKDB_PATH is :memory:)")
	}

	db, err := database.InitDB(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	rows, err := database.ReadEstimateTable(ctx, db)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		fmt.Printf("no size estimates stored in %s; run \"kafka estimate-size\" first\n", database.EstimateTableName)
		return nil
	}

	topicWidth := topicColWidth(rows)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tMESSAGES\tAVG\tESTIMATED\tLOW\tHIGH\tSAMPLE\tESTIMATED AT")
	for _, r := range rows {
		lines := wrapText(r.Topic, topicWidth)
		// First line carries the data columns; wrapped continuation lines
		// repeat only the topic cell, leaving the rest blank.
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\t%d\t%s\n",
			lines[0], r.TotalMessages, humanBytes(int64(r.AvgMessageBytes)),
			humanBytes(r.EstimatedBytes), humanBytes(r.LowBytes), humanBytes(r.HighBytes),
			r.SampleCount, r.EstimatedAt.Format(time.RFC3339))
		for _, cont := range lines[1:] {
			fmt.Fprintf(tw, "%s\t\t\t\t\t\t\t\n", cont)
		}
	}
	return tw.Flush()
}

// topicColWidth returns the width to wrap the TOPIC column to so each row fits
// the terminal: the terminal width minus what the fixed columns need. It
// returns the widest topic (no wrapping) when the terminal size is unknown or
// there is ample room, and never goes below minTopicColWidth.
func topicColWidth(rows []database.EstimateRow) int {
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

	// Width the non-topic columns occupy, including tabwriter's 2-space
	// padding between all 8 columns.
	fixed := fixedColsWidth(rows) + 2*7
	avail := termWidth - fixed
	if avail < minTopicColWidth {
		avail = minTopicColWidth
	}
	if widest < avail {
		return widest
	}
	return avail
}

// fixedColsWidth sums the rendered widths of every column except TOPIC, each
// taken as the wider of its header and its widest value.
func fixedColsWidth(rows []database.EstimateRow) int {
	w := []int{len("MESSAGES"), len("AVG"), len("ESTIMATED"), len("LOW"), len("HIGH"), len("SAMPLE"), len("ESTIMATED AT")}
	for _, r := range rows {
		cells := []string{
			fmt.Sprintf("%d", r.TotalMessages),
			humanBytes(int64(r.AvgMessageBytes)),
			humanBytes(r.EstimatedBytes),
			humanBytes(r.LowBytes),
			humanBytes(r.HighBytes),
			fmt.Sprintf("%d", r.SampleCount),
			r.EstimatedAt.Format(time.RFC3339),
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

// wrapText splits s into lines no longer than width, preferring to break after
// a topic separator ('.', '-', '_') so names break on meaningful boundaries,
// and hard-splitting any single run longer than width. It always returns at
// least one line.
func wrapText(s string, width int) []string {
	if width < 1 {
		width = 1
	}
	var lines []string
	for len(s) > width {
		// Look for the last separator at or before the width boundary; keep
		// the separator on the current line.
		brk := -1
		for i := width - 1; i >= 0; i-- {
			if s[i] == '.' || s[i] == '-' || s[i] == '_' {
				brk = i + 1
				break
			}
		}
		if brk <= 0 || brk > width {
			brk = width // no separator in range: hard split.
		}
		lines = append(lines, strings.TrimRight(s[:brk], " "))
		s = s[brk:]
	}
	lines = append(lines, s)
	return lines
}

func init() {
	kafkaCmd.AddCommand(kafkaShowSizesCmd)
}
