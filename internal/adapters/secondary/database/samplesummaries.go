package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/couac"
)

// ReadAllSampleSummaries queries all sample tables in the database (ending with _sample)
// and returns a map of topicName -> SampleSummary.
func ReadAllSampleSummaries(ctx context.Context, db *couac.DB) (map[string]SampleSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	res, err := conn.Query(ctx, "SHOW TABLES;")
	if err != nil {
		return nil, fmt.Errorf("show tables: %w", err)
	}
	defer res.Close()

	var sampleTables []string
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		tblCol := rec.Column(0).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			tbl := tblCol.Value(i)
			if strings.HasSuffix(tbl, "_sample") {
				sampleTables = append(sampleTables, tbl)
			}
		}
	}

	summaries := make(map[string]SampleSummary, len(sampleTables))
	for _, tbl := range sampleTables {
		qRes, qErr := conn.Query(ctx, fmt.Sprintf(`SELECT topic, detected_format, message_name, cut_confluence_prefix
			FROM %q LIMIT 1`, tbl))
		if qErr != nil {
			continue
		}
		if qRes.Reader.Next() {
			rec := qRes.Reader.RecordBatch()
			if rec.NumRows() > 0 {
				topicCol := rec.Column(0).(*array.String)
				formatCol := rec.Column(1).(*array.String)
				msgCol := rec.Column(2).(*array.String)
				cutCol := rec.Column(3).(*array.Boolean)

				top := strings.Clone(topicCol.Value(0))
				format := ""
				if !formatCol.IsNull(0) {
					format = strings.Clone(formatCol.Value(0))
				}
				msgName := ""
				if !msgCol.IsNull(0) {
					msgName = strings.Clone(msgCol.Value(0))
				}
				cut := false
				if !cutCol.IsNull(0) {
					cut = cutCol.Value(0)
				}

				summaries[top] = SampleSummary{
					Format:              format,
					MessageName:         msgName,
					CutConfluencePrefix: cut,
				}
			}
		}
		qRes.Close()
	}

	return summaries, nil
}
