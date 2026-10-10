package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

// EstimateRow is one topic's size estimate, derived from its message count and
// a sample of its messages.
type EstimateRow struct {
	Topic           string
	EstimatedAt     time.Time
	Partitions      int32
	TotalMessages   int64
	SampleCount     int32
	AvgMessageBytes float64
	StdDevBytes     float64
	MinMessageBytes int32
	MaxMessageBytes int32
	EstimatedBytes  int64
	LowBytes        int64
	HighBytes       int64
}

var estimateSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "estimated_at", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "partitions", Type: arrow.PrimitiveTypes.Int32},
	{Name: "total_messages", Type: arrow.PrimitiveTypes.Int64},
	{Name: "sample_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "avg_message_bytes", Type: arrow.PrimitiveTypes.Float64},
	{Name: "stddev_bytes", Type: arrow.PrimitiveTypes.Float64},
	{Name: "min_message_bytes", Type: arrow.PrimitiveTypes.Int32},
	{Name: "max_message_bytes", Type: arrow.PrimitiveTypes.Int32},
	{Name: "estimated_bytes", Type: arrow.PrimitiveTypes.Int64},
	{Name: "low_bytes", Type: arrow.PrimitiveTypes.Int64},
	{Name: "high_bytes", Type: arrow.PrimitiveTypes.Int64},
}, nil)

// EstimateTableName is the DuckDB table that holds topic size estimates.
const EstimateTableName = "kafka_topic_size_estimates"

// WriteEstimateTable replaces the topic size estimate table with rows. It does
// nothing when rows is empty.
func WriteEstimateTable(ctx context.Context, db *couac.DB, rows []EstimateRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	if len(rows) == 0 {
		return nil
	}
	rec := buildEstimateRecord(rows)
	defer rec.Release()

	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.IngestReplace(ctx, EstimateTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", EstimateTableName, err)
	}
	return nil
}

// ReadEstimateTable returns every row of the topic size estimate table,
// ordered by estimated size descending. It returns an empty slice (nil error)
// when the table does not exist yet.
func ReadEstimateTable(ctx context.Context, db *couac.DB) ([]EstimateRow, error) {
	if db == nil {
		return nil, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, EstimateTableName)
	if schema == nil {
		return nil, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT topic, estimated_at, partitions, total_messages,
		sample_count, avg_message_bytes, stddev_bytes, min_message_bytes, max_message_bytes,
		estimated_bytes, low_bytes, high_bytes
		FROM %q ORDER BY estimated_bytes DESC, topic`, EstimateTableName))
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", EstimateTableName, err)
	}
	defer res.Close()

	var rows []EstimateRow
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		topic := rec.Column(0).(*array.String)
		estimatedAt := rec.Column(1).(*array.Timestamp)
		partitions := rec.Column(2).(*array.Int32)
		totalMessages := rec.Column(3).(*array.Int64)
		sampleCount := rec.Column(4).(*array.Int32)
		avgBytes := rec.Column(5).(*array.Float64)
		stdDev := rec.Column(6).(*array.Float64)
		minBytes := rec.Column(7).(*array.Int32)
		maxBytes := rec.Column(8).(*array.Int32)
		estimatedBytes := rec.Column(9).(*array.Int64)
		lowBytes := rec.Column(10).(*array.Int64)
		highBytes := rec.Column(11).(*array.Int64)
		for i := 0; i < int(rec.NumRows()); i++ {
			rows = append(rows, EstimateRow{
				Topic:           strings.Clone(topic.Value(i)),
				EstimatedAt:     estimatedAt.Value(i).ToTime(arrow.Microsecond),
				Partitions:      partitions.Value(i),
				TotalMessages:   totalMessages.Value(i),
				SampleCount:     sampleCount.Value(i),
				AvgMessageBytes: avgBytes.Value(i),
				StdDevBytes:     stdDev.Value(i),
				MinMessageBytes: minBytes.Value(i),
				MaxMessageBytes: maxBytes.Value(i),
				EstimatedBytes:  estimatedBytes.Value(i),
				LowBytes:        lowBytes.Value(i),
				HighBytes:       highBytes.Value(i),
			})
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", EstimateTableName, err)
	}
	return rows, nil
}

// UpsertEstimateRows updates or inserts estimate rows into the DuckDB table,
// preserving estimates for topics not present in rows.
func UpsertEstimateRows(ctx context.Context, db *couac.DB, rows []EstimateRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	if len(rows) == 0 {
		return nil
	}
	existing, err := ReadEstimateTable(ctx, db)
	if err != nil {
		return err
	}
	merged := make(map[string]EstimateRow, len(existing)+len(rows))
	for _, r := range existing {
		merged[r.Topic] = r
	}
	for _, r := range rows {
		merged[r.Topic] = r
	}
	combined := make([]EstimateRow, 0, len(merged))
	for _, r := range merged {
		combined = append(combined, r)
	}
	return WriteEstimateTable(ctx, db, combined)
}

func buildEstimateRecord(rows []EstimateRow) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, estimateSchema)
	defer b.Release()

	topic := b.Field(0).(*array.StringBuilder)
	estimatedAt := b.Field(1).(*array.TimestampBuilder)
	partitions := b.Field(2).(*array.Int32Builder)
	totalMessages := b.Field(3).(*array.Int64Builder)
	sampleCount := b.Field(4).(*array.Int32Builder)
	avgBytes := b.Field(5).(*array.Float64Builder)
	stdDev := b.Field(6).(*array.Float64Builder)
	minBytes := b.Field(7).(*array.Int32Builder)
	maxBytes := b.Field(8).(*array.Int32Builder)
	estimatedBytes := b.Field(9).(*array.Int64Builder)
	lowBytes := b.Field(10).(*array.Int64Builder)
	highBytes := b.Field(11).(*array.Int64Builder)

	for _, r := range rows {
		topic.Append(r.Topic)
		estimatedAt.Append(arrow.Timestamp(r.EstimatedAt.UnixMicro()))
		partitions.Append(r.Partitions)
		totalMessages.Append(r.TotalMessages)
		sampleCount.Append(r.SampleCount)
		avgBytes.Append(r.AvgMessageBytes)
		stdDev.Append(r.StdDevBytes)
		minBytes.Append(r.MinMessageBytes)
		maxBytes.Append(r.MaxMessageBytes)
		estimatedBytes.Append(r.EstimatedBytes)
		lowBytes.Append(r.LowBytes)
		highBytes.Append(r.HighBytes)
	}
	return b.NewRecordBatch()
}
