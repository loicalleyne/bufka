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

// SampleHeader is a Kafka record header.
type SampleHeader struct {
	Key   string
	Value []byte
}

// SampleRow is one sampled Kafka message plus its schema-detection outcome.
type SampleRow struct {
	Topic               string
	Partition           int32
	Offset              int64
	Timestamp           time.Time
	Key                 []byte
	Value               []byte
	Headers             []SampleHeader
	DetectedFormat      string
	MessageName         string
	CutConfluencePrefix bool
	// ConfluentHeaderLen is 0 when the value has no Confluent header.
	ConfluentHeaderLen      int32
	ConfluentSchemaID       *int32
	ConfluentMessageIndexes []int32
	DecodeOK                bool
	DecodedJSON             string
	// Reviewed is true when the match was a structural tie and a
	// topicsample.Advisor was consulted.
	Reviewed bool
	// ReviewPick is the advisor's choice, empty if it had no opinion.
	ReviewPick string
	// ReviewConfidence is the advisor's confidence in ReviewPick.
	ReviewConfidence float64
	// ReviewAgrees is true when ReviewPick equals MessageName.
	ReviewAgrees bool
}

var sampleSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "partition", Type: arrow.PrimitiveTypes.Int32},
	{Name: "offset", Type: arrow.PrimitiveTypes.Int64},
	{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "key", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "headers", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	))},
	{Name: "detected_format", Type: arrow.BinaryTypes.String},
	{Name: "message_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cut_confluence_prefix", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "confluent_header_len", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "confluent_schema_id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "confluent_message_indexes", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	{Name: "decode_ok", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "decoded_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "reviewed", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "review_pick", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "review_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	{Name: "review_agrees", Type: arrow.FixedWidthTypes.Boolean},
}, nil)

// SampleSummary is the per-topic schema-detection summary derived from the
// rows stored in a topic's _sample table.
type SampleSummary struct {
	Format              string
	MessageName         string
	CutConfluencePrefix bool
	// MessageIndexes are the distinct Confluent message-index paths seen.
	MessageIndexes [][]int32
	// MatchedCount is the number of rows whose value decoded cleanly.
	MatchedCount int
	// SampleCount is the number of rows stored.
	SampleCount int
}

// ReadSampleSummary derives the schema-detection summary of a topic from the
// rows stored in its _sample table. The second return value is false when the
// table does not exist or holds no rows.
func ReadSampleSummary(ctx context.Context, db *couac.DB, table string) (SampleSummary, bool, error) {
	if db == nil {
		return SampleSummary{}, false, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return SampleSummary{}, false, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, table)
	if schema == nil {
		return SampleSummary{}, false, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT detected_format, message_name, cut_confluence_prefix,
		confluent_message_indexes, decode_ok FROM %q`, table))
	if err != nil {
		return SampleSummary{}, false, fmt.Errorf("query %s: %w", table, err)
	}
	defer res.Close()

	var s SampleSummary
	seenIndexes := make(map[string]bool)
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		format := rec.Column(0).(*array.String)
		messageName := rec.Column(1).(*array.String)
		cut := rec.Column(2).(*array.Boolean)
		indexes := rec.Column(3).(*array.List)
		indexValues := indexes.ListValues().(*array.Int32)
		decodeOK := rec.Column(4).(*array.Boolean)
		for i := 0; i < int(rec.NumRows()); i++ {
			s.SampleCount++
			// Detection is per-topic, so every row carries the same verdict;
			// take it from the first non-null occurrence.
			if s.Format == "" && !format.IsNull(i) {
				s.Format = strings.Clone(format.Value(i))
			}
			if s.MessageName == "" && !messageName.IsNull(i) {
				s.MessageName = strings.Clone(messageName.Value(i))
			}
			if !cut.IsNull(i) && cut.Value(i) {
				s.CutConfluencePrefix = true
			}
			if decodeOK.Value(i) {
				s.MatchedCount++
			}
			if !indexes.IsNull(i) {
				start, end := indexes.ValueOffsets(i)
				path := make([]int32, 0, end-start)
				for j := start; j < end; j++ {
					path = append(path, indexValues.Value(int(j)))
				}
				key := fmt.Sprint(path)
				if !seenIndexes[key] {
					seenIndexes[key] = true
					s.MessageIndexes = append(s.MessageIndexes, path)
				}
			}
		}
	}
	if err := res.Reader.Err(); err != nil {
		return SampleSummary{}, false, fmt.Errorf("read %s: %w", table, err)
	}
	if s.SampleCount == 0 {
		return SampleSummary{}, false, nil
	}
	return s, true, nil
}

// ReadSampleTable returns every row stored in a topic's _sample table,
// ordered by timestamp descending. It returns (nil, false, nil) when the
// table does not exist or holds no rows. It is symmetric to WriteSampleTable.
func ReadSampleTable(ctx context.Context, db *couac.DB, table string) ([]SampleRow, bool, error) {
	if db == nil {
		return nil, false, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return nil, false, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, table)
	if schema == nil {
		return nil, false, nil
	}

	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT topic, partition, "offset", timestamp, key, value,
		headers, detected_format, message_name, cut_confluence_prefix, confluent_header_len,
		confluent_schema_id, confluent_message_indexes, decode_ok, decoded_json,
		reviewed, review_pick, review_confidence, review_agrees
		FROM %q ORDER BY timestamp DESC, partition, "offset"`, table))
	if err != nil {
		return nil, false, fmt.Errorf("query %s: %w", table, err)
	}
	defer res.Close()

	var rows []SampleRow
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		topic := rec.Column(0).(*array.String)
		partition := rec.Column(1).(*array.Int32)
		offset := rec.Column(2).(*array.Int64)
		ts := rec.Column(3).(*array.Timestamp)
		key := rec.Column(4).(*array.Binary)
		value := rec.Column(5).(*array.Binary)
		headers := rec.Column(6).(*array.List)
		headerStruct := headers.ListValues().(*array.Struct)
		headerKey := headerStruct.Field(0).(*array.String)
		headerValue := headerStruct.Field(1).(*array.Binary)
		format := rec.Column(7).(*array.String)
		messageName := rec.Column(8).(*array.String)
		cut := rec.Column(9).(*array.Boolean)
		headerLen := rec.Column(10).(*array.Int32)
		schemaID := rec.Column(11).(*array.Int32)
		indexes := rec.Column(12).(*array.List)
		indexValues := indexes.ListValues().(*array.Int32)
		decodeOK := rec.Column(13).(*array.Boolean)
		decoded := rec.Column(14).(*array.String)
		reviewed := rec.Column(15).(*array.Boolean)
		reviewPick := rec.Column(16).(*array.String)
		reviewConfidence := rec.Column(17).(*array.Float64)
		reviewAgrees := rec.Column(18).(*array.Boolean)

		for i := 0; i < int(rec.NumRows()); i++ {
			row := SampleRow{
				Topic:               strings.Clone(topic.Value(i)),
				Partition:           partition.Value(i),
				Offset:              offset.Value(i),
				Timestamp:           ts.Value(i).ToTime(arrow.Microsecond),
				DetectedFormat:      strings.Clone(format.Value(i)),
				CutConfluencePrefix: cut.Value(i),
				DecodeOK:            decodeOK.Value(i),
				Reviewed:            reviewed.Value(i),
				ReviewAgrees:        reviewAgrees.Value(i),
			}
			if !key.IsNull(i) {
				row.Key = append([]byte(nil), key.Value(i)...)
			}
			if !value.IsNull(i) {
				row.Value = append([]byte(nil), value.Value(i)...)
			}
			if !headers.IsNull(i) {
				start, end := headers.ValueOffsets(i)
				for j := start; j < end; j++ {
					h := SampleHeader{Key: strings.Clone(headerKey.Value(int(j)))}
					if !headerValue.IsNull(int(j)) {
						h.Value = append([]byte(nil), headerValue.Value(int(j))...)
					}
					row.Headers = append(row.Headers, h)
				}
			}
			if !messageName.IsNull(i) {
				row.MessageName = strings.Clone(messageName.Value(i))
			}
			if !headerLen.IsNull(i) {
				row.ConfluentHeaderLen = headerLen.Value(i)
			}
			if !schemaID.IsNull(i) {
				id := schemaID.Value(i)
				row.ConfluentSchemaID = &id
			}
			if !indexes.IsNull(i) {
				start, end := indexes.ValueOffsets(i)
				path := make([]int32, 0, end-start)
				for j := start; j < end; j++ {
					path = append(path, indexValues.Value(int(j)))
				}
				row.ConfluentMessageIndexes = path
			}
			if !decoded.IsNull(i) {
				row.DecodedJSON = strings.Clone(decoded.Value(i))
			}
			if !reviewPick.IsNull(i) {
				row.ReviewPick = strings.Clone(reviewPick.Value(i))
			}
			if !reviewConfidence.IsNull(i) {
				row.ReviewConfidence = reviewConfidence.Value(i)
			}
			rows = append(rows, row)
		}
	}
	if err := res.Reader.Err(); err != nil {
		return nil, false, fmt.Errorf("read %s: %w", table, err)
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	return rows, true, nil
}

// WriteSampleTable replaces table with rows. It does nothing when rows is empty.
func WriteSampleTable(ctx context.Context, db *couac.DB, table string, rows []SampleRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	if len(rows) == 0 {
		return nil
	}
	rec := buildSampleRecord(rows)
	defer rec.Release()

	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.IngestReplace(ctx, table, rec); err != nil {
		return fmt.Errorf("replace %s: %w", table, err)
	}
	return nil
}

func buildSampleRecord(rows []SampleRow) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, sampleSchema)
	defer b.Release()

	topic := b.Field(0).(*array.StringBuilder)
	partition := b.Field(1).(*array.Int32Builder)
	offset := b.Field(2).(*array.Int64Builder)
	ts := b.Field(3).(*array.TimestampBuilder)
	key := b.Field(4).(*array.BinaryBuilder)
	value := b.Field(5).(*array.BinaryBuilder)
	headers := b.Field(6).(*array.ListBuilder)
	header := headers.ValueBuilder().(*array.StructBuilder)
	headerKey := header.FieldBuilder(0).(*array.StringBuilder)
	headerValue := header.FieldBuilder(1).(*array.BinaryBuilder)
	format := b.Field(7).(*array.StringBuilder)
	messageName := b.Field(8).(*array.StringBuilder)
	cut := b.Field(9).(*array.BooleanBuilder)
	headerLen := b.Field(10).(*array.Int32Builder)
	schemaID := b.Field(11).(*array.Int32Builder)
	indexes := b.Field(12).(*array.ListBuilder)
	index := indexes.ValueBuilder().(*array.Int32Builder)
	decodeOK := b.Field(13).(*array.BooleanBuilder)
	decoded := b.Field(14).(*array.StringBuilder)
	reviewed := b.Field(15).(*array.BooleanBuilder)
	reviewPick := b.Field(16).(*array.StringBuilder)
	reviewConfidence := b.Field(17).(*array.Float64Builder)
	reviewAgrees := b.Field(18).(*array.BooleanBuilder)

	for _, r := range rows {
		topic.Append(r.Topic)
		partition.Append(r.Partition)
		offset.Append(r.Offset)
		ts.Append(arrow.Timestamp(r.Timestamp.UnixMicro()))
		appendBinary(key, r.Key)
		appendBinary(value, r.Value)

		headers.Append(true)
		for _, h := range r.Headers {
			header.Append(true)
			headerKey.Append(h.Key)
			appendBinary(headerValue, h.Value)
		}

		format.Append(r.DetectedFormat)
		appendString(messageName, r.MessageName)
		cut.Append(r.CutConfluencePrefix)
		if r.ConfluentHeaderLen > 0 {
			headerLen.Append(r.ConfluentHeaderLen)
		} else {
			headerLen.AppendNull()
		}
		if r.ConfluentSchemaID != nil {
			schemaID.Append(*r.ConfluentSchemaID)
		} else {
			schemaID.AppendNull()
		}
		if r.ConfluentMessageIndexes != nil {
			indexes.Append(true)
			index.AppendValues(r.ConfluentMessageIndexes, nil)
		} else {
			indexes.AppendNull()
		}
		decodeOK.Append(r.DecodeOK)
		appendString(decoded, r.DecodedJSON)
		reviewed.Append(r.Reviewed)
		appendString(reviewPick, r.ReviewPick)
		if r.ReviewPick != "" {
			reviewConfidence.Append(r.ReviewConfidence)
		} else {
			reviewConfidence.AppendNull()
		}
		reviewAgrees.Append(r.ReviewAgrees)
	}
	return b.NewRecordBatch()
}

func appendBinary(b *array.BinaryBuilder, v []byte) {
	if v == nil {
		b.AppendNull()
		return
	}
	b.Append(v)
}

func appendString(b *array.StringBuilder, v string) {
	if v == "" {
		b.AppendNull()
		return
	}
	b.Append(v)
}
