package database

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

// TopicSchemaTableName is the DuckDB table name holding detected topic schemas and overrides.
const TopicSchemaTableName = "kafka_topic_schemas"

// TopicSchemaRow defines one Kafka topic's detected schema, overrides, and sample statistics.
type TopicSchemaRow struct {
	Topic               string
	Table               string
	Format              string
	MessageName         string
	ProtoFile           string
	ImportRoot          string
	CutConfluencePrefix bool
	MessageOverride     bool
	SampleCount         int32
	MatchedCount        int32
	UpdatedAt           time.Time
	// ResolvedBy is how the schema was determined (topicsample.ResolvedBy*); empty for legacy rows.
	ResolvedBy string
	// SpecFile is the AsyncAPI spec file documenting the topic, if any.
	SpecFile string
	// SpecMessage is the AsyncAPI message chosen for the topic, if any.
	SpecMessage string
	// Warnings are spec-validation findings. Stored as a JSON array in a VARCHAR
	// column (NULL when empty).
	Warnings []string
}

// topicSchemaNewColumns are the columns added after the original schema; existing
// databases get them via ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
var topicSchemaNewColumns = []string{"resolved_by", "spec_file", "spec_message", "warnings"}

var topicSchemaTableSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "table", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "format", Type: arrow.BinaryTypes.String},
	{Name: "message_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "proto_file", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "import_root", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "cut_confluence_prefix", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "message_override", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "sample_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "matched_count", Type: arrow.PrimitiveTypes.Int32},
	{Name: "updated_at", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "resolved_by", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "spec_file", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "spec_message", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "warnings", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func appendNullable(b *array.StringBuilder, v string) {
	if v == "" {
		b.AppendNull()
		return
	}
	b.Append(v)
}

// migrateTopicSchemaColumns adds any columns missing from an existing table.
func migrateTopicSchemaColumns(ctx context.Context, conn *couac.Conn, schema *arrow.Schema) error {
	for _, col := range topicSchemaNewColumns {
		if _, ok := schema.FieldsByName(col); ok {
			continue
		}
		q := fmt.Sprintf(`ALTER TABLE %q ADD COLUMN IF NOT EXISTS %s VARCHAR`, TopicSchemaTableName, col)
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("migrate %s.%s: %w", TopicSchemaTableName, col, err)
		}
	}
	return nil
}

func buildTopicSchemaRecord(rows []TopicSchemaRow) arrow.RecordBatch {
	pool := memory.DefaultAllocator
	b := array.NewRecordBuilder(pool, topicSchemaTableSchema)
	defer b.Release()

	topicB := b.Field(0).(*array.StringBuilder)
	tableB := b.Field(1).(*array.StringBuilder)
	formatB := b.Field(2).(*array.StringBuilder)
	msgNameB := b.Field(3).(*array.StringBuilder)
	protoFileB := b.Field(4).(*array.StringBuilder)
	importRootB := b.Field(5).(*array.StringBuilder)
	cutConfB := b.Field(6).(*array.BooleanBuilder)
	msgOverrideB := b.Field(7).(*array.BooleanBuilder)
	sampleCountB := b.Field(8).(*array.Int32Builder)
	matchedCountB := b.Field(9).(*array.Int32Builder)
	updatedAtB := b.Field(10).(*array.TimestampBuilder)
	resolvedByB := b.Field(11).(*array.StringBuilder)
	specFileB := b.Field(12).(*array.StringBuilder)
	specMessageB := b.Field(13).(*array.StringBuilder)
	warningsB := b.Field(14).(*array.StringBuilder)

	for _, r := range rows {
		topicB.Append(r.Topic)
		if r.Table != "" {
			tableB.Append(r.Table)
		} else {
			tableB.AppendNull()
		}
		formatB.Append(r.Format)
		if r.MessageName != "" {
			msgNameB.Append(r.MessageName)
		} else {
			msgNameB.AppendNull()
		}
		if r.ProtoFile != "" {
			protoFileB.Append(r.ProtoFile)
		} else {
			protoFileB.AppendNull()
		}
		if r.ImportRoot != "" {
			importRootB.Append(r.ImportRoot)
		} else {
			importRootB.AppendNull()
		}
		cutConfB.Append(r.CutConfluencePrefix)
		msgOverrideB.Append(r.MessageOverride)
		sampleCountB.Append(r.SampleCount)
		matchedCountB.Append(r.MatchedCount)

		updatedAt := r.UpdatedAt
		if updatedAt.IsZero() {
			updatedAt = time.Now()
		}
		ts, _ := arrow.TimestampFromTime(updatedAt, arrow.Microsecond)
		updatedAtB.Append(ts)
		appendNullable(resolvedByB, r.ResolvedBy)
		appendNullable(specFileB, r.SpecFile)
		appendNullable(specMessageB, r.SpecMessage)
		if len(r.Warnings) > 0 {
			js, _ := json.Marshal(r.Warnings)
			warningsB.Append(string(js))
		} else {
			warningsB.AppendNull()
		}
	}

	return b.NewRecordBatch()
}

// EnsureTopicSchemaTable ensures that the kafka_topic_schemas table exists in db.
func EnsureTopicSchemaTable(ctx context.Context, db *couac.DB) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, TopicSchemaTableName)
	if schema != nil {
		return migrateTopicSchemaColumns(ctx, conn, schema)
	}
	rec := buildTopicSchemaRecord(nil)
	defer rec.Release()
	if _, err := conn.IngestReplace(ctx, TopicSchemaTableName, rec); err != nil {
		return fmt.Errorf("init %s: %w", TopicSchemaTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// WriteTopicSchemas replaces the kafka_topic_schemas table with rows and checkpoints the database.
func WriteTopicSchemas(ctx context.Context, db *couac.DB, rows []TopicSchemaRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	rec := buildTopicSchemaRecord(rows)
	defer rec.Release()

	if _, err := conn.IngestReplace(ctx, TopicSchemaTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", TopicSchemaTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadTopicSchemas returns all saved topic schemas keyed by topic name.
func ReadTopicSchemas(ctx context.Context, db *couac.DB) (map[string]TopicSchemaRow, error) {
	out := make(map[string]TopicSchemaRow)
	if db == nil {
		return out, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return out, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, TopicSchemaTableName)
	if schema == nil {
		return out, nil
	}
	if err := migrateTopicSchemaColumns(ctx, conn, schema); err != nil {
		return out, err
	}

	query := fmt.Sprintf(`SELECT topic, "table", format, message_name, proto_file, import_root,
		cut_confluence_prefix, message_override, sample_count, matched_count, updated_at,
			resolved_by, spec_file, spec_message, warnings
		FROM %q ORDER BY topic`, TopicSchemaTableName)
	res, err := conn.Query(ctx, query)
	if err != nil {
		return out, fmt.Errorf("query %s: %w", TopicSchemaTableName, err)
	}
	defer res.Close()

	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		topicCol := rec.Column(0).(*array.String)
		tableCol := rec.Column(1).(*array.String)
		formatCol := rec.Column(2).(*array.String)
		msgNameCol := rec.Column(3).(*array.String)
		protoFileCol := rec.Column(4).(*array.String)
		importRootCol := rec.Column(5).(*array.String)
		cutConfCol := rec.Column(6).(*array.Boolean)
		msgOverrideCol := rec.Column(7).(*array.Boolean)
		sampleCountCol := rec.Column(8).(*array.Int32)
		matchedCountCol := rec.Column(9).(*array.Int32)
		updatedAtCol := rec.Column(10).(*array.Timestamp)
		resolvedByCol := rec.Column(11).(*array.String)
		specFileCol := rec.Column(12).(*array.String)
		specMessageCol := rec.Column(13).(*array.String)
		warningsCol := rec.Column(14).(*array.String)
		str := func(c *array.String, i int) string {
			if c.IsValid(i) {
				return strings.Clone(c.Value(i))
			}
			return ""
		}

		for i := 0; i < int(rec.NumRows()); i++ {
			var tableName, msgName, protoFile, importRoot, format string
			if tableCol.IsValid(i) {
				tableName = strings.Clone(tableCol.Value(i))
			}
			if formatCol.IsValid(i) {
				format = strings.Clone(formatCol.Value(i))
			}
			if msgNameCol.IsValid(i) {
				msgName = strings.Clone(msgNameCol.Value(i))
			}
			if protoFileCol.IsValid(i) {
				protoFile = strings.Clone(protoFileCol.Value(i))
			}
			if importRootCol.IsValid(i) {
				importRoot = strings.Clone(importRootCol.Value(i))
			}

			var warnings []string
			if w := str(warningsCol, i); w != "" {
				_ = json.Unmarshal([]byte(w), &warnings)
			}
			topic := strings.Clone(topicCol.Value(i))
			out[topic] = TopicSchemaRow{
				Topic:               topic,
				Table:               tableName,
				Format:              format,
				MessageName:         msgName,
				ProtoFile:           protoFile,
				ImportRoot:          importRoot,
				CutConfluencePrefix: cutConfCol.Value(i),
				MessageOverride:     msgOverrideCol.Value(i),
				SampleCount:         sampleCountCol.Value(i),
				MatchedCount:        matchedCountCol.Value(i),
				UpdatedAt:           updatedAtCol.Value(i).ToTime(arrow.Microsecond),
				ResolvedBy:          str(resolvedByCol, i),
				SpecFile:            str(specFileCol, i),
				SpecMessage:         str(specMessageCol, i),
				Warnings:            warnings,
			}
		}
	}
	if err := res.Reader.Err(); err != nil {
		return out, fmt.Errorf("read %s: %w", TopicSchemaTableName, err)
	}
	return out, nil
}

// UpsertTopicSchemas updates or inserts topic schemas in DuckDB, merging with
// existing records. If an existing record has MessageOverride set and the incoming
// row does not explicitly set MessageOverride, the user-chosen schema configuration
// (Format, MessageName, ProtoFile, ImportRoot, CutConfluencePrefix, MessageOverride)
// is preserved while updating sample counts and metadata.
func UpsertTopicSchemas(ctx context.Context, db *couac.DB, rows []TopicSchemaRow) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	if len(rows) == 0 {
		return nil
	}
	existing, err := ReadTopicSchemas(ctx, db)
	if err != nil {
		return err
	}

	for _, row := range rows {
		prev, found := existing[row.Topic]
		if found {
			if prev.MessageOverride && !row.MessageOverride {
				// Keep user-overridden schema configuration
				row.Format = prev.Format
				row.MessageName = prev.MessageName
				row.ProtoFile = prev.ProtoFile
				row.ImportRoot = prev.ImportRoot
				row.CutConfluencePrefix = prev.CutConfluencePrefix
				row.MessageOverride = true
			}
			if row.Table == "" {
				row.Table = prev.Table
			}
		}
		if row.UpdatedAt.IsZero() {
			row.UpdatedAt = time.Now()
		}
		existing[row.Topic] = row
	}

	merged := make([]TopicSchemaRow, 0, len(existing))
	for _, r := range existing {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Topic < merged[j].Topic
	})

	return WriteTopicSchemas(ctx, db, merged)
}
