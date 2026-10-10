package database

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufka/pkg/schemafield"
	"github.com/loicalleyne/couac"
)

// TopicFieldsTableName is the DuckDB table holding the documented fields of each topic's message.
const TopicFieldsTableName = "kafka_topic_fields"

// TopicFieldRow is one field of a topic's message. Enum is stored as a JSON
// array in a VARCHAR column (NULL when empty).
type TopicFieldRow struct {
	Topic       string
	Source      string // "proto" or "asyncapi"
	Message     string
	Position    int32
	FieldPath   string
	Type        string
	Format      string
	Repeated    bool
	Required    bool
	Nullable    bool
	Description string
	Enum        []string
	Default     string
	Example     string
	FieldNumber int32
	Oneof       string
}

var topicFieldsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "topic", Type: arrow.BinaryTypes.String},
	{Name: "source", Type: arrow.BinaryTypes.String},
	{Name: "message", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "position", Type: arrow.PrimitiveTypes.Int32},
	{Name: "field_path", Type: arrow.BinaryTypes.String},
	{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "format", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "repeated", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "required", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "nullable", Type: arrow.FixedWidthTypes.Boolean},
	{Name: "description", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "enum", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "default_value", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "example", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "field_number", Type: arrow.PrimitiveTypes.Int32},
	{Name: "oneof", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func buildTopicFieldsRecord(topic, source, message string, fields []schemafield.FieldDef) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, topicFieldsSchema)
	defer b.Release()
	s := func(i int) *array.StringBuilder { return b.Field(i).(*array.StringBuilder) }
	bo := func(i int) *array.BooleanBuilder { return b.Field(i).(*array.BooleanBuilder) }
	for i, f := range fields {
		s(0).Append(topic)
		s(1).Append(source)
		appendNullable(s(2), message)
		b.Field(3).(*array.Int32Builder).Append(int32(i))
		s(4).Append(f.Path)
		appendNullable(s(5), f.Type)
		appendNullable(s(6), f.Format)
		bo(7).Append(f.Repeated)
		bo(8).Append(f.Required)
		bo(9).Append(f.Nullable)
		appendNullable(s(10), f.Description)
		if len(f.Enum) > 0 {
			js, _ := json.Marshal(f.Enum)
			s(11).Append(string(js))
		} else {
			s(11).AppendNull()
		}
		appendNullable(s(12), f.Default)
		appendNullable(s(13), f.Example)
		b.Field(14).(*array.Int32Builder).Append(f.Number)
		appendNullable(s(15), f.Oneof)
	}
	return b.NewRecordBatch()
}

// EnsureTopicFieldsTable ensures that the kafka_topic_fields table exists in db.
func EnsureTopicFieldsTable(ctx context.Context, db *couac.DB) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return ensureTopicFieldsTable(ctx, conn)
}

func ensureTopicFieldsTable(ctx context.Context, conn *couac.Conn) error {
	if schema, _ := conn.GetTableSchema(ctx, nil, nil, TopicFieldsTableName); schema != nil {
		return nil
	}
	rec := buildTopicFieldsRecord("", "", "", nil)
	defer rec.Release()
	if _, err := conn.IngestReplace(ctx, TopicFieldsTableName, rec); err != nil {
		return fmt.Errorf("init %s: %w", TopicFieldsTableName, err)
	}
	return nil
}

func sqlQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// ReplaceTopicFields deletes the stored fields of topic and inserts fields
// (source "proto" or "asyncapi", message the message they belong to), in one
// transaction. Passing no fields just clears the topic.
func ReplaceTopicFields(ctx context.Context, db *couac.DB, topic string, source string, message string, fields []schemafield.FieldDef) (err error) {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if err := ensureTopicFieldsTable(ctx, conn); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil {
			_, _ = conn.Exec(ctx, "ROLLBACK")
		}
	}()
	if _, err = conn.Exec(ctx, fmt.Sprintf(`DELETE FROM %q WHERE topic = %s`, TopicFieldsTableName, sqlQuote(topic))); err != nil {
		return fmt.Errorf("delete %s: %w", TopicFieldsTableName, err)
	}
	if len(fields) > 0 {
		rec := buildTopicFieldsRecord(topic, source, message, fields)
		defer rec.Release()
		if _, err = conn.Ingest(ctx, TopicFieldsTableName, rec); err != nil {
			return fmt.Errorf("insert %s: %w", TopicFieldsTableName, err)
		}
	}
	if _, err = conn.Exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadTopicFields returns the stored fields of topic in position order. An
// unknown topic (or missing table) yields an empty, non-nil slice.
func ReadTopicFields(ctx context.Context, db *couac.DB, topic string) ([]TopicFieldRow, error) {
	out := []TopicFieldRow{}
	if db == nil {
		return out, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return out, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if schema, _ := conn.GetTableSchema(ctx, nil, nil, TopicFieldsTableName); schema == nil {
		return out, nil
	}
	q := fmt.Sprintf(`SELECT topic, source, message, position, field_path, type, format, repeated, required,
		nullable, description, enum, default_value, example, field_number, oneof
		FROM %q WHERE topic = %s ORDER BY position`, TopicFieldsTableName, sqlQuote(topic))
	res, err := conn.Query(ctx, q)
	if err != nil {
		return out, fmt.Errorf("query %s: %w", TopicFieldsTableName, err)
	}
	defer res.Close()
	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		str := func(c, i int) string {
			col := rec.Column(c).(*array.String)
			if col.IsValid(i) {
				return strings.Clone(col.Value(i))
			}
			return ""
		}
		bl := func(c, i int) bool { return rec.Column(c).(*array.Boolean).Value(i) }
		for i := 0; i < int(rec.NumRows()); i++ {
			var enum []string
			if e := str(11, i); e != "" {
				_ = json.Unmarshal([]byte(e), &enum)
			}
			out = append(out, TopicFieldRow{
				Topic: str(0, i), Source: str(1, i), Message: str(2, i),
				Position:  rec.Column(3).(*array.Int32).Value(i),
				FieldPath: str(4, i), Type: str(5, i), Format: str(6, i),
				Repeated: bl(7, i), Required: bl(8, i), Nullable: bl(9, i),
				Description: str(10, i), Enum: enum, Default: str(12, i), Example: str(13, i),
				FieldNumber: rec.Column(14).(*array.Int32).Value(i), Oneof: str(15, i),
			})
		}
	}
	if err := res.Reader.Err(); err != nil {
		return out, fmt.Errorf("read %s: %w", TopicFieldsTableName, err)
	}
	return out, nil
}
