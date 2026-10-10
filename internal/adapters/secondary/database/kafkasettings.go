package database

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/couac"
)

// KafkaSettingsTableName is the DuckDB table holding user-edited Kafka
// settings as key/value rows. Values are the raw text the user entered; SASL
// entries hold the NAME of an environment variable, never a credential.
const KafkaSettingsTableName = "kafka_settings"

var kafkaSettingsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "key", Type: arrow.BinaryTypes.String},
	{Name: "value", Type: arrow.BinaryTypes.String},
}, nil)

// WriteKafkaSettings replaces all saved Kafka settings with kv. Empty values
// are not stored: an empty setting means "use the environment default".
func WriteKafkaSettings(ctx context.Context, db *couac.DB, kv map[string]string) error {
	if db == nil {
		return fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	b := array.NewRecordBuilder(memory.DefaultAllocator, kafkaSettingsSchema)
	defer b.Release()
	kB := b.Field(0).(*array.StringBuilder)
	vB := b.Field(1).(*array.StringBuilder)
	for k, v := range kv {
		if strings.TrimSpace(v) == "" {
			continue
		}
		kB.Append(k)
		vB.Append(v)
	}
	rec := b.NewRecordBatch()
	defer rec.Release()

	if _, err := conn.IngestReplace(ctx, KafkaSettingsTableName, rec); err != nil {
		return fmt.Errorf("replace %s: %w", KafkaSettingsTableName, err)
	}
	_ = db.Checkpoint(ctx)
	return nil
}

// ReadKafkaSettings returns the saved settings. The map is empty (never nil)
// when nothing has been saved, in which case callers use environment values.
func ReadKafkaSettings(ctx context.Context, db *couac.DB) (map[string]string, error) {
	out := map[string]string{}
	if db == nil {
		return out, fmt.Errorf("duckdb is nil")
	}
	conn, err := db.Connect()
	if err != nil {
		return out, fmt.Errorf("connect to duckdb: %w", err)
	}
	defer func() { _ = conn.Close() }()

	schema, _ := conn.GetTableSchema(ctx, nil, nil, KafkaSettingsTableName)
	if schema == nil {
		return out, nil
	}
	res, err := conn.Query(ctx, fmt.Sprintf(`SELECT key, value FROM %q`, KafkaSettingsTableName))
	if err != nil {
		return out, fmt.Errorf("query %s: %w", KafkaSettingsTableName, err)
	}
	defer res.Close()

	for res.Reader.Next() {
		rec := res.Reader.RecordBatch()
		kc, vc := rec.Column(0).(*array.String), rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			out[strings.Clone(kc.Value(i))] = strings.Clone(vc.Value(i))
		}
	}
	if err := res.Reader.Err(); err != nil {
		return map[string]string{}, fmt.Errorf("read %s: %w", KafkaSettingsTableName, err)
	}
	return out, nil
}
