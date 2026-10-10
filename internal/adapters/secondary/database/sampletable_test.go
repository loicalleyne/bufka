package database

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/bufka/internal/config"
)

func TestWriteSampleTable(t *testing.T) {
	db, err := InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "sample.duckdb")})
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()

	id := int32(7)
	rows := []SampleRow{
		{
			Topic: "a.b", Partition: 1, Offset: 10, Timestamp: time.Unix(1700000000, 0),
			Key: []byte("k"), Value: []byte{0, 0, 0, 0, 7, 0, 0x0a, 0x01, 'x'},
			Headers:        []SampleHeader{{Key: "trace", Value: []byte("1")}, {Key: "trace", Value: nil}},
			DetectedFormat: "protobuf", MessageName: "t.Screen", CutConfluencePrefix: true,
			ConfluentHeaderLen: 6, ConfluentSchemaID: &id, ConfluentMessageIndexes: []int32{0},
			DecodeOK: true, DecodedJSON: `{"name":"x"}`,
		},
		{Topic: "a.b", Partition: 0, Offset: 3, Timestamp: time.Unix(1700000001, 0), DetectedFormat: "protobuf"},
	}
	ctx := context.Background()
	for range 2 {
		if err := WriteSampleTable(ctx, db, "kafka_a_b_sample", rows); err != nil {
			t.Fatalf("WriteSampleTable: %v", err)
		}
	}

	conn, err := db.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	res, err := conn.Query(ctx, `SELECT count(*)::BIGINT, max(len(headers))::BIGINT, max(confluent_message_indexes[1])::BIGINT,
		any_value(typeof(value)), any_value(typeof(headers)) FROM kafka_a_b_sample`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer res.Close()
	if !res.Reader.Next() {
		t.Fatal("no result")
	}
	rec := res.Reader.RecordBatch()
	count := rec.Column(0).(*array.Int64).Value(0)
	maxHeaders := rec.Column(1).(*array.Int64).Value(0)
	valueType := rec.Column(3).(*array.String).Value(0)
	headersType := rec.Column(4).(*array.String).Value(0)
	if count != 2 {
		t.Errorf("rows = %d, want 2 (table must be replaced, not appended)", count)
	}
	if maxHeaders != 2 {
		t.Errorf("max headers = %d, want 2", maxHeaders)
	}
	if valueType != "BLOB" {
		t.Errorf("value type = %s, want BLOB", valueType)
	}
	if headersType != `STRUCT("key" VARCHAR, "value" BLOB)[]` {
		t.Errorf("headers type = %s", headersType)
	}
}

func TestReadSampleTable(t *testing.T) {
	db, err := InitDB(&config.Config{DuckDBPath: filepath.Join(t.TempDir(), "sample.duckdb")})
	if err != nil {
		t.Skipf("duckdb unavailable: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	rows, ok, err := ReadSampleTable(ctx, db, "kafka_missing_sample")
	if err != nil {
		t.Fatalf("ReadSampleTable on missing table: %v", err)
	}
	if ok || rows != nil {
		t.Fatalf("ReadSampleTable on missing table = (%v, %v), want (nil, false)", rows, ok)
	}

	id := int32(7)
	want := []SampleRow{
		{
			Topic: "a.b", Partition: 1, Offset: 10, Timestamp: time.Unix(1700000000, 0),
			Key: []byte("k"), Value: []byte{0, 0, 0, 0, 7, 0, 0x0a, 0x01, 'x'},
			Headers:        []SampleHeader{{Key: "trace", Value: []byte("1")}, {Key: "trace", Value: nil}},
			DetectedFormat: "protobuf", MessageName: "t.Screen", CutConfluencePrefix: true,
			ConfluentHeaderLen: 6, ConfluentSchemaID: &id, ConfluentMessageIndexes: []int32{0},
			DecodeOK: true, DecodedJSON: `{"name":"x"}`,
			Reviewed: true, ReviewPick: "t.Screen", ReviewConfidence: 0.75, ReviewAgrees: true,
		},
		{Topic: "a.b", Partition: 0, Offset: 3, Timestamp: time.Unix(1700000001, 0), DetectedFormat: "protobuf"},
	}
	if err := WriteSampleTable(ctx, db, "kafka_a_b_sample", want); err != nil {
		t.Fatalf("WriteSampleTable: %v", err)
	}

	got, ok, err := ReadSampleTable(ctx, db, "kafka_a_b_sample")
	if err != nil {
		t.Fatalf("ReadSampleTable: %v", err)
	}
	if !ok {
		t.Fatal("ReadSampleTable ok = false, want true")
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	// Rows come back newest-first by timestamp; want[1] (offset 3) has the
	// later timestamp.
	first, second := got[0], got[1]
	if first.Topic != "a.b" || first.Partition != 0 || first.Offset != 3 {
		t.Errorf("first row = %+v, want the offset-3 row (newest timestamp first)", first)
	}
	if second.Partition != 1 || second.Offset != 10 || second.MessageName != "t.Screen" {
		t.Errorf("second row = %+v, want the offset-10 row", second)
	}
	if second.ConfluentSchemaID == nil || *second.ConfluentSchemaID != id {
		t.Errorf("second row ConfluentSchemaID = %v, want %d", second.ConfluentSchemaID, id)
	}
	if len(second.Headers) != 2 || second.Headers[0].Key != "trace" || string(second.Headers[0].Value) != "1" {
		t.Errorf("second row Headers = %+v", second.Headers)
	}
	if !second.ReviewAgrees || second.ReviewPick != "t.Screen" {
		t.Errorf("second row review fields = %+v", second)
	}
}
