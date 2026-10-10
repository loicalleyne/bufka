package kafkaingest_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"google.golang.org/protobuf/encoding/protowire"
)

// writeProto writes test.proto to a temp dir and returns the file name and import dir.
func writeProto(t *testing.T, body string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "test.proto"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return "test.proto", dir
}

func encodeEvent(id int64, name string) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(id))
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, name)
	return b
}

func TestIngestorAddsKafkaMetadataColumns(t *testing.T) {
	protoFile, importDir := writeProto(t, `syntax = "proto3";
package test;
message Event {
  int64 id = 1;
  string name = 2;
}
`)
	topic := "proto-topic"
	out := make(chan kafkaingest.TopicBatch, 5)
	ingestor, err := kafkaingest.NewIngestor([]config.KafkaTopicConfig{{
		Topic: topic, ProtoFile: protoFile, MessageName: "test.Event",
		ImportPaths: []string{importDir},
	}}, 2, time.Hour, out)
	if err != nil {
		t.Fatal(err)
	}

	in := make(chan kafka.Message, 5)
	done := make(chan struct{})
	go func() {
		ingestor.Run(context.Background(), in)
		close(done)
	}()

	now := time.Now().Truncate(time.Microsecond)
	in <- kafka.Message{Topic: topic, Partition: 3, Offset: 10, Key: []byte("k1"), Value: encodeEvent(1, "a"), Timestamp: now}
	// Invalid protobuf: must be dropped without misaligning metadata rows.
	in <- kafka.Message{Topic: topic, Partition: 3, Offset: 11, Value: []byte{0xff, 0xff, 0xff}, Timestamp: now}
	in <- kafka.Message{Topic: topic, Partition: 4, Offset: 12, Value: encodeEvent(2, "b"), Timestamp: now}
	close(in)
	<-done

	batch := <-out
	rec := batch.Record
	defer rec.Release()

	if rec.NumRows() != 2 {
		t.Fatalf("rows = %d, want 2", rec.NumRows())
	}
	wantCols := []string{"_kafka_partition", "_kafka_offset", "_kafka_timestamp", "_kafka_key", "id", "name"}
	if int(rec.NumCols()) != len(wantCols) {
		t.Fatalf("schema = %s", rec.Schema())
	}
	for i, n := range wantCols {
		if got := rec.Schema().Field(i).Name; got != n {
			t.Fatalf("col %d = %q, want %q", i, got, n)
		}
	}

	part := rec.Column(0).(*array.Int32)
	off := rec.Column(1).(*array.Int64)
	key := rec.Column(3).(*array.Binary)
	ids := rec.Column(4).(*array.Int64)
	if part.Value(0) != 3 || off.Value(0) != 10 || ids.Value(0) != 1 || string(key.Value(0)) != "k1" {
		t.Errorf("row 0 mismatch: part=%d off=%d id=%d key=%q", part.Value(0), off.Value(0), ids.Value(0), key.Value(0))
	}
	if part.Value(1) != 4 || off.Value(1) != 12 || ids.Value(1) != 2 || !key.IsNull(1) {
		t.Errorf("row 1 mismatch: part=%d off=%d id=%d keyNull=%v", part.Value(1), off.Value(1), ids.Value(1), key.IsNull(1))
	}
	ts := rec.Column(2).(*array.Timestamp)
	if got := ts.Value(0).ToTime(arrow.Microsecond); !got.Equal(now) {
		t.Errorf("timestamp = %v, want %v", got, now)
	}
}

func TestIngestorRejectsMetadataNameCollision(t *testing.T) {
	protoFile, importDir := writeProto(t, `syntax = "proto3";
package test;
message Event {
  int64 _KAFKA_offset = 1;
}
`)
	_, err := kafkaingest.NewIngestor([]config.KafkaTopicConfig{{
		Topic: "t", ProtoFile: protoFile, MessageName: "test.Event",
		ImportPaths: []string{importDir},
	}}, 2, time.Hour, make(chan kafkaingest.TopicBatch, 1))
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("expected collision error, got %v", err)
	}
}
