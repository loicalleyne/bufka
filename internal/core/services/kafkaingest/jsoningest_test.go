package kafkaingest_test

import (
	"context"
	"testing"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
)

func TestJSONIngestorEmitsBatches(t *testing.T) {
	topic := "json-topic"
	out := make(chan kafkaingest.TopicBatch, 5)
	ingestor := kafkaingest.NewJSONIngestor(topic, 2, 100*time.Millisecond, out)

	in := make(chan kafka.Message, 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		ingestor.Run(ctx, in)
		close(done)
	}()

	now := time.Now().Truncate(time.Microsecond)
	in <- kafka.Message{
		Topic:     topic,
		Partition: 0,
		Key:       []byte("k1"),
		Value:     []byte(`{"a": 1}`),
		Timestamp: now,
	}
	in <- kafka.Message{
		Topic:     topic,
		Partition: 0,
		Key:       []byte("k2"),
		Value:     []byte(`{"a": 2}`),
		Timestamp: now,
	}

	select {
	case batch := <-out:
		if batch.Topic != topic {
			t.Fatalf("expected topic %s, got %s", topic, batch.Topic)
		}
		if batch.Record.NumRows() != 2 {
			t.Fatalf("expected 2 rows, got %d", batch.Record.NumRows())
		}
		batch.Record.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for batch")
	}

	close(in)
	<-done
}
