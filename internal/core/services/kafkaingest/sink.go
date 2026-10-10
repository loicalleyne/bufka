package kafkaingest

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow-go/v18/arrow"
)

// BatchWriter persists an Arrow record batch into a named table.
type BatchWriter interface {
	WriteBatch(ctx context.Context, table string, rec arrow.RecordBatch) error
}

// RunSink writes every batch from in to its topic's table until in is closed.
// It accepts an optional onErr callback for custom error handling.
// It keeps draining after ctx is canceled so the Ingestor's shutdown flush is not blocked,
// and it releases every record. Write failures are logged only if onErr is nil.
func RunSink(ctx context.Context, in <-chan TopicBatch, w BatchWriter, tables map[string]string, onErr ...func(topic string, err error)) {
	var errCb func(topic string, err error)
	if len(onErr) > 0 && onErr[0] != nil {
		errCb = onErr[0]
	}
	writeCtx := context.WithoutCancel(ctx)
	for batch := range in {
		writeTopicBatch(writeCtx, batch, w, tables, errCb)
	}
}

func writeTopicBatch(ctx context.Context, batch TopicBatch, w BatchWriter, tables map[string]string, onErr func(string, error)) {
	defer batch.Record.Release()
	table, ok := tables[batch.Topic]
	if !ok {
		if onErr != nil {
			onErr(batch.Topic, fmt.Errorf("no table mapping for topic %q", batch.Topic))
		} else {
			log.Printf("dropping Kafka batch for topic %q with no table mapping", batch.Topic)
		}
		return
	}
	if err := w.WriteBatch(ctx, table, batch.Record); err != nil {
		if onErr != nil {
			onErr(batch.Topic, err)
		} else {
			log.Printf("write Kafka batch for topic %q to table %q: %v", batch.Topic, table, err)
		}
		return
	}
}
