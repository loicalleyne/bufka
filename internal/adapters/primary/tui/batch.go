package tui

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/loicalleyne/bufka/internal/adapters/secondary/database"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
	"github.com/loicalleyne/bufka/internal/core/services/kafkaingest"
	"github.com/loicalleyne/bufka/internal/core/services/topicsample"
	"github.com/loicalleyne/bufka/internal/core/services/topicsize"
	"github.com/loicalleyne/bufka/pkg/autoscale"
)

// batchKind distinguishes the operation running in a bulk job.
type batchKind int

const (
	batchSample batchKind = iota
	batchEstimate
	batchBoth
)

func (k batchKind) String() string {
	switch k {
	case batchSample:
		return "Sampling"
	case batchEstimate:
		return "Estimating"
	case batchBoth:
		return "Sampling & Estimating"
	}
	return ""
}

// batchProgressMsg is sent as each topic is processed during a bulk operation.
type batchProgressMsg struct {
	kind         batchKind
	done         int
	total        int
	currentTopic string
}

// batchFinishedMsg is sent when a bulk operation finishes (or is canceled).
type batchFinishedMsg struct {
	kind      batchKind
	processed int
	total     int
	err       error
}

// runBatchProcess executes sample, estimate, or both for the provided list of
// topics concurrently (autoscaled from 8 to 64 by throughput, see
// pkg/autoscale), sending progress updates
// via sub along the way. Kafka reads and detection run in parallel; DuckDB
// writes are serialized.
func runBatchProcess(ctx context.Context, deps Deps, topics []string, kind batchKind, sub chan<- interface{}) {
	total := len(topics)

	var (
		mu           sync.Mutex // guards processed, estimateRows and DuckDB writes
		processed    int
		estimateRows []database.EstimateRow
	)

	pool := autoscale.NewPool()
	for _, topic := range topics {
		if pool.Go(ctx, func() {
			if ctx.Err() != nil {
				return
			}

			if kind == batchSample || kind == batchBoth {
				_ = runSampleForTopic(ctx, deps, &mu, topic)
			}

			var row database.EstimateRow
			var ok bool
			if kind == batchEstimate || kind == batchBoth {
				row, ok = runEstimateForTopic(ctx, deps, topic)
			}

			mu.Lock()
			if ok {
				estimateRows = append(estimateRows, row)
			}
			processed++
			done := processed
			mu.Unlock()

			select {
			case sub <- batchProgressMsg{kind: kind, done: done - 1, total: total, currentTopic: topic}:
			case <-ctx.Done():
			}
		}) != nil {
			break
		}
	}
	pool.Wait()

	if len(estimateRows) > 0 && deps.DB != nil {
		_ = database.UpsertEstimateRows(context.Background(), deps.DB, estimateRows)
	}

	sub <- batchFinishedMsg{
		kind:      kind,
		processed: processed,
		total:     total,
		err:       ctx.Err(),
	}
}

// runSampleForTopic samples and detects one topic, then persists it. The
// Kafka read and detection run unlocked; dbMu serializes the DuckDB writes.
func runSampleForTopic(ctx context.Context, deps Deps, dbMu *sync.Mutex, topic string) error {
	count := deps.SampleCount
	if count <= 0 {
		count = 50
	}
	timeout := deps.SampleTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	table, err := kafkaingest.SampleTableName(topic)
	if err != nil {
		return err
	}

	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
	cancel()
	if err != nil && !errors.Is(err, kafka.ErrTopicEmpty) {
		return err
	}

	values := make([][]byte, len(records))
	for i, r := range records {
		values[i] = r.Value
	}
	res := topicsample.Detect(topic, values, deps.Messages, deps.AsyncAPI, deps.Advisor)

	rows := make([]database.SampleRow, len(records))
	for i, r := range records {
		d := res.Rows[i]
		rows[i] = database.SampleRow{
			Topic:               r.Topic,
			Partition:           r.Partition,
			Offset:              r.Offset,
			Timestamp:           r.Timestamp,
			Key:                 r.Key,
			Value:               r.Value,
			DetectedFormat:      res.Format,
			MessageName:         res.MessageName,
			CutConfluencePrefix: res.CutConfluencePrefix,
			DecodeOK:            d.DecodeOK,
			DecodedJSON:         d.Decoded,
		}
	}

	if deps.DB != nil {
		dbMu.Lock()
		defer dbMu.Unlock()
	}
	if deps.DB != nil && len(rows) > 0 {
		_ = database.WriteSampleTable(ctx, deps.DB, table, rows)
	}

	if deps.DB != nil {
		_ = database.SaveDetection(ctx, deps.DB, topic, table, res, deps.AsyncAPI, deps.Messages)
	}
	return nil
}

func runEstimateForTopic(ctx context.Context, deps Deps, topic string) (database.EstimateRow, bool) {
	counts, err := kafka.TopicMessageCount(ctx, deps.KafkaConf, topic)
	if err != nil {
		return database.EstimateRow{}, false
	}
	count := deps.SampleCount
	if count <= 0 {
		count = 50
	}
	timeout := deps.SampleTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	records, err := kafka.SampleTopic(sampleCtx, deps.KafkaConf, topic, count)
	cancel()
	if err != nil && !errors.Is(err, kafka.ErrTopicEmpty) {
		return database.EstimateRow{}, false
	}

	sizes := make([]int, len(records))
	for i, r := range records {
		size := len(r.Key) + len(r.Value)
		for _, h := range r.Headers {
			size += len(h.Key) + len(h.Value)
		}
		sizes[i] = size
	}
	res := topicsize.Estimate(counts.Total, topicsize.Stats(sizes))
	return database.EstimateRow{
		Topic:           topic,
		EstimatedAt:     time.Now().UTC(),
		Partitions:      int32(counts.Partitions),
		TotalMessages:   res.TotalMessages,
		SampleCount:     int32(res.SampleCount),
		AvgMessageBytes: res.AvgMessageBytes,
		StdDevBytes:     res.StdDevBytes,
		MinMessageBytes: int32(res.MinMessageBytes),
		MaxMessageBytes: int32(res.MaxMessageBytes),
		EstimatedBytes:  res.EstimatedBytes,
		LowBytes:        res.LowBytes,
		HighBytes:       res.HighBytes,
	}, true
}
