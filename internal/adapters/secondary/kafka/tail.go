package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TailTopic streams new records from every partition of topic to out until
// ctx is canceled. It starts each partition at the end offset, so only
// records produced after the call are delivered. It joins no consumer group
// and commits no offsets, so it never interferes with real consumers. The
// out channel is never closed by TailTopic; the caller owns it.
func TailTopic(ctx context.Context, conf *KafkaClientConf, topic string, out chan<- SampledRecord) error {
	conf = conf.Snapshot()
	if conf == nil {
		return fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return fmt.Errorf("kafka seed brokers are required")
	}

	_, ends, err := partitionBounds(ctx, conf, topic)
	if err != nil {
		return err
	}

	partitions := make(map[int32]kgo.Offset, len(ends))
	for partition := range ends {
		partitions[partition] = kgo.NewOffset().AtEnd()
	}

	client, err := kgo.NewClient(sampleOpts(conf, map[string]map[int32]kgo.Offset{topic: partitions})...)
	if err != nil {
		return fmt.Errorf("create Kafka tail client: %w", err)
	}
	defer client.Close()

	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			if fetchErr.Err == nil || errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				continue
			}
			return fmt.Errorf("tail topic %q partition %d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}
		var stop bool
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			select {
			case out <- toSampledRecord(r):
			case <-ctx.Done():
				stop = true
			}
		})
	}
	return ctx.Err()
}

// FetchTopic performs a one-shot retrieval of the newest perPartitionCount
// records from EVERY partition of topic (unlike SampleTopic, which caps the
// total across all partitions). It joins no consumer group and commits no
// offsets. Fetching stops when every partition's tail has been read, when no
// record arrives for a few seconds, or when ctx is done; records read so far
// are returned in the latter cases. Records are returned sorted by partition,
// then newest-first within each partition.
func FetchTopic(ctx context.Context, conf *KafkaClientConf, topic string, perPartitionCount int) ([]SampledRecord, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return nil, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return nil, fmt.Errorf("kafka seed brokers are required")
	}
	if perPartitionCount <= 0 {
		return nil, fmt.Errorf("per-partition count must be greater than zero")
	}

	starts, ends, err := partitionBounds(ctx, conf, topic)
	if err != nil {
		return nil, err
	}

	partitions := make(map[int32]kgo.Offset)
	remaining := make(map[int32]int64)
	for partition, end := range ends {
		from := max(starts[partition], end-int64(perPartitionCount))
		if from >= end {
			continue
		}
		partitions[partition] = kgo.NewOffset().At(from)
		remaining[partition] = end - 1
	}
	if len(partitions) == 0 {
		return nil, ErrTopicEmpty
	}

	client, err := kgo.NewClient(sampleOpts(conf, map[string]map[int32]kgo.Offset{topic: partitions})...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka fetch client: %w", err)
	}
	defer client.Close()

	var records []SampledRecord
	for len(remaining) > 0 && ctx.Err() == nil {
		pollCtx, cancel := ctx, context.CancelFunc(func() {})
		if len(records) > 0 {
			pollCtx, cancel = context.WithTimeout(ctx, sampleIdleTimeout)
		}
		fetches := client.PollFetches(pollCtx)
		idle := pollCtx.Err() != nil && ctx.Err() == nil
		cancel()

		for _, fetchErr := range fetches.Errors() {
			if fetchErr.Err == nil || errors.Is(fetchErr.Err, context.Canceled) || errors.Is(fetchErr.Err, context.DeadlineExceeded) {
				continue
			}
			return nil, fmt.Errorf("fetch topic %q partition %d: %w", fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
		}
		fetched := 0
		fetches.EachRecord(func(r *kgo.Record) {
			fetched++
			records = append(records, toSampledRecord(r))
			if last, ok := remaining[r.Partition]; ok && r.Offset >= last {
				delete(remaining, r.Partition)
			}
		})
		if idle && fetched == 0 {
			break
		}
	}

	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Partition != records[j].Partition {
			return records[i].Partition < records[j].Partition
		}
		return records[i].Offset > records[j].Offset
	})
	return records, nil
}
