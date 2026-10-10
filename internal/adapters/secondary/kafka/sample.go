package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// sampleIdleTimeout ends sampling when no record arrives for this long after
// the first one, e.g. when a partition tail is a transaction control marker.
const sampleIdleTimeout = 5 * time.Second

// ErrTopicEmpty is returned by SampleTopic when no partition holds any record.
var ErrTopicEmpty = errors.New("topic has no records (start offset == end offset on every partition)")

// Header is a Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// SampledRecord is a raw, unmunged Kafka record read by SampleTopic.
type SampledRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []Header
}

// SampleTopic reads up to count records from the tail of every partition of
// topic and returns the count newest, ordered by timestamp descending. It
// does not join a consumer group or commit offsets. Sampling stops when every
// partition's tail has been read, when no record arrives for a few seconds,
// or when ctx is done; records read so far are returned in the latter cases.
func SampleTopic(ctx context.Context, conf *KafkaClientConf, topic string, count int) ([]SampledRecord, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return nil, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return nil, fmt.Errorf("kafka seed brokers are required")
	}
	if count <= 0 {
		return nil, fmt.Errorf("sample count must be greater than zero")
	}

	starts, ends, err := partitionBounds(ctx, conf, topic)
	if err != nil {
		return nil, err
	}

	partitions := make(map[int32]kgo.Offset)
	remaining := make(map[int32]int64)
	for partition, end := range ends {
		from := max(starts[partition], end-int64(count))
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
		return nil, fmt.Errorf("create Kafka sample client: %w", err)
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
		return records[i].Timestamp.After(records[j].Timestamp)
	})
	if len(records) > count {
		records = records[:count]
	}
	return records, nil
}

// TopicCounts is the message population of a topic, derived from partition
// offsets without reading any record.
type TopicCounts struct {
	// Total is the sum of (endOffset - startOffset) over every partition. It
	// is an upper bound on retained messages: log compaction and transaction
	// control markers mean some offsets may not correspond to a live record.
	Total int64
	// Partitions is the number of partitions the topic has.
	Partitions int
}

// TopicMessageCount returns the total number of messages in topic, computed
// from the start and end offsets of each partition. It joins no consumer
// group and reads no records.
func TopicMessageCount(ctx context.Context, conf *KafkaClientConf, topic string) (TopicCounts, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return TopicCounts{}, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return TopicCounts{}, fmt.Errorf("kafka seed brokers are required")
	}
	starts, ends, err := partitionBounds(ctx, conf, topic)
	if err != nil {
		return TopicCounts{}, err
	}
	var total int64
	for partition, end := range ends {
		if n := end - starts[partition]; n > 0 {
			total += n
		}
	}
	return TopicCounts{Total: total, Partitions: len(ends)}, nil
}

// PartitionBounds returns the start and end offsets of every partition of topic.
func PartitionBounds(ctx context.Context, conf *KafkaClientConf, topic string) (map[int32]int64, map[int32]int64, error) {
	conf = conf.Snapshot()
	return partitionBounds(ctx, conf, topic)
}

// partitionBounds returns the start and end offsets of every partition of topic.
func partitionBounds(ctx context.Context, conf *KafkaClientConf, topic string) (map[int32]int64, map[int32]int64, error) {
	client, err := kgo.NewClient(adminOpts(conf)...)
	if err != nil {
		return nil, nil, fmt.Errorf("create Kafka admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	startOffsets, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}
	if err := startOffsets.Error(); err != nil {
		return nil, nil, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}
	if err := endOffsets.Error(); err != nil {
		return nil, nil, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	starts := make(map[int32]int64)
	startOffsets.Each(func(o kadm.ListedOffset) { starts[o.Partition] = o.Offset })
	ends := make(map[int32]int64)
	endOffsets.Each(func(o kadm.ListedOffset) { ends[o.Partition] = o.Offset })
	if len(ends) == 0 {
		return nil, nil, fmt.Errorf("kafka topic %q not found or has no partitions", topic)
	}
	return starts, ends, nil
}

func toSampledRecord(r *kgo.Record) SampledRecord {
	headers := make([]Header, 0, len(r.Headers))
	for _, h := range r.Headers {
		headers = append(headers, Header{Key: h.Key, Value: h.Value})
	}
	return SampledRecord{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
		Key:       r.Key,
		Value:     r.Value,
		Headers:   headers,
	}
}
