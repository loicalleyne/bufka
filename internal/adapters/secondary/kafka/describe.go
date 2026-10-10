package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionDetail describes one partition of a topic: its leader broker, its
// start and end offsets and the number of messages between them (lag).
type PartitionDetail struct {
	Partition int32
	Leader    int32
	Replicas  int
	Start     int64
	End       int64
	Messages  int64
}

// TopicConfig is one topic configuration entry.
type TopicConfig struct {
	Key   string
	Value string
}

// TopicDescription is the metadata of a topic: its partition and replication
// layout, per-partition offsets and its configuration.
type TopicDescription struct {
	Topic             string
	Partitions        int
	ReplicationFactor int
	PartitionDetails  []PartitionDetail
	Configs           []TopicConfig
}

// DescribeTopic returns metadata for topic: partition count, replication
// factor, per-partition leaders and start/end offsets (and the message count
// between them), plus the topic's configuration. It joins no consumer group
// and reads no records.
func DescribeTopic(ctx context.Context, conf *KafkaClientConf, topic string) (TopicDescription, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return TopicDescription{}, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return TopicDescription{}, fmt.Errorf("kafka seed brokers are required")
	}

	client, err := kgo.NewClient(adminOpts(conf)...)
	if err != nil {
		return TopicDescription{}, fmt.Errorf("create Kafka admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	details, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return TopicDescription{}, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	td, ok := details[topic]
	if !ok {
		return TopicDescription{}, fmt.Errorf("kafka topic %q not found", topic)
	}
	if td.Err != nil {
		return TopicDescription{}, fmt.Errorf("describe topic %q: %w", topic, td.Err)
	}

	startOffsets, err := admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return TopicDescription{}, fmt.Errorf("list start offsets for %q: %w", topic, err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return TopicDescription{}, fmt.Errorf("list end offsets for %q: %w", topic, err)
	}

	desc := TopicDescription{
		Topic:             topic,
		Partitions:        len(td.Partitions),
		ReplicationFactor: td.Partitions.NumReplicas(),
	}
	for _, p := range td.Partitions {
		pd := PartitionDetail{
			Partition: p.Partition,
			Leader:    p.Leader,
			Replicas:  len(p.Replicas),
		}
		if o, ok := startOffsets.Lookup(topic, p.Partition); ok {
			pd.Start = o.Offset
		}
		if o, ok := endOffsets.Lookup(topic, p.Partition); ok {
			pd.End = o.Offset
		}
		if n := pd.End - pd.Start; n > 0 {
			pd.Messages = n
		}
		desc.PartitionDetails = append(desc.PartitionDetails, pd)
	}
	sort.Slice(desc.PartitionDetails, func(i, j int) bool {
		return desc.PartitionDetails[i].Partition < desc.PartitionDetails[j].Partition
	})

	configs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err == nil {
		if rc, err := configs.On(topic, nil); err == nil {
			for _, c := range rc.Configs {
				desc.Configs = append(desc.Configs, TopicConfig{Key: c.Key, Value: c.MaybeValue()})
			}
			sort.Slice(desc.Configs, func(i, j int) bool {
				return desc.Configs[i].Key < desc.Configs[j].Key
			})
		}
	}

	return desc, nil
}
