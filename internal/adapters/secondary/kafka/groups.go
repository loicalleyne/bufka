package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerGroup is a summary of one consumer group for the group picker.
type ConsumerGroup struct {
	Name     string
	State    string
	Protocol string
	Members  int
}

// GroupPartitionLag is a group's committed offset, the partition end offset
// and the lag (end - committed) for one topic-partition.
type GroupPartitionLag struct {
	Topic     string
	Partition int32
	Committed int64
	End       int64
	Lag       int64
	ClientID  string
	MemberID  string
}

// ConsumerGroupDescription is the detailed view of a consumer group: its
// state and per-topic-partition committed offsets, end offsets and lag.
type ConsumerGroupDescription struct {
	Name       string
	State      string
	Protocol   string
	Members    int
	Partitions []GroupPartitionLag
}

// ListConsumerGroups returns every consumer group on the cluster. Unlike
// consuming, which is restricted to conf.ConsumerGroupPrefix so it only ever
// joins/creates groups the cluster's ACLs permit, browsing is read-only and
// is not restricted by that prefix.
func ListConsumerGroups(ctx context.Context, conf *KafkaClientConf) ([]ConsumerGroup, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return nil, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return nil, fmt.Errorf("kafka seed brokers are required")
	}

	client, err := kgo.NewClient(adminOpts(conf)...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	listed, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}

	names := make([]string, 0, len(listed))
	for _, g := range listed.Sorted() {
		names = append(names, g.Group)
	}

	groups := make([]ConsumerGroup, 0, len(names))
	byName := make(map[string]*ConsumerGroup, len(names))
	for i := range names {
		g := listed[names[i]]
		groups = append(groups, ConsumerGroup{Name: g.Group, State: g.State, Protocol: g.ProtocolType})
	}
	for i := range groups {
		byName[groups[i].Name] = &groups[i]
	}

	if len(names) > 0 {
		if described, err := admin.DescribeGroups(ctx, names...); err == nil {
			for name, d := range described {
				if g, ok := byName[name]; ok {
					g.Members = len(d.Members)
					if d.Protocol != "" {
						g.Protocol = d.Protocol
					}
					if d.State != "" {
						g.State = d.State
					}
				}
			}
		}
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	return groups, nil
}

// DescribeConsumerGroup returns the per-topic-partition committed offsets, end
// offsets and lag for group, plus its member assignments. Like
// ListConsumerGroups, this is read-only and not restricted to
// conf.ConsumerGroupPrefix, so any group on the cluster can be inspected.
func DescribeConsumerGroup(ctx context.Context, conf *KafkaClientConf, group string) (ConsumerGroupDescription, error) {
	conf = conf.Snapshot()
	if conf == nil {
		return ConsumerGroupDescription{}, fmt.Errorf("kafka config is nil")
	}
	if len(conf.Seeds) == 0 {
		return ConsumerGroupDescription{}, fmt.Errorf("kafka seed brokers are required")
	}

	client, err := kgo.NewClient(adminOpts(conf)...)
	if err != nil {
		return ConsumerGroupDescription{}, fmt.Errorf("create Kafka admin client: %w", err)
	}
	defer client.Close()
	admin := kadm.NewClient(client)

	desc := ConsumerGroupDescription{Name: group}

	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return ConsumerGroupDescription{}, fmt.Errorf("describe consumer group %q: %w", group, err)
	}
	d, ok := described[group]
	if !ok {
		return ConsumerGroupDescription{}, fmt.Errorf("consumer group %q not found", group)
	}
	if d.Err != nil {
		return ConsumerGroupDescription{}, fmt.Errorf("describe consumer group %q: %w", group, d.Err)
	}
	desc.State = d.State
	desc.Protocol = d.Protocol
	desc.Members = len(d.Members)

	// Map each topic-partition to the member that owns it.
	type owner struct{ clientID, memberID string }
	owners := make(map[string]map[int32]owner)
	for _, m := range d.Members {
		c, ok := m.Assigned.AsConsumer()
		if !ok {
			continue
		}
		for _, t := range c.Topics {
			if owners[t.Topic] == nil {
				owners[t.Topic] = make(map[int32]owner)
			}
			for _, p := range t.Partitions {
				owners[t.Topic][p] = owner{clientID: m.ClientID, memberID: m.MemberID}
			}
		}
	}

	committed, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return ConsumerGroupDescription{}, fmt.Errorf("fetch committed offsets for %q: %w", group, err)
	}

	// Collect the topics committed to, to look up end offsets.
	topicSet := make(map[string]bool)
	committed.Each(func(o kadm.OffsetResponse) { topicSet[o.Topic] = true })
	for t := range owners {
		topicSet[t] = true
	}
	topics := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topics = append(topics, t)
	}

	ends := kadm.ListedOffsets{}
	if len(topics) > 0 {
		ends, err = admin.ListEndOffsets(ctx, topics...)
		if err != nil {
			return ConsumerGroupDescription{}, fmt.Errorf("list end offsets for group %q: %w", group, err)
		}
	}

	committed.Each(func(o kadm.OffsetResponse) {
		end := int64(-1)
		if lo, ok := ends.Lookup(o.Topic, o.Partition); ok {
			end = lo.Offset
		}
		lag := int64(-1)
		if end >= 0 && o.At >= 0 {
			lag = end - o.At
			if lag < 0 {
				lag = 0
			}
		}
		gp := GroupPartitionLag{
			Topic:     o.Topic,
			Partition: o.Partition,
			Committed: o.At,
			End:       end,
			Lag:       lag,
		}
		if ow, ok := owners[o.Topic][o.Partition]; ok {
			gp.ClientID = ow.clientID
			gp.MemberID = ow.memberID
		}
		desc.Partitions = append(desc.Partitions, gp)
	})

	sort.Slice(desc.Partitions, func(i, j int) bool {
		if desc.Partitions[i].Topic != desc.Partitions[j].Topic {
			return desc.Partitions[i].Topic < desc.Partitions[j].Topic
		}
		return desc.Partitions[i].Partition < desc.Partitions[j].Partition
	})
	return desc, nil
}
