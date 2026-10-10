package kafka

import (
	"context"
	"strings"
	"testing"
)

// TestDescribeConsumerGroupUnrestrictedByPrefix verifies that
// DescribeConsumerGroup is never rejected on name alone, with or without a
// configured ConsumerGroupPrefix: browsing is read-only and not scoped to
// the prefix used for actually consuming. The seeds here are unroutable, so
// any error must come from the failed broker connection, not a prefix check.
func TestDescribeConsumerGroupUnrestrictedByPrefix(t *testing.T) {
	for _, prefix := range []string{"team-a.", ""} {
		conf := NewKafkaConfig()
		conf.Seeds = []string{"127.0.0.1:0"}
		conf.ConsumerGroupPrefix = prefix

		_, err := DescribeConsumerGroup(context.Background(), conf, "other-group")
		if err != nil && strings.Contains(err.Error(), "outside the configured prefix") {
			t.Errorf("prefix %q: unexpected prefix rejection: %v", prefix, err)
		}
	}
}

func TestFetchTopicValidatesCount(t *testing.T) {
	conf := NewKafkaConfig()
	conf.Seeds = []string{"127.0.0.1:0"}
	if _, err := FetchTopic(context.Background(), conf, "t", 0); err == nil {
		t.Fatal("expected error for non-positive per-partition count")
	}
}
