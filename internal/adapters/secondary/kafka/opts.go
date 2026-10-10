package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func clientOpts(conf *KafkaClientConf, clientID string) []kgo.Opt {
	opts := make([]kgo.Opt, 0, 6)
	opts = append(opts, kgo.SeedBrokers(conf.Seeds...))
	if clientID != "" {
		opts = append(opts, kgo.ClientID(clientID))
	}
	opts = append(opts, kgo.ConsumeTopics(conf.Topics...))
	if conf.ConsumerGroup != "" {
		opts = append(opts, kgo.ConsumerGroup(conf.ConsumerGroup))
	}
	opts = append(opts, authOpts(conf)...)
	return opts
}

// adminOpts builds options for a client used only for admin requests
// (e.g. listing topics), without joining a consumer group or topic.
func adminOpts(conf *KafkaClientConf) []kgo.Opt {
	opts := make([]kgo.Opt, 0, 3)
	opts = append(opts, kgo.SeedBrokers(conf.Seeds...))
	if conf.InstancePrefix != "" {
		opts = append(opts, kgo.ClientID(conf.InstancePrefix))
	}
	opts = append(opts, authOpts(conf)...)
	return opts
}

// SampleOpts builds options for a group-less client that reads exact
// partition offsets, including SASL and TLS authentication.
func SampleOpts(conf *KafkaClientConf, partitions map[string]map[int32]kgo.Offset) []kgo.Opt {
	conf = conf.Snapshot()
	return sampleOpts(conf, partitions)
}

// AdminOpts builds options for a client used only for admin requests
// or partition-based consuming, including SASL and TLS authentication.
func AdminOpts(conf *KafkaClientConf) []kgo.Opt {
	conf = conf.Snapshot()
	return adminOpts(conf)
}

// sampleOpts builds options for a group-less client that reads exact
// partition offsets.
func sampleOpts(conf *KafkaClientConf, partitions map[string]map[int32]kgo.Offset) []kgo.Opt {
	return append(adminOpts(conf), kgo.ConsumePartitions(partitions))
}

func authOpts(conf *KafkaClientConf) []kgo.Opt {
	var opts []kgo.Opt
	if conf.TLSDialer != nil {
		opts = append(opts, kgo.Dialer(conf.TLSDialer.DialContext))
	}
	if conf.User != "" || conf.Password != "" {
		opts = append(opts, kgo.SASL(plain.Auth{User: conf.User, Pass: conf.Password}.AsMechanism()))
	}
	return opts
}
