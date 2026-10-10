// Package topicsize estimates the total on-the-wire size of a Kafka topic
// from its message count and the sizes of a sample of its messages. The
// estimate is simply totalMessages * averageSampleSize; the sample's spread
// (standard deviation and min/max) is reported so the estimate's reliability
// can be judged.
//
// It is deliberately free of Kafka and database dependencies: callers pass in
// the per-message byte sizes they measured and the authoritative message
// count, and receive a pure Result.
package topicsize

import "math"

// SampleStats summarises the serialized sizes of a set of sampled messages.
// A size is whatever the caller measured per message, e.g. key + value +
// headers in bytes.
type SampleStats struct {
	Count   int
	SumSize int64
	MinSize int
	MaxSize int
	// AvgSize is the mean message size in bytes, 0 when Count is 0.
	AvgSize float64
	// StdDevSize is the population standard deviation of the sample sizes.
	StdDevSize float64
}

// Result is the size estimate for one topic.
type Result struct {
	// TotalMessages is the authoritative message count (partition offsets).
	TotalMessages int64
	// SampleCount is how many messages the estimate is based on.
	SampleCount int
	// AvgMessageBytes is the mean sampled message size in bytes.
	AvgMessageBytes float64
	// StdDevBytes is the standard deviation of the sampled message sizes; a
	// large value relative to AvgMessageBytes means the estimate is rough.
	StdDevBytes float64
	// MinMessageBytes and MaxMessageBytes bound the sampled message sizes.
	MinMessageBytes int
	MaxMessageBytes int
	// EstimatedBytes is TotalMessages * AvgMessageBytes, rounded.
	EstimatedBytes int64
	// LowBytes and HighBytes bound the estimate using the sampled min and max
	// message sizes (TotalMessages * Min and TotalMessages * Max).
	LowBytes  int64
	HighBytes int64
}

// Stats summarises sizes, the measured serialized byte size of each sampled
// message. An empty slice yields a zero-value SampleStats.
func Stats(sizes []int) SampleStats {
	s := SampleStats{Count: len(sizes)}
	if len(sizes) == 0 {
		return s
	}
	s.MinSize = sizes[0]
	s.MaxSize = sizes[0]
	for _, sz := range sizes {
		s.SumSize += int64(sz)
		if sz < s.MinSize {
			s.MinSize = sz
		}
		if sz > s.MaxSize {
			s.MaxSize = sz
		}
	}
	s.AvgSize = float64(s.SumSize) / float64(len(sizes))
	var variance float64
	for _, sz := range sizes {
		d := float64(sz) - s.AvgSize
		variance += d * d
	}
	variance /= float64(len(sizes))
	s.StdDevSize = math.Sqrt(variance)
	return s
}

// Estimate combines the authoritative message count with sample statistics.
// When totalMessages or the sample is empty, EstimatedBytes is 0.
func Estimate(totalMessages int64, stats SampleStats) Result {
	res := Result{
		TotalMessages:   totalMessages,
		SampleCount:     stats.Count,
		AvgMessageBytes: stats.AvgSize,
		StdDevBytes:     stats.StdDevSize,
		MinMessageBytes: stats.MinSize,
		MaxMessageBytes: stats.MaxSize,
	}
	if totalMessages <= 0 || stats.Count == 0 {
		return res
	}
	total := float64(totalMessages)
	res.EstimatedBytes = int64(math.Round(total * stats.AvgSize))
	res.LowBytes = int64(math.Round(total * float64(stats.MinSize)))
	res.HighBytes = int64(math.Round(total * float64(stats.MaxSize)))
	return res
}
