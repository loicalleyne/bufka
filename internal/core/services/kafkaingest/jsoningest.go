package kafkaingest

import (
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
)

// RawJSONSchema is the Arrow schema for storing raw JSON-formatted Kafka records.
var RawJSONSchema = arrow.NewSchema([]arrow.Field{
	{Name: "_kafka_partition", Type: arrow.PrimitiveTypes.Int32},
	{Name: "_kafka_offset", Type: arrow.PrimitiveTypes.Int64},
	{Name: "_kafka_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
	{Name: "_kafka_key", Type: arrow.BinaryTypes.Binary, Nullable: true},
	{Name: "value", Type: arrow.BinaryTypes.String},
}, nil)

// JSONIngestor buffers raw JSON Kafka messages into Arrow RecordBatches.
type JSONIngestor struct {
	topic         string
	batchSize     int
	flushInterval time.Duration
	output        chan<- TopicBatch

	builder    *array.RecordBuilder
	count      int
	partB      *array.Int32Builder
	offsetB    *array.Int64Builder
	timeB      *array.TimestampBuilder
	keyB       *array.BinaryBuilder
	valB       *array.StringBuilder
	nextOffset int64
}

// NewJSONIngestor creates an ingestor for JSON topics.
func NewJSONIngestor(topic string, batchSize int, flushInterval time.Duration, output chan<- TopicBatch) *JSONIngestor {
	pool := memory.DefaultAllocator
	b := array.NewRecordBuilder(pool, RawJSONSchema)
	return &JSONIngestor{
		topic:         topic,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		output:        output,
		builder:       b,
		partB:         b.Field(0).(*array.Int32Builder),
		offsetB:       b.Field(1).(*array.Int64Builder),
		timeB:         b.Field(2).(*array.TimestampBuilder),
		keyB:          b.Field(3).(*array.BinaryBuilder),
		valB:          b.Field(4).(*array.StringBuilder),
	}
}

// Run decodes and buffers messages until ctx is canceled or input is closed.
func (j *JSONIngestor) Run(ctx context.Context, input <-chan kafka.Message) {
	ticker := time.NewTicker(j.flushInterval)
	defer ticker.Stop()
	defer j.release()

	for {
		select {
		case <-ctx.Done():
			j.flush()
			return

		case <-ticker.C:
			j.flush()

		case msg, ok := <-input:
			if !ok {
				j.flush()
				return
			}
			j.append(msg)
			if j.count >= j.batchSize {
				j.flush()
			}
		}
	}
}

func (j *JSONIngestor) append(msg kafka.Message) {
	j.partB.Append(msg.Partition)
	j.offsetB.Append(msg.Offset)

	ts, _ := arrow.TimestampFromTime(msg.Timestamp, arrow.Microsecond)
	j.timeB.Append(ts)

	if len(msg.Key) > 0 {
		j.keyB.Append(msg.Key)
	} else {
		j.keyB.AppendNull()
	}

	j.valB.Append(string(msg.Value))
	j.count++
}

func (j *JSONIngestor) flush() {
	if j.count == 0 {
		return
	}
	rec := j.builder.NewRecordBatch()
	j.count = 0
	j.output <- TopicBatch{
		Topic:  j.topic,
		Record: rec,
	}
}

func (j *JSONIngestor) release() {
	if j.builder != nil {
		j.builder.Release()
		j.builder = nil
	}
}
