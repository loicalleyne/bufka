package kafkaingest

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/loicalleyne/bufka/internal/adapters/secondary/kafka"
)

// kafkaMetaFields are the Kafka record metadata columns prepended to every
// protobuf-decoded batch. They match the leading columns of RawJSONSchema.
var kafkaMetaFields = RawJSONSchema.Fields()[:4]

// metaBuilder accumulates Kafka record metadata alongside a transcoder so the
// two can be zipped into a single record batch on flush.
type metaBuilder struct {
	builder *array.RecordBuilder
	partB   *array.Int32Builder
	offsetB *array.Int64Builder
	timeB   *array.TimestampBuilder
	keyB    *array.BinaryBuilder
}

func newMetaBuilder(mem memory.Allocator) *metaBuilder {
	b := array.NewRecordBuilder(mem, arrow.NewSchema(kafkaMetaFields, nil))
	return &metaBuilder{
		builder: b,
		partB:   b.Field(0).(*array.Int32Builder),
		offsetB: b.Field(1).(*array.Int64Builder),
		timeB:   b.Field(2).(*array.TimestampBuilder),
		keyB:    b.Field(3).(*array.BinaryBuilder),
	}
}

func (m *metaBuilder) append(msg kafka.Message) {
	m.partB.Append(msg.Partition)
	m.offsetB.Append(msg.Offset)
	ts, _ := arrow.TimestampFromTime(msg.Timestamp, arrow.Microsecond)
	m.timeB.Append(ts)
	if len(msg.Key) > 0 {
		m.keyB.Append(msg.Key)
	} else {
		m.keyB.AppendNull()
	}
}

// combine returns a new record with the buffered metadata columns followed by
// the columns of data. It resets the metadata builder and releases data.
func (m *metaBuilder) combine(data arrow.RecordBatch) (arrow.RecordBatch, error) {
	meta := m.builder.NewRecordBatch()
	defer meta.Release()
	defer data.Release()
	if meta.NumRows() != data.NumRows() {
		return nil, fmt.Errorf("kafka metadata row count %d does not match decoded row count %d", meta.NumRows(), data.NumRows())
	}

	fields := make([]arrow.Field, 0, int(meta.NumCols()+data.NumCols()))
	fields = append(fields, meta.Schema().Fields()...)
	fields = append(fields, data.Schema().Fields()...)
	cols := make([]arrow.Array, 0, len(fields))
	cols = append(cols, meta.Columns()...)
	cols = append(cols, data.Columns()...)

	md := data.Schema().Metadata()
	return array.NewRecordBatch(arrow.NewSchema(fields, &md), cols, data.NumRows()), nil
}

func (m *metaBuilder) release() {
	if m.builder != nil {
		m.builder.Release()
		m.builder = nil
	}
}

// checkMetaCollisions reports an error when the decoded protobuf schema
// already has a top-level column named like one of the Kafka metadata columns.
// The comparison ignores case because DuckDB and BigQuery identifiers do.
func checkMetaCollisions(schema *arrow.Schema) error {
	for _, f := range schema.Fields() {
		for _, m := range kafkaMetaFields {
			if strings.EqualFold(f.Name, m.Name) {
				return fmt.Errorf("protobuf message has a field named %q, which collides with the Kafka metadata column %q", f.Name, m.Name)
			}
		}
	}
	return nil
}
