package format

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/extensions"
)

// ColumnFor describes an Arrow field as a result column.
func ColumnFor(f arrow.Field) Column {
	return Column{Name: f.Name, Type: DuckDBType(f.Type), Right: isNumeric(f.Type)}
}

func isNumeric(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	}
	return false
}

// DuckDBType names an Arrow type as DuckDB does.
func DuckDBType(t arrow.DataType) string {
	switch dt := t.(type) {
	case *arrow.Int8Type:
		return "TINYINT"
	case *arrow.Int16Type:
		return "SMALLINT"
	case *arrow.Int32Type:
		return "INTEGER"
	case *arrow.Int64Type:
		return "BIGINT"
	case *arrow.Uint8Type:
		return "UTINYINT"
	case *arrow.Uint16Type:
		return "USMALLINT"
	case *arrow.Uint32Type:
		return "UINTEGER"
	case *arrow.Uint64Type:
		return "UBIGINT"
	case *arrow.Float16Type, *arrow.Float32Type:
		return "FLOAT"
	case *arrow.Float64Type:
		return "DOUBLE"
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return "VARCHAR"
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.BinaryViewType, *arrow.FixedSizeBinaryType:
		return "BLOB"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "DATE"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "TIME"
	case *arrow.TimestampType:
		if dt.TimeZone != "" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		switch dt.Unit {
		case arrow.Second:
			return "TIMESTAMP_S"
		case arrow.Millisecond:
			return "TIMESTAMP_MS"
		case arrow.Nanosecond:
			return "TIMESTAMP_NS"
		}
		return "TIMESTAMP"
	case *arrow.DurationType, *arrow.MonthDayNanoIntervalType, *arrow.DayTimeIntervalType, *arrow.MonthIntervalType:
		return "INTERVAL"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.ListType:
		return DuckDBType(dt.Elem()) + "[]"
	case *arrow.LargeListType:
		return DuckDBType(dt.Elem()) + "[]"
	case *arrow.ListViewType:
		return DuckDBType(dt.Elem()) + "[]"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("%s[%d]", DuckDBType(dt.Elem()), dt.Len())
	case *arrow.MapType:
		return fmt.Sprintf("MAP(%s, %s)", DuckDBType(dt.KeyType()), DuckDBType(dt.ItemType()))
	case *arrow.StructType:
		parts := make([]string, dt.NumFields())
		for i, f := range dt.Fields() {
			parts[i] = quoteIdentIfNeeded(f.Name) + " " + DuckDBType(f.Type)
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	case *arrow.DictionaryType:
		return DuckDBType(dt.ValueType)
	case *arrow.NullType:
		return "NULL"
	case *extensions.UUIDType:
		return "UUID"
	case *extensions.JSONType:
		return "JSON"
	case arrow.ExtensionType:
		return DuckDBType(dt.StorageType())
	}
	return strings.ToUpper(t.String())
}

func quoteIdentIfNeeded(s string) string {
	for i, r := range s {
		if !(r == '_' || (r >= 'a' && r <= 'z') || (i > 0 && r >= '0' && r <= '9')) {
			return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
		}
	}
	return s
}

// CellAt converts row i of arr into a Cell.
func CellAt(arr arrow.Array, i int) Cell {
	if arr.IsNull(i) {
		return Cell{Null: true}
	}
	v := value(arr, i)
	return Cell{Text: display(v, true), Value: jsonValue(v)}
}

// value extracts a Go value from arr[i] (arr[i] is not null). Nested types
// yield []any, orderedMap and structValue.
func value(arr arrow.Array, i int) any {
	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(i)
	case *array.Int8:
		return int64(a.Value(i))
	case *array.Int16:
		return int64(a.Value(i))
	case *array.Int32:
		return int64(a.Value(i))
	case *array.Int64:
		return a.Value(i)
	case *array.Uint8:
		return uint64(a.Value(i))
	case *array.Uint16:
		return uint64(a.Value(i))
	case *array.Uint32:
		return uint64(a.Value(i))
	case *array.Uint64:
		return a.Value(i)
	case *array.Float16:
		return float64(a.Value(i).Float32())
	case *array.Float32:
		return floatValue{float64(a.Value(i)), 32}
	case *array.Float64:
		return floatValue{a.Value(i), 64}
	case *array.String:
		return strings.Clone(a.Value(i))
	case *array.LargeString:
		return strings.Clone(a.Value(i))
	case *array.StringView:
		return strings.Clone(a.Value(i))
	case *array.Binary:
		return blob(bytes.Clone(a.Value(i)))
	case *array.LargeBinary:
		return blob(bytes.Clone(a.Value(i)))
	case *array.BinaryView:
		return blob(bytes.Clone(a.Value(i)))
	case *array.FixedSizeBinary:
		return blob(bytes.Clone(a.Value(i)))
	case *array.Date32:
		return dateValue(a.Value(i).ToTime())
	case *array.Date64:
		return dateValue(a.Value(i).ToTime())
	case *array.Time32:
		return timeOfDay(a.Value(i).ToTime(a.DataType().(*arrow.Time32Type).Unit))
	case *array.Time64:
		return timeOfDay(a.Value(i).ToTime(a.DataType().(*arrow.Time64Type).Unit))
	case *array.Timestamp:
		dt := a.DataType().(*arrow.TimestampType)
		return timestampValue{t: a.Value(i).ToTime(dt.Unit), unit: dt.Unit, tz: dt.TimeZone != ""}
	case *array.Duration:
		d := a.Value(i)
		return intervalValue{nanos: int64(d) * unitNanos(a.DataType().(*arrow.DurationType).Unit)}
	case *array.MonthDayNanoInterval:
		v := a.Value(i)
		return intervalValue{months: v.Months, days: v.Days, nanos: v.Nanoseconds}
	case *array.DayTimeInterval:
		v := a.Value(i)
		return intervalValue{days: v.Days, nanos: int64(v.Milliseconds) * int64(time.Millisecond)}
	case *array.MonthInterval:
		return intervalValue{months: int32(a.Value(i))}
	case *array.Decimal128:
		dt := a.DataType().(*arrow.Decimal128Type)
		return decimalValue(decimal128.Num(a.Value(i)).ToString(dt.Scale))
	case *array.Decimal256:
		dt := a.DataType().(*arrow.Decimal256Type)
		return decimalValue(decimal256.Num(a.Value(i)).ToString(dt.Scale))
	case *array.Map:
		start, end := a.ValueOffsets(i)
		keys, items := a.Keys(), a.Items()
		m := orderedMap{}
		for j := int(start); j < int(end); j++ {
			m.keys = append(m.keys, nested(keys, j))
			m.vals = append(m.vals, nested(items, j))
		}
		return m
	case array.ListLike:
		start, end := a.ValueOffsets(i)
		vals := a.ListValues()
		out := make([]any, 0, end-start)
		for j := int(start); j < int(end); j++ {
			out = append(out, nested(vals, j))
		}
		return out
	case *array.Struct:
		st := a.DataType().(*arrow.StructType)
		sv := structValue{}
		for f := range st.NumFields() {
			sv.names = append(sv.names, st.Field(f).Name)
			sv.vals = append(sv.vals, nested(a.Field(f), i))
		}
		return sv
	case *array.Dictionary:
		return value(a.Dictionary(), a.GetValueIndex(i))
	case array.ExtensionArray:
		if u, ok := a.(*extensions.UUIDArray); ok {
			return strings.Clone(u.ValueStr(i))
		}
		if j, ok := a.(*extensions.JSONArray); ok {
			return strings.Clone(j.ValueStr(i))
		}
		return value(a.Storage(), i)
	}
	return strings.Clone(arr.ValueStr(i))
}

func nested(arr arrow.Array, i int) any {
	if arr.IsNull(i) {
		return nil
	}
	return value(arr, i)
}

func unitNanos(u arrow.TimeUnit) int64 {
	switch u {
	case arrow.Second:
		return int64(time.Second)
	case arrow.Millisecond:
		return int64(time.Millisecond)
	case arrow.Microsecond:
		return int64(time.Microsecond)
	}
	return 1
}

type (
	blob         []byte
	dateValue    time.Time
	timeOfDay    time.Time
	decimalValue string
	floatValue   struct {
		f    float64
		bits int
	}
	timestampValue struct {
		t    time.Time
		unit arrow.TimeUnit
		tz   bool
	}
	intervalValue struct {
		months, days int32
		nanos        int64
	}
	orderedMap  struct{ keys, vals []any }
	structValue struct {
		names []string
		vals  []any
	}
)

func formatFloat(f floatValue) string {
	switch {
	case math.IsNaN(f.f):
		return "nan"
	case math.IsInf(f.f, 1):
		return "inf"
	case math.IsInf(f.f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f.f, 'g', -1, f.bits)
}

func formatTimestamp(v timestampValue) string {
	t := v.t
	if !v.tz {
		t = t.UTC()
	}
	layout := "2006-01-02 15:04:05"
	if v.unit != arrow.Second {
		frac := t.Nanosecond()
		switch {
		case frac == 0:
		case v.unit == arrow.Millisecond:
			layout += ".000"
		case v.unit == arrow.Nanosecond:
			layout += ".000000000"
		default:
			layout += ".000000"
		}
	}
	s := t.Format(layout)
	if strings.Contains(layout, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	if v.tz {
		_, off := t.Zone()
		sign := "+"
		if off < 0 {
			sign, off = "-", -off
		}
		s += fmt.Sprintf("%s%02d", sign, off/3600)
		if m := off % 3600 / 60; m != 0 {
			s += fmt.Sprintf(":%02d", m)
		}
	}
	return s
}

func formatInterval(v intervalValue) string {
	var parts []string
	if y, m := v.months/12, v.months%12; y != 0 || m != 0 {
		if y != 0 {
			parts = append(parts, plural(int64(y), "year"))
		}
		if m != 0 {
			parts = append(parts, plural(int64(m), "month"))
		}
	}
	if v.days != 0 {
		parts = append(parts, plural(int64(v.days), "day"))
	}
	if v.nanos != 0 || len(parts) == 0 {
		n := v.nanos
		sign := ""
		if n < 0 {
			sign, n = "-", -n
		}
		h := n / int64(time.Hour)
		n %= int64(time.Hour)
		mi := n / int64(time.Minute)
		n %= int64(time.Minute)
		s := n / int64(time.Second)
		us := n % int64(time.Second) / int64(time.Microsecond)
		str := fmt.Sprintf("%s%02d:%02d:%02d", sign, h, mi, s)
		if us != 0 {
			str += strings.TrimRight(fmt.Sprintf(".%06d", us), "0")
		}
		parts = append(parts, str)
	}
	return strings.Join(parts, " ")
}

func plural(n int64, unit string) string {
	if n == 1 || n == -1 {
		return fmt.Sprintf("%d %s", n, unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

func formatBlob(b []byte) string {
	var s strings.Builder
	for _, c := range b {
		if c >= 32 && c < 127 && c != '\\' {
			s.WriteByte(c)
		} else {
			fmt.Fprintf(&s, `\x%02X`, c)
		}
	}
	return s.String()
}

// needsQuote mirrors DuckDB's VARCHAR cast of nested values: a string
// inside a list/struct/map is quoted only when it would otherwise be
// ambiguous.
func needsQuote(s string) bool {
	if s == "" || strings.EqualFold(s, "null") || strings.TrimSpace(s) != s {
		return true
	}
	return strings.ContainsAny(s, `'",[]{}():=\`+"\n\t")
}

// display renders a value as DuckDB prints it.
func display(v any, top bool) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		if top || !needsQuote(x) {
			return x
		}
		return "'" + strings.ReplaceAll(x, "'", `\'`) + "'"
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 32)
	case floatValue:
		return formatFloat(x)
	case blob:
		return formatBlob(x)
	case dateValue:
		return time.Time(x).UTC().Format("2006-01-02")
	case timeOfDay:
		s := time.Time(x).UTC().Format("15:04:05.000000")
		return strings.TrimRight(strings.TrimRight(s, "0"), ".")
	case timestampValue:
		return formatTimestamp(x)
	case intervalValue:
		return formatInterval(x)
	case decimalValue:
		return string(x)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = display(e, false)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case orderedMap:
		parts := make([]string, len(x.keys))
		for i := range x.keys {
			parts[i] = display(x.keys[i], false) + "=" + display(x.vals[i], false)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	case structValue:
		parts := make([]string, len(x.names))
		for i := range x.names {
			parts[i] = "'" + x.names[i] + "': " + display(x.vals[i], false)
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(v)
}

// jsonValue converts a value into something encoding/json renders the way
// DuckDB's json mode does.
func jsonValue(v any) any {
	switch x := v.(type) {
	case nil, string, bool, int64, uint64:
		return x
	case floatValue:
		if math.IsNaN(x.f) || math.IsInf(x.f, 0) {
			return formatFloat(x)
		}
		return json.Number(formatFloat(x))
	case decimalValue:
		return json.Number(string(x))
	case blob:
		return base64.StdEncoding.EncodeToString(x)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = jsonValue(e)
		}
		return out
	case orderedMap:
		out := make(map[string]any, len(x.keys))
		for i := range x.keys {
			out[display(x.keys[i], true)] = jsonValue(x.vals[i])
		}
		return out
	case structValue:
		return orderedRow{cols: structCols(x.names), row: structCells(x.vals)}
	}
	return display(v, true)
}

func structCols(names []string) []Column {
	cols := make([]Column, len(names))
	for i, n := range names {
		cols[i] = Column{Name: n}
	}
	return cols
}

func structCells(vals []any) []Cell {
	cells := make([]Cell, len(vals))
	for i, v := range vals {
		cells[i] = Cell{Value: jsonValue(v), Null: v == nil}
	}
	return cells
}
