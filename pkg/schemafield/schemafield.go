// Package schemafield holds a source-neutral description of a message field.
//
// It is the common model that schema loaders (protobuf descriptors, AsyncAPI
// specs, ...) flatten their message definitions into, so that callers can
// display or query field documentation without knowing where it came from.
// Nested fields are flattened into dotted paths; see FieldDef.Path.
package schemafield

// Source identifies the kind of schema a FieldDef was derived from.
type Source string

const (
	// SourceProto marks fields derived from protobuf descriptors.
	SourceProto Source = "proto"
	// SourceAsyncAPI marks fields derived from AsyncAPI (JSON Schema) payloads.
	SourceAsyncAPI Source = "asyncapi"
)

// FieldDef describes a single, possibly nested, field of a message.
type FieldDef struct {
	// Path is the dotted path of the field from the message root, e.g.
	// "Store.Currencies.Primary". Array items are addressed with a "[]"
	// suffix on the array field ("Origin[].Name"); map values with "{}".
	Path string
	// Type is a human-readable type: a scalar (string, integer, int64,
	// boolean, ...), "object", "array<T>", "map<string,T>", or the name of a
	// referenced message/schema/enum.
	Type string
	// Format is the JSON Schema format (date-time, email, int64, ...). Empty
	// for proto fields.
	Format string
	// Repeated reports whether the field is a list (proto repeated, JSON array).
	Repeated bool
	// Required reports whether the field must be present.
	Required bool
	// Nullable reports whether the field may be explicitly null.
	Nullable bool
	// Description is the field documentation with surrounding whitespace trimmed.
	Description string
	// Enum lists the allowed values, stringified.
	Enum []string
	// Default is the stringified default value, if any.
	Default string
	// Example is the stringified example value, if any (JSON for composites).
	Example string
	// Number is the protobuf field number; 0 for non-proto sources.
	Number int32
	// Oneof is the name of the oneof group the field belongs to, if any.
	Oneof string
	// Source identifies where the definition came from.
	Source Source
}
