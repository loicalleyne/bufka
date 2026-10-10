package protoinspect

import (
	"strings"

	"github.com/loicalleyne/bufka/pkg/schemafield"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// maxFieldDepth caps how many message levels FieldDefs descends into.
const maxFieldDepth = 8

// FieldDefs returns the flattened field definitions of the message, computed
// from Descriptor and the source locations of its file. Message-typed fields
// are emitted and then expanded: nested fields use "parent.child", repeated
// message fields "parent[].child", and map fields with message values
// "field{}.child". Well-known types (google.protobuf.*) are not expanded, and
// a message already on the current path is not expanded again.
func (m MessageInfo) FieldDefs() []schemafield.FieldDef {
	if m.Descriptor == nil {
		return nil
	}
	var out []schemafield.FieldDef
	onPath := map[protoreflect.FullName]bool{m.Descriptor.FullName(): true}
	appendFieldDefs(&out, m.Descriptor, "", 1, onPath)
	return out
}

func appendFieldDefs(out *[]schemafield.FieldDef, md protoreflect.MessageDescriptor, prefix string, depth int, onPath map[protoreflect.FullName]bool) {
	locs := md.ParentFile().SourceLocations()
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		path := prefix + string(fd.Name())
		def := schemafield.FieldDef{
			Path:        path,
			Number:      int32(fd.Number()),
			Repeated:    fd.IsList(),
			Required:    fd.Cardinality() == protoreflect.Required,
			Description: fieldDescription(locs.ByDescriptor(fd)),
			Source:      schemafield.SourceProto,
		}
		if oo := fd.ContainingOneof(); oo != nil {
			if oo.IsSynthetic() {
				def.Nullable = true
			} else {
				def.Oneof = string(oo.Name())
			}
		}
		if fd.HasDefault() && fd.Enum() == nil {
			def.Default = fd.Default().String()
		} else if fd.HasDefault() {
			if ev := fd.DefaultEnumValue(); ev != nil {
				def.Default = string(ev.Name())
			}
		}

		// elem is the descriptor whose type names the field and which may be expanded.
		var child protoreflect.MessageDescriptor
		childPrefix := path
		switch {
		case fd.IsMap():
			v := fd.MapKey()
			val := fd.MapValue()
			def.Type = "map<" + kindName(v) + "," + kindName(val) + ">"
			if val.Message() != nil {
				child = val.Message()
				childPrefix = path + "{}"
			}
			if val.Enum() != nil {
				def.Enum = enumValues(val.Enum())
			}
		default:
			def.Type = kindName(fd)
			if fd.Enum() != nil {
				def.Enum = enumValues(fd.Enum())
			}
			if fd.Message() != nil {
				child = fd.Message()
				if fd.IsList() {
					childPrefix = path + "[]"
				}
			}
		}
		*out = append(*out, def)

		if child == nil || child.FullName().Parent() == "google.protobuf" ||
			depth >= maxFieldDepth || onPath[child.FullName()] {
			continue
		}
		onPath[child.FullName()] = true
		appendFieldDefs(out, child, childPrefix+".", depth+1, onPath)
		delete(onPath, child.FullName())
	}
}

func kindName(fd protoreflect.FieldDescriptor) string {
	switch {
	case fd.Message() != nil:
		return string(fd.Message().FullName())
	case fd.Enum() != nil:
		return string(fd.Enum().FullName())
	}
	return fd.Kind().String()
}

func enumValues(ed protoreflect.EnumDescriptor) []string {
	vals := ed.Values()
	names := make([]string, 0, vals.Len())
	for i := 0; i < vals.Len(); i++ {
		names = append(names, string(vals.Get(i).Name()))
	}
	return names
}

// fieldDescription joins leading and trailing comments, collapsing line
// breaks and comment indentation into single spaces.
func fieldDescription(loc protoreflect.SourceLocation) string {
	var parts []string
	for _, c := range []string{loc.LeadingComments, loc.TrailingComments} {
		if s := strings.Join(strings.Fields(c), " "); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " ")
}
