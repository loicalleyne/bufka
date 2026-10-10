package asyncapi

import (
	"slices"
	"strings"

	"github.com/loicalleyne/bufka/pkg/schemafield"
)

// prop is a named, still-unresolved property schema.
type prop struct {
	name string
	raw  any
}

// schema is a JSON Schema node with refs and allOf/oneOf/anyOf resolved one
// level: its own properties are merged but not yet resolved themselves.
type schema struct {
	typ, format, desc string
	nullable          bool
	enum              []string
	def, example      string
	props             []prop
	required          []string
	items             any // raw array item schema
	additional        any // raw additionalProperties schema (object form only)
	refName           string
	extRef            string
	composite         string   // "oneOf"/"anyOf" when alternatives were merged
	alts              []string // names/types of alternatives
	refs              []string // refs followed to reach this node (cycle guard)
	cycle             bool     // a ref on the ancestor chain was revisited
}

func (s *schema) isObject() bool {
	return s.typ == "object" || (s.typ == "" && (len(s.props) > 0 || s.additional != nil))
}

func (s *schema) addProp(name string, raw any) {
	for i := range s.props {
		if s.props[i].name == name {
			s.props[i].raw = raw
			return
		}
	}
	s.props = append(s.props, prop{name, raw})
}

func (s *schema) addRequired(names []string) {
	for _, n := range names {
		if !slices.Contains(s.required, n) {
			s.required = append(s.required, n)
		}
	}
}

// schema resolves v. ancestors holds the refs on the path from the payload
// root to v; a ref already present marks a cycle.
func (l *loader) schema(v any, ancestors []string) *schema {
	s := &schema{}
	o, _ := v.(*obj)
	seen := slices.Clone(ancestors)
	for i := 0; o != nil && o.has("$ref"); i++ {
		ref := strings.TrimSpace(o.str("$ref"))
		if !strings.HasPrefix(ref, "#") {
			l.warn(errExternal(ref))
			s.extRef = ref
			return s
		}
		if slices.Contains(seen, ref) || i > 32 {
			s.cycle = true
			if s.refName == "" {
				s.refName = lastSeg(ref)
			}
			return s
		}
		seen = append(seen, ref)
		s.refs = append(s.refs, ref)
		if s.refName == "" {
			s.refName = lastSeg(ref)
		}
		t, ok := l.pointer(ref)
		if !ok {
			s.extRef = ref
			return s
		}
		o, _ = t.(*obj)
	}
	if o == nil {
		return s
	}
	s.typ = typeString(o.get("type"), &s.nullable)
	s.format = o.str("format")
	s.desc = strings.TrimSpace(o.str("description"))
	if b, ok := o.get("nullable").(bool); ok && b {
		s.nullable = true
	}
	for _, e := range o.list("enum") {
		if e == nil {
			s.nullable = true
			continue
		}
		s.enum = append(s.enum, stringify(e))
	}
	if s.typ == "" {
		s.typ = enumType(o.list("enum"))
	}
	if o.has("default") {
		s.def = stringify(o.get("default"))
	}
	if o.has("example") {
		s.example = stringify(o.get("example"))
	} else if ex := o.list("examples"); len(ex) > 0 {
		s.example = stringify(ex[0])
	}
	if p := o.obj("properties"); p != nil {
		for _, k := range p.keys {
			s.addProp(k, p.get(k))
		}
	}
	s.addRequired(strList(o.list("required")))
	s.items = o.get("items")
	if ap, ok := o.get("additionalProperties").(*obj); ok {
		s.additional = ap
	}
	for _, sub := range o.list("allOf") {
		c := l.schema(sub, seen)
		s.merge(c)
	}
	for _, kw := range []string{"oneOf", "anyOf"} {
		list := o.list(kw)
		if len(list) == 0 {
			continue
		}
		s.composite = kw
		for _, sub := range list {
			c := l.schema(sub, seen)
			s.alts = append(s.alts, typeName(c))
			for _, p := range c.props {
				s.addProp(p.name, p.raw)
			}
			// Properties of alternatives are never unconditionally required.
		}
	}
	return s
}

func (s *schema) merge(c *schema) {
	if s.typ == "" {
		s.typ = c.typ
	}
	if s.format == "" {
		s.format = c.format
	}
	if s.desc == "" {
		s.desc = c.desc
	}
	s.nullable = s.nullable || c.nullable
	if len(s.enum) == 0 {
		s.enum = c.enum
	}
	if s.def == "" {
		s.def = c.def
	}
	if s.example == "" {
		s.example = c.example
	}
	if s.items == nil {
		s.items = c.items
	}
	if s.additional == nil {
		s.additional = c.additional
	}
	// Own properties take precedence over inherited ones; keep inherited
	// ones first so the base type's fields come before the extension's.
	own := s.props
	s.props = slices.Clone(c.props)
	for _, p := range own {
		s.addProp(p.name, p.raw)
	}
	s.addRequired(c.required)
	s.refs = append(s.refs, c.refs...)
	s.cycle = s.cycle || c.cycle
}

func typeString(v any, nullable *bool) string {
	switch t := v.(type) {
	case string:
		return t
	case []any:
		var out []string
		for _, e := range t {
			es, _ := e.(string)
			if es == "null" {
				*nullable = true
				continue
			}
			out = append(out, es)
		}
		return strings.Join(out, "|")
	}
	return ""
}

// enumType infers the JSON Schema type of an enum declared without "type"
// from its values; "" when they are empty or of mixed kinds.
func enumType(vals []any) string {
	typ := ""
	for _, v := range vals {
		var t string
		switch n := v.(type) {
		case nil:
			continue
		case string:
			t = "string"
		case bool:
			t = "boolean"
		case int, int64, uint64:
			t = "integer"
		case float64:
			t = "number"
			if n == float64(int64(n)) {
				t = "integer"
			}
		default:
			return ""
		}
		switch {
		case typ == "":
			typ = t
		case typ == t:
		case typ == "integer" && t == "number", typ == "number" && t == "integer":
			typ = "number"
		default:
			return ""
		}
	}
	return typ
}

func strList(v []any) []string {
	out := make([]string, 0, len(v))
	for _, e := range v {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// typeName renders the FieldDef.Type of a resolved schema without looking
// into array items (see fieldType for that).
func typeName(s *schema) string {
	switch {
	case s.extRef != "":
		return s.extRef
	case s.composite != "" && s.refName == "":
		return s.composite + "<" + strings.Join(s.alts, "|") + ">"
	case s.refName != "" && (s.isObject() || s.composite != "" || s.typ == "" || s.cycle):
		return s.refName
	case s.typ == "" && len(s.props) > 0:
		return "object"
	}
	return s.typ
}

func (l *loader) fieldType(s *schema, ancestors []string) (string, *schema) {
	if s.typ == "array" {
		if s.items == nil {
			return "array", nil
		}
		it := l.schema(s.items, ancestors)
		inner, _ := l.fieldType(it, ancestors)
		return "array<" + inner + ">", it
	}
	if s.isObject() && len(s.props) == 0 && s.additional != nil && s.refName == "" {
		v := l.schema(s.additional, ancestors)
		inner, _ := l.fieldType(v, ancestors)
		return "map<string," + inner + ">", nil
	}
	return typeName(s), nil
}

// flatten emits fields for s's properties, recursing into nested objects,
// array items and map values.
func (l *loader) flatten(s *schema, prefix string, depth int) []schemafield.FieldDef {
	if depth > maxDepth || s.cycle {
		return nil
	}
	var out []schemafield.FieldDef
	for _, p := range s.props {
		c := l.schema(p.raw, s.refs)
		c.refs = append(slices.Clone(s.refs), c.refs...) // full ancestor chain
		typ, item := l.fieldType(c, c.refs)
		path := prefix + p.name
		f := schemafield.FieldDef{
			Path:        path,
			Type:        typ,
			Format:      c.format,
			Repeated:    c.typ == "array",
			Required:    slices.Contains(s.required, p.name),
			Nullable:    c.nullable,
			Description: c.desc,
			Enum:        c.enum,
			Default:     c.def,
			Example:     c.example,
			Source:      schemafield.SourceAsyncAPI,
		}
		if item != nil && f.Format == "" {
			f.Format = item.format
		}
		if item != nil && len(f.Enum) == 0 {
			f.Enum = item.enum
		}
		out = append(out, f)
		switch {
		case item != nil:
			item.refs = append(slices.Clone(c.refs), item.refs...)
			out = append(out, l.flatten(item, path+"[].", depth+1)...)
		case len(c.props) > 0:
			out = append(out, l.flatten(c, path+".", depth+1)...)
		case c.additional != nil:
			v := l.schema(c.additional, c.refs)
			v.refs = append(slices.Clone(c.refs), v.refs...)
			out = append(out, l.flatten(v, path+"{}.", depth+1)...)
		}
	}
	return out
}
