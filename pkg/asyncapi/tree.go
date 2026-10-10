package asyncapi

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// obj is an order-preserving mapping node. Values are *obj, []any, or
// scalars (string, int, float64, bool, nil).
type obj struct {
	keys []string
	m    map[string]any
}

func (o *obj) has(k string) bool {
	if o == nil {
		return false
	}
	_, ok := o.m[k]
	return ok
}

func (o *obj) get(k string) any {
	if o == nil {
		return nil
	}
	return o.m[k]
}

func (o *obj) obj(k string) *obj {
	v, _ := o.get(k).(*obj)
	return v
}

func (o *obj) list(k string) []any {
	v, _ := o.get(k).([]any)
	return v
}

func (o *obj) str(k string) string {
	switch v := o.get(k).(type) {
	case nil:
		return ""
	case string:
		return v
	case *obj, []any:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

// getFold returns the value for k compared case-insensitively.
func (o *obj) getFold(k string) any {
	if o == nil {
		return nil
	}
	if v, ok := o.m[k]; ok {
		return v
	}
	for _, kk := range o.keys {
		if strings.EqualFold(kk, k) {
			return o.m[kk]
		}
	}
	return nil
}

// parse decodes YAML (a superset of JSON) into the generic tree.
func parse(data []byte) (any, error) {
	var n yaml.Node
	if err := yaml.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	if n.Kind == 0 {
		return nil, nil
	}
	return convert(&n, 0)
}

func convert(n *yaml.Node, depth int) (any, error) {
	if depth > 200 {
		return nil, fmt.Errorf("document nested too deeply")
	}
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil, nil
		}
		return convert(n.Content[0], depth+1)
	case yaml.AliasNode:
		return convert(n.Alias, depth+1)
	case yaml.MappingNode:
		o := &obj{m: make(map[string]any, len(n.Content)/2)}
		for i := 0; i+1 < len(n.Content); i += 2 {
			kn := n.Content[i]
			if kn.Tag == "!!merge" {
				// "<<: *anchor" merge keys.
				v, err := convert(n.Content[i+1], depth+1)
				if err != nil {
					return nil, err
				}
				if mo, ok := v.(*obj); ok {
					for _, k := range mo.keys {
						if !o.has(k) {
							o.keys = append(o.keys, k)
							o.m[k] = mo.m[k]
						}
					}
				}
				continue
			}
			v, err := convert(n.Content[i+1], depth+1)
			if err != nil {
				return nil, err
			}
			k := kn.Value
			if !o.has(k) {
				o.keys = append(o.keys, k)
			}
			o.m[k] = v
		}
		return o, nil
	case yaml.SequenceNode:
		out := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			v, err := convert(c, depth+1)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case yaml.ScalarNode:
		var v any
		if err := n.Decode(&v); err != nil {
			return n.Value, nil
		}
		return v, nil
	}
	return nil, nil
}

// stringify renders a value for Enum/Default/Example: scalars verbatim,
// composites as compact JSON.
func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case *obj, []any:
		b, err := json.Marshal(plain(t))
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	default:
		return fmt.Sprint(t)
	}
}

// plain converts the tree back to ordinary maps/slices for JSON encoding.
func plain(v any) any {
	switch t := v.(type) {
	case *obj:
		m := make(map[string]any, len(t.keys))
		for _, k := range t.keys {
			m[k] = plain(t.m[k])
		}
		return m
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = plain(e)
		}
		return out
	default:
		return t
	}
}
