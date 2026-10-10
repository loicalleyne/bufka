package asyncapi

import (
	"fmt"
	"regexp"
	"strings"
)

// maxDepth caps how deep nested payload properties are flattened.
const maxDepth = 10

var (
	schemeRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)
	topicRe  = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

type loader struct {
	file   string
	root   *obj
	errs   []LoadError
	warned map[string]bool
}

func (l *loader) warn(err error) {
	if l.warned == nil {
		l.warned = map[string]bool{}
	}
	if l.warned[err.Error()] {
		return
	}
	l.warned[err.Error()] = true
	l.errs = append(l.errs, LoadError{File: l.file, Err: err})
}

// splitChannel maps a channel name to a topic and optional event type.
// ok is false for channels that are not Kafka topics (URLs).
func splitChannel(name string) (topic, eventType string, ok bool) {
	name = strings.TrimSpace(name)
	if name == "" || schemeRe.MatchString(name) {
		return "", "", false
	}
	if i := strings.Index(name, "/"); i > 0 && topicRe.MatchString(name[:i]) {
		return name[:i], strings.Trim(name[i+1:], "/"), true
	}
	return name, "", true
}

func (l *loader) topics() []*TopicSpec {
	channels := l.root.obj("channels")
	if channels == nil {
		return nil
	}
	ver := l.root.str("asyncapi")
	v3 := strings.HasPrefix(strings.TrimSpace(ver), "3")
	info := l.root.obj("info")
	byTopic := map[string]*TopicSpec{}
	var order []*TopicSpec
	for _, key := range channels.keys {
		ch, _ := l.deref(channels.get(key), 0)
		if ch == nil {
			continue
		}
		name := key
		if v3 {
			if a := ch.str("address"); a != "" {
				name = a
			}
		}
		if schemeRe.MatchString(key) {
			continue
		}
		topic, eventType, ok := splitChannel(name)
		if !ok {
			continue
		}
		summary := strings.TrimSpace(ch.str("summary"))
		desc := strings.TrimSpace(ch.str("description"))
		var msgs []MessageSpec
		if v3 {
			if m := ch.obj("messages"); m != nil {
				for _, mk := range m.keys {
					msgs = append(msgs, l.messages(m.get(mk), mk)...)
				}
			}
			for _, op := range l.operationsFor(key) {
				if summary == "" {
					summary = strings.TrimSpace(op.str("summary"))
				}
				if desc == "" {
					desc = strings.TrimSpace(op.str("description"))
				}
			}
		} else {
			for _, opKey := range []string{"subscribe", "publish"} {
				op, _ := l.deref(ch.get(opKey), 0)
				if op == nil {
					continue
				}
				if summary == "" {
					summary = strings.TrimSpace(op.str("summary"))
				}
				if desc == "" {
					desc = strings.TrimSpace(op.str("description"))
				}
				msgs = append(msgs, l.messages(op.get("message"), "")...)
			}
		}
		for i := range msgs {
			msgs[i].EventType = eventType
		}
		ts := byTopic[topic]
		if ts == nil {
			ts = &TopicSpec{
				Topic:           topic,
				SpecFile:        l.file,
				SpecTitle:       strings.TrimSpace(info.str("title")),
				SpecVersion:     info.str("version"),
				AsyncAPIVersion: ver,
			}
			byTopic[topic] = ts
			order = append(order, ts)
		}
		if ts.Summary == "" {
			ts.Summary = summary
		}
		if ts.Description == "" {
			ts.Description = desc
		}
		if ts.CleanupPolicy == "" {
			ts.CleanupPolicy = cleanupPolicy(ch)
		}
		ts.Messages = append(ts.Messages, msgs...)
	}
	return order
}

func cleanupPolicy(ch *obj) string {
	k, _ := ch.obj("bindings").get("kafka").(*obj)
	tc, _ := k.getFold("topicConfiguration").(*obj)
	if tc == nil {
		return ""
	}
	if v := tc.str("cleanup.policy"); v != "" {
		return v
	}
	// Also accept nested "cleanup: {policy: ...}".
	return tc.obj("cleanup").str("policy")
}

// operationsFor returns 3.x operations whose channel references key.
func (l *loader) operationsFor(key string) []*obj {
	ops := l.root.obj("operations")
	if ops == nil {
		return nil
	}
	var out []*obj
	for _, k := range ops.keys {
		op, _ := l.deref(ops.get(k), 0)
		if op == nil {
			continue
		}
		ref := strings.TrimSpace(op.obj("channel").str("$ref"))
		if !strings.HasPrefix(ref, "#/channels/") {
			continue
		}
		if unescapePointer(strings.TrimPrefix(ref, "#/channels/")) == key {
			out = append(out, op)
		}
	}
	return out
}

// messages resolves a message value (inline, $ref, or oneOf list).
func (l *loader) messages(v any, fallbackName string) []MessageSpec {
	m, refName := l.deref(v, 0)
	if m == nil {
		return nil
	}
	if alts := m.list("oneOf"); alts != nil {
		var out []MessageSpec
		for _, a := range alts {
			out = append(out, l.messages(a, "")...)
		}
		return out
	}
	ms := MessageSpec{
		Name:        m.str("name"),
		Title:       strings.TrimSpace(m.str("title")),
		Summary:     strings.TrimSpace(m.str("summary")),
		Description: strings.TrimSpace(m.str("description")),
		ContentType: m.str("contentType"),
	}
	if ms.Name == "" {
		ms.Name = refName
	}
	if ms.Name == "" {
		ms.Name = fallbackName
	}
	if ms.ContentType == "" {
		ms.ContentType = l.root.str("defaultContentType")
	}
	if h, _ := l.deref(m.get("headers"), 0); h != nil {
		key, _ := l.deref(h.obj("properties").get("key"), 0)
		ms.KeyDescription = strings.TrimSpace(key.str("description"))
	}
	if m.has("payload") {
		s := l.schema(m.get("payload"), nil)
		ms.Required = s.required
		ms.Fields = l.flatten(s, "", 1)
	}
	return []MessageSpec{ms}
}

// deref follows local $refs. It returns the target object and the last
// path segment of the first ref followed (the component name).
func (l *loader) deref(v any, depth int) (*obj, string) {
	o, _ := v.(*obj)
	name := ""
	for i := 0; o != nil && o.has("$ref") && i < 32; i++ {
		ref := strings.TrimSpace(o.str("$ref"))
		target, ok := l.pointer(ref)
		if !ok {
			return nil, name
		}
		if name == "" {
			name = lastSeg(ref)
		}
		t, _ := target.(*obj)
		o = t
	}
	return o, name
}

func lastSeg(ref string) string {
	i := strings.LastIndex(ref, "/")
	return unescapePointer(ref[i+1:])
}

func unescapePointer(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
}

// pointer resolves a local JSON pointer ref. External refs produce a warning.
func (l *loader) pointer(ref string) (any, bool) {
	if !strings.HasPrefix(ref, "#") {
		l.warn(errExternal(ref))
		return nil, false
	}
	p := strings.TrimPrefix(ref, "#")
	var cur any = l.root
	if p == "" || p == "/" {
		return cur, true
	}
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		seg = unescapePointer(seg)
		switch c := cur.(type) {
		case *obj:
			if !c.has(seg) {
				l.warn(fmt.Errorf("unresolved $ref %s", ref))
				return nil, false
			}
			cur = c.get(seg)
		case []any:
			var idx int
			if _, err := fmt.Sscanf(seg, "%d", &idx); err != nil || idx < 0 || idx >= len(c) {
				l.warn(fmt.Errorf("unresolved $ref %s", ref))
				return nil, false
			}
			cur = c[idx]
		default:
			l.warn(fmt.Errorf("unresolved $ref %s", ref))
			return nil, false
		}
	}
	return cur, true
}

func errExternal(ref string) error { return fmt.Errorf("%w: %s", ErrExternalRef, ref) }
