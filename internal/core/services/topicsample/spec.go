package topicsample

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/loicalleyne/bufka/pkg/asyncapi"
)

const maxWarnings = 10

// applySpec documents (and for JSON, validates) res against the AsyncAPI
// spec for topic. A nil index or an undocumented topic leaves res untouched.
func applySpec(res *TopicResult, topic string, specs *asyncapi.Index) {
	if specs == nil || topic == "" {
		return
	}
	ts, ok := specs.Lookup(topic)
	if !ok {
		return
	}
	ref := &SpecRef{File: ts.SpecFile, SpecTitle: ts.SpecTitle, Topic: ts.Topic}
	for _, m := range ts.Messages {
		ref.Candidates = append(ref.Candidates, m.Name)
	}
	res.Spec = ref

	switch res.Format {
	case FormatJSON:
		var samples []map[string]any
		for _, r := range res.Rows {
			if !r.DecodeOK {
				continue
			}
			var v any
			if json.Unmarshal([]byte(r.Decoded), &v) != nil {
				continue
			}
			if obj, ok := v.(map[string]any); ok {
				samples = append(samples, obj)
			}
		}
		if len(ts.Messages) > 0 {
			msg := chooseMessage(ts.Messages, samples)
			ref.Message = msg.Name
			ref.EventType = msg.EventType
			res.Warnings = validate(msg, samples)
		}
		res.ResolvedBy = ResolvedByAsyncAPI
	case FormatEmpty:
		// No samples: keep "empty" (callers treat it as "re-sample later")
		// and just attach the documentation.
		if len(ts.Messages) == 1 {
			ref.Message = ts.Messages[0].Name
			ref.EventType = ts.Messages[0].EventType
		}
		res.ResolvedBy = ResolvedByAsyncAPI
	default:
		res.Warnings = append(res.Warnings, "AsyncAPI documents this topic as JSON but samples are "+res.Format)
	}
}

func isTopLevel(path string) bool { return !strings.ContainsAny(path, ".[{") }

func scalarString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// chooseMessage picks the message best describing samples.
func chooseMessage(msgs []asyncapi.MessageSpec, samples []map[string]any) asyncapi.MessageSpec {
	if len(msgs) == 1 {
		return msgs[0]
	}
	pool := msgs
	var matched []asyncapi.MessageSpec
	for _, m := range msgs {
		disc := map[string]string{}
		for _, f := range m.Fields {
			if isTopLevel(f.Path) && len(f.Enum) == 1 {
				disc[f.Path] = f.Enum[0]
			}
		}
		if len(disc) == 0 || len(samples) == 0 {
			continue
		}
		ok := true
		for _, s := range samples {
			for k, want := range disc {
				if v, has := s[k]; !has || scalarString(v) != want {
					ok = false
				}
			}
		}
		if ok {
			matched = append(matched, m)
		}
	}
	if len(matched) == 1 {
		return matched[0]
	}
	if len(matched) > 1 {
		pool = matched
	}
	keys := map[string]bool{}
	for _, s := range samples {
		for k := range s {
			keys[k] = true
		}
	}
	best, bestN := pool[0], -1
	for _, m := range pool {
		n := 0
		for _, f := range m.Fields {
			if isTopLevel(f.Path) && keys[f.Path] {
				n++
			}
		}
		if n > bestN {
			best, bestN = m, n
		}
	}
	return best
}

// instances resolves the parent path (segments like "Origin[]") to every
// object present in sample.
func instances(sample map[string]any, parent string) []map[string]any {
	cur := []map[string]any{sample}
	if parent == "" {
		return cur
	}
	for _, seg := range strings.Split(parent, ".") {
		arr := strings.HasSuffix(seg, "[]")
		name := strings.TrimSuffix(seg, "[]")
		var next []map[string]any
		for _, o := range cur {
			v, ok := o[name]
			if !ok {
				continue
			}
			if arr {
				list, _ := v.([]any)
				for _, e := range list {
					if m, ok := e.(map[string]any); ok {
						next = append(next, m)
					}
				}
			} else if m, ok := v.(map[string]any); ok {
				next = append(next, m)
			}
		}
		cur = next
	}
	return cur
}

func splitPath(p string) (parent, name string) {
	i := strings.LastIndex(p, ".")
	if i < 0 {
		return "", strings.TrimSuffix(p, "[]")
	}
	return p[:i], strings.TrimSuffix(p[i+1:], "[]")
}

// validate reports required-field and undefined-key findings for samples.
func validate(msg asyncapi.MessageSpec, samples []map[string]any) []string {
	total := len(samples)
	if total == 0 {
		return nil
	}
	var warns []string
	add := func(w string) {
		if len(warns) < maxWarnings && !slices.Contains(warns, w) {
			warns = append(warns, w)
		}
	}

	children := map[string]map[string]bool{}
	var parentOrder []string
	type req struct{ parent, name, path string }
	var reqs []req
	seenReq := map[string]bool{}
	for _, f := range msg.Fields {
		if strings.Contains(f.Path, "{}") {
			continue
		}
		parent, name := splitPath(f.Path)
		if strings.Contains(parent, "{}") {
			continue
		}
		if children[parent] == nil {
			children[parent] = map[string]bool{}
			parentOrder = append(parentOrder, parent)
		}
		children[parent][name] = true
		if f.Required && !seenReq[f.Path] {
			seenReq[f.Path] = true
			reqs = append(reqs, req{parent, name, f.Path})
		}
	}
	for _, name := range msg.Required {
		if !seenReq[name] {
			seenReq[name] = true
			reqs = append(reqs, req{"", name, name})
		}
	}

	for _, r := range reqs {
		n := 0
		for _, s := range samples {
			for _, o := range instances(s, r.parent) {
				if _, ok := o[r.name]; !ok {
					n++
					break
				}
			}
		}
		if n > 0 {
			add(fmt.Sprintf("required field %s missing in %d/%d sampled messages", r.path, n, total))
		}
	}

	for _, parent := range parentOrder {
		defined := children[parent]
		// A nested parent is checked only when the spec defines it as an
		// object with properties (it has children, by construction).
		counts := map[string]int{}
		for _, s := range samples {
			seen := map[string]bool{}
			for _, o := range instances(s, parent) {
				for k := range o {
					if !defined[k] {
						seen[k] = true
					}
				}
			}
			for k := range seen {
				counts[k]++
			}
		}
		keys := make([]string, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			full := k
			if parent != "" {
				full = parent + "." + k
			}
			add(fmt.Sprintf("key %s not defined in spec in %d/%d messages", full, counts[k], total))
		}
	}
	return warns
}
