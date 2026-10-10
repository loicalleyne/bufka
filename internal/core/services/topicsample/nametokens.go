package topicsample

import (
	"slices"
	"strings"
	"unicode"
)

// topicTokens splits a Kafka topic name into lowercase words on any
// non-alphanumeric character, e.g. "a.order-items" -> {a, order, item}.
func topicTokens(topic string) map[string]bool {
	words := strings.FieldsFunc(topic, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make(map[string]bool, len(words))
	for _, w := range words {
		out[normalizeToken(w)] = true
	}
	return out
}

// messageSegments splits a fully-qualified message name into its
// '.'/'_'-separated parts, each as ordered lowercase CamelCase words, e.g.
// "pkg.HTTPServerConfig" -> [[pkg] [http server config]].
func messageSegments(fqn string) [][]string {
	var out [][]string
	for _, part := range strings.FieldsFunc(fqn, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		words := splitCamel(part)
		for i, w := range words {
			words[i] = strings.ToLower(w)
		}
		out = append(out, words)
	}
	return out
}

// messageTokens is the normalized word set of a fully-qualified message name.
func messageTokens(fqn string) map[string]bool {
	out := make(map[string]bool)
	for _, seg := range messageSegments(fqn) {
		for _, w := range seg {
			out[normalizeToken(w)] = true
		}
	}
	return out
}

// nameScore counts topic words that appear in the message name, either
// exactly or as an abbreviation of consecutive words (aol -> AmericaOnLine).
func nameScore(topicWords map[string]bool, fqn string) int {
	tokens := messageTokens(fqn)
	segments := messageSegments(fqn)
	n := 0
	for t := range topicWords {
		if tokens[t] || slices.ContainsFunc(segments, func(seg []string) bool { return abbreviates(t, seg) }) {
			n++
		}
	}
	return n
}

const maxAbbrevLen = 4

// abbreviates reports whether abbr (2-4 letters) starts with the first letter
// of a word in seg and its remaining letters appear in order in the words
// that follow it in the same segment.
func abbreviates(abbr string, words []string) bool {
	if len(abbr) < 2 || len(abbr) > maxAbbrevLen {
		return false
	}
	for i, w := range words {
		if w == "" || w[0] != abbr[0] {
			continue
		}
		rest := abbr[1:]
		for _, next := range words[i+1:] {
			for j := 0; j < len(next) && len(rest) > 0; j++ {
				if next[j] == rest[0] {
					rest = rest[1:]
				}
			}
		}
		if len(rest) == 0 {
			return true
		}
	}
	return false
}

func splitCamel(s string) []string {
	runes := []rune(s)
	var words []string
	start := 0
	for i := 1; i < len(runes); i++ {
		prev, cur := runes[i-1], runes[i]
		nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
		if (unicode.IsUpper(cur) && (unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower))) ||
			(unicode.IsDigit(cur) != unicode.IsDigit(prev)) {
			words = append(words, string(runes[start:i]))
			start = i
		}
	}
	return append(words, string(runes[start:]))
}

// normalizeToken lowercases w and drops a plural "s" so that "submissions"
// and "Submission" compare equal.
func normalizeToken(w string) string {
	w = strings.ToLower(w)
	if len(w) > 3 && strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss") {
		w = w[:len(w)-1]
	}
	return w
}
