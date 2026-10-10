package topicsample

import (
	"strings"

	"github.com/loicalleyne/bufka/pkg/protoinspect"
)

// Topic mention levels, best first.
const (
	mentionNone = iota
	mentionInFile
	mentionInMessageComments
)

// topicMention reports where topic is named in a message's .proto source: in
// the message's own comments, elsewhere in its file, or not at all.
func topicMention(topic string, info protoinspect.MessageInfo) int {
	switch {
	case topic == "":
		return mentionNone
	case containsTopic(info.Comments, topic):
		return mentionInMessageComments
	case containsTopic(info.FileSource, topic):
		return mentionInFile
	default:
		return mentionNone
	}
}

// containsTopic finds topic in text as a whole name, so "a.b" does not match
// inside "a.b.v1" or "xa.b", while a sentence-ending "a.b." still matches.
func containsTopic(text, topic string) bool {
	for from := 0; ; {
		i := strings.Index(text[from:], topic)
		if i < 0 {
			return false
		}
		start, end := from+i, from+i+len(topic)
		if (start == 0 || !isTopicChar(text[start-1])) && topicEnds(text, end) {
			return true
		}
		from = start + 1
	}
}

func topicEnds(text string, end int) bool {
	if end == len(text) {
		return true
	}
	if text[end] == '.' {
		return end+1 == len(text) || !isTopicChar(text[end+1])
	}
	return !isTopicChar(text[end])
}

func isTopicChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
}
