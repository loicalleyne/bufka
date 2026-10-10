// Package timeutil provides helpers for time and duration conversions,
// specifically supporting duration representations with days (e.g. "1d", "2d13h56s").
package timeutil

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var dayPattern = regexp.MustCompile(`^(\d+)d(.*)$`)

// ParseDuration parses a duration string that may optionally contain a leading day specification (e.g. "1d", "2d13h56s").
// Negative durations are not allowed.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("negative duration not allowed")
	}

	var dayDur time.Duration
	if matches := dayPattern.FindStringSubmatch(s); len(matches) == 3 {
		days, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day value: %w", err)
		}
		dayDur = time.Duration(days) * 24 * time.Hour
		s = matches[2]
	}

	if s == "" {
		return dayDur, nil
	}

	restDur, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return dayDur + restDur, nil
}

// FormatDuration converts a duration into a human-readable string using days where appropriate.
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}

	days := d / (24 * time.Hour)
	d -= days * 24 * time.Hour

	var b strings.Builder
	if days > 0 {
		fmt.Fprintf(&b, "%dd", days)
	}

	hours := d / time.Hour
	d -= hours * time.Hour
	if hours > 0 {
		fmt.Fprintf(&b, "%dh", hours)
	}

	mins := d / time.Minute
	d -= mins * time.Minute
	if mins > 0 {
		fmt.Fprintf(&b, "%dm", mins)
	}

	secs := d / time.Second
	if secs > 0 || b.Len() == 0 {
		fmt.Fprintf(&b, "%ds", secs)
	}

	return b.String()
}
