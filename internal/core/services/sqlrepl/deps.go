package sqlrepl

import "context"

// TopicCache describes one topic configured in bufka's cache.
type TopicCache struct {
	Topic string
	// Table is the DuckDB table the topic is synced into.
	Table string
	// Active reports whether a sync job is running.
	Active bool
	// Err is the last sync error, if any.
	Err string
}

// Deps connects the bufka-specific dot-commands to the rest of bufka. Any
// nil function disables the commands that need it.
type Deps struct {
	// Topics lists the cached topics.
	Topics func(ctx context.Context) ([]TopicCache, error)
	// Sync starts (or resumes) the background sync of topic.
	Sync func(ctx context.Context, topic string) error
	// StartUI starts DuckDB's UI server (checking its port is free first)
	// and returns its URL.
	StartUI func(ctx context.Context) (string, error)
	// OpenURL opens a URL or file with the OS (open / xdg-open).
	OpenURL func(target string) error
	// Compact checkpoints and compacts the database.
	Compact func(ctx context.Context) error
}
