package ping

import (
	"fmt"
	"net/http"

	"github.com/loicalleyne/bufka/pkg/server"
)

// Config holds the configuration for the ping handler.
type Config struct {
	Path string
	Mux  *http.ServeMux
}

type handler struct {
	*Config
}

// New creates a new ping handler.
func New(config *Config) *handler {
	return &handler{Config: config}
}

// Configure registers the ping route on the ServeMux.
func (h *handler) Configure(srv server.Server) {
	h.Mux.HandleFunc(h.Path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"healthy","service":"bufka"}`)
	})
}
