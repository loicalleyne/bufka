// Package duckdbui is a PRIMARY adapter (inbound) that reverse-proxies
// requests to DuckDB's built-in UI server (started via CALL
// start_ui_server()) so it can be reached through bufka's own HTTP
// server instead of exposing DuckDB's UI port directly.
package duckdbui

import (
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/loicalleyne/bufka/pkg/server"
)

// Config holds the configuration for the DuckDB UI proxy handler.
type Config struct {
	// Path is the mount prefix, e.g. "/duckdb-ui/". Must end with "/".
	Path string
	// Target is the DuckDB UI server address, e.g. "http://localhost:4213".
	Target string
	Mux    *http.ServeMux
}

type handler struct {
	*Config
}

// New creates a new DuckDB UI reverse-proxy handler.
func New(config *Config) *handler {
	return &handler{Config: config}
}

// Configure registers the reverse-proxy route on the ServeMux.
func (h *handler) Configure(srv server.Server) {
	target, err := url.Parse(h.Target)
	if err != nil {
		log.Printf("duckdb ui proxy: invalid target %q: %v", h.Target, err)
		return
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	h.Mux.Handle(h.Path, http.StripPrefix(h.Path[:len(h.Path)-1], proxy))
}
