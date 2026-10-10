/*
Copyright © 2026
*/
// Package http wires the stdlib HTTP server with all route handlers.
//
// This is a PRIMARY adapter (inbound).
// Add new route handlers here: create a sub-package and register it with
// srv.Use(yourhandler.New(...)).
//
// Allowed imports:
//   - internal/core/ports/inbound (interfaces, if using explicit ports)
//   - internal/core/services (business logic)
//   - internal/config
//   - pkg/httpserver
//
// Forbidden:
//   - internal/adapters/secondary/* (outbound adapters — use ports instead)
package http

import (
	"net/http"

	"github.com/loicalleyne/bufka/internal/adapters/primary/http/duckdbui"
	"github.com/loicalleyne/bufka/internal/adapters/primary/http/ping"
	"github.com/loicalleyne/bufka/internal/core/services"
	httpsrv "github.com/loicalleyne/bufka/pkg/httpserver"
	"github.com/loicalleyne/bufka/pkg/server"
)

// New creates and wires the HTTP server with all registered route handlers.
// Framework-specific server mechanics live in pkg/httpserver.
func New(cfg *httpsrv.ServerConfig, services *services.Services) server.Server {
	srv := httpsrv.New(cfg).(*httpsrv.Server)

	// Register ping handler
	srv.Use(ping.New(&ping.Config{
		Path: "/ping",
		Mux:  srv.Mux,
	}))

	// Reverse-proxy DuckDB's built-in UI server (CALL start_ui_server())
	// through this HTTP server instead of exposing its port directly.
	if cfg.Config.DuckDBUIEnabled {
		srv.Use(duckdbui.New(&duckdbui.Config{
			Path:   "/duckdb-ui/",
			Target: cfg.Config.DuckDBUIAddr,
			Mux:    srv.Mux,
		}))
	}

	// Route group for /api/v1 using a nested ServeMux + middleware chain
	apiMux := http.NewServeMux()
	// Route-specific middlewares — wrap apiMux before mounting it
	// var apiHandler http.Handler = apiMux
	// apiHandler = yourlogging.Middleware(apiHandler)        // structured request logging
	// apiHandler = yourrequestid.Middleware(apiHandler)      // inject X-Request-Id header
	// apiHandler = yourauth.Middleware(services)(apiHandler) // JWT / session authorization
	// srv.Mux.Handle("/api/v1/", http.StripPrefix("/api/v1", apiHandler))
	//
	// Without extra middlewares, mount directly:
	srv.Mux.Handle("/api/v1/", http.StripPrefix("/api/v1", apiMux))

	// TODO: register your handlers into apiMux
	// srv.Use(yourhandler.New(&yourhandler.Config{
	//     Mux:      apiMux,
	//     Services: services,
	// }))
	_ = apiMux

	return srv
}
