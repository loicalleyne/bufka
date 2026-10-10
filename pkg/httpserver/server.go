/*
Copyright © 2026
*/
// Package httpserver provides the reusable stdlib net/http server.
//
// Route registration is handled by handlers in
// internal/adapters/primary/http/ via the Use method.
package httpserver

import (
	"context"
	"fmt"
	"net/http"

	"github.com/loicalleyne/bufka/internal/config"
	"github.com/loicalleyne/bufka/pkg/logger"
	srv "github.com/loicalleyne/bufka/pkg/server"
)

var _ srv.Server = (*Server)(nil)

// Server is the stdlib-backed implementation of srv.Server.
// Mux is exported so that handlers registered via Use can mount routes.
type Server struct {
	Mux     *http.ServeMux
	httpSrv *http.Server
	cfg     *config.Config
	log     logger.Logger
}

// ServerConfig is the configuration data for the server.
type ServerConfig struct {
	Config *config.Config
	Logger logger.Logger
}

// New creates a new stdlib HTTP server.
// Route registration happens in internal/adapters/primary/http/http.go
// via srv.Use(handler).
func New(cfg *ServerConfig) srv.Server {
	mux := http.NewServeMux()

	s := &Server{
		Mux: mux,
		httpSrv: &http.Server{
			Addr:         fmt.Sprintf(":%d", cfg.Config.Server.Port),
			Handler:      mux,
			ReadTimeout:  cfg.Config.Server.ReadTimeout,
			WriteTimeout: cfg.Config.Server.WriteTimeout,
			IdleTimeout:  cfg.Config.Server.IdleTimeout,
		},
		cfg: cfg.Config,
		log: cfg.Logger,
	}

	return s
}

// Run starts the HTTP server in a non-blocking goroutine.
func (s *Server) Run(errChan chan<- error) {
	go func() {
		s.log.Info("Server listening on port %d", s.cfg.Server.Port)
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()
}

// Stop gracefully shuts down the stdlib server.
func (s *Server) Stop(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}

// Use registers a handler that configures routes or middleware on this server.
// Returns the server for chaining.
func (s *Server) Use(handler srv.ServerHandler) srv.Server {
	handler.Configure(s)
	return s
}

// Handler provides the base fields for route handlers registered via Use.
// Embed this struct or reference it in your own Config when building a handler
// that mounts routes on the ServeMux.
type Handler struct {
	Path string
	Mux  *http.ServeMux
}
