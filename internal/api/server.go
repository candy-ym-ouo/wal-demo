package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"wal-demo/internal/store"
)

// Config controls the demo server's network and dangerous endpoints.
type Config struct {
	Address    string
	AllowCrash bool
}

// Server owns the HTTP listener and handler dependencies.
type Server struct {
	store *store.Store
	cfg   Config
	http  *http.Server
}

func New(st *store.Store, cfg Config) *Server {
	if cfg.Address == "" {
		cfg.Address = "127.0.0.1:8888"
	}
	s := &Server{store: st, cfg: cfg}
	s.http = &http.Server{
		Addr:              cfg.Address,
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return s
}

func (s *Server) ListenAndServe() error {
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func (s *Server) Handler() http.Handler { return s.http.Handler }
