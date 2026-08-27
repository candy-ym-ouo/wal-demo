package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"wal-demo/internal/api"
	"wal-demo/internal/store"
	"wal-demo/internal/wal"
)

func main() {
	var (
		address    = flag.String("addr", "127.0.0.1:8888", "HTTP listen address")
		dataDir    = flag.String("data", "./data", "directory for WAL segments and snapshot")
		allowCrash = flag.Bool("allow-crash", false, "enable loopback crash simulation endpoint")
	)
	flag.Parse()

	absDir, err := filepath.Abs(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	logHandle, err := wal.Open(absDir, wal.DefaultOptions())
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}
	defer func() {
		if err := logHandle.Close(); err != nil {
			log.Printf("close WAL: %v", err)
		}
	}()
	st, err := store.New(logHandle, absDir)
	if err != nil {
		log.Fatalf("recover store: %v", err)
	}
	server := api.New(st, api.Config{Address: *address, AllowCrash: *allowCrash})
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	log.Printf("WAL demo listening on http://%s (data: %s)", *address, absDir)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		log.Printf("received %s, shutting down", sig)
	case err := <-errCh:
		if err != nil {
			log.Fatalf("HTTP server: %v", err)
		}
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown: %v", err)
	}
}
