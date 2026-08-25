package api

import (
	"io/fs"
	"log"
	"net/http"

	"wal-demo/web"
)

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	handleMethod(mux, http.MethodPost, "/api/v1/write", s.handleWrite)
	handleMethod(mux, http.MethodGet, "/api/v1/kv", s.handleItems)
	mux.HandleFunc("/api/v1/kv/", s.handleKeyRoute)
	handleMethod(mux, http.MethodPost, "/api/v1/write/batch", s.handleBatch)
	handleMethod(mux, http.MethodPost, "/api/v1/sync", s.handleSync)
	handleMethod(mux, http.MethodPost, "/api/v1/snapshot", s.handleSnapshot)
	handleMethod(mux, http.MethodGet, "/api/v1/snapshots", s.handleSnapshots)
	mux.HandleFunc("/api/v1/snapshots/", s.handleSnapshotRoute)
	handleMethod(mux, http.MethodGet, "/api/v1/log", s.handleLog)
	handleMethod(mux, http.MethodGet, "/api/v1/metrics", s.handleMetrics)
	handleMethod(mux, http.MethodGet, "/api/v1/status", s.handleStatus)
	handleMethod(mux, http.MethodGet, "/api/v1/health", s.handleHealth)
	handleMethod(mux, http.MethodGet, "/api/v1/segments", s.handleSegments)
	handleMethod(mux, http.MethodPost, "/api/v1/verify", s.handleVerify)
	handleMethod(mux, http.MethodPost, "/api/v1/corrupt", s.handleCorrupt)
	handleMethod(mux, http.MethodPost, "/api/v1/recover", s.handleRecover)
	handleMethod(mux, http.MethodPost, "/api/v1/crash", s.handleCrash)
	assets, err := fs.Sub(web.Assets, ".")
	if err != nil {
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(assets)))
	return recoverMiddleware(logMiddleware(mux))
}

func handleMethod(mux *http.ServeMux, method, path string, handler http.HandlerFunc) {
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		handler(w, r)
	})
}

func logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s", r.Method, r.URL.RequestURI())
		next.ServeHTTP(w, r)
	})
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("request panic: %v", recovered)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
