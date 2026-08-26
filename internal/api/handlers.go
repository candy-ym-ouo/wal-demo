package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"wal-demo/internal/store"
	"wal-demo/internal/wal"
)

type writeRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	var request writeRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	seq, err := s.store.Set(request.Key, []byte(request.Value))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"seq": seq})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	seq, err := s.store.Delete(keyFromPath(r))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"seq": seq})
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := keyFromPath(r)
	value, err := s.store.Get(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": key, "value": string(value)})
}

func (s *Server) handleKeyRoute(w http.ResponseWriter, r *http.Request) {
	if keyFromPath(r) == "" {
		writeError(w, http.StatusBadRequest, "key must not be empty")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r)
	case http.MethodDelete:
		s.handleDelete(w, r)
	default:
		w.Header().Set("Allow", "GET, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func keyFromPath(r *http.Request) string {
	return strings.TrimPrefix(r.URL.Path, "/api/v1/kv/")
}

func (s *Server) handleItems(w http.ResponseWriter, r *http.Request) {
	offset := queryInt(r, "offset", 0)
	limit := queryInt(r, "limit", 50)
	if offset < 0 || limit < 1 || limit > 500 {
		writeError(w, http.StatusBadRequest, "offset must be non-negative and limit must be 1..500")
		return
	}
	writeJSON(w, http.StatusOK, s.store.ItemsPage(offset, limit))
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	metrics := s.store.WAL().Metrics()
	writeJSON(w, http.StatusOK, map[string]any{
		"keys":       s.store.Count(),
		"appliedSeq": s.store.AppliedSeq(),
		"lastSeq":    metrics.LastSeq,
		"durableSeq": metrics.DurableSeq,
		"pending":    metrics.PendingBytes,
	})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Count int `json:"count"`
	}
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if request.Count < 1 || request.Count > 1000 {
		writeError(w, http.StatusBadRequest, "count must be between 1 and 1000")
		return
	}
	stamp := time.Now().UnixNano()
	pairs := make([]store.Pair, 0, request.Count)
	for i := 0; i < request.Count; i++ {
		key := fmt.Sprintf("batch-%d-%04d", stamp, i)
		pairs = append(pairs, store.Pair{Key: key, Value: []byte(fmt.Sprintf("value-%04d", i))})
	}
	seqs, err := s.store.SetBatch(pairs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"seqs": seqs, "durable": s.store.WAL().DurableSeq()})
}

func (s *Server) handleSync(w http.ResponseWriter, _ *http.Request) {
	seq, err := s.store.WAL().Sync()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"durableSeq": seq})
}

func (s *Server) handleSnapshot(w http.ResponseWriter, _ *http.Request) {
	seq, err := s.store.Snapshot()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshotSeq": seq})
}

func (s *Server) handleSnapshots(w http.ResponseWriter, _ *http.Request) {
	snapshots, err := s.store.ListSnapshots()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": snapshots})
}

func (s *Server) handleSnapshotRoute(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/api/v1/snapshots/")
	if name == "" || strings.Contains(name, "/") {
		writeError(w, http.StatusBadRequest, "snapshot name must be a single path segment")
		return
	}
	switch r.Method {
	case http.MethodPost:
		info, err := s.store.SaveSnapshot(name)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, info)
	case http.MethodGet:
		info, err := s.store.InspectSnapshot(name)
		if errors.Is(err, os.ErrNotExist) {
			writeError(w, http.StatusNotFound, "snapshot not found")
			return
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, info)
	case http.MethodDelete:
		if err := s.store.DeleteSnapshot(name); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				writeError(w, http.StatusNotFound, "snapshot not found")
				return
			}
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, POST, DELETE")
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleLog(w http.ResponseWriter, r *http.Request) {
	from := queryInt(r, "from", 1)
	limit := queryInt(r, "limit", 50)
	if from < 1 || limit < 1 || limit > 500 {
		writeError(w, http.StatusBadRequest, "from must be positive and limit must be 1..500")
		return
	}
	entries, err := s.store.WAL().ReadRange(r.Context(), uint64(from), limit)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": entries})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.WAL().Metrics())
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	metrics := s.store.WAL().Metrics()
	status := "ok"
	code := http.StatusOK
	if metrics.ChecksumFailures > 0 {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}
	writeJSON(w, code, map[string]any{
		"status":     status,
		"keys":       s.store.Count(),
		"appliedSeq": s.store.AppliedSeq(),
		"durableSeq": metrics.DurableSeq,
	})
}

func (s *Server) handleSegments(w http.ResponseWriter, _ *http.Request) {
	segments := s.store.WAL().SegmentInfo()
	writeJSON(w, http.StatusOK, map[string]any{
		"count":    len(segments),
		"segments": segments,
	})
}

func (s *Server) handleVerify(w http.ResponseWriter, _ *http.Request) {
	result, err := s.store.WAL().Verify()
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":        err.Error(),
			"verification": result,
		})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleCorrupt(w http.ResponseWriter, r *http.Request) {
	seq := queryInt(r, "seq", 0)
	if seq < 1 {
		writeError(w, http.StatusBadRequest, "seq is required")
		return
	}
	if err := s.store.WAL().Corrupt(uint64(seq)); err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, wal.ErrNotFound) {
			status = http.StatusNotFound
		}
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"corruptedSeq": seq})
}

func (s *Server) handleRecover(w http.ResponseWriter, _ *http.Request) {
	result, err := s.store.WAL().Recover(nil)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	stats, err := s.store.Reload()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovery": result, "replay": stats})
}

func (s *Server) handleCrash(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		writeError(w, http.StatusForbidden, "crash simulation is loopback-only")
		return
	}
	if !s.cfg.AllowCrash {
		writeError(w, http.StatusForbidden, "start with -allow-crash to enable process termination")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"message": "process exits in 200ms; restart to observe recovery"})
	go func() {
		time.Sleep(200 * time.Millisecond)
		os.Exit(86)
	}()
}

func decodeJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request must contain one JSON object")
	}
	return nil
}

func queryInt(r *http.Request, name string, fallback int) int {
	text := r.URL.Query().Get(name)
	if text == "" {
		return fallback
	}
	value, err := strconv.Atoi(text)
	if err != nil {
		return fallback
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"error": message})
}
