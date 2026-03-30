package telemetry

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

type Server struct {
	aggregator *Aggregator
	mux        *http.ServeMux
}

func NewServer(aggregator *Aggregator) *Server {
	mux := http.NewServeMux()
	server := &Server{
		aggregator: aggregator,
		mux:        mux,
	}
	mux.HandleFunc("/status/live", server.handleLive)
	mux.HandleFunc("/status/history", server.handleHistory)
	mux.HandleFunc("/status/health", server.handleHealth)
	mux.HandleFunc("/status/stream", server.handleStream)
	mux.HandleFunc("/status", server.handleIndex)
	mux.HandleFunc("/status/", server.handleIndex)
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.aggregator.Snapshot())
}

func (s *Server) handleHistory(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshots": s.aggregator.History(),
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.aggregator.Snapshot().Health)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch, cancel := s.aggregator.Subscribe()
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	for {
		select {
		case snapshot, ok := <-ch:
			if !ok {
				return
			}
			payload, err := json.Marshal(snapshot)
			if err != nil {
				http.Error(w, "encode snapshot", http.StatusInternalServerError)
				return
			}
			if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", payload); err != nil {
				return
			}
			flusher.Flush()
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	content, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "status ui unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(content)
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
