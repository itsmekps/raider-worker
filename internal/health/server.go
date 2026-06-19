package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
)

// Checker is any dependency that can report its health.
type Checker interface {
	Ping(ctx context.Context) error
}

// Server exposes /health and /ready endpoints for container orchestration.
type Server struct {
	server   *http.Server
	checkers map[string]Checker
}

type healthResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Checks    map[string]string `json:"checks,omitempty"`
}

func NewServer(port string, checkers map[string]Checker) *Server {
	mux := http.NewServeMux()
	s := &Server{
		checkers: checkers,
		server: &http.Server{
			Addr:         ":" + port,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 5 * time.Second,
		},
	}

	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	return s
}

// handleHealth is a liveness probe — always returns 200 if the process is running.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Status:    "ok",
		Timestamp: time.Now().UTC(),
	})
}

// handleReady is a readiness probe — returns 200 only when all dependency checks pass.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	checks := make(map[string]string, len(s.checkers))
	allOK := true

	for name, checker := range s.checkers {
		if err := checker.Ping(ctx); err != nil {
			checks[name] = "unhealthy: " + err.Error()
			allOK = false
		} else {
			checks[name] = "ok"
		}
	}

	status := "ok"
	code := http.StatusOK
	if !allOK {
		status = "degraded"
		code = http.StatusServiceUnavailable
	}

	writeJSON(w, code, healthResponse{
		Status:    status,
		Timestamp: time.Now().UTC(),
		Checks:    checks,
	})
}

func (s *Server) Start() {
	go func() {
		logger.Get().Info("health server listening", zap.String("addr", s.server.Addr))
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Get().Error("health server error", zap.Error(err))
		}
	}()
}

func (s *Server) Stop(ctx context.Context) {
	if err := s.server.Shutdown(ctx); err != nil {
		logger.Get().Error("health server shutdown error", zap.Error(err))
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
