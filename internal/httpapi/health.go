package httpapi

import (
	"log/slog"
	"net/http"
)

func handleHealthz(logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := encode(w, http.StatusOK, map[string]string{
			"status": "ok",
		}); err != nil {
			logger.Error("encode health response", "error", err)
		}
	})
}

func handleReadyz(logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := encode(w, http.StatusOK, map[string]string{
			"status": "ready",
		}); err != nil {
			logger.Error("encode readiness response", "error", err)
		}
	})
}