package httpapi

import (
	"log/slog"
	"net/http"
)

func addRoutes(mux *http.ServeMux, logger *slog.Logger) {
	mux.Handle("GET /healthz", handleHealthz(logger))
	mux.Handle("GET /readyz", handleReadyz(logger))
}