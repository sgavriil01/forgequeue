package httpapi

import (
	"log/slog"
	"net/http"
)

func NewServer(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()

	addRoutes(mux, logger)

	var handler http.Handler = mux
	handler = recoverPanic(logger, handler)
	handler = logRequests(logger, handler)

	return handler
}