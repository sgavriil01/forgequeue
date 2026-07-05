package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"
)

type requestIDContextKey struct{}

func requestIDFromContext(ctx context.Context) string {
	value, ok := ctx.Value(requestIDContextKey{}).(string)
	if !ok {
		return ""
	}

	return value
}

func logRequests(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()

			requestID := r.Header.Get("X-Request-ID")
			if requestID == "" {
				requestID = newRequestID()
			}

			ctx := context.WithValue(r.Context(), requestIDContextKey{}, requestID)
			r = r.WithContext(ctx)

			w.Header().Set("X-Request-ID", requestID)

			lrw := &loggingResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(lrw, r)

			logger.Info(
				"http request completed",
				"request_id", requestID,
				"method", r.Method,
				"path", r.URL.Path,
				"status", lrw.statusCode,
				"duration", time.Since(startedAt).String(),
			)
		})
	}
}

func recoverPanic(logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if value := recover(); value != nil {
					logger.Error(
						"panic recovered",
						"request_id", requestIDFromContext(r.Context()),
						"method", r.Method,
						"path", r.URL.Path,
						"panic", value,
					)

					http.Error(w, "internal server error", http.StatusInternalServerError)
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *loggingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.ResponseWriter.WriteHeader(statusCode)
}

func newRequestID() string {
	var bytes [16]byte

	if _, err := rand.Read(bytes[:]); err != nil {
		return time.Now().Format("20060102150405.000000000")
	}

	return hex.EncodeToString(bytes[:])
}
