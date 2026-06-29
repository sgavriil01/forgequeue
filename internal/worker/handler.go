package worker

import (
	"context"

	db "github.com/sgavriil01/forgequeue/internal/db/sqlc"
)

type JobHandler interface {
	Kind() string
	Handle(ctx context.Context, job db.Job) error
}

type HandlerFunc struct {
	kind string
	fn   func(ctx context.Context, job db.Job) error
}

func NewHandlerFunc(kind string, fn func(ctx context.Context, job db.Job) error) HandlerFunc {
	return HandlerFunc{
		kind: kind,
		fn:   fn,
	}
}

func (h HandlerFunc) Kind() string {
	return h.kind
}

func (h HandlerFunc) Handle(ctx context.Context, job db.Job) error {
	return h.fn(ctx, job)
}