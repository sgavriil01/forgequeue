package worker

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrInvalidHandler            = errors.New("invalid handler")
	ErrHandlerAlreadyRegistered = errors.New("handler already registered")
)

type Registry struct {
	handlers map[string]JobHandler
}

func NewRegistry(handlers ...JobHandler) (*Registry, error) {
	r := &Registry{
		handlers: make(map[string]JobHandler),
	}

	for _, handler := range handlers {
		if err := r.Register(handler); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *Registry) Register(handler JobHandler) error {
	if handler == nil {
		return ErrInvalidHandler
	}

	kind := strings.TrimSpace(handler.Kind())
	if kind == "" {
		return ErrInvalidHandler
	}

	if _, exists := r.handlers[kind]; exists {
		return fmt.Errorf("%w: %s", ErrHandlerAlreadyRegistered, kind)
	}

	r.handlers[kind] = handler

	return nil
}

func (r *Registry) HandlerFor(kind string) (JobHandler, bool) {
	handler, ok := r.handlers[kind]
	return handler, ok
}