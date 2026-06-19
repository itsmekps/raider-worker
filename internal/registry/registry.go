package registry

import (
	"fmt"
	"sync"

	"github.com/raider/worker/internal/event"
)

// versionKey uniquely identifies a processor by event type + version.
type versionKey struct {
	eventType string
	version   int
}

// Registry maps (eventType, version) pairs to event.Processor implementations.
// All methods are safe for concurrent use after initial registration.
type Registry struct {
	mu         sync.RWMutex
	processors map[versionKey]event.Processor
}

func New() *Registry {
	return &Registry{
		processors: make(map[versionKey]event.Processor),
	}
}

// Register associates a Processor with an eventType and version.
// Panics on duplicate registration to catch wiring mistakes at startup.
func (r *Registry) Register(eventType string, version int, p event.Processor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := versionKey{eventType: eventType, version: version}
	if _, exists := r.processors[k]; exists {
		panic(fmt.Sprintf("registry: duplicate processor for %s v%d", eventType, version))
	}
	r.processors[k] = p
}

// Resolve returns the processor for (eventType, version).
// Returns ErrUnknownEventType if no processor is found.
func (r *Registry) Resolve(eventType string, version int) (event.Processor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	k := versionKey{eventType: eventType, version: version}
	p, ok := r.processors[k]
	if !ok {
		return nil, &event.ErrUnknownEventType{EventType: fmt.Sprintf("%s v%d", eventType, version)}
	}
	return p, nil
}

// Has reports whether a processor is registered for the given type and version.
func (r *Registry) Has(eventType string, version int) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.processors[versionKey{eventType: eventType, version: version}]
	return ok
}
