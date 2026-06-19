package event

import (
	"errors"
	"fmt"
)

// ErrInvalidEnvelope is non-retriable — goes straight to DLQ.
type ErrInvalidEnvelope struct {
	Reason string
}

func (e *ErrInvalidEnvelope) Error() string {
	return fmt.Sprintf("invalid envelope: %s", e.Reason)
}

// ErrBusinessValidation is non-retriable — goes straight to DLQ.
type ErrBusinessValidation struct {
	Reason string
}

func (e *ErrBusinessValidation) Error() string {
	return "business validation: " + e.Reason
}

// ErrUnknownEventType signals no processor is registered for this event type + version.
type ErrUnknownEventType struct {
	EventType string
}

func (e *ErrUnknownEventType) Error() string {
	return "no processor registered for event type: " + e.EventType
}

// IsRetriable classifies whether an error should go to the retry topic or DLQ.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}
	var envelope *ErrInvalidEnvelope
	if errors.As(err, &envelope) {
		return false
	}
	var biz *ErrBusinessValidation
	if errors.As(err, &biz) {
		return false
	}
	return true
}
