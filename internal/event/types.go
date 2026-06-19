package event

import (
	"context"
	"encoding/json"
	"time"
)

// Event is the parsed, validated envelope passed to processors.
type Event struct {
	Version   int             `json:"version"`
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	TenantID  string          `json:"tenantId"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`

	// Kafka metadata — populated by the consumer layer.
	Topic     string
	Partition int32
	Offset    int64

	// Retry metadata — populated when reprocessing a republished retry.
	RetryCount       int
	FailureReason    string
	FirstFailureTime *time.Time
}

// Processor is the contract every business handler must satisfy.
type Processor interface {
	Process(ctx context.Context, e Event) error
}

// RawMessage wraps a raw Kafka record before envelope parsing.
type RawMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Context key type — unexported to avoid collisions.
type contextKey string

const (
	CtxTenantID        contextKey = "tenantId"
	CtxEventID         contextKey = "eventId"
	CtxTopic           contextKey = "topic"
	CtxPartition       contextKey = "partition"
	CtxOffset          contextKey = "offset"
	CtxProcessingStart contextKey = "processingStartTime"
	CtxProcessor       contextKey = "processor"
)

// EnrichContext attaches all required observability fields to a context.
func EnrichContext(ctx context.Context, e Event, processorName string) context.Context {
	ctx = context.WithValue(ctx, CtxTenantID, e.TenantID)
	ctx = context.WithValue(ctx, CtxEventID, e.EventID)
	ctx = context.WithValue(ctx, CtxTopic, e.Topic)
	ctx = context.WithValue(ctx, CtxPartition, e.Partition)
	ctx = context.WithValue(ctx, CtxOffset, e.Offset)
	ctx = context.WithValue(ctx, CtxProcessingStart, time.Now())
	ctx = context.WithValue(ctx, CtxProcessor, processorName)
	return ctx
}
