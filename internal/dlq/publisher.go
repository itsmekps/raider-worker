package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/metrics"
)

// Record is the full DLQ payload — original event plus failure metadata.
type Record struct {
	OriginalPayload json.RawMessage `json:"originalPayload"`
	Topic           string          `json:"topic"`
	Partition       int32           `json:"partition"`
	Offset          int64           `json:"offset"`
	EventType       string          `json:"eventType"`
	TenantID        string          `json:"tenantId"`
	EventID         string          `json:"eventId"`
	FailureReason   string          `json:"failureReason"`
	RetryCount      int             `json:"retryCount"`
	Timestamp       time.Time       `json:"timestamp"`
}

// Publisher sends events to their topic-specific DLQ.
type Publisher struct {
	client *kgo.Client
}

func NewPublisher(client *kgo.Client) *Publisher {
	return &Publisher{client: client}
}

// Publish writes a DLQ record to <originalTopic>.dlq.
func (p *Publisher) Publish(ctx context.Context, e event.Event, rawValue []byte, failureReason string, retryCount int) {
	dlqTopic := e.Topic + ".dlq"

	rec := Record{
		OriginalPayload: rawValue,
		Topic:           e.Topic,
		Partition:       e.Partition,
		Offset:          e.Offset,
		EventType:       e.EventType,
		TenantID:        e.TenantID,
		EventID:         e.EventID,
		FailureReason:   failureReason,
		RetryCount:      retryCount,
		Timestamp:       time.Now().UTC(),
	}

	payload, err := json.Marshal(rec)
	if err != nil {
		logger.Get().Error("dlq: failed to marshal record",
			zap.String("eventId", e.EventID),
			zap.Error(err),
		)
		return
	}

	p.client.Produce(ctx, &kgo.Record{
		Topic: dlqTopic,
		Key:   []byte(e.TenantID),
		Value: payload,
	}, func(_ *kgo.Record, produceErr error) {
		if produceErr != nil {
			logger.Get().Error("dlq: failed to produce record",
				zap.String("dlqTopic", dlqTopic),
				zap.String("eventId", e.EventID),
				zap.Error(produceErr),
			)
			return
		}
		metrics.MessagesDLQ.WithLabelValues(e.Topic, e.EventType, e.TenantID).Inc()
		logger.Get().Warn("event sent to DLQ",
			zap.String("dlqTopic", dlqTopic),
			zap.String("eventId", e.EventID),
			zap.String("tenantId", e.TenantID),
			zap.String("reason", failureReason),
		)
	})
}

// PublishRaw sends a raw (unparseable) message to a topic's DLQ.
// Used when envelope parsing itself fails.
func (p *Publisher) PublishRaw(ctx context.Context, raw event.RawMessage, failureReason string) {
	dlqTopic := raw.Topic + ".dlq"

	rec := Record{
		OriginalPayload: raw.Value,
		Topic:           raw.Topic,
		Partition:       raw.Partition,
		Offset:          raw.Offset,
		FailureReason:   failureReason,
		Timestamp:       time.Now().UTC(),
	}

	payload, err := json.Marshal(rec)
	if err != nil {
		logger.Get().Error("dlq: failed to marshal raw record", zap.Error(err))
		return
	}

	p.client.Produce(ctx, &kgo.Record{
		Topic: dlqTopic,
		Key:   raw.Key,
		Value: payload,
	}, func(_ *kgo.Record, produceErr error) {
		if produceErr != nil {
			logger.Get().Error("dlq: failed to produce raw record",
				zap.String("dlqTopic", dlqTopic),
				zap.String("topic", raw.Topic),
				zap.Int64("offset", raw.Offset),
				zap.Error(produceErr),
			)
			return
		}
		logger.Get().Warn("raw message sent to DLQ",
			zap.String("dlqTopic", dlqTopic),
			zap.String("reason", fmt.Sprintf("parse failure: %s", failureReason)),
		)
	})
}
