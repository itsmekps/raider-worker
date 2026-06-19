package retry

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

// RetryEnvelope wraps the original event with retry metadata that travels with it.
type RetryEnvelope struct {
	Version   int             `json:"version"`
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	TenantID  string          `json:"tenantId"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`

	OriginalTopic    string    `json:"originalTopic"`
	RetryCount       int       `json:"retryCount"`
	FailureReason    string    `json:"failureReason"`
	FirstFailureTime time.Time `json:"firstFailureTime"`
	LastFailureTime  time.Time `json:"lastFailureTime"`
	NextRetryAfter   time.Time `json:"nextRetryAfter"`
}

// exponential backoff schedule: 30s, 1m, 5m, 15m, 30m
var backoffSchedule = []time.Duration{
	30 * time.Second,
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
}

// ParseRetryEnvelope deserialises a message consumed from a "<topic>.retry"
// topic. These are not processed directly — they are handed to the
// Scheduler, which holds them until NextRetryAfter is due.
func ParseRetryEnvelope(raw []byte) (RetryEnvelope, error) {
	var env RetryEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return RetryEnvelope{}, fmt.Errorf("unmarshal retry envelope: %w", err)
	}
	if env.EventID == "" || env.OriginalTopic == "" {
		return RetryEnvelope{}, fmt.Errorf("invalid retry envelope: missing eventId or originalTopic")
	}
	return env, nil
}

// Publisher sends events to retry topics with exponential backoff metadata.
type Publisher struct {
	client     *kgo.Client
	maxRetries int
}

func NewPublisher(client *kgo.Client, maxRetries int) *Publisher {
	return &Publisher{client: client, maxRetries: maxRetries}
}

// ShouldRetry returns true when the event has not exhausted its retry budget.
func (p *Publisher) ShouldRetry(retryCount int) bool {
	return retryCount < p.maxRetries
}

// Publish sends the event to <originalTopic>.retry with incremented retry metadata.
func (p *Publisher) Publish(ctx context.Context, e event.Event, rawValue []byte, failureReason string) {
	retryTopic := e.Topic + ".retry"
	now := time.Now().UTC()

	retryCount := e.RetryCount + 1
	firstFailure := now
	if e.FirstFailureTime != nil {
		firstFailure = *e.FirstFailureTime
	}

	backoffIdx := retryCount - 1
	if backoffIdx >= len(backoffSchedule) {
		backoffIdx = len(backoffSchedule) - 1
	}
	nextRetry := now.Add(backoffSchedule[backoffIdx])

	env := RetryEnvelope{
		Version:          e.Version,
		EventID:          e.EventID,
		EventType:        e.EventType,
		TenantID:         e.TenantID,
		Timestamp:        e.Timestamp,
		Data:             e.Data,
		OriginalTopic:    e.Topic,
		RetryCount:       retryCount,
		FailureReason:    failureReason,
		FirstFailureTime: firstFailure,
		LastFailureTime:  now,
		NextRetryAfter:   nextRetry,
	}

	payload, err := json.Marshal(env)
	if err != nil {
		logger.Get().Error("retry: failed to marshal retry envelope",
			zap.String("eventId", e.EventID),
			zap.Error(err),
		)
		return
	}

	p.client.Produce(ctx, &kgo.Record{
		Topic: retryTopic,
		Key:   []byte(e.TenantID),
		Value: payload,
	}, func(_ *kgo.Record, produceErr error) {
		if produceErr != nil {
			logger.Get().Error("retry: failed to produce to retry topic",
				zap.String("retryTopic", retryTopic),
				zap.String("eventId", e.EventID),
				zap.Error(produceErr),
			)
			return
		}
		metrics.MessagesRetried.WithLabelValues(e.Topic, e.EventType, e.TenantID).Inc()
		logger.Get().Warn("event sent to retry topic",
			zap.String("retryTopic", retryTopic),
			zap.String("eventId", e.EventID),
			zap.String("tenantId", e.TenantID),
			zap.Int("retryCount", retryCount),
			zap.Time("nextRetryAfter", nextRetry),
			zap.String("reason", failureReason),
		)
	})
}
