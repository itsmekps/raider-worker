package middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/idempotency"
	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/metrics"
)

// Pipeline executes the full middleware chain in spec order:
// Context Enrichment → Idempotency Check → Metrics → Recovery → Timeout → Business Processor
type Pipeline struct {
	idempotency       idempotency.Store
	processingTimeout time.Duration
}

func NewPipeline(store idempotency.Store, processingTimeout time.Duration) *Pipeline {
	return &Pipeline{
		idempotency:       store,
		processingTimeout: processingTimeout,
	}
}

// Execute runs the full chain for a single event.
// processorName is used for logging and metrics labels.
func (p *Pipeline) Execute(
	ctx context.Context,
	e event.Event,
	processorName string,
	processor event.Processor,
) error {
	// Context enrichment
	ctx = event.EnrichContext(ctx, e, processorName)

	log := logger.With(
		zap.String("tenantId", e.TenantID),
		zap.String("eventId", e.EventID),
		zap.String("topic", e.Topic),
		zap.Int32("partition", e.Partition),
		zap.Int64("offset", e.Offset),
		zap.String("processor", processorName),
		zap.String("eventType", e.EventType),
	)

	start := time.Now()

	// Metrics: record receipt
	metrics.MessagesReceived.WithLabelValues(e.Topic, e.TenantID).Inc()

	// Idempotency check
	already, err := p.idempotency.IsProcessed(ctx, e.EventID)
	if err != nil {
		log.Warn("idempotency check failed — proceeding without dedup", zap.Error(err))
	}
	if already {
		log.Info("duplicate event skipped")
		return nil
	}

	// Timeout wrapper
	timeoutCtx, cancel := context.WithTimeout(ctx, p.processingTimeout)
	defer cancel()

	// Panic recovery + business processor execution
	var processingErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				log.Error("panic recovered in processor",
					zap.Any("panic", r),
					zap.ByteString("stack", stack),
				)
				metrics.MessagesFailed.WithLabelValues(
					e.Topic, e.EventType, e.TenantID, "panic",
				).Inc()
				processingErr = fmt.Errorf("panic: %v", r)
			}
		}()
		processingErr = processor.Process(timeoutCtx, e)
	}()

	duration := time.Since(start)
	metrics.ProcessingDuration.WithLabelValues(e.Topic, e.EventType, processorName).Observe(duration.Seconds())

	if processingErr != nil {
		metrics.MessagesFailed.WithLabelValues(
			e.Topic, e.EventType, e.TenantID, "processing_error",
		).Inc()
		log.Error("processor failed",
			zap.Error(processingErr),
			zap.Duration("duration", duration),
		)
		return processingErr
	}

	// Mark complete in idempotency store
	if markErr := p.idempotency.MarkProcessed(ctx, e.EventID); markErr != nil {
		log.Warn("failed to mark event as processed in idempotency store", zap.Error(markErr))
	}

	metrics.MessagesProcessed.WithLabelValues(e.Topic, e.EventType, e.TenantID).Inc()
	log.Info("event processed successfully", zap.Duration("duration", duration))
	return nil
}
