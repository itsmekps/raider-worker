package consumer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/config"
	"github.com/raider/worker/internal/dlq"
	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/middleware"
	"github.com/raider/worker/internal/registry"
	"github.com/raider/worker/internal/retry"
	"github.com/raider/worker/internal/workerpool"
)

// Consumer owns the franz-go client, worker pools, and the main polling loop.
type Consumer struct {
	cfg      *config.Config
	client   *kgo.Client
	registry *registry.Registry
	pipeline *middleware.Pipeline
	retry    *retry.Publisher
	dlq      *dlq.Publisher
	pools    map[string]*workerpool.Pool

	stopOnce sync.Once
	stopCh   chan struct{}
}

func New(
	cfg *config.Config,
	client *kgo.Client,
	reg *registry.Registry,
	pipeline *middleware.Pipeline,
	retryPub *retry.Publisher,
	dlqPub *dlq.Publisher,
) *Consumer {
	pools := map[string]*workerpool.Pool{
		"cases":         workerpool.New("cases", cfg.Workers.CaseCount, cfg.Workers.QueueBufferSize),
		"payments":      workerpool.New("payments", cfg.Workers.PaymentCount, cfg.Workers.QueueBufferSize),
		"visits":        workerpool.New("visits", cfg.Workers.VisitCount, cfg.Workers.QueueBufferSize),
		"notifications": workerpool.New("notifications", cfg.Workers.NotificationCount, cfg.Workers.QueueBufferSize),
	}

	return &Consumer{
		cfg:      cfg,
		client:   client,
		registry: reg,
		pipeline: pipeline,
		retry:    retryPub,
		dlq:      dlqPub,
		pools:    pools,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the poll loop. Blocks until ctx is cancelled or Stop() is called.
func (c *Consumer) Start(ctx context.Context) {
	log := logger.Get()
	log.Info("consumer starting", zap.Strings("topics", c.cfg.Kafka.Topics))

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stopCh:
			return
		default:
		}

		fetches := c.client.PollFetches(ctx)

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) {
					return
				}
				log.Error("kafka fetch error",
					zap.String("topic", fe.Topic),
					zap.Int32("partition", fe.Partition),
					zap.Error(fe.Err),
				)
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			raw := event.RawMessage{
				Topic:     rec.Topic,
				Partition: rec.Partition,
				Offset:    rec.Offset,
				Key:       rec.Key,
				Value:     rec.Value,
			}
			c.dispatch(ctx, raw, rec)
		})
	}
}

// dispatch routes a single raw Kafka record to the correct worker pool.
func (c *Consumer) dispatch(ctx context.Context, raw event.RawMessage, rec *kgo.Record) {
	log := logger.With(
		zap.String("topic", raw.Topic),
		zap.Int32("partition", raw.Partition),
		zap.Int64("offset", raw.Offset),
	)

	// Parse and validate envelope — non-retriable on failure.
	e, err := event.ParseAndValidate(raw)
	if err != nil {
		log.Warn("envelope parse failed — sending to DLQ", zap.Error(err))
		c.dlq.PublishRaw(ctx, raw, err.Error())
		c.commitOffset(ctx, rec)
		return
	}

	// Resolve processor before dispatching to the pool.
	processor, err := c.registry.Resolve(e.EventType, e.Version)
	if err != nil {
		var unknown *event.ErrUnknownEventType
		if errors.As(err, &unknown) {
			log.Warn("unknown event type — sending to DLQ",
				zap.String("eventType", e.EventType),
				zap.Int("version", e.Version),
				zap.String("eventId", e.EventID),
				zap.String("tenantId", e.TenantID),
			)
		} else {
			log.Error("registry resolve error", zap.Error(err))
		}
		c.dlq.Publish(ctx, e, raw.Value, err.Error(), 0)
		c.commitOffset(ctx, rec)
		return
	}

	pool, ok := c.pools[topicBase(raw.Topic)]
	if !ok {
		log.Warn("no pool configured for topic — dropping to DLQ", zap.String("topic", raw.Topic))
		c.dlq.Publish(ctx, e, raw.Value, "no worker pool for topic", 0)
		c.commitOffset(ctx, rec)
		return
	}

	// Backpressure: pause topic fetching when queue is saturated.
	if pool.IsFull() {
		log.Warn("worker pool full — pausing fetch", zap.String("topic", raw.Topic))
		c.client.PauseFetchTopics(raw.Topic)
	}

	processorName := e.EventType
	capturedEvent := e
	capturedRaw := make([]byte, len(raw.Value))
	copy(capturedRaw, raw.Value)

	pool.Submit(workerpool.Job{
		Ctx: ctx,
		Execute: func(jobCtx context.Context) error {
			defer c.commitOffset(jobCtx, rec)
			return c.processWithFallback(jobCtx, capturedEvent, capturedRaw, processorName, processor)
		},
	})

	// Resume once there is room again.
	if !pool.IsFull() {
		c.client.ResumeFetchTopics(raw.Topic)
	}
}

// processWithFallback runs the middleware pipeline and routes errors to retry or DLQ.
func (c *Consumer) processWithFallback(
	ctx context.Context,
	e event.Event,
	rawValue []byte,
	processorName string,
	processor event.Processor,
) error {
	err := c.pipeline.Execute(ctx, e, processorName, processor)
	if err == nil {
		return nil
	}

	if event.IsRetriable(err) && c.retry.ShouldRetry(e.RetryCount) {
		c.retry.Publish(ctx, e, rawValue, err.Error())
	} else {
		c.dlq.Publish(ctx, e, rawValue, err.Error(), e.RetryCount)
	}
	return err
}

// commitOffset manually commits the processed record's offset.
func (c *Consumer) commitOffset(ctx context.Context, rec *kgo.Record) {
	if err := c.client.CommitRecords(ctx, rec); err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Get().Error("failed to commit offset",
				zap.String("topic", rec.Topic),
				zap.Int32("partition", rec.Partition),
				zap.Int64("offset", rec.Offset),
				zap.Error(err),
			)
		}
	}
}

// Stop signals shutdown: stops polling, drains pools, then flushes offsets.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		logger.Get().Info("consumer stopping — draining worker pools")
		close(c.stopCh)

		var wg sync.WaitGroup
		for topic, pool := range c.pools {
			wg.Add(1)
			go func(t string, p *workerpool.Pool) {
				defer wg.Done()
				p.Stop()
				logger.Get().Info("worker pool drained", zap.String("topic", t))
			}(topic, pool)
		}
		wg.Wait()

		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.client.CommitUncommittedOffsets(flushCtx); err != nil {
			logger.Get().Error("failed to flush offsets on shutdown", zap.Error(err))
		}

		logger.Get().Info("consumer stopped cleanly")
	})
}

// topicBase strips .retry / .dlq suffixes to find the base pool key.
func topicBase(topic string) string {
	for _, suffix := range []string{".retry", ".dlq"} {
		if len(topic) > len(suffix) && topic[len(topic)-len(suffix):] == suffix {
			return topic[:len(topic)-len(suffix)]
		}
	}
	return topic
}
