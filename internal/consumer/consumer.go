package consumer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/config"
	"github.com/raider/worker/internal/dlq"
	"github.com/raider/worker/internal/event"
	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/middleware"
	"github.com/raider/worker/internal/offsetmanager"
	"github.com/raider/worker/internal/registry"
	"github.com/raider/worker/internal/retry"
	"github.com/raider/worker/internal/workerpool"
)

// Consumer owns the franz-go client, worker pools, and the main polling loop.
//
// Offsets are never committed by workers. Every dispatch path — success,
// DLQ, or handoff to the retry scheduler — ends in offsets.Complete(), and
// only the Manager decides what is safe to commit (the highest contiguous
// completed offset per partition). See internal/offsetmanager.
type Consumer struct {
	cfg       *config.Config
	client    *kgo.Client
	registry  *registry.Registry
	pipeline  *middleware.Pipeline
	retry     *retry.Publisher
	scheduler *retry.Scheduler
	dlq       *dlq.Publisher
	offsets   *offsetmanager.Manager
	pools     map[string]*workerpool.Pool

	stopOnce sync.Once
	stopCh   chan struct{}
}

func New(
	cfg *config.Config,
	client *kgo.Client,
	reg *registry.Registry,
	pipeline *middleware.Pipeline,
	retryPub *retry.Publisher,
	scheduler *retry.Scheduler,
	dlqPub *dlq.Publisher,
	offsets *offsetmanager.Manager,
) *Consumer {
	bufferPerLane := func(laneCount int) int {
		b := cfg.Workers.QueueBufferSize / laneCount
		if b < 50 {
			b = 50
		}
		return b
	}

	pools := map[string]*workerpool.Pool{
		"cases":         workerpool.New("cases", cfg.Workers.CaseCount, bufferPerLane(cfg.Workers.CaseCount)),
		"payments":      workerpool.New("payments", cfg.Workers.PaymentCount, bufferPerLane(cfg.Workers.PaymentCount)),
		"visits":        workerpool.New("visits", cfg.Workers.VisitCount, bufferPerLane(cfg.Workers.VisitCount)),
		"notifications": workerpool.New("notifications", cfg.Workers.NotificationCount, bufferPerLane(cfg.Workers.NotificationCount)),
	}

	return &Consumer{
		cfg:       cfg,
		client:    client,
		registry:  reg,
		pipeline:  pipeline,
		retry:     retryPub,
		scheduler: scheduler,
		dlq:       dlqPub,
		offsets:   offsets,
		pools:     pools,
		stopCh:    make(chan struct{}),
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
			// Track must happen here, synchronously, in strict poll order —
			// this is what lets the offset manager know the true earliest
			// in-flight offset per partition.
			c.offsets.Track(raw.Topic, raw.Partition, raw.Offset, rec.LeaderEpoch)
			c.dispatch(ctx, raw)
		})
	}
}

// dispatch routes a single raw Kafka record. Retry-topic messages are handed
// to the scheduler (not processed); everything else goes through the normal
// envelope → registry → worker-pool path.
func (c *Consumer) dispatch(ctx context.Context, raw event.RawMessage) {
	if strings.HasSuffix(raw.Topic, ".retry") {
		c.dispatchRetry(ctx, raw)
		return
	}

	log := logger.With(
		zap.String("topic", raw.Topic),
		zap.Int32("partition", raw.Partition),
		zap.Int64("offset", raw.Offset),
	)

	e, err := event.ParseAndValidate(raw)
	if err != nil {
		log.Warn("envelope parse failed — sending to DLQ", zap.Error(err))
		c.dlq.PublishRaw(ctx, raw, err.Error())
		c.offsets.Complete(raw.Topic, raw.Partition, raw.Offset)
		return
	}

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
		c.offsets.Complete(raw.Topic, raw.Partition, raw.Offset)
		return
	}

	pool, ok := c.pools[raw.Topic]
	if !ok {
		log.Warn("no pool configured for topic — dropping to DLQ", zap.String("topic", raw.Topic))
		c.dlq.Publish(ctx, e, raw.Value, "no worker pool for topic", 0)
		c.offsets.Complete(raw.Topic, raw.Partition, raw.Offset)
		return
	}

	key := e.AffinityKey()
	if pool.IsLaneFull(key) {
		log.Warn("worker lane saturated — backpressure engaged",
			zap.String("topic", raw.Topic), zap.String("affinityKey", key))
	}

	processorName := e.EventType
	capturedEvent := e
	capturedRaw := make([]byte, len(raw.Value))
	copy(capturedRaw, raw.Value)

	pool.Submit(key, workerpool.Job{
		Ctx: ctx,
		Execute: func(jobCtx context.Context) error {
			defer c.offsets.Complete(raw.Topic, raw.Partition, raw.Offset)
			return c.processWithFallback(jobCtx, capturedEvent, capturedRaw, processorName, processor)
		},
	})
}

// dispatchRetry hands a "<topic>.retry" message to the scheduler instead of
// processing it. The scheduler durably persists it in Redis and republishes
// to the original topic when its backoff window elapses.
func (c *Consumer) dispatchRetry(ctx context.Context, raw event.RawMessage) {
	defer c.offsets.Complete(raw.Topic, raw.Partition, raw.Offset)

	log := logger.With(
		zap.String("topic", raw.Topic),
		zap.Int32("partition", raw.Partition),
		zap.Int64("offset", raw.Offset),
	)

	env, err := retry.ParseRetryEnvelope(raw.Value)
	if err != nil {
		log.Error("invalid retry envelope — sending to DLQ", zap.Error(err))
		c.dlq.PublishRaw(ctx, raw, "invalid retry envelope: "+err.Error())
		return
	}

	var scheduleErr error
	for attempt := 0; attempt < 3; attempt++ {
		if scheduleErr = c.scheduler.Schedule(ctx, env); scheduleErr == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}

	log.Error("failed to persist retry schedule after retries — sending to DLQ",
		zap.String("eventId", env.EventID), zap.Error(scheduleErr))
	c.dlq.Publish(ctx, event.Event{
		EventID:   env.EventID,
		EventType: env.EventType,
		TenantID:  env.TenantID,
		Topic:     env.OriginalTopic,
		Partition: raw.Partition,
		Offset:    raw.Offset,
	}, raw.Value, "failed to schedule retry: "+scheduleErr.Error(), env.RetryCount)
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

// Stop signals shutdown: stops polling, drains every partition's in-flight
// work via the offset manager, then does a final flush. The offset manager
// itself is started/stopped by main.go since it outlives a single Stop call
// (it needs to flush once more after pools fully drain).
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

		logger.Get().Info("consumer stopped cleanly")
	})
}
