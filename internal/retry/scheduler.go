package retry

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
)

const (
	scheduleKey   = "retry:schedule"
	payloadKeyFmt = "retry:payload:%s"
	payloadTTL    = 24 * time.Hour
)

// Scheduler persists retry envelopes in a Redis sorted set keyed by
// nextRetryAt and republishes them to their original topic when due.
//
// Kafka has no native delayed delivery — consuming a "<topic>.retry" message
// and forwarding it immediately (the old behavior) meant configured backoff
// delays were never actually honored. This decouples the delay from Kafka
// entirely: the retry topic message is durably handed off to Redis, and the
// scheduler's poll loop is the only thing that re-injects it into Kafka,
// exactly when it's due.
type Scheduler struct {
	redis        *redis.Client
	kafka        *kgo.Client
	pollInterval time.Duration
	stopCh       chan struct{}
	stopped      chan struct{}
}

func NewScheduler(redisClient *redis.Client, kafkaClient *kgo.Client, pollInterval time.Duration) *Scheduler {
	return &Scheduler{
		redis:        redisClient,
		kafka:        kafkaClient,
		pollInterval: pollInterval,
		stopCh:       make(chan struct{}),
		stopped:      make(chan struct{}),
	}
}

// Schedule durably persists env to be republished at env.NextRetryAfter.
func (s *Scheduler) Schedule(ctx context.Context, env RetryEnvelope) error {
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal retry envelope: %w", err)
	}

	pipe := s.redis.TxPipeline()
	pipe.Set(ctx, fmt.Sprintf(payloadKeyFmt, env.EventID), payload, payloadTTL)
	pipe.ZAdd(ctx, scheduleKey, redis.Z{
		Score:  float64(env.NextRetryAfter.Unix()),
		Member: env.EventID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// Run polls for due retries and republishes them. Safe to run from multiple
// worker instances concurrently — claiming a due item uses ZREM, which only
// one instance can successfully perform for a given member.
func (s *Scheduler) Run(ctx context.Context) {
	defer close(s.stopped)
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.tick(ctx)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context) {
	now := fmt.Sprintf("%d", time.Now().Unix())
	due, err := s.redis.ZRangeByScore(ctx, scheduleKey, &redis.ZRangeBy{
		Min: "0", Max: now, Count: 500,
	}).Result()
	if err != nil {
		logger.Get().Error("retry scheduler: poll failed", zap.Error(err))
		return
	}

	for _, eventID := range due {
		s.claim(ctx, eventID)
	}
}

func (s *Scheduler) claim(ctx context.Context, eventID string) {
	removed, err := s.redis.ZRem(ctx, scheduleKey, eventID).Result()
	if err != nil {
		logger.Get().Error("retry scheduler: claim failed", zap.String("eventId", eventID), zap.Error(err))
		return
	}
	if removed == 0 {
		return // another instance claimed it first
	}

	payloadKey := fmt.Sprintf(payloadKeyFmt, eventID)
	raw, err := s.redis.Get(ctx, payloadKey).Bytes()
	if err != nil {
		logger.Get().Error("retry scheduler: payload missing for claimed retry",
			zap.String("eventId", eventID), zap.Error(err))
		return
	}

	var env RetryEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		logger.Get().Error("retry scheduler: corrupt payload", zap.String("eventId", eventID), zap.Error(err))
		return
	}

	s.republish(ctx, env)
	s.redis.Del(ctx, payloadKey)
}

// republishEnvelope is the standard envelope shape with retry metadata
// folded in, so the normal envelope parser on the original topic picks up
// the carried-forward retryCount.
type republishEnvelope struct {
	Version          int             `json:"version"`
	EventID          string          `json:"eventId"`
	EventType        string          `json:"eventType"`
	TenantID         string          `json:"tenantId"`
	Timestamp        time.Time       `json:"timestamp"`
	Data             json.RawMessage `json:"data"`
	RetryCount       int             `json:"retryCount"`
	FailureReason    string          `json:"failureReason"`
	FirstFailureTime time.Time       `json:"firstFailureTime"`
}

func (s *Scheduler) republish(ctx context.Context, env RetryEnvelope) {
	out := republishEnvelope{
		Version:          env.Version,
		EventID:          env.EventID,
		EventType:        env.EventType,
		TenantID:         env.TenantID,
		Timestamp:        env.Timestamp,
		Data:             env.Data,
		RetryCount:       env.RetryCount,
		FailureReason:    env.FailureReason,
		FirstFailureTime: env.FirstFailureTime,
	}

	payload, err := json.Marshal(out)
	if err != nil {
		logger.Get().Error("retry scheduler: failed to marshal republish payload",
			zap.String("eventId", env.EventID), zap.Error(err))
		return
	}

	s.kafka.Produce(ctx, &kgo.Record{
		Topic: env.OriginalTopic,
		Key:   []byte(env.TenantID),
		Value: payload,
	}, func(_ *kgo.Record, produceErr error) {
		if produceErr != nil {
			logger.Get().Error("retry scheduler: republish failed — rescheduling shortly",
				zap.String("eventId", env.EventID), zap.Error(produceErr))
			env.NextRetryAfter = time.Now().Add(15 * time.Second)
			if schedErr := s.Schedule(context.Background(), env); schedErr != nil {
				logger.Get().Error("retry scheduler: failed to reschedule after republish failure",
					zap.String("eventId", env.EventID), zap.Error(schedErr))
			}
			return
		}
		logger.Get().Info("retry republished to original topic",
			zap.String("topic", env.OriginalTopic),
			zap.String("eventId", env.EventID),
			zap.Int("retryCount", env.RetryCount),
		)
	})
}

func (s *Scheduler) Stop() {
	close(s.stopCh)
	<-s.stopped
}
