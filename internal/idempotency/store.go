package idempotency

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is the idempotency backend interface.
// Keeping it an interface allows future swapping to MongoDB or another backend.
type Store interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
}

const defaultTTL = 48 * time.Hour

// RedisStore uses Redis SET NX for atomic idempotency checks.
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client, ttl: defaultTTL}
}

func (s *RedisStore) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	val, err := s.client.Exists(ctx, key(eventID)).Result()
	if err != nil {
		return false, err
	}
	return val > 0, nil
}

// MarkProcessed uses SET NX so concurrent duplicate deliveries are safe.
func (s *RedisStore) MarkProcessed(ctx context.Context, eventID string) error {
	return s.client.Set(ctx, key(eventID), "1", s.ttl).Err()
}

func key(eventID string) string {
	return "idempotency:" + eventID
}
