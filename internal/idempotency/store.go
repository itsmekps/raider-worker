package idempotency

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Store is the idempotency backend interface. TryAcquire is a single atomic
// operation — it replaces the old IsProcessed-then-MarkProcessed pattern,
// which had a race window where two concurrent deliveries could both see
// "not yet processed" and both run the business logic.
type Store interface {
	// TryAcquire atomically claims ownership of eventID. Returns true if
	// this call claimed it (caller must process the event); false if
	// another caller already owns it, or it was already completed
	// (caller must skip).
	TryAcquire(ctx context.Context, eventID string) (bool, error)

	// Release relinquishes ownership of eventID. Must be called when
	// processing fails, so that a future retry of the same eventID (retries
	// reuse the original eventID) can be acquired and processed again.
	// Must NOT be called after a successful Process — the key should remain
	// set until its TTL expires, acting as the "already processed" marker.
	Release(ctx context.Context, eventID string) error
}

const defaultTTL = 48 * time.Hour

// RedisStore uses Redis SET NX EX as the atomic acquire primitive — matching
// the production hardening spec: SET event:{event_id} processing NX EX 172800
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client, ttl: defaultTTL}
}

func (s *RedisStore) TryAcquire(ctx context.Context, eventID string) (bool, error) {
	return s.client.SetNX(ctx, key(eventID), "processing", s.ttl).Result()
}

func (s *RedisStore) Release(ctx context.Context, eventID string) error {
	return s.client.Del(ctx, key(eventID)).Err()
}

func key(eventID string) string {
	return "idempotency:" + eventID
}
