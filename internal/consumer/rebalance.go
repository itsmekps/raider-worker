package consumer

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/offsetmanager"
)

// drainTimeout bounds how long a partition revoke waits for in-flight jobs
// to finish before committing best-effort and releasing anyway. Without a
// bound, a stuck processor (e.g. blocked on a hung external call) could
// stall the entire consumer group's rebalance indefinitely.
const drainTimeout = 30 * time.Second

// RebalanceHandler provides callbacks for Kafka partition rebalance events.
// On revoke it drains in-flight work for each released partition via the
// offset manager, flushes the final contiguous offset, and only then lets
// franz-go release the partition — eliminating the window where a worker
// could still be processing offset N while a later offset is committed.
type RebalanceHandler struct {
	offsets *offsetmanager.Manager
}

func NewRebalanceHandler(offsets *offsetmanager.Manager) *RebalanceHandler {
	return &RebalanceHandler{offsets: offsets}
}

func (h *RebalanceHandler) OnPartitionsAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		logger.Get().Info("partitions assigned",
			zap.String("topic", topic),
			zap.Int32s("partitions", partitions),
		)
	}
}

func (h *RebalanceHandler) OnPartitionsRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	for topic, partitions := range revoked {
		for _, partition := range partitions {
			logger.Get().Info("partition revoked — draining in-flight work",
				zap.String("topic", topic), zap.Int32("partition", partition))

			drainCtx, cancel := context.WithTimeout(ctx, drainTimeout)
			if err := h.offsets.Drain(drainCtx, topic, partition); err != nil {
				logger.Get().Error("rebalance: drain timed out — flushing best-effort",
					zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
			}
			cancel()

			if err := h.offsets.FlushPartition(ctx, topic, partition); err != nil {
				logger.Get().Error("rebalance: failed to flush offset before partition release",
					zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
			}
			h.offsets.Forget(topic, partition)
		}
	}
	logger.Get().Info("rebalance: revoked partitions drained and flushed")
}

func (h *RebalanceHandler) OnPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	for topic, partitions := range lost {
		for _, partition := range partitions {
			logger.Get().Warn("partition lost — discarding tracking state (offsets already invalid)",
				zap.String("topic", topic), zap.Int32("partition", partition))
			h.offsets.Forget(topic, partition)
		}
	}
}
