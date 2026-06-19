package consumer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
)

// RebalanceHandler provides callbacks for Kafka partition rebalance events.
// Before a partition is revoked, completed offsets are committed to prevent
// duplicate processing after reassignment.
type RebalanceHandler struct{}

func (h *RebalanceHandler) OnPartitionsAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		logger.Get().Info("partitions assigned",
			zap.String("topic", topic),
			zap.Int32s("partitions", partitions),
		)
	}
}

func (h *RebalanceHandler) OnPartitionsRevoked(ctx context.Context, client *kgo.Client, revoked map[string][]int32) {
	for topic, partitions := range revoked {
		logger.Get().Info("partitions being revoked — committing offsets",
			zap.String("topic", topic),
			zap.Int32s("partitions", partitions),
		)
	}
	if err := client.CommitUncommittedOffsets(ctx); err != nil {
		logger.Get().Error("rebalance: failed to commit offsets before partition release", zap.Error(err))
		return
	}
	logger.Get().Info("rebalance: offsets committed before partition release")
}

func (h *RebalanceHandler) OnPartitionsLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	for topic, partitions := range lost {
		logger.Get().Warn("partitions lost",
			zap.String("topic", topic),
			zap.Int32s("partitions", partitions),
		)
	}
}
