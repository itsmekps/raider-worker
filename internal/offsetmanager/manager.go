package offsetmanager

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
)

type partitionKey struct {
	topic     string
	partition int32
}

// partitionState tracks completion of offsets for a single partition.
// Offsets may complete out of order (workers run concurrently across lanes);
// the state only exposes the highest *contiguous* completed offset, so a
// crash never silently skips a still-in-flight message.
type partitionState struct {
	mu            sync.Mutex
	epoch         int32
	initialized   bool
	nextCommit    int64
	completed     map[int64]struct{}
	inFlight      int
	draining      bool
	drainCh       chan struct{}
	lastCommitted int64
}

// Manager is the single authority for committing offsets. Workers never call
// CommitRecords directly — they call Track() before dispatch and Complete()
// when a message is fully handled (success, DLQ, or scheduled retry all
// count as "handled" from Kafka's perspective).
type Manager struct {
	client *kgo.Client

	mu         sync.Mutex
	partitions map[partitionKey]*partitionState

	flushInterval time.Duration
	stopCh        chan struct{}
	stopped       chan struct{}
}

// New creates a Manager. client may be nil at construction time if the kgo
// client itself depends on rebalance callbacks that wrap this Manager (see
// SetClient) — it must be set before Start/Drain/FlushPartition are called.
func New(client *kgo.Client, flushInterval time.Duration) *Manager {
	return &Manager{
		client:        client,
		partitions:    make(map[partitionKey]*partitionState),
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
		stopped:       make(chan struct{}),
	}
}

// SetClient attaches the kafka client after construction. Must be called
// before the consumer's poll loop starts — rebalance callbacks (the only
// thing that can invoke Drain/FlushPartition before Start) only fire as a
// result of polling, so this ordering is safe without extra synchronization.
func (m *Manager) SetClient(client *kgo.Client) {
	m.client = client
}

func (m *Manager) state(topic string, partition int32) *partitionState {
	k := partitionKey{topic, partition}

	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.partitions[k]
	if !ok {
		st = &partitionState{
			completed:     make(map[int64]struct{}),
			lastCommitted: -1,
		}
		m.partitions[k] = st
	}
	return st
}

// Track registers an offset as in-flight. Must be called once per record,
// in consumption order, before the record is handed to a worker.
func (m *Manager) Track(topic string, partition int32, offset int64, leaderEpoch int32) {
	st := m.state(topic, partition)

	st.mu.Lock()
	st.epoch = leaderEpoch
	if !st.initialized {
		// The first offset we ever see for this partition session is, by
		// construction, the earliest possible start of the contiguous run.
		st.nextCommit = offset
		st.initialized = true
	}
	st.inFlight++
	st.mu.Unlock()
}

// Complete marks an offset as fully handled. Safe to call from any goroutine,
// any order, any number of times concurrently across different offsets.
func (m *Manager) Complete(topic string, partition int32, offset int64) {
	st := m.state(topic, partition)

	st.mu.Lock()
	st.completed[offset] = struct{}{}
	st.inFlight--
	if st.draining && st.inFlight <= 0 && st.drainCh != nil {
		select {
		case st.drainCh <- struct{}{}:
		default:
		}
	}
	st.mu.Unlock()
}

// committableOffset advances the contiguous cursor and returns the new
// highest committable offset, if it moved since the last call.
func (st *partitionState) committableOffset() (int64, int32, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()

	advanced := false
	for {
		if _, ok := st.completed[st.nextCommit]; !ok {
			break
		}
		delete(st.completed, st.nextCommit)
		st.nextCommit++
		advanced = true
	}

	if !advanced {
		return 0, 0, false
	}
	committable := st.nextCommit - 1
	if committable <= st.lastCommitted {
		return 0, 0, false
	}
	st.lastCommitted = committable
	return committable, st.epoch, true
}

// Drain blocks until in-flight work for the partition reaches zero, or ctx
// is cancelled. Used during rebalance revocation before releasing a
// partition, and during graceful shutdown.
func (m *Manager) Drain(ctx context.Context, topic string, partition int32) error {
	st := m.state(topic, partition)

	st.mu.Lock()
	if st.inFlight <= 0 {
		st.mu.Unlock()
		return nil
	}
	st.draining = true
	st.drainCh = make(chan struct{}, 1)
	ch := st.drainCh
	st.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FlushPartition commits the current highest contiguous offset for a single
// partition immediately. Used right after Drain, before a partition is
// released back to Kafka during a rebalance.
func (m *Manager) FlushPartition(ctx context.Context, topic string, partition int32) error {
	st := m.state(topic, partition)
	offset, epoch, ok := st.committableOffset()
	if !ok {
		return nil
	}
	rec := &kgo.Record{Topic: topic, Partition: partition, Offset: offset, LeaderEpoch: epoch}
	return m.client.CommitRecords(ctx, rec)
}

// Forget releases bookkeeping for a partition this instance no longer owns.
// Call after a clean revoke-and-flush, or when partitions are lost.
func (m *Manager) Forget(topic string, partition int32) {
	m.mu.Lock()
	delete(m.partitions, partitionKey{topic, partition})
	m.mu.Unlock()
}

// Start runs the periodic flush loop. Blocks until ctx is cancelled or Stop
// is called.
func (m *Manager) Start(ctx context.Context) {
	defer close(m.stopped)
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.FlushAll(ctx)
		}
	}
}

// FlushAll commits the current highest contiguous offset for every tracked
// partition that has advanced since the last flush. Safe to call directly
// (e.g. during graceful shutdown) in addition to the periodic loop.
func (m *Manager) FlushAll(ctx context.Context) {
	m.mu.Lock()
	keys := make([]partitionKey, 0, len(m.partitions))
	states := make([]*partitionState, 0, len(m.partitions))
	for k, st := range m.partitions {
		keys = append(keys, k)
		states = append(states, st)
	}
	m.mu.Unlock()

	records := make([]*kgo.Record, 0, len(states))
	for i, st := range states {
		offset, epoch, ok := st.committableOffset()
		if !ok {
			continue
		}
		records = append(records, &kgo.Record{
			Topic:       keys[i].topic,
			Partition:   keys[i].partition,
			Offset:      offset,
			LeaderEpoch: epoch,
		})
	}
	if len(records) == 0 {
		return
	}
	if err := m.client.CommitRecords(ctx, records...); err != nil {
		logger.Get().Error("offsetmanager: commit failed", zap.Error(err))
	}
}

func (m *Manager) Stop() {
	close(m.stopCh)
	<-m.stopped
}
