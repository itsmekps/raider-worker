package workerpool

import (
	"context"
	"hash/fnv"
	"sync"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/metrics"
)

// Job is a unit of work submitted to the pool.
type Job struct {
	Ctx     context.Context
	Execute func(ctx context.Context) error
}

type lane struct {
	jobs chan Job
}

// Pool is a key-affinity bounded worker pool for a single topic.
// Jobs sharing the same affinity key are always routed to the same lane and
// execute strictly in submission order — this is what preserves per-entity
// ordering (e.g. tenant:case) even though different keys process in
// parallel across lanes.
type Pool struct {
	topic string
	lanes []*lane
	wg    sync.WaitGroup
}

// New creates a Pool with laneCount lanes, each buffered to bufferPerLane.
func New(topic string, laneCount, bufferPerLane int) *Pool {
	if laneCount < 1 {
		laneCount = 1
	}
	if bufferPerLane < 1 {
		bufferPerLane = 1
	}

	p := &Pool{topic: topic, lanes: make([]*lane, laneCount)}
	for i := 0; i < laneCount; i++ {
		l := &lane{jobs: make(chan Job, bufferPerLane)}
		p.lanes[i] = l
		p.wg.Add(1)
		go p.runLane(l)
	}
	return p
}

func (p *Pool) runLane(l *lane) {
	defer p.wg.Done()
	for job := range l.jobs {
		metrics.WorkerPoolActive.WithLabelValues(p.topic).Inc()

		if err := job.Execute(job.Ctx); err != nil {
			// Errors are handled inside Execute (retry/DLQ decisions happen there).
			logger.Get().Debug("worker: job returned error", zap.String("topic", p.topic), zap.Error(err))
		}

		metrics.WorkerPoolActive.WithLabelValues(p.topic).Dec()
		metrics.WorkerPoolQueueDepth.WithLabelValues(p.topic).Set(float64(p.QueueDepth()))
	}
}

func (p *Pool) laneFor(key string) *lane {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	idx := int(h.Sum32() % uint32(len(p.lanes)))
	return p.lanes[idx]
}

// Submit enqueues a job onto the lane owned by key. Blocks only if that
// specific lane is full — other lanes (other keys) are unaffected, which is
// the backpressure mechanism: a single hot key throttles itself without
// stalling unrelated entities.
func (p *Pool) Submit(key string, job Job) {
	l := p.laneFor(key)
	l.jobs <- job
	metrics.WorkerPoolQueueDepth.WithLabelValues(p.topic).Set(float64(p.QueueDepth()))
}

// IsLaneFull reports whether the lane owned by key is currently at capacity.
func (p *Pool) IsLaneFull(key string) bool {
	l := p.laneFor(key)
	return len(l.jobs) >= cap(l.jobs)
}

// QueueDepth returns the total queued jobs across all lanes.
func (p *Pool) QueueDepth() int {
	total := 0
	for _, l := range p.lanes {
		total += len(l.jobs)
	}
	return total
}

// Stop closes all lane channels and waits for every lane goroutine to drain.
func (p *Pool) Stop() {
	for _, l := range p.lanes {
		close(l.jobs)
	}
	p.wg.Wait()
	logger.Get().Info("worker pool stopped", zap.String("topic", p.topic))
}
