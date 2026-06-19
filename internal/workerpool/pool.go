package workerpool

import (
	"context"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/metrics"
)

// Job is a unit of work submitted to the pool.
type Job struct {
	Ctx     context.Context
	Execute func(ctx context.Context) error
}

// Pool is a bounded goroutine pool for a single topic.
// It enforces backpressure via a buffered channel — callers block when full.
type Pool struct {
	topic       string
	jobs        chan Job
	workerCount int
	activeCount atomic.Int64
	wg          sync.WaitGroup
}

// New creates a Pool and starts workerCount goroutines immediately.
func New(topic string, workerCount, bufferSize int) *Pool {
	p := &Pool{
		topic:       topic,
		jobs:        make(chan Job, bufferSize),
		workerCount: workerCount,
	}
	p.start()
	return p
}

func (p *Pool) start() {
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.runWorker()
	}
}

func (p *Pool) runWorker() {
	defer p.wg.Done()
	for job := range p.jobs {
		p.activeCount.Add(1)
		metrics.WorkerPoolActive.WithLabelValues(p.topic).Set(float64(p.activeCount.Load()))

		if err := job.Execute(job.Ctx); err != nil {
			// Errors are handled inside Execute (retry/DLQ decisions happen there).
			// Workers never stop on error — they log at the pool level for visibility.
			logger.Get().Debug("worker: job returned error",
				zap.String("topic", p.topic),
				zap.Error(err),
			)
		}

		p.activeCount.Add(-1)
		metrics.WorkerPoolActive.WithLabelValues(p.topic).Set(float64(p.activeCount.Load()))
		metrics.WorkerPoolQueueDepth.WithLabelValues(p.topic).Set(float64(len(p.jobs)))
	}
}

// Submit enqueues a job. Blocks when the buffer is full (backpressure).
// Callers should check context cancellation before submitting during shutdown.
func (p *Pool) Submit(job Job) {
	p.jobs <- job
	metrics.WorkerPoolQueueDepth.WithLabelValues(p.topic).Set(float64(len(p.jobs)))
}

// TrySubmit attempts a non-blocking submit. Returns false when the buffer is full.
func (p *Pool) TrySubmit(job Job) bool {
	select {
	case p.jobs <- job:
		metrics.WorkerPoolQueueDepth.WithLabelValues(p.topic).Set(float64(len(p.jobs)))
		return true
	default:
		return false
	}
}

// QueueDepth returns the current number of pending jobs.
func (p *Pool) QueueDepth() int {
	return len(p.jobs)
}

// IsFull returns true when the job buffer is at capacity.
func (p *Pool) IsFull() bool {
	return len(p.jobs) == cap(p.jobs)
}

// Stop closes the job channel and waits for all in-flight workers to finish.
func (p *Pool) Stop() {
	close(p.jobs)
	p.wg.Wait()
	logger.Get().Info("worker pool stopped", zap.String("topic", p.topic))
}
