package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_received_total",
		Help: "Total number of Kafka messages received.",
	}, []string{"topic", "tenant_id"})

	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Total number of messages successfully processed.",
	}, []string{"topic", "event_type", "tenant_id"})

	MessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_failed_total",
		Help: "Total number of messages that failed processing.",
	}, []string{"topic", "event_type", "tenant_id", "reason"})

	MessagesRetried = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_retried_total",
		Help: "Total number of messages sent to retry topic.",
	}, []string{"topic", "event_type", "tenant_id"})

	MessagesDLQ = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "messages_dlq_total",
		Help: "Total number of messages sent to DLQ.",
	}, []string{"topic", "event_type", "tenant_id"})

	ProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "processing_duration_seconds",
		Help:    "Time spent processing a message end-to-end.",
		Buckets: prometheus.DefBuckets,
	}, []string{"topic", "event_type", "processor"})

	WorkerPoolActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_active",
		Help: "Number of currently active workers.",
	}, []string{"topic"})

	WorkerPoolQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_queue_depth",
		Help: "Current depth of the worker job queue.",
	}, []string{"topic"})

	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "consumer_lag",
		Help: "Kafka consumer lag per topic-partition.",
	}, []string{"topic", "partition"})

	MongoDBDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mongodb_duration_seconds",
		Help:    "MongoDB operation latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})

	RedisDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "redis_duration_seconds",
		Help:    "Redis operation latency.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation"})
)
