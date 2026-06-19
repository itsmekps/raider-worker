package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Kafka      KafkaConfig
	Mongo      MongoConfig
	Redis      RedisConfig
	Workers    WorkerConfig
	Server     ServerConfig
	Log        LogConfig
	Timeouts   TimeoutConfig
	Retry      RetryConfig
}

type KafkaConfig struct {
	Brokers         []string
	GroupID         string
	Topics          []string
	RetryTopics     []string
	DLQTopics       []string
	TLSEnabled      bool
}

type MongoConfig struct {
	URI      string
	Database string
	TLS      bool
}

type RedisConfig struct {
	URI string
}

type WorkerConfig struct {
	CaseCount         int
	PaymentCount      int
	VisitCount        int
	NotificationCount int
	QueueBufferSize   int
}

type ServerConfig struct {
	PrometheusPort string
	HealthPort     string
}

type LogConfig struct {
	Level string
}

type TimeoutConfig struct {
	KafkaProcessing time.Duration
	MongoDB         time.Duration
	Redis           time.Duration
	ExternalAPI     time.Duration
}

type RetryConfig struct {
	MaxRetries int
}

func Load() *Config {
	brokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	topics := []string{"cases", "payments", "visits", "notifications"}
	retryTopics := []string{"cases.retry", "payments.retry", "visits.retry", "notifications.retry"}
	dlqTopics := []string{"cases.dlq", "payments.dlq", "visits.dlq", "notifications.dlq"}

	return &Config{
		Kafka: KafkaConfig{
			Brokers:     strings.Split(brokers, ","),
			GroupID:     getEnv("KAFKA_GROUP_ID", "debt-recovery-workers"),
			Topics:      topics,
			RetryTopics: retryTopics,
			DLQTopics:   dlqTopics,
			TLSEnabled:  getEnvBool("KAFKA_TLS_ENABLED", false),
		},
		Mongo: MongoConfig{
			URI:      getEnv("MONGO_URI", "mongodb://localhost:27017"),
			Database: getEnv("MONGO_DATABASE", "debt_recovery"),
			TLS:      getEnvBool("MONGO_TLS_ENABLED", false),
		},
		Redis: RedisConfig{
			URI: getEnv("REDIS_URI", "redis://localhost:6379"),
		},
		Workers: WorkerConfig{
			CaseCount:         getEnvInt("WORKER_CASE_COUNT", 50),
			PaymentCount:      getEnvInt("WORKER_PAYMENT_COUNT", 20),
			VisitCount:        getEnvInt("WORKER_VISIT_COUNT", 20),
			NotificationCount: getEnvInt("WORKER_NOTIFICATION_COUNT", 50),
			QueueBufferSize:   getEnvInt("WORKER_QUEUE_BUFFER_SIZE", 10000),
		},
		Server: ServerConfig{
			PrometheusPort: getEnv("PROMETHEUS_PORT", "9090"),
			HealthPort:     getEnv("HEALTH_PORT", "8080"),
		},
		Log: LogConfig{
			Level: getEnv("LOG_LEVEL", "info"),
		},
		Timeouts: TimeoutConfig{
			KafkaProcessing: time.Duration(getEnvInt("TIMEOUT_KAFKA_MS", 30000)) * time.Millisecond,
			MongoDB:         time.Duration(getEnvInt("TIMEOUT_MONGO_MS", 5000)) * time.Millisecond,
			Redis:           time.Duration(getEnvInt("TIMEOUT_REDIS_MS", 2000)) * time.Millisecond,
			ExternalAPI:     time.Duration(getEnvInt("TIMEOUT_EXTERNAL_API_MS", 10000)) * time.Millisecond,
		},
		Retry: RetryConfig{
			MaxRetries: getEnvInt("RETRY_MAX", 5),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}
