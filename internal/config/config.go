package config

import (
	"fmt"
	"net/url"
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
			URI:      buildMongoURI(),
			Database: resolveMongoDatabase(),
			TLS:      getEnvBool("MONGO_TLS_ENABLED", false),
		},
		Redis: RedisConfig{
			URI: buildRedisURI(),
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

// buildRedisURI prefers discrete REDIS_HOST/REDIS_PORT/REDIS_PASSWORD vars
// (common in deployment configs that inject credentials separately) and
// falls back to a single REDIS_URI connection string.
func buildRedisURI() string {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		return getEnv("REDIS_URI", "redis://localhost:6379")
	}

	port := getEnv("REDIS_PORT", "6379")
	password := os.Getenv("REDIS_PASSWORD")
	if password == "" {
		return fmt.Sprintf("redis://%s:%s", host, port)
	}
	return fmt.Sprintf("redis://:%s@%s:%s", url.QueryEscape(password), host, port)
}

// buildMongoURI prefers discrete MONGODB_HOST/MONGODB_USER/MONGODB_PASSWORD
// vars (e.g. an Atlas SRV hostname) and falls back to a single MONGO_URI
// connection string. When MONGODB_HOST is set, an SRV-style URI is built
// since Atlas hosts are SRV records (mongodb+srv://...) and Atlas always
// requires TLS.
func buildMongoURI() string {
	host := os.Getenv("MONGODB_HOST")
	if host == "" {
		return getEnv("MONGO_URI", "mongodb://localhost:27017")
	}

	dbName := resolveMongoDatabase()
	user := os.Getenv("MONGODB_USER")
	password := os.Getenv("MONGODB_PASSWORD")
	if user == "" || password == "" {
		return fmt.Sprintf("mongodb+srv://%s/%s?retryWrites=true&w=majority&tls=true", host, dbName)
	}
	return fmt.Sprintf("mongodb+srv://%s:%s@%s/%s?retryWrites=true&w=majority&tls=true",
		url.QueryEscape(user), url.QueryEscape(password), host, dbName)
}

func resolveMongoDatabase() string {
	if name := os.Getenv("MONGODB_NAME"); name != "" {
		return name
	}
	return getEnv("MONGO_DATABASE", "debt_recovery")
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
