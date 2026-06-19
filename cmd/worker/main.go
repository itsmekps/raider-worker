package main

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"

	"github.com/raider/worker/internal/config"
	"github.com/raider/worker/internal/consumer"
	"github.com/raider/worker/internal/dlq"
	"github.com/raider/worker/internal/health"
	"github.com/raider/worker/internal/idempotency"
	"github.com/raider/worker/internal/logger"
	"github.com/raider/worker/internal/middleware"
	"github.com/raider/worker/internal/offsetmanager"
	"github.com/raider/worker/internal/processor"
	"github.com/raider/worker/internal/registry"
	"github.com/raider/worker/internal/retry"
)

const (
	offsetFlushInterval = 1 * time.Second
	retryPollInterval   = 1 * time.Second
)

func main() {
	// Best-effort: only present in local/dev runs. In containers, env vars
	// are injected directly (docker-compose env_file, k8s, etc.) and no
	// .env file exists, so a missing file here is not an error.
	_ = godotenv.Load()

	cfg := config.Load()

	if err := logger.Init(cfg.Log.Level); err != nil {
		panic("failed to init logger: " + err.Error())
	}
	defer logger.Sync()

	log := logger.Get()
	log.Info("starting debt-recovery worker", zap.String("group", cfg.Kafka.GroupID))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ── Redis ────────────────────────────────────────────────────────────────
	redisOpts, err := redis.ParseURL(cfg.Redis.URI)
	if err != nil {
		log.Fatal("invalid REDIS_URI", zap.Error(err))
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, cfg.Timeouts.Redis)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		log.Fatal("redis ping failed", zap.Error(err))
	}
	pingCancel()
	log.Info("redis connected")

	// ── MongoDB ──────────────────────────────────────────────────────────────
	mongoOpts := options.Client().ApplyURI(cfg.Mongo.URI)
	if cfg.Mongo.TLS {
		mongoOpts.SetTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	mongoClient, err := mongo.Connect(ctx, mongoOpts)
	if err != nil {
		log.Fatal("mongo connect failed", zap.Error(err))
	}
	defer func() {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutCancel()
		_ = mongoClient.Disconnect(shutCtx)
	}()

	pingCtx2, pingCancel2 := context.WithTimeout(ctx, cfg.Timeouts.MongoDB)
	if err := mongoClient.Ping(pingCtx2, nil); err != nil {
		log.Fatal("mongo ping failed", zap.Error(err))
	}
	pingCancel2()
	log.Info("mongodb connected")

	// ── Offset manager + rebalance handler ────────────────────────────────────
	// The offset manager needs the kafka client, but the kafka client's
	// rebalance options need the handler that wraps the offset manager —
	// constructed before the client exists. Resolved with two-phase init:
	// the client reference is attached right after NewClient returns, and
	// well before the poll loop starts (which is the only thing that can
	// trigger a rebalance callback).
	offsetMgr := offsetmanager.New(nil, offsetFlushInterval)
	rebalanceHandler := consumer.NewRebalanceHandler(offsetMgr)

	// ── Kafka client (franz-go) ──────────────────────────────────────────────
	allTopics := append(cfg.Kafka.Topics, cfg.Kafka.RetryTopics...)

	kopts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Kafka.GroupID),
		kgo.ConsumeTopics(allTopics...),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(rebalanceHandler.OnPartitionsAssigned),
		kgo.OnPartitionsRevoked(rebalanceHandler.OnPartitionsRevoked),
		kgo.OnPartitionsLost(rebalanceHandler.OnPartitionsLost),
	}
	if cfg.Kafka.TLSEnabled {
		kopts = append(kopts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))

		// SASL/PLAIN credentials — only populated if env vars are set.
		kafkaUser := os.Getenv("KAFKA_SASL_USER")
		kafkaPass := os.Getenv("KAFKA_SASL_PASSWORD")
		if kafkaUser != "" && kafkaPass != "" {
			kopts = append(kopts, kgo.SASL(plain.Auth{
				User: kafkaUser,
				Pass: kafkaPass,
			}.AsMechanism()))
		}
	}

	kafkaClient, err := kgo.NewClient(kopts...)
	if err != nil {
		log.Fatal("kafka client init failed", zap.Error(err))
	}
	defer kafkaClient.Close()
	offsetMgr.SetClient(kafkaClient)
	log.Info("kafka client connected", zap.Strings("brokers", cfg.Kafka.Brokers))

	// ── Core services ────────────────────────────────────────────────────────
	idempotencyStore := idempotency.NewRedisStore(redisClient)
	dlqPublisher := dlq.NewPublisher(kafkaClient)
	retryPublisher := retry.NewPublisher(kafkaClient, cfg.Retry.MaxRetries)
	retryScheduler := retry.NewScheduler(redisClient, kafkaClient, retryPollInterval)

	// ── Registry — register all processors here ──────────────────────────────
	reg := registry.New()
	reg.Register("VISIT_APPROVED", 1, processor.NewVisitApprovedProcessor())
	reg.Register("NOTIFICATION_TRIGGERED", 1, processor.NewNotificationTriggeredProcessor())
	// Add future processors here:
	// reg.Register("CASE_ASSIGNED", 1, processor.NewCaseAssignedProcessor())

	// ── Middleware pipeline ──────────────────────────────────────────────────
	pipeline := middleware.NewPipeline(idempotencyStore, cfg.Timeouts.KafkaProcessing)

	// ── Consumer ─────────────────────────────────────────────────────────────
	cons := consumer.New(cfg, kafkaClient, reg, pipeline, retryPublisher, retryScheduler, dlqPublisher, offsetMgr)

	// ── Health server ─────────────────────────────────────────────────────────
	healthCheckers := map[string]health.Checker{
		"redis":   &redisHealthChecker{client: redisClient},
		"mongodb": &mongoHealthChecker{client: mongoClient},
	}
	healthServer := health.NewServer(cfg.Server.HealthPort, healthCheckers)
	healthServer.Start()

	// ── Prometheus metrics server ────────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.Server.PrometheusPort,
		Handler: metricsMux,
	}
	go func() {
		log.Info("prometheus metrics server listening", zap.String("addr", metricsServer.Addr))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Start background components ───────────────────────────────────────────
	go offsetMgr.Start(ctx)
	go retryScheduler.Run(ctx)
	go cons.Start(ctx)
	log.Info("worker running — waiting for shutdown signal")

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	log.Info("shutdown signal received")
	cancel() // stop the consumer poll loop

	cons.Stop() // drain worker pools — all in-flight Complete() calls land before this returns

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()

	offsetMgr.FlushAll(shutCtx) // final commit of everything pools just completed
	offsetMgr.Stop()
	retryScheduler.Stop()

	healthServer.Stop(shutCtx)

	if err := metricsServer.Shutdown(shutCtx); err != nil {
		log.Error("metrics server shutdown error", zap.Error(err))
	}

	log.Info("shutdown complete")
}

// ── Lightweight health check adapters ────────────────────────────────────────

type redisHealthChecker struct{ client *redis.Client }

func (r *redisHealthChecker) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

type mongoHealthChecker struct{ client *mongo.Client }

func (m *mongoHealthChecker) Ping(ctx context.Context) error {
	return m.client.Ping(ctx, nil)
}
