package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	apphttp "eng-lab-v1/internal/http"
	"eng-lab-v1/internal/payments"
	"eng-lab-v1/internal/processor"
	"eng-lab-v1/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := configFromEnv()
	st := store.NewRedis(store.RedisConfig{
		Addr:            cfg.redisAddr,
		ProcessingLease: cfg.processingLease,
	})
	defer st.Close()

	service := newPaymentService(cfg, st, logger)
	if cfg.runsWorkers() {
		service.Start(ctx)
		logger.Info("payment workers started", "workers", cfg.workerCount, "role", cfg.role)
	}

	if !cfg.servesHTTP() {
		logger.Info("worker process started", "role", cfg.role)
		<-ctx.Done()
		return
	}

	runHTTPServer(ctx, cfg, service, logger)
}

func newPaymentService(cfg config, st *store.Redis, logger *slog.Logger) *payments.Service {
	var defaultProcessor payments.Processor
	var fallbackProcessor payments.Processor

	if cfg.runsWorkers() {
		defaultProcessor = processor.NewHTTPClient(processor.Config{
			Name:        payments.ProcessorDefault,
			BaseURL:     cfg.defaultURL,
			Timeout:     cfg.processorTimeout,
			MaxConns:    cfg.maxProcessorConns,
			IdleConns:   cfg.maxProcessorConns,
			HealthTTL:   5 * time.Second,
			HealthGrace: cfg.healthGrace,
			HealthCache: st,
		})
		fallbackProcessor = processor.NewHTTPClient(processor.Config{
			Name:        payments.ProcessorFallback,
			BaseURL:     cfg.fallbackURL,
			Timeout:     cfg.processorTimeout,
			MaxConns:    cfg.maxProcessorConns,
			IdleConns:   cfg.maxProcessorConns,
			HealthTTL:   5 * time.Second,
			HealthGrace: cfg.healthGrace,
			HealthCache: st,
		})
	}

	return payments.NewService(payments.ServiceConfig{
		Store:             st,
		DefaultProcessor:  defaultProcessor,
		FallbackProcessor: fallbackProcessor,
		Logger:            logger,
		WorkerCount:       cfg.workerCount,
		QueueWait:         cfg.queueWait,
		RetryDelay:        cfg.retryDelay,
		MaxQueueDepth:     cfg.maxQueueDepth,
		FallbackQueueSize: cfg.fallbackQueueSize,
	})
}

func runHTTPServer(ctx context.Context, cfg config, service *payments.Service, logger *slog.Logger) {
	mux := http.NewServeMux()
	postAckMetrics := &apphttp.PostAckMetrics{}
	apphttp.Register(mux, service, logger, apphttp.HandlerConfig{
		AcceptMode:        cfg.postAcceptMode,
		AckEnqueueTimeout: cfg.postAckEnqueueTimeout,
		PostAckMetrics:    postAckMetrics,
	})

	if cfg.postAcceptMode == apphttp.AcceptModeAckFirst {
		go postAckMetricsLoop(ctx, logger, postAckMetrics)
	}

	server := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 800 * time.Millisecond,
		ReadTimeout:       1500 * time.Millisecond,
		WriteTimeout:      1500 * time.Millisecond,
		IdleTimeout:       30 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.listenAddr, "post_accept_mode", cfg.postAcceptMode)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", "err", err)
	}
}

func postAckMetricsLoop(ctx context.Context, logger *slog.Logger, metrics *apphttp.PostAckMetrics) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snapshot := metrics.Snapshot()
			logger.Info("post ack metrics",
				"post_ack_enqueued_total", snapshot.Enqueued,
				"post_ack_enqueue_failed_total", snapshot.EnqueueFailed,
				"post_ack_flush_failed_total", snapshot.FlushFailed,
			)
		}
	}
}

type appRole string

const (
	roleAPI    appRole = "api"
	roleWorker appRole = "worker"
	roleAll    appRole = "all"
)

type config struct {
	role              appRole
	listenAddr        string
	redisAddr         string
	defaultURL        string
	fallbackURL       string
	processorTimeout  time.Duration
	processingLease   time.Duration
	healthGrace       time.Duration
	maxProcessorConns int
	workerCount       int
	queueWait         time.Duration
	retryDelay        time.Duration
	maxQueueDepth     int64
	fallbackQueueSize int64
	postAcceptMode          apphttp.AcceptMode
	postAckEnqueueTimeout   time.Duration
}

func configFromEnv() config {
	return config{
		role:              envRole("APP_ROLE", roleAll),
		listenAddr:        envString("LISTEN_ADDR", ":8080"),
		redisAddr:         envString("REDIS_ADDR", "valkey:6379"),
		defaultURL:        envString("PROCESSOR_DEFAULT_URL", "http://payment-processor-default:8080"),
		fallbackURL:       envString("PROCESSOR_FALLBACK_URL", "http://payment-processor-fallback:8080"),
		processorTimeout:  envDurationMS("PROCESSOR_TIMEOUT_MS", 10*time.Second),
		processingLease:   envDurationMS("PROCESSING_LEASE_MS", 25*time.Second),
		healthGrace:       envDurationMS("PROCESSOR_HEALTH_GRACE_MS", 1200*time.Millisecond),
		maxProcessorConns: envInt("MAX_PROCESSOR_CONNS", 64),
		workerCount:       envInt("WORKERS", 20),
		queueWait:         envDurationMS("QUEUE_WAIT_MS", 700*time.Millisecond),
		retryDelay:        envDurationMS("RETRY_DELAY_MS", 80*time.Millisecond),
		maxQueueDepth:     int64(envInt("MAX_QUEUE_DEPTH", 20000)),
		fallbackQueueSize: int64(envIntAllowZero("FALLBACK_QUEUE_SIZE", 200)),
		postAcceptMode:        envAcceptMode("POST_ACCEPT_MODE", apphttp.AcceptModeDurable),
		postAckEnqueueTimeout: envDurationMS("POST_ACK_ENQUEUE_TIMEOUT_MS", 500*time.Millisecond),
	}
}

func (c config) runsWorkers() bool {
	return c.role == roleWorker || c.role == roleAll
}

func (c config) servesHTTP() bool {
	return c.role == roleAPI || c.role == roleAll
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envIntAllowZero(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}

func envRole(key string, fallback appRole) appRole {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch appRole(strings.ToLower(strings.TrimSpace(value))) {
	case roleAPI:
		return roleAPI
	case roleWorker:
		return roleWorker
	case roleAll:
		return roleAll
	default:
		return fallback
	}
}

func envAcceptMode(key string, fallback apphttp.AcceptMode) apphttp.AcceptMode {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	switch apphttp.AcceptMode(strings.ToLower(strings.TrimSpace(value))) {
	case apphttp.AcceptModeDurable:
		return apphttp.AcceptModeDurable
	case apphttp.AcceptModeAckFirst:
		return apphttp.AcceptModeAckFirst
	default:
		return fallback
	}
}

func envDurationMS(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return time.Duration(parsed) * time.Millisecond
}
