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

	defaultProcessor := processor.NewHTTPClient(processor.Config{
		Name:        payments.ProcessorDefault,
		BaseURL:     cfg.defaultURL,
		Timeout:     cfg.processorTimeout,
		MaxConns:    cfg.maxProcessorConns,
		IdleConns:   cfg.maxProcessorConns,
		HealthTTL:   5 * time.Second,
		HealthGrace: cfg.healthGrace,
		HealthCache: st,
	})
	fallbackProcessor := processor.NewHTTPClient(processor.Config{
		Name:        payments.ProcessorFallback,
		BaseURL:     cfg.fallbackURL,
		Timeout:     cfg.processorTimeout,
		MaxConns:    cfg.maxProcessorConns,
		IdleConns:   cfg.maxProcessorConns,
		HealthTTL:   5 * time.Second,
		HealthGrace: cfg.healthGrace,
		HealthCache: st,
	})

	service := payments.NewService(payments.ServiceConfig{
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

	service.Start(ctx)

	mux := http.NewServeMux()
	apphttp.Register(mux, service, logger)

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
		logger.Info("api listening", "addr", cfg.listenAddr)
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

type config struct {
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
}

func configFromEnv() config {
	return config{
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
	}
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
