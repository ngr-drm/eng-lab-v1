package payments

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Store interface {
	EnqueuePayment(ctx context.Context, payment Payment, maxQueueDepth int64) (bool, error)
	RecoverProcessing(ctx context.Context) error
	PopPending(ctx context.Context, wait time.Duration) (Payment, bool, error)
	Requeue(ctx context.Context, payment Payment, delay time.Duration) error
	Confirm(ctx context.Context, payment Payment, processor ProcessorName) (bool, error)
	Summary(ctx context.Context, from, to *time.Time) (Summary, error)
}

type Processor interface {
	Name() ProcessorName
	Process(ctx context.Context, payment Payment) error
	Healthy(ctx context.Context) bool
}

type ServiceConfig struct {
	Store             Store
	DefaultProcessor  Processor
	FallbackProcessor Processor
	Logger            *slog.Logger
	WorkerCount       int
	QueueWait         time.Duration
	RetryDelay        time.Duration
	MaxQueueDepth     int64
}

type Service struct {
	store             Store
	defaultProcessor  Processor
	fallbackProcessor Processor
	logger            *slog.Logger
	workerCount       int
	queueWait         time.Duration
	retryDelay        time.Duration
	maxQueueDepth     int64
	startOnce         sync.Once
}

func NewService(cfg ServiceConfig) *Service {
	workerCount := cfg.WorkerCount
	if workerCount <= 0 {
		workerCount = 8
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		store:             cfg.Store,
		defaultProcessor:  cfg.DefaultProcessor,
		fallbackProcessor: cfg.FallbackProcessor,
		logger:            logger,
		workerCount:       workerCount,
		queueWait:         cfg.QueueWait,
		retryDelay:        cfg.RetryDelay,
		maxQueueDepth:     cfg.MaxQueueDepth,
	}
}

func (s *Service) Accept(ctx context.Context, payment Payment) (bool, error) {
	if payment.CorrelationID == "" {
		return false, errors.New("correlationId is required")
	}
	if payment.AmountCents <= 0 {
		return false, errors.New("amount must be positive")
	}
	if payment.RequestedAt.IsZero() {
		payment.RequestedAt = time.Now().UTC()
	}
	return s.store.EnqueuePayment(ctx, payment, s.maxQueueDepth)
}

func (s *Service) Summary(ctx context.Context, from, to *time.Time) (Summary, error) {
	return s.store.Summary(ctx, from, to)
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		if err := s.store.RecoverProcessing(ctx); err != nil {
			s.logger.Warn("failed to recover processing queue", "err", err)
		}
		for i := 0; i < s.workerCount; i++ {
			go s.worker(ctx, i)
		}
	})
}

func (s *Service) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		payment, ok, err := s.store.PopPending(ctx, s.queueWait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Warn("failed to pop pending payment", "worker", id, "err", err)
			time.Sleep(30 * time.Millisecond)
			continue
		}
		if !ok {
			continue
		}

		s.processOne(ctx, id, payment)
	}
}

func (s *Service) processOne(ctx context.Context, workerID int, payment Payment) {
	processors := s.processorOrder(ctx)
	for _, proc := range processors {
		if err := proc.Process(ctx, payment); err == nil {
			confirmed, confirmErr := s.store.Confirm(ctx, payment, proc.Name())
			if confirmErr != nil {
				s.logger.Error("payment confirmed remotely but local confirmation failed", "worker", workerID, "processor", proc.Name(), "correlationId", payment.CorrelationID, "err", confirmErr)
				_ = s.store.Requeue(context.Background(), payment, s.retryDelay)
				return
			}
			if confirmed {
				return
			}
			return
		}
	}

	if err := s.store.Requeue(ctx, payment, s.retryDelay); err != nil {
		s.logger.Warn("failed to requeue payment", "worker", workerID, "correlationId", payment.CorrelationID, "err", err)
	}
}

func (s *Service) processorOrder(ctx context.Context) []Processor {
	defaultHealthy := s.defaultProcessor.Healthy(ctx)
	fallbackHealthy := s.fallbackProcessor.Healthy(ctx)

	if defaultHealthy {
		if fallbackHealthy {
			return []Processor{s.defaultProcessor, s.fallbackProcessor}
		}
		return []Processor{s.defaultProcessor}
	}
	if fallbackHealthy {
		return []Processor{s.fallbackProcessor, s.defaultProcessor}
	}
	return []Processor{s.defaultProcessor, s.fallbackProcessor}
}
