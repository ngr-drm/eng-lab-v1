package payments

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type Store interface {
	EnqueuePayment(ctx context.Context, payment Payment, maxQueueDepth int64) (bool, error)
	RecoverProcessing(ctx context.Context) error
	PopPending(ctx context.Context, wait time.Duration, preferredProcessor ProcessorName) (Payment, bool, error)
	PendingDepth(ctx context.Context) (int64, error)
	QueueDepth(ctx context.Context) (QueueDepth, error)
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
	FallbackQueueSize int64
	IngressEnabled    bool
	IngressQueueSize  int
	IngressFlushers   int
	IngressTimeout    time.Duration
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
	fallbackQueueSize int64
	ingressEnabled    bool
	ingressQueue      chan Payment
	ingressFlushers   int
	ingressTimeout    time.Duration
	metrics           serviceMetrics
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
	ingressQueueSize := cfg.IngressQueueSize
	if ingressQueueSize <= 0 {
		ingressQueueSize = 2000
	}
	ingressFlushers := cfg.IngressFlushers
	if ingressFlushers <= 0 {
		ingressFlushers = 4
	}
	ingressTimeout := cfg.IngressTimeout
	if ingressTimeout <= 0 {
		ingressTimeout = 250 * time.Millisecond
	}

	service := &Service{
		store:             cfg.Store,
		defaultProcessor:  cfg.DefaultProcessor,
		fallbackProcessor: cfg.FallbackProcessor,
		logger:            logger,
		workerCount:       workerCount,
		queueWait:         cfg.QueueWait,
		retryDelay:        cfg.RetryDelay,
		maxQueueDepth:     cfg.MaxQueueDepth,
		fallbackQueueSize: cfg.FallbackQueueSize,
		ingressEnabled:    cfg.IngressEnabled,
		ingressFlushers:   ingressFlushers,
		ingressTimeout:    ingressTimeout,
	}
	if cfg.IngressEnabled {
		service.ingressQueue = make(chan Payment, ingressQueueSize)
	}
	return service
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

	if s.ingressEnabled {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		select {
		case s.ingressQueue <- payment:
			s.metrics.ingressAccepted.Add(1)
			return true, nil
		default:
			s.metrics.ingressFull.Add(1)
			s.metrics.queueFull.Add(1)
			return false, ErrQueueFull
		}
	}

	enqueued, err := s.store.EnqueuePayment(ctx, payment, s.maxQueueDepth)
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			s.metrics.queueFull.Add(1)
		}
		return false, err
	}
	if enqueued {
		s.metrics.enqueued.Add(1)
	} else {
		s.metrics.duplicates.Add(1)
	}
	return enqueued, nil
}

func (s *Service) Summary(ctx context.Context, from, to *time.Time) (Summary, error) {
	return s.store.Summary(ctx, from, to)
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		go s.recoverLoop(ctx)
		go s.metricsLoop(ctx)
		if s.ingressEnabled {
			for i := 0; i < s.ingressFlushers; i++ {
				go s.ingressFlusher(ctx)
			}
		}
		for i := 0; i < s.workerCount; i++ {
			go s.worker(ctx, i)
		}
	})
}

func (s *Service) recoverLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		if err := s.store.RecoverProcessing(ctx); err != nil && ctx.Err() == nil {
			s.logger.Warn("failed to recover processing queue", "err", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) ingressFlusher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case payment := <-s.ingressQueue:
			s.flushIngressPayment(ctx, payment)
		}
	}
}

func (s *Service) flushIngressPayment(ctx context.Context, payment Payment) {
	for {
		enqueueCtx := ctx
		var cancel context.CancelFunc
		if s.ingressTimeout > 0 {
			enqueueCtx, cancel = context.WithTimeout(ctx, s.ingressTimeout)
		}

		enqueued, err := s.store.EnqueuePayment(enqueueCtx, payment, s.maxQueueDepth)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			if enqueued {
				s.metrics.enqueued.Add(1)
			} else {
				s.metrics.duplicates.Add(1)
			}
			return
		}

		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, ErrQueueFull) {
			s.metrics.queueFull.Add(1)
		}
		s.metrics.ingressFlushRetries.Add(1)

		timer := time.NewTimer(s.retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		preferredProcessor := s.selectNewProcessor(ctx)
		payment, ok, err := s.store.PopPending(ctx, s.queueWait, preferredProcessor)
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
	proc := s.processorByName(payment.ProcessorAttempt)
	if proc == nil {
		if err := s.store.Requeue(ctx, payment, s.retryDelay); err != nil {
			s.logger.Warn("failed to requeue payment without processor", "worker", workerID, "correlationId", payment.CorrelationID, "err", err)
		}
		return
	}

	err := proc.Process(ctx, payment)

	if err == nil {
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

	s.metrics.retries.Add(1)
	if err := s.store.Requeue(ctx, payment, s.retryDelay); err != nil {
		s.logger.Warn("failed to requeue payment", "worker", workerID, "correlationId", payment.CorrelationID, "err", err)
	}
}

func (s *Service) selectNewProcessor(ctx context.Context) ProcessorName {
	defaultHealthy := s.defaultProcessor.Healthy(ctx)
	fallbackHealthy := s.fallbackProcessor.Healthy(ctx)

	if defaultHealthy {
		if fallbackHealthy && s.shouldShedToFallback(ctx) {
			return ProcessorFallback
		}
		return ProcessorDefault
	}
	if fallbackHealthy {
		return ProcessorFallback
	}
	return ProcessorDefault
}

func (s *Service) processorByName(name ProcessorName) Processor {
	if name == ProcessorDefault {
		return s.defaultProcessor
	}
	if name == ProcessorFallback {
		return s.fallbackProcessor
	}
	return nil
}

func (s *Service) shouldShedToFallback(ctx context.Context) bool {
	if s.fallbackQueueSize <= 0 {
		return false
	}
	depth, err := s.store.PendingDepth(ctx)
	if err != nil {
		s.logger.Warn("failed to read pending queue depth", "err", err)
		return false
	}
	return depth >= s.fallbackQueueSize
}

func (s *Service) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.logMetrics(ctx)
		}
	}
}

func (s *Service) logMetrics(ctx context.Context) {
	depth, err := s.store.QueueDepth(ctx)
	if err != nil {
		s.logger.Warn("failed to read queue metrics", "err", err)
		return
	}

	s.logger.Info("payments metrics",
		"pending_depth", depth.Pending,
		"processing_depth", depth.Processing,
		"in_flight", depth.Total(),
		"ingress_depth", s.ingressDepth(),
		"enqueued_total", s.metrics.enqueued.Load(),
		"duplicates_total", s.metrics.duplicates.Load(),
		"queue_full_total", s.metrics.queueFull.Load(),
		"ingress_accepted_total", s.metrics.ingressAccepted.Load(),
		"ingress_full_total", s.metrics.ingressFull.Load(),
		"ingress_flush_retries_total", s.metrics.ingressFlushRetries.Load(),
		"retries_total", s.metrics.retries.Load(),
	)
}

func (s *Service) ingressDepth() int {
	if !s.ingressEnabled || s.ingressQueue == nil {
		return 0
	}
	return len(s.ingressQueue)
}

type serviceMetrics struct {
	enqueued            atomic.Int64
	duplicates          atomic.Int64
	queueFull           atomic.Int64
	ingressAccepted     atomic.Int64
	ingressFull         atomic.Int64
	ingressFlushRetries atomic.Int64
	retries             atomic.Int64
}
