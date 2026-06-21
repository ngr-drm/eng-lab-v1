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
	ProcessorCoolingDown(ctx context.Context, processor ProcessorName) (bool, error)
	MarkProcessorCooldown(ctx context.Context, processor ProcessorName, ttl time.Duration) error
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
	ProcessorCooldown time.Duration
	MaxQueueDepth     int64
	FallbackQueueSize int64
}

type Service struct {
	store             Store
	defaultProcessor  Processor
	fallbackProcessor Processor
	logger            *slog.Logger
	workerCount       int
	queueWait         time.Duration
	retryDelay        time.Duration
	processorCooldown time.Duration
	maxQueueDepth     int64
	fallbackQueueSize int64
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
	processorCooldown := cfg.ProcessorCooldown
	if processorCooldown <= 0 {
		processorCooldown = 700 * time.Millisecond
	}
	return &Service{
		store:             cfg.Store,
		defaultProcessor:  cfg.DefaultProcessor,
		fallbackProcessor: cfg.FallbackProcessor,
		logger:            logger,
		workerCount:       workerCount,
		queueWait:         cfg.QueueWait,
		retryDelay:        cfg.RetryDelay,
		processorCooldown: processorCooldown,
		maxQueueDepth:     cfg.MaxQueueDepth,
		fallbackQueueSize: cfg.FallbackQueueSize,
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

	startedAt := time.Now()
	err := proc.Process(ctx, payment)
	s.metrics.observeProcessor(proc.Name(), time.Since(startedAt))

	if err == nil {
		confirmed, confirmErr := s.store.Confirm(ctx, payment, proc.Name())
		if confirmErr != nil {
			s.metrics.confirmFailures.Add(1)
			s.logger.Error("payment confirmed remotely but local confirmation failed", "worker", workerID, "processor", proc.Name(), "correlationId", payment.CorrelationID, "err", confirmErr)
			_ = s.store.Requeue(context.Background(), payment, s.retryDelay)
			return
		}
		if confirmed {
			s.metrics.confirmed(proc.Name())
			return
		}
		s.metrics.confirmFailures.Add(1)
		return
	}

	s.metrics.retries.Add(1)
	s.markProcessorCooldown(ctx, proc.Name(), err)
	if err := s.store.Requeue(ctx, payment, s.retryDelay); err != nil {
		s.logger.Warn("failed to requeue payment", "worker", workerID, "correlationId", payment.CorrelationID, "err", err)
	}
}

func (s *Service) selectNewProcessor(ctx context.Context) ProcessorName {
	defaultAvailable := s.processorAvailable(ctx, s.defaultProcessor)
	fallbackAvailable := s.processorAvailable(ctx, s.fallbackProcessor)

	if defaultAvailable {
		if fallbackAvailable && s.shouldShedToFallback(ctx) {
			return ProcessorFallback
		}
		return ProcessorDefault
	}
	if fallbackAvailable {
		return ProcessorFallback
	}
	return ProcessorDefault
}

func (s *Service) processorAvailable(ctx context.Context, proc Processor) bool {
	if !proc.Healthy(ctx) {
		return false
	}
	coolingDown, err := s.store.ProcessorCoolingDown(ctx, proc.Name())
	if err != nil {
		s.logger.Warn("failed to read processor cooldown", "processor", proc.Name(), "err", err)
		return true
	}
	return !coolingDown
}

func (s *Service) markProcessorCooldown(ctx context.Context, processor ProcessorName, cause error) {
	if s.processorCooldown <= 0 {
		return
	}
	if err := s.store.MarkProcessorCooldown(ctx, processor, s.processorCooldown); err != nil {
		s.logger.Warn("failed to mark processor cooldown", "processor", processor, "cause", cause, "err", err)
		return
	}
	s.metrics.processorCooldown(processor)
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
		"leasing_depth", depth.Leasing,
		"processing_depth", depth.Processing,
		"in_flight", depth.Total(),
		"enqueued_total", s.metrics.enqueued.Load(),
		"retries_total", s.metrics.retries.Load(),
		"cooldown_default_total", s.metrics.cooldownDefault.Load(),
		"cooldown_fallback_total", s.metrics.cooldownFallback.Load(),
	)
}

type serviceMetrics struct {
	enqueued                atomic.Int64
	duplicates              atomic.Int64
	queueFull               atomic.Int64
	confirmedDefault        atomic.Int64
	confirmedFallback       atomic.Int64
	retries                 atomic.Int64
	confirmFailures         atomic.Int64
	processorDefaultCalls   atomic.Int64
	processorDefaultNanos   atomic.Int64
	processorFallbackCalls  atomic.Int64
	processorFallbackNanos  atomic.Int64
	cooldownDefault         atomic.Int64
	cooldownFallback        atomic.Int64
}

func (m *serviceMetrics) confirmed(processor ProcessorName) {
	switch processor {
	case ProcessorDefault:
		m.confirmedDefault.Add(1)
	case ProcessorFallback:
		m.confirmedFallback.Add(1)
	}
}

func (m *serviceMetrics) observeProcessor(processor ProcessorName, duration time.Duration) {
	switch processor {
	case ProcessorDefault:
		m.processorDefaultCalls.Add(1)
		m.processorDefaultNanos.Add(duration.Nanoseconds())
	case ProcessorFallback:
		m.processorFallbackCalls.Add(1)
		m.processorFallbackNanos.Add(duration.Nanoseconds())
	}
}

func (m *serviceMetrics) processorCooldown(processor ProcessorName) {
	switch processor {
	case ProcessorDefault:
		m.cooldownDefault.Add(1)
	case ProcessorFallback:
		m.cooldownFallback.Add(1)
	}
}

func (m *serviceMetrics) avgProcessorMS(processor ProcessorName) int64 {
	var calls, nanos int64
	switch processor {
	case ProcessorDefault:
		calls = m.processorDefaultCalls.Load()
		nanos = m.processorDefaultNanos.Load()
	case ProcessorFallback:
		calls = m.processorFallbackCalls.Load()
		nanos = m.processorFallbackNanos.Load()
	}
	if calls == 0 {
		return 0
	}
	return (nanos / calls) / int64(time.Millisecond)
}
