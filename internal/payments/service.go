package payments

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultAcceptTimeout       = 75 * time.Millisecond
	defaultAcceptVerifyTimeout = 25 * time.Millisecond
	pendingDepthRefresh        = 100 * time.Millisecond
)

type Store interface {
	EnqueuePayment(ctx context.Context, payment Payment, maxQueueDepth int64) (bool, error)
	PaymentExists(ctx context.Context, correlationID string) (bool, error)
	DropPending(ctx context.Context, payment Payment) (bool, error)
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
	AcceptTimeout     time.Duration
	AcceptVerifyTimeout time.Duration
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
	acceptTimeout     time.Duration
	acceptVerifyTimeout time.Duration
	maxQueueDepth     int64
	fallbackQueueSize int64
	cachedPendingDepth atomic.Int64
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
	acceptTimeout := cfg.AcceptTimeout
	if acceptTimeout <= 0 {
		acceptTimeout = defaultAcceptTimeout
	}
	acceptVerifyTimeout := cfg.AcceptVerifyTimeout
	if acceptVerifyTimeout <= 0 {
		acceptVerifyTimeout = defaultAcceptVerifyTimeout
	}
	return &Service{
		store:             cfg.Store,
		defaultProcessor:  cfg.DefaultProcessor,
		fallbackProcessor: cfg.FallbackProcessor,
		logger:            logger,
		workerCount:       workerCount,
		queueWait:         cfg.QueueWait,
		retryDelay:        cfg.RetryDelay,
		acceptTimeout:     acceptTimeout,
		acceptVerifyTimeout: acceptVerifyTimeout,
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

	acceptCtx, cancel := context.WithTimeout(ctx, s.acceptTimeout)
	defer cancel()

	enqueued, err := s.store.EnqueuePayment(acceptCtx, payment, s.maxQueueDepth)
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			s.metrics.queueFull.Add(1)
			return false, err
		}
		if ctx.Err() != nil {
			s.metrics.acceptCanceled.Add(1)
			s.dropCanceledAccept(payment)
			return false, ctx.Err()
		}
		s.metrics.enqueueErrors.Add(1)
		s.acceptAmbiguousEnqueue(payment.CorrelationID)
		return true, nil
	}
	if enqueued {
		if ctx.Err() != nil {
			s.metrics.acceptCanceled.Add(1)
			s.dropCanceledAccept(payment)
			return false, ctx.Err()
		}
		s.metrics.enqueued.Add(1)
	} else {
		s.metrics.duplicates.Add(1)
	}
	return enqueued, nil
}

func (s *Service) acceptAmbiguousEnqueue(correlationID string) {
	ctx, cancel := context.WithTimeout(context.Background(), s.acceptVerifyTimeout)
	defer cancel()

	exists, err := s.store.PaymentExists(ctx, correlationID)
	if err != nil {
		s.metrics.acceptUnknown.Add(1)
		return
	}
	if exists {
		s.metrics.acceptRecovered.Add(1)
		return
	}
	s.metrics.acceptAssumed.Add(1)
}

func (s *Service) dropCanceledAccept(payment Payment) {
	ctx, cancel := context.WithTimeout(context.Background(), s.acceptVerifyTimeout)
	defer cancel()

	dropped, err := s.store.DropPending(ctx, payment)
	if err != nil {
		s.metrics.acceptDropErrors.Add(1)
		return
	}
	if dropped {
		s.metrics.acceptDropped.Add(1)
	}
}

func (s *Service) Summary(ctx context.Context, from, to *time.Time) (Summary, error) {
	return s.store.Summary(ctx, from, to)
}

func (s *Service) Start(ctx context.Context) {
	s.startOnce.Do(func() {
		go s.recoverLoop(ctx)
		go s.metricsLoop(ctx)
		if s.fallbackQueueSize > 0 {
			s.refreshPendingDepth(ctx)
			go s.pendingDepthLoop(ctx)
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

func (s *Service) pendingDepthLoop(ctx context.Context) {
	ticker := time.NewTicker(pendingDepthRefresh)
	defer ticker.Stop()

	for {
		s.refreshPendingDepth(ctx)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) refreshPendingDepth(ctx context.Context) {
	depth, err := s.store.PendingDepth(ctx)
	if err != nil {
		s.metrics.pendingDepthReadErrors.Add(1)
		return
	}
	s.cachedPendingDepth.Store(depth)
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
	return s.cachedPendingDepth.Load() >= s.fallbackQueueSize
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
		"enqueued_total", s.metrics.enqueued.Load(),
		"duplicates_total", s.metrics.duplicates.Load(),
		"queue_full_total", s.metrics.queueFull.Load(),
		"enqueue_errors_total", s.metrics.enqueueErrors.Load(),
		"accept_recovered_total", s.metrics.acceptRecovered.Load(),
		"accept_unknown_total", s.metrics.acceptUnknown.Load(),
		"accept_assumed_total", s.metrics.acceptAssumed.Load(),
		"accept_canceled_total", s.metrics.acceptCanceled.Load(),
		"accept_dropped_total", s.metrics.acceptDropped.Load(),
		"accept_drop_errors_total", s.metrics.acceptDropErrors.Load(),
		"pending_depth_cached", s.cachedPendingDepth.Load(),
		"pending_depth_read_errors_total", s.metrics.pendingDepthReadErrors.Load(),
		"confirmed_default_total", s.metrics.confirmedDefault.Load(),
		"confirmed_fallback_total", s.metrics.confirmedFallback.Load(),
		"retries_total", s.metrics.retries.Load(),
		"confirm_failures_total", s.metrics.confirmFailures.Load(),
		"processor_default_avg_ms", s.metrics.avgProcessorMS(ProcessorDefault),
		"processor_fallback_avg_ms", s.metrics.avgProcessorMS(ProcessorFallback),
	)
}

type serviceMetrics struct {
	enqueued               atomic.Int64
	duplicates             atomic.Int64
	queueFull              atomic.Int64
	enqueueErrors          atomic.Int64
	acceptRecovered        atomic.Int64
	acceptUnknown          atomic.Int64
	acceptAssumed          atomic.Int64
	acceptCanceled         atomic.Int64
	acceptDropped          atomic.Int64
	acceptDropErrors       atomic.Int64
	pendingDepthReadErrors atomic.Int64
	confirmedDefault       atomic.Int64
	confirmedFallback      atomic.Int64
	retries                atomic.Int64
	confirmFailures        atomic.Int64
	processorDefaultCalls  atomic.Int64
	processorDefaultNanos  atomic.Int64
	processorFallbackCalls atomic.Int64
	processorFallbackNanos atomic.Int64
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
