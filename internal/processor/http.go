package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"payment-processor-fbk/internal/payments"
)

type Config struct {
	Name        payments.ProcessorName
	BaseURL     string
	Timeout     time.Duration
	MaxConns    int
	IdleConns   int
	HealthTTL   time.Duration
	HealthGrace time.Duration
	HealthCache HealthCache
}

type HealthCache interface {
	GetProcessorHealth(ctx context.Context, processor payments.ProcessorName) (HealthSnapshot, bool, error)
	TryStartHealthCheck(ctx context.Context, processor payments.ProcessorName, ttl time.Duration) (bool, error)
	SetProcessorHealth(ctx context.Context, processor payments.ProcessorName, snapshot HealthSnapshot, ttl time.Duration) error
}

type HealthSnapshot struct {
	CheckedAt     time.Time
	Failing       bool
	MinResponseMS int
	Success       bool
}

type HTTPClient struct {
	name      payments.ProcessorName
	baseURL   string
	client    *http.Client
	healthTTL time.Duration
	grace     time.Duration
	cache     HealthCache
	mu        sync.Mutex
	cached    healthState
}

type healthState struct {
	checkedAt      time.Time
	failing        bool
	minResponseMS  int
	everSuccessful bool
}

func NewHTTPClient(cfg Config) *HTTPClient {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 900 * time.Millisecond
	}
	maxConns := cfg.MaxConns
	if maxConns <= 0 {
		maxConns = 64
	}
	idleConns := cfg.IdleConns
	if idleConns <= 0 {
		idleConns = maxConns
	}
	healthTTL := cfg.HealthTTL
	if healthTTL <= 0 {
		healthTTL = 5 * time.Second
	}
	grace := cfg.HealthGrace
	if grace <= 0 {
		grace = 1200 * time.Millisecond
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 200 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          idleConns * 2,
		MaxIdleConnsPerHost:   idleConns,
		MaxConnsPerHost:       maxConns,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: 100 * time.Millisecond,
	}

	return &HTTPClient{
		name:      cfg.Name,
		baseURL:   cfg.BaseURL,
		client:    &http.Client{Timeout: timeout, Transport: transport},
		healthTTL: healthTTL,
		grace:     grace,
		cache:     cfg.HealthCache,
	}
}

func (c *HTTPClient) Name() payments.ProcessorName {
	return c.name
}

func (c *HTTPClient) Process(ctx context.Context, payment payments.Payment) error {
	payload := processorPayment{
		CorrelationID: payment.CorrelationID,
		Amount:        amountJSON(payments.FormatCents(payment.AmountCents)),
		RequestedAt:   payment.RequestedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/payments", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(resp.Status)
	}
	return nil
}

func (c *HTTPClient) Healthy(ctx context.Context) bool {
	now := time.Now()
	if healthy, ok := c.localHealth(now); ok {
		return healthy
	}

	if c.cache != nil {
		if snapshot, ok, err := c.cache.GetProcessorHealth(ctx, c.name); err == nil && ok {
			c.updateHealth(snapshot.CheckedAt, snapshot.Failing, snapshot.MinResponseMS, snapshot.Success)
			return snapshot.Success && !snapshot.Failing && time.Duration(snapshot.MinResponseMS)*time.Millisecond <= c.grace
		}

		allowed, err := c.cache.TryStartHealthCheck(ctx, c.name, c.healthTTL)
		if err != nil || !allowed {
			return c.localHealthOrOptimistic(now)
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 350*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, c.baseURL+"/payments/service-health", nil)
	if err != nil {
		return false
	}
	resp, err := c.client.Do(req)
	if err != nil {
		c.updateHealth(now, true, 0, false)
		c.setSharedHealth(ctx, HealthSnapshot{CheckedAt: now, Failing: true, MinResponseMS: 0, Success: false})
		return false
	}
	defer resp.Body.Close()

	var payload serviceHealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.updateHealth(now, true, 0, false)
		c.setSharedHealth(ctx, HealthSnapshot{CheckedAt: now, Failing: true, MinResponseMS: 0, Success: false})
		return false
	}
	c.updateHealth(now, payload.Failing, payload.MinResponseTime, true)
	c.setSharedHealth(ctx, HealthSnapshot{
		CheckedAt:     now,
		Failing:       payload.Failing,
		MinResponseMS: payload.MinResponseTime,
		Success:       true,
	})
	return !payload.Failing && time.Duration(payload.MinResponseTime)*time.Millisecond <= c.grace
}

func (c *HTTPClient) localHealth(now time.Time) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached.checkedAt.IsZero() || now.Sub(c.cached.checkedAt) >= c.healthTTL {
		return false, false
	}
	return c.cached.everSuccessful && !c.cached.failing && time.Duration(c.cached.minResponseMS)*time.Millisecond <= c.grace, true
}

func (c *HTTPClient) localHealthOrOptimistic(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached.checkedAt.IsZero() {
		return true
	}
	return c.cached.everSuccessful && !c.cached.failing && time.Duration(c.cached.minResponseMS)*time.Millisecond <= c.grace
}

func (c *HTTPClient) setSharedHealth(ctx context.Context, snapshot HealthSnapshot) {
	if c.cache == nil {
		return
	}
	_ = c.cache.SetProcessorHealth(ctx, c.name, snapshot, c.healthTTL)
}

func (c *HTTPClient) updateHealth(checkedAt time.Time, failing bool, minResponseMS int, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached = healthState{
		checkedAt:      checkedAt,
		failing:        failing,
		minResponseMS:  minResponseMS,
		everSuccessful: success || c.cached.everSuccessful,
	}
}

type processorPayment struct {
	CorrelationID string     `json:"correlationId"`
	Amount        amountJSON `json:"amount"`
	RequestedAt   string     `json:"requestedAt"`
}

type amountJSON string

func (a amountJSON) MarshalJSON() ([]byte, error) {
	return []byte(a), nil
}

type serviceHealthResponse struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}
