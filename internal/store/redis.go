package store

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"payment-processor-fbk/internal/payments"
	"payment-processor-fbk/internal/processor"
)

type RedisConfig struct {
	Addr string
}

type Redis struct {
	client *redis.Client
}

func NewRedis(cfg RedisConfig) *Redis {
	addr := cfg.Addr
	if addr == "" {
		addr = "valkey:6379"
	}
	return &Redis{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     64,
			MinIdleConns: 8,
			MaxRetries:   1,
		}),
	}
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) EnqueuePayment(ctx context.Context, payment payments.Payment, maxQueueDepth int64) (bool, error) {
	values := []interface{}{
		payment.CorrelationID,
		payment.AmountCents,
		payment.RequestedAt.UTC().UnixMilli(),
		payment.RequestedAt.UTC().Format(time.RFC3339Nano),
		maxQueueDepth,
	}
	result, err := enqueueScript.Run(ctx, r.client, nil, values...).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (r *Redis) PopPending(ctx context.Context, wait time.Duration) (payments.Payment, bool, error) {
	timeout := int(wait.Seconds())
	if timeout < 1 {
		timeout = 1
	}

	correlationID, err := r.client.BRPopLPush(ctx, pendingQueueKey, processingQueueKey, time.Duration(timeout)*time.Second).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return payments.Payment{}, false, nil
		}
		return payments.Payment{}, false, err
	}
	payment, ok, err := r.paymentByCorrelationID(ctx, correlationID)
	if err != nil || ok {
		return payment, ok, err
	}
	_ = r.client.LRem(ctx, processingQueueKey, 0, correlationID).Err()
	return payments.Payment{}, false, nil
}

func (r *Redis) Requeue(ctx context.Context, payment payments.Payment, delay time.Duration) error {
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return requeueScript.Run(ctx, r.client, nil, payment.CorrelationID).Err()
}

func (r *Redis) RecoverProcessing(ctx context.Context) error {
	for {
		_, err := r.client.RPopLPush(ctx, processingQueueKey, pendingQueueKey).Result()
		if errors.Is(err, redis.Nil) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *Redis) Confirm(ctx context.Context, payment payments.Payment, processor payments.ProcessorName) (bool, error) {
	result, err := confirmScript.Run(ctx, r.client, nil,
		payment.CorrelationID,
		string(processor),
		payment.AmountCents,
		payment.RequestedAt.UTC().UnixMilli(),
	).Int()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (r *Redis) Summary(ctx context.Context, from, to *time.Time) (payments.Summary, error) {
	min := "-inf"
	max := "+inf"
	if from != nil {
		min = strconv.FormatInt(from.UTC().UnixMilli(), 10)
	}
	if to != nil {
		max = strconv.FormatInt(to.UTC().UnixMilli(), 10)
	}

	defaultBucket, err := r.bucket(ctx, payments.ProcessorDefault, min, max)
	if err != nil {
		return payments.Summary{}, err
	}
	fallbackBucket, err := r.bucket(ctx, payments.ProcessorFallback, min, max)
	if err != nil {
		return payments.Summary{}, err
	}
	return payments.Summary{Default: defaultBucket, Fallback: fallbackBucket}, nil
}

func (r *Redis) GetProcessorHealth(ctx context.Context, name payments.ProcessorName) (processor.HealthSnapshot, bool, error) {
	fields, err := r.client.HGetAll(ctx, processorHealthKey(name)).Result()
	if err != nil {
		return processor.HealthSnapshot{}, false, err
	}
	if len(fields) == 0 {
		return processor.HealthSnapshot{}, false, nil
	}

	checkedAtMS, err := strconv.ParseInt(fields["checked_at_ms"], 10, 64)
	if err != nil {
		return processor.HealthSnapshot{}, false, err
	}
	minResponseMS, err := strconv.Atoi(fields["min_response_ms"])
	if err != nil {
		return processor.HealthSnapshot{}, false, err
	}

	return processor.HealthSnapshot{
		CheckedAt:     time.UnixMilli(checkedAtMS).UTC(),
		Failing:       fields["failing"] == "1",
		MinResponseMS: minResponseMS,
		Success:       fields["success"] == "1",
	}, true, nil
}

func (r *Redis) TryStartHealthCheck(ctx context.Context, name payments.ProcessorName, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, processorHealthLockKey(name), "1", ttl).Result()
}

func (r *Redis) SetProcessorHealth(ctx context.Context, name payments.ProcessorName, snapshot processor.HealthSnapshot, ttl time.Duration) error {
	failing := "0"
	if snapshot.Failing {
		failing = "1"
	}
	success := "0"
	if snapshot.Success {
		success = "1"
	}

	key := processorHealthKey(name)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, map[string]any{
		"checked_at_ms":   snapshot.CheckedAt.UTC().UnixMilli(),
		"failing":         failing,
		"min_response_ms": snapshot.MinResponseMS,
		"success":         success,
	})
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *Redis) bucket(ctx context.Context, processor payments.ProcessorName, min, max string) (payments.SummaryBucket, error) {
	if min == "-inf" && max == "+inf" {
		return r.aggregateBucket(ctx, processor)
	}

	members, err := r.client.ZRangeByScore(ctx, confirmedSetKey(processor), &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()
	if err != nil {
		return payments.SummaryBucket{}, err
	}
	var bucket payments.SummaryBucket
	bucket.TotalRequests = int64(len(members))
	if len(members) == 0 {
		return bucket, nil
	}

	pipe := r.client.Pipeline()
	commands := make([]*redis.StringCmd, 0, len(members))
	for _, correlationID := range members {
		commands = append(commands, pipe.HGet(ctx, paymentKey(correlationID), "amount_cents"))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return payments.SummaryBucket{}, err
	}
	for _, command := range commands {
		amount, err := command.Int64()
		if err != nil {
			return payments.SummaryBucket{}, err
		}
		bucket.TotalCents += amount
	}
	return bucket, nil
}

func (r *Redis) aggregateBucket(ctx context.Context, processor payments.ProcessorName) (payments.SummaryBucket, error) {
	fields, err := r.client.HGetAll(ctx, summaryKey(processor)).Result()
	if err != nil {
		return payments.SummaryBucket{}, err
	}
	if len(fields) == 0 {
		return payments.SummaryBucket{}, nil
	}

	totalRequests, err := strconv.ParseInt(fields["total_requests"], 10, 64)
	if err != nil {
		return payments.SummaryBucket{}, err
	}
	totalCents, err := strconv.ParseInt(fields["total_cents"], 10, 64)
	if err != nil {
		return payments.SummaryBucket{}, err
	}
	return payments.SummaryBucket{
		TotalRequests: totalRequests,
		TotalCents:    totalCents,
	}, nil
}

func (r *Redis) paymentByCorrelationID(ctx context.Context, correlationID string) (payments.Payment, bool, error) {
	fields, err := r.client.HGetAll(ctx, paymentKey(correlationID)).Result()
	if err != nil {
		return payments.Payment{}, false, err
	}
	if len(fields) == 0 || fields["status"] == "confirmed" {
		return payments.Payment{}, false, nil
	}
	amount, err := strconv.ParseInt(fields["amount_cents"], 10, 64)
	if err != nil {
		return payments.Payment{}, false, err
	}
	requestedAt, err := time.Parse(time.RFC3339Nano, fields["requested_at"])
	if err != nil {
		return payments.Payment{}, false, err
	}
	return payments.Payment{
		CorrelationID: correlationID,
		AmountCents:   amount,
		RequestedAt:   requestedAt.UTC(),
	}, true, nil
}

const (
	pendingQueueKey    = "payments:pending"
	processingQueueKey = "payments:processing"
)

func paymentKey(correlationID string) string {
	return "payment:" + correlationID
}

func confirmedSetKey(processor payments.ProcessorName) string {
	return "payments:confirmed:" + string(processor)
}

func summaryKey(processor payments.ProcessorName) string {
	return "payments:summary:" + string(processor)
}

func processorHealthKey(processor payments.ProcessorName) string {
	return "processor:health:" + string(processor)
}

func processorHealthLockKey(processor payments.ProcessorName) string {
	return "processor:health-lock:" + string(processor)
}

var enqueueScript = redis.NewScript(`
local key = 'payment:' .. ARGV[1]
local queue = 'payments:pending'
local max_queue_depth = tonumber(ARGV[5])

if redis.call('EXISTS', key) == 1 then
  return 0
end

if max_queue_depth > 0 and redis.call('LLEN', queue) >= max_queue_depth then
  return redis.error_reply('queue is full')
end

redis.call('HSET', key,
  'amount_cents', ARGV[2],
  'requested_at_ms', ARGV[3],
  'requested_at', ARGV[4],
  'status', 'pending'
)
redis.call('RPUSH', queue, ARGV[1])
return 1
`)

var confirmScript = redis.NewScript(`
local key = 'payment:' .. ARGV[1]
local processor = ARGV[2]
local amount_cents = ARGV[3]
local requested_at_ms = ARGV[4]

if redis.call('EXISTS', key) == 0 then
  redis.call('LREM', 'payments:processing', 0, ARGV[1])
  return 0
end

if redis.call('HGET', key, 'status') == 'confirmed' then
  redis.call('LREM', 'payments:processing', 0, ARGV[1])
  return 0
end

redis.call('HSET', key,
  'status', 'confirmed',
  'processor', processor
)
redis.call('LREM', 'payments:processing', 0, ARGV[1])
redis.call('ZADD', 'payments:confirmed:' .. processor, requested_at_ms, ARGV[1])
redis.call('HINCRBY', 'payments:summary:' .. processor, 'total_requests', 1)
redis.call('HINCRBY', 'payments:summary:' .. processor, 'total_cents', amount_cents)
return 1
`)

var requeueScript = redis.NewScript(`
redis.call('LREM', 'payments:processing', 0, ARGV[1])
if redis.call('HGET', 'payment:' .. ARGV[1], 'status') ~= 'confirmed' then
  redis.call('RPUSH', 'payments:pending', ARGV[1])
end
return 1
`)
