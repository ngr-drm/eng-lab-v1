package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"eng-lab-v1/internal/payments"
	"eng-lab-v1/internal/processor"
)

type RedisConfig struct {
	Addr            string
	ProcessingLease time.Duration
}

type Redis struct {
	client          *redis.Client
	processingLease time.Duration
}

func NewRedis(cfg RedisConfig) *Redis {
	addr := cfg.Addr
	if addr == "" {
		addr = "valkey:6379"
	}
	processingLease := cfg.ProcessingLease
	if processingLease <= 0 {
		processingLease = 15 * time.Second
	}
	return &Redis{
		client: redis.NewClient(&redis.Options{
			Addr:         addr,
			PoolSize:     64,
			MinIdleConns: 8,
			MaxRetries:   1,
		}),
		processingLease: processingLease,
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
	switch result {
	case 1:
		return true, nil
	case 0:
		return false, nil
	case -1:
		return false, payments.ErrQueueFull
	default:
		return false, fmt.Errorf("unexpected enqueue result %d", result)
	}
}

func (r *Redis) PopPending(ctx context.Context, wait time.Duration, preferredProcessor payments.ProcessorName) (payments.Payment, bool, error) {
	deadline := time.Now().Add(wait)
	for {
		now := time.Now()
		result, err := popPendingScript.Run(ctx, r.client, nil,
			now.UTC().UnixMilli(),
			now.Add(r.processingLease).UTC().UnixMilli(),
			string(preferredProcessor),
		).Slice()
		if err != nil {
			return payments.Payment{}, false, err
		}
		payment, ok, err := paymentFromPopResult(result)
		if err != nil || ok {
			return payment, ok, err
		}
		if !deadline.After(time.Now()) {
			return payments.Payment{}, false, nil
		}

		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return payments.Payment{}, false, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Redis) PendingDepth(ctx context.Context) (int64, error) {
	return r.client.LLen(ctx, pendingQueueKey).Result()
}

func (r *Redis) QueueDepth(ctx context.Context) (payments.QueueDepth, error) {
	pipe := r.client.Pipeline()
	pending := pipe.LLen(ctx, pendingQueueKey)
	processing := pipe.ZCard(ctx, processingLeasesKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return payments.QueueDepth{}, err
	}
	return payments.QueueDepth{
		Pending:    pending.Val(),
		Processing: processing.Val(),
	}, nil
}

func (r *Redis) ProcessorCoolingDown(ctx context.Context, processor payments.ProcessorName) (bool, error) {
	result, err := r.client.Exists(ctx, processorCooldownKey(processor)).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

func (r *Redis) MarkProcessorCooldown(ctx context.Context, processor payments.ProcessorName, ttl time.Duration) error {
	if ttl <= 0 {
		return nil
	}
	return r.client.Set(ctx, processorCooldownKey(processor), "1", ttl).Err()
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
	return requeueScript.Run(ctx, r.client, nil, payment.CorrelationID, payment.LeaseID).Err()
}

func (r *Redis) RecoverProcessing(ctx context.Context) error {
	now := time.Now().UTC().UnixMilli()
	for recovered := 1; recovered > 0; {
		count, err := recoverProcessingScript.Run(ctx, r.client, nil, now, 100).Int()
		if err != nil {
			return err
		}
		recovered = count
	}
	return nil
}

func (r *Redis) Confirm(ctx context.Context, payment payments.Payment, processor payments.ProcessorName) (bool, error) {
	result, err := confirmScript.Run(ctx, r.client, nil,
		payment.CorrelationID,
		string(processor),
		payment.AmountCents,
		payment.RequestedAt.UTC().UnixMilli(),
		payment.LeaseID,
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

const (
	pendingQueueKey      = "payments:pending"
	processingLeasesKey = "payments:processing-leases"
	emptyPopResultCode   = int64(0)
	paymentPopResultCode = int64(1)
	skippedPopResultCode = int64(2)
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

func processorCooldownKey(processor payments.ProcessorName) string {
	return "processor:cooldown:" + string(processor)
}

var enqueueScript = redis.NewScript(`
local key = 'payment:' .. ARGV[1]
local queue = 'payments:pending'
local processing = 'payments:processing-leases'
local max_queue_depth = tonumber(ARGV[5])

if redis.call('EXISTS', key) == 1 then
  return 0
end

if max_queue_depth > 0 and (redis.call('LLEN', queue) + redis.call('ZCARD', processing)) >= max_queue_depth then
  return -1
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

var popPendingScript = redis.NewScript(`
local id = redis.call('LPOP', 'payments:pending')
if not id then
  return {0}
end

local key = 'payment:' .. id
if redis.call('HGET', key, 'status') ~= 'pending' then
  return {2}
end

local lease_id = tostring(redis.call('INCR', 'payments:lease-seq'))
local processor_attempt = redis.call('HGET', key, 'processor_attempt')
if not processor_attempt or processor_attempt == '' then
  processor_attempt = ARGV[3]
end

redis.call('HSET', key,
  'status', 'processing',
  'lease_id', lease_id,
  'lease_until_ms', ARGV[2],
  'processor_attempt', processor_attempt
)
redis.call('ZADD', 'payments:processing-leases', ARGV[2], id)
return {
  1,
  id,
  redis.call('HGET', key, 'amount_cents'),
  redis.call('HGET', key, 'requested_at'),
  lease_id,
  processor_attempt
}
`)

var confirmScript = redis.NewScript(`
local key = 'payment:' .. ARGV[1]
local processor = ARGV[2]
local amount_cents = ARGV[3]
local requested_at_ms = ARGV[4]
local lease_id = ARGV[5]

if redis.call('EXISTS', key) == 0 then
  return 0
end

if redis.call('HGET', key, 'status') == 'confirmed' then
  return 0
end

if redis.call('HGET', key, 'status') ~= 'processing' then
  return 0
end

if redis.call('HGET', key, 'lease_id') ~= lease_id then
  return 0
end

redis.call('HSET', key,
  'status', 'confirmed',
  'processor', processor
)
redis.call('HDEL', key, 'lease_id', 'lease_until_ms')
redis.call('ZREM', 'payments:processing-leases', ARGV[1])
redis.call('ZADD', 'payments:confirmed:' .. processor, requested_at_ms, ARGV[1])
redis.call('HINCRBY', 'payments:summary:' .. processor, 'total_requests', 1)
redis.call('HINCRBY', 'payments:summary:' .. processor, 'total_cents', amount_cents)
return 1
`)

var requeueScript = redis.NewScript(`
local key = 'payment:' .. ARGV[1]

if redis.call('HGET', key, 'status') ~= 'processing' then
  return 0
end

if redis.call('HGET', key, 'lease_id') ~= ARGV[2] then
  return 0
end

redis.call('HSET', key, 'status', 'pending')
redis.call('HDEL', key, 'lease_id', 'lease_until_ms')
redis.call('ZREM', 'payments:processing-leases', ARGV[1])
redis.call('RPUSH', 'payments:pending', ARGV[1])
return 1
`)

var recoverProcessingScript = redis.NewScript(`
local expired = redis.call('ZRANGEBYSCORE', 'payments:processing-leases', '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local recovered = 0

for _, id in ipairs(expired) do
  local key = 'payment:' .. id
  if redis.call('HGET', key, 'status') == 'processing' then
    redis.call('HSET', key, 'status', 'pending')
    redis.call('HDEL', key, 'lease_id', 'lease_until_ms')
    redis.call('RPUSH', 'payments:pending', id)
    recovered = recovered + 1
  end
  redis.call('ZREM', 'payments:processing-leases', id)
end

return recovered
`)

func paymentFromPopResult(result []interface{}) (payments.Payment, bool, error) {
	if len(result) == 0 {
		return payments.Payment{}, false, nil
	}

	code, err := int64Value(result[0])
	if err != nil {
		return payments.Payment{}, false, err
	}
	if code == emptyPopResultCode || code == skippedPopResultCode {
		return payments.Payment{}, false, nil
	}
	if code != paymentPopResultCode {
		return payments.Payment{}, false, fmt.Errorf("unknown pop result code %d", code)
	}
	if len(result) != 6 {
		return payments.Payment{}, false, fmt.Errorf("invalid pop result length %d", len(result))
	}

	amount, err := strconv.ParseInt(stringValue(result[2]), 10, 64)
	if err != nil {
		return payments.Payment{}, false, err
	}
	requestedAt, err := time.Parse(time.RFC3339Nano, stringValue(result[3]))
	if err != nil {
		return payments.Payment{}, false, err
	}

	return payments.Payment{
		CorrelationID:    stringValue(result[1]),
		AmountCents:      amount,
		RequestedAt:      requestedAt.UTC(),
		LeaseID:          stringValue(result[4]),
		ProcessorAttempt: payments.ProcessorName(stringValue(result[5])),
	}, true, nil
}

func int64Value(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected integer value %T", value)
	}
}

func stringValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}
