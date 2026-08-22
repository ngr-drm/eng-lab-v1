package diagnostics

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const cpuStatPath = "/sys/fs/cgroup/cpu.stat"

var latencyBounds = [...]time.Duration{
	1 * time.Millisecond,
	2 * time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	20 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
}

type Collector struct {
	logger   *slog.Logger
	interval time.Duration
	post     latencyHistogram
	enqueue  latencyHistogram
}

func New(logger *slog.Logger, interval time.Duration) *Collector {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &Collector{logger: logger, interval: interval}
}

func (c *Collector) ObservePost(duration time.Duration) {
	c.post.observe(duration)
}

func (c *Collector) ObserveEnqueue(duration time.Duration) {
	c.enqueue.observe(duration)
}

func (c *Collector) Start(ctx context.Context) {
	go c.loop(ctx)
}

func (c *Collector) loop(ctx context.Context) {
	previousCPU, cpuAvailable := readCPUStat(cpuStatPath)
	if !cpuAvailable {
		c.logger.Warn("cgroup cpu diagnostics unavailable", "path", cpuStatPath)
	}

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			post := c.post.snapshotAndReset()
			enqueue := c.enqueue.snapshotAndReset()
			attrs := []any{
				"post_count", post.count,
				"post_p99_bucket_ms", post.p99UpperMS,
				"enqueue_count", enqueue.count,
				"enqueue_p99_bucket_ms", enqueue.p99UpperMS,
			}

			if currentCPU, ok := readCPUStat(cpuStatPath); ok {
				if cpuAvailable {
					delta := currentCPU.delta(previousCPU)
					attrs = append(attrs,
						"cpu_usage_ms", delta.usageUsec/1000,
						"cpu_periods", delta.periods,
						"cpu_throttled_periods", delta.throttledPeriods,
						"cpu_throttled_ms", delta.throttledUsec/1000,
						"cpu_throttled_ratio_pct", delta.throttledRatio(),
					)
				}
				previousCPU = currentCPU
				cpuAvailable = true
			}

			c.logger.Info("runtime diagnostics", attrs...)
		}
	}
}

type latencyHistogram struct {
	buckets [len(latencyBounds) + 1]atomic.Uint64
}

func (h *latencyHistogram) observe(duration time.Duration) {
	for i, upperBound := range latencyBounds {
		if duration <= upperBound {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(h.buckets)-1].Add(1)
}

type latencySnapshot struct {
	count      uint64
	p99UpperMS int64
}

func (h *latencyHistogram) snapshotAndReset() latencySnapshot {
	counts := make([]uint64, len(h.buckets))
	var total uint64
	for i := range h.buckets {
		counts[i] = h.buckets[i].Swap(0)
		total += counts[i]
	}
	if total == 0 {
		return latencySnapshot{}
	}

	target := (total*99 + 99) / 100
	var cumulative uint64
	for i, count := range counts {
		cumulative += count
		if cumulative < target {
			continue
		}
		if i < len(latencyBounds) {
			return latencySnapshot{count: total, p99UpperMS: latencyBounds[i].Milliseconds()}
		}
		return latencySnapshot{count: total, p99UpperMS: 101}
	}

	return latencySnapshot{count: total, p99UpperMS: 101}
}

type cpuStat struct {
	usageUsec        uint64
	periods          uint64
	throttledPeriods uint64
	throttledUsec    uint64
}

func readCPUStat(path string) (cpuStat, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return cpuStat{}, false
	}
	stat, err := parseCPUStat(string(data))
	return stat, err == nil
}

func parseCPUStat(value string) (cpuStat, error) {
	fields := strings.Fields(value)
	if len(fields)%2 != 0 {
		return cpuStat{}, fmt.Errorf("invalid cpu.stat field count %d", len(fields))
	}

	var stat cpuStat
	for i := 0; i < len(fields); i += 2 {
		parsed, err := strconv.ParseUint(fields[i+1], 10, 64)
		if err != nil {
			return cpuStat{}, fmt.Errorf("invalid cpu.stat value for %s: %w", fields[i], err)
		}
		switch fields[i] {
		case "usage_usec":
			stat.usageUsec = parsed
		case "nr_periods":
			stat.periods = parsed
		case "nr_throttled":
			stat.throttledPeriods = parsed
		case "throttled_usec":
			stat.throttledUsec = parsed
		}
	}
	return stat, nil
}

func (c cpuStat) delta(previous cpuStat) cpuStat {
	return cpuStat{
		usageUsec:        subtract(c.usageUsec, previous.usageUsec),
		periods:          subtract(c.periods, previous.periods),
		throttledPeriods: subtract(c.throttledPeriods, previous.throttledPeriods),
		throttledUsec:    subtract(c.throttledUsec, previous.throttledUsec),
	}
}

func (c cpuStat) throttledRatio() float64 {
	if c.periods == 0 {
		return 0
	}
	return float64(c.throttledPeriods) * 100 / float64(c.periods)
}

func subtract(current, previous uint64) uint64 {
	if current < previous {
		return 0
	}
	return current - previous
}
