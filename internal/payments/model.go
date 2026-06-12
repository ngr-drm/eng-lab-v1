package payments

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type ProcessorName string

const (
	ProcessorDefault  ProcessorName = "default"
	ProcessorFallback ProcessorName = "fallback"
)

type Payment struct {
	CorrelationID string
	AmountCents   int64
	RequestedAt   time.Time
}

type ConfirmedPayment struct {
	Payment
	Processor ProcessorName
}

type Summary struct {
	Default  SummaryBucket
	Fallback SummaryBucket
}

type SummaryBucket struct {
	TotalRequests int64
	TotalCents    int64
}

func ParseCents(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, errors.New("amount is required")
	}
	if strings.HasPrefix(s, "-") {
		return 0, errors.New("amount must be positive")
	}

	parts := strings.Split(s, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}

	whole := parts[0]
	if whole == "" {
		whole = "0"
	}
	reais, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid amount %q", raw)
	}

	var cents int64
	if len(parts) == 2 {
		fraction := parts[1]
		if len(fraction) > 2 {
			return 0, fmt.Errorf("amount has more than two decimal places")
		}
		for len(fraction) < 2 {
			fraction += "0"
		}
		cents, err = strconv.ParseInt(fraction, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid amount %q", raw)
		}
	}

	total := reais*100 + cents
	if total <= 0 {
		return 0, errors.New("amount must be positive")
	}
	return total, nil
}

func FormatCents(cents int64) string {
	negative := cents < 0
	if negative {
		cents = -cents
	}
	value := fmt.Sprintf("%d.%02d", cents/100, cents%100)
	if negative {
		return "-" + value
	}
	return value
}
