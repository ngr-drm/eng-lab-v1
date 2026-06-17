package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"eng-lab-v1/internal/payments"
)

type Service interface {
	Accept(ctx context.Context, payment payments.Payment) (bool, error)
	Summary(ctx context.Context, from, to *time.Time) (payments.Summary, error)
}

type Handler struct {
	service Service
	logger  *slog.Logger
}

func Register(mux *http.ServeMux, service Service, logger *slog.Logger) {
	handler := Handler{service: service, logger: logger}
	mux.HandleFunc("POST /payments", handler.payments)
	mux.HandleFunc("GET /payments-summary", handler.summary)
	mux.HandleFunc("GET /health", handler.health)
}

func (h Handler) payments(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var input paymentInput
	decoder := json.NewDecoder(r.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	correlationID := strings.TrimSpace(input.CorrelationID)
	if correlationID == "" {
		http.Error(w, "invalid correlationId", http.StatusBadRequest)
		return
	}

	amount, err := payments.ParseCents(input.Amount.String())
	if err != nil {
		http.Error(w, "invalid amount", http.StatusBadRequest)
		return
	}

	requestedAt := time.Now().UTC()
	_, err = h.service.Accept(r.Context(), payments.Payment{
		CorrelationID: correlationID,
		AmountCents:   amount,
		RequestedAt:   requestedAt,
	})
	if err != nil {
		if errors.Is(err, payments.ErrQueueFull) {
			http.Error(w, "queue full", http.StatusTooManyRequests)
			return
		}
		h.logger.Warn("failed to accept payment", "err", err)
		http.Error(w, "payment not accepted", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h Handler) summary(w http.ResponseWriter, r *http.Request) {
	from, ok := parseOptionalTime(w, r.URL.Query().Get("from"), "from")
	if !ok {
		return
	}
	to, ok := parseOptionalTime(w, r.URL.Query().Get("to"), "to")
	if !ok {
		return
	}

	summary, err := h.service.Summary(r.Context(), from, to)
	if err != nil {
		h.logger.Warn("failed to build summary", "err", err)
		http.Error(w, "summary unavailable", http.StatusServiceUnavailable)
		return
	}

	writeJSON(w, summaryOutput{
		Default: summaryBucketOutput{
			TotalRequests: summary.Default.TotalRequests,
			TotalAmount:   moneyJSON(payments.FormatCents(summary.Default.TotalCents)),
		},
		Fallback: summaryBucketOutput{
			TotalRequests: summary.Fallback.TotalRequests,
			TotalAmount:   moneyJSON(payments.FormatCents(summary.Fallback.TotalCents)),
		},
	})
}

func (h Handler) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func parseOptionalTime(w http.ResponseWriter, value, name string) (*time.Time, bool) {
	if strings.TrimSpace(value) == "" {
		return nil, true
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		http.Error(w, "invalid "+name, http.StatusBadRequest)
		return nil, false
	}
	utc := parsed.UTC()
	return &utc, true
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

type paymentInput struct {
	CorrelationID string      `json:"correlationId"`
	Amount        json.Number `json:"amount"`
}

type summaryOutput struct {
	Default  summaryBucketOutput `json:"default"`
	Fallback summaryBucketOutput `json:"fallback"`
}

type summaryBucketOutput struct {
	TotalRequests int64     `json:"totalRequests"`
	TotalAmount   moneyJSON `json:"totalAmount"`
}

type moneyJSON string

func (m moneyJSON) MarshalJSON() ([]byte, error) {
	return []byte(m), nil
}
