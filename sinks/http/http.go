package httpsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkpayload"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg            sinkapi.Config
	client         *http.Client
	url            string
	method         string
	format         string
	timeout        time.Duration
	headers        map[string]string
	bearerTokenEnv string
	successRules   []statusRule
	retryRules     []statusRule
	permanentRules []statusRule
}

type statusRule struct {
	min int
	max int
}

func New(cfg sinkapi.Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) ID() string   { return s.cfg.ID }
func (s *Sink) Type() string { return s.cfg.Type }

func (s *Sink) Init(context.Context) error {
	s.url = sinkutil.String(s.cfg, "url")
	if s.url == "" {
		return errors.New("http sink requires url")
	}
	s.method = strings.ToUpper(firstNonEmpty(sinkutil.String(s.cfg, "method"), http.MethodPost))
	s.format = firstNonEmpty(sinkutil.String(s.cfg, "format"), sinkpayload.FormatOASBatchJSON)
	timeout, err := sinkutil.Duration(s.cfg, "timeout", 10*time.Second)
	if err != nil {
		return err
	}
	s.timeout = timeout
	s.headers = sinkutil.StringMap(s.cfg, "headers")
	if s.headers == nil {
		s.headers = map[string]string{}
	}
	s.bearerTokenEnv = sinkutil.String(s.cfg, "bearer_token_env")
	s.successRules = parseStatusRules(sinkutil.StringSlice(s.cfg, "success_statuses"), []statusRule{{min: 200, max: 299}})
	s.retryRules = parseStatusRules(sinkutil.StringSlice(s.cfg, "retry_on_statuses"), []statusRule{{min: 429, max: 429}, {min: 500, max: 599}})
	s.permanentRules = parseStatusRules(sinkutil.StringSlice(s.cfg, "permanent_on_statuses"), []statusRule{{min: 400, max: 499}})
	s.client = &http.Client{Timeout: timeout}
	return nil
}

func (s *Sink) SealBatch(_ context.Context, batch sinkapi.Batch, meta delivery.PreparedDispatch) (delivery.PreparedDispatch, error) {
	payload, contentType, contentHeaders, err := sinkpayload.EncodeBatch(batch, s.format)
	if err != nil {
		return delivery.PreparedDispatch{}, err
	}
	headers := make(map[string]string, len(s.headers)+len(contentHeaders)+4)
	for k, v := range s.headers {
		headers[k] = v
	}
	for k, v := range contentHeaders {
		headers[k] = v
	}
	headers["X-OAS-Sink-ID"] = s.cfg.ID
	headers["X-OAS-Batch-ID"] = meta.BatchID
	headers["X-OAS-Ledger-Range"] = fmt.Sprintf("%d-%d", meta.LedgerMinOffset, meta.LedgerMaxOffset)
	meta.Payload = payload
	meta.PayloadSHA256 = hashPayload(payload)
	meta.ContentType = contentType
	meta.Headers = headers
	meta.PayloadFormat = s.format
	meta.Destination = map[string]any{
		"url":    s.url,
		"method": s.method,
	}
	return meta, nil
}

func (s *Sink) SendPrepared(ctx context.Context, prepared delivery.PreparedDispatch) (sinkapi.Result, error) {
	req, err := http.NewRequestWithContext(ctx, s.method, s.url, bytes.NewReader(prepared.Payload))
	if err != nil {
		return sinkapi.Result{}, err
	}
	for k, v := range prepared.Headers {
		req.Header.Set(k, v)
	}
	if s.bearerTokenEnv != "" {
		if token := strings.TrimSpace(os.Getenv(s.bearerTokenEnv)); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return sinkapi.Result{}, err
	}
	defer resp.Body.Close()
	if s.matches(resp.StatusCode, s.successRules) {
		return sinkapi.Result{
			Acked: prepared.EventCount,
			Checkpoint: schema.SinkCheckpoint{
				SinkID:        prepared.SinkID,
				AckedAt:       time.Now().UTC(),
				DeliveryCount: prepared.EventCount,
			},
		}, nil
	}
	message := httpStatusError(resp)
	if s.matches(resp.StatusCode, s.retryRules) || (resp.StatusCode >= 400 && resp.StatusCode < 500 && !s.matches(resp.StatusCode, s.permanentRules)) {
		return sinkapi.Result{}, errors.New(message)
	}
	return sinkapi.Result{}, delivery.NewPermanentError(errors.New(message))
}

func (s *Sink) SendBatch(ctx context.Context, batch sinkapi.Batch) (sinkapi.Result, error) {
	prepared, err := s.SealBatch(ctx, batch, delivery.PreparedDispatch{
		SinkID:        s.cfg.ID,
		BatchID:       "replay-" + strconv.FormatInt(time.Now().UTC().UnixNano(), 10),
		EventCount:    len(batch.Events),
		CreatedAt:     time.Now().UTC(),
		PayloadFormat: s.format,
	})
	if err != nil {
		return sinkapi.Result{}, err
	}
	return s.SendPrepared(ctx, prepared)
}

func (s *Sink) Flush(context.Context) error  { return nil }
func (s *Sink) Health(context.Context) error { return nil }
func (s *Sink) Close(context.Context) error  { return nil }

func parseStatusRules(values []string, fallback []statusRule) []statusRule {
	if len(values) == 0 {
		return fallback
	}
	out := make([]statusRule, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if strings.Contains(value, "-") {
			parts := strings.SplitN(value, "-", 2)
			min, errMin := strconv.Atoi(parts[0])
			max, errMax := strconv.Atoi(parts[1])
			if errMin == nil && errMax == nil {
				out = append(out, statusRule{min: min, max: max})
			}
			continue
		}
		status, err := strconv.Atoi(value)
		if err == nil {
			out = append(out, statusRule{min: status, max: status})
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func (s *Sink) matches(code int, rules []statusRule) bool {
	for _, rule := range rules {
		if code >= rule.min && code <= rule.max {
			if code == 429 && rule.min == 400 && rule.max == 499 {
				continue
			}
			return true
		}
	}
	return false
}

func httpStatusError(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	message := strings.TrimSpace(string(body))
	if message == "" {
		return "http sink returned " + resp.Status
	}
	return fmt.Sprintf("http sink returned %s: %s", resp.Status, message)
}

func hashPayload(payload []byte) string {
	return schema.HashBytes(payload)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
