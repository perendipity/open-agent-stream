package httpsink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	resolved, err := s.resolve()
	if err != nil {
		return err
	}
	s.url = resolved.URL
	s.method = resolved.Method
	s.format = resolved.Format
	s.timeout = resolved.Timeout
	s.headers = resolved.Headers
	s.bearerTokenEnv = resolved.BearerTokenEnv
	s.successRules = resolved.SuccessRules
	s.retryRules = resolved.RetryRules
	s.permanentRules = resolved.PermanentRules
	s.client = &http.Client{Timeout: resolved.Timeout}
	return nil
}

func (s *Sink) resolve() (resolvedConfig, error) {
	resolved := resolvedConfig{
		URL:            sinkutil.String(s.cfg, "url"),
		Method:         strings.ToUpper(firstNonEmpty(sinkutil.String(s.cfg, "method"), http.MethodPost)),
		Format:         firstNonEmpty(sinkutil.String(s.cfg, "format"), sinkpayload.FormatOASBatchJSON),
		Headers:        sinkutil.StringMap(s.cfg, "headers"),
		BearerTokenEnv: sinkutil.String(s.cfg, "bearer_token_env"),
		ProbeURL:       sinkutil.String(s.cfg, "probe_url"),
		ProbeMethod:    strings.ToUpper(firstNonEmpty(sinkutil.String(s.cfg, "probe_method"), http.MethodHead)),
		SuccessRules:   parseStatusRules(sinkutil.StringSlice(s.cfg, "success_statuses"), []statusRule{{min: 200, max: 299}}),
		RetryRules:     parseStatusRules(sinkutil.StringSlice(s.cfg, "retry_on_statuses"), []statusRule{{min: 429, max: 429}, {min: 500, max: 599}}),
		PermanentRules: parseStatusRules(sinkutil.StringSlice(s.cfg, "permanent_on_statuses"), []statusRule{{min: 400, max: 499}}),
	}
	if resolved.URL == "" {
		return resolvedConfig{}, errors.New("http sink requires url")
	}
	if _, err := url.ParseRequestURI(resolved.URL); err != nil {
		return resolvedConfig{}, fmt.Errorf("http sink url: %w", err)
	}
	timeout, err := sinkutil.Duration(s.cfg, "timeout", 10*time.Second)
	if err != nil {
		return resolvedConfig{}, err
	}
	resolved.Timeout = timeout
	if resolved.Headers == nil {
		resolved.Headers = map[string]string{}
	}
	if resolved.ProbeURL != "" {
		if _, err := url.ParseRequestURI(resolved.ProbeURL); err != nil {
			return resolvedConfig{}, fmt.Errorf("http sink probe_url: %w", err)
		}
	}
	return resolved, nil
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

func (s *Sink) Flush(context.Context) error { return nil }
func (s *Sink) Health(ctx context.Context) error {
	resolved, err := s.resolve()
	if err != nil {
		return err
	}
	if resolved.BearerTokenEnv != "" && strings.TrimSpace(os.Getenv(resolved.BearerTokenEnv)) == "" {
		return fmt.Errorf("http sink bearer_token_env %q is set but empty", resolved.BearerTokenEnv)
	}
	if resolved.ProbeURL == "" {
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, resolved.ProbeMethod, resolved.ProbeURL, nil)
	if err != nil {
		return err
	}
	for k, v := range resolved.Headers {
		req.Header.Set(k, v)
	}
	if resolved.BearerTokenEnv != "" {
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(os.Getenv(resolved.BearerTokenEnv)))
	}
	client := &http.Client{Timeout: resolved.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		return nil
	}
	return errors.New("http sink probe returned " + resp.Status)
}
func (s *Sink) Close(context.Context) error { return nil }

type resolvedConfig struct {
	URL            string
	Method         string
	Format         string
	Timeout        time.Duration
	Headers        map[string]string
	BearerTokenEnv string
	ProbeURL       string
	ProbeMethod    string
	SuccessRules   []statusRule
	RetryRules     []statusRule
	PermanentRules []statusRule
}

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
