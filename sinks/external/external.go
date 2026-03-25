package external

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkpayload"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/externalapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

type Sink struct {
	cfg            sinkapi.Config
	argv           []string
	pluginType     string
	format         string
	timeout        time.Duration
	maxOutputBytes int
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
	s.argv = resolved.Argv
	s.pluginType = resolved.PluginType
	s.format = resolved.Format
	s.timeout = resolved.Timeout
	s.maxOutputBytes = resolved.MaxOutputBytes
	return nil
}

func (s *Sink) SealBatch(_ context.Context, batch sinkapi.Batch, meta delivery.PreparedDispatch) (delivery.PreparedDispatch, error) {
	payload, contentType, headers, err := sinkpayload.EncodeBatch(batch, s.format)
	if err != nil {
		return delivery.PreparedDispatch{}, err
	}
	meta.Payload = payload
	meta.PayloadSHA256 = schema.HashBytes(payload)
	meta.ContentType = contentType
	meta.Headers = headers
	meta.PayloadFormat = s.format
	meta.Destination = map[string]any{
		"argv":             append([]string(nil), s.argv...),
		"plugin_type":      s.pluginType,
		"timeout_ms":       s.timeout.Milliseconds(),
		"max_output_bytes": s.maxOutputBytes,
	}
	return meta, nil
}

func (s *Sink) SendPrepared(ctx context.Context, prepared delivery.PreparedDispatch) (sinkapi.Result, error) {
	if err := s.handshake(ctx, prepared); err != nil {
		return sinkapi.Result{}, err
	}
	response, err := s.invoke(ctx, prepared, externalapi.ActionSend)
	if err != nil {
		return sinkapi.Result{}, err
	}
	switch response.Status {
	case externalapi.StatusOK:
		return sinkapi.Result{
			Acked: prepared.EventCount,
			Checkpoint: schema.SinkCheckpoint{
				SinkID:        prepared.SinkID,
				AckedAt:       time.Now().UTC(),
				DeliveryCount: prepared.EventCount,
			},
		}, nil
	case externalapi.StatusPermanent:
		return sinkapi.Result{}, delivery.NewPermanentError(errors.New(firstNonEmpty(response.Message, "external sink reported a permanent failure")))
	case externalapi.StatusRetry:
		return sinkapi.Result{}, errors.New(firstNonEmpty(response.Message, "external sink requested a retry"))
	default:
		return sinkapi.Result{}, fmt.Errorf("external sink returned unsupported status %q", response.Status)
	}
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
	if _, err := exec.LookPath(resolved.Argv[0]); err != nil {
		return err
	}
	prepared := delivery.PreparedDispatch{
		SinkID:      s.cfg.ID,
		BatchID:     "healthcheck",
		CreatedAt:   time.Now().UTC(),
		Destination: map[string]any{"argv": resolved.Argv, "plugin_type": resolved.PluginType, "timeout_ms": resolved.Timeout.Milliseconds(), "max_output_bytes": resolved.MaxOutputBytes},
	}
	if err := s.handshake(ctx, prepared); err != nil {
		return err
	}
	response, err := s.invoke(ctx, prepared, externalapi.ActionHealth)
	if err != nil {
		return err
	}
	if response.Status != externalapi.StatusOK {
		return errors.New(firstNonEmpty(response.Message, "external sink health failed"))
	}
	return nil
}

func (s *Sink) Close(context.Context) error { return nil }

type resolvedConfig struct {
	Argv           []string
	PluginType     string
	Format         string
	Timeout        time.Duration
	MaxOutputBytes int
}

func (s *Sink) resolve() (resolvedConfig, error) {
	argv := sinkutil.StringSlice(s.cfg, "argv")
	if len(argv) == 0 {
		return resolvedConfig{}, errors.New("external sink requires settings.argv")
	}
	pluginType := strings.TrimSpace(sinkutil.String(s.cfg, "plugin_type"))
	if pluginType == "" {
		return resolvedConfig{}, errors.New("external sink requires settings.plugin_type")
	}
	timeout, err := sinkutil.Duration(s.cfg, "timeout", 30*time.Second)
	if err != nil {
		return resolvedConfig{}, err
	}
	return resolvedConfig{
		Argv:           argv,
		PluginType:     pluginType,
		Format:         firstNonEmpty(sinkutil.String(s.cfg, "format"), sinkpayload.FormatCanonicalJSONLGZ),
		Timeout:        timeout,
		MaxOutputBytes: sinkutil.Int(s.cfg, "max_output_bytes", 4096),
	}, nil
}

func (s *Sink) handshake(ctx context.Context, prepared delivery.PreparedDispatch) error {
	response, err := s.invoke(ctx, prepared, externalapi.ActionHandshake)
	if err != nil {
		return err
	}
	if response.ProtocolVersion != externalapi.ProtocolVersion {
		return fmt.Errorf("external sink handshake protocol mismatch: got %q, want %q", response.ProtocolVersion, externalapi.ProtocolVersion)
	}
	if response.Status != externalapi.StatusOK {
		return errors.New(firstNonEmpty(response.Message, "external sink handshake failed"))
	}
	expectedPluginType := stringFromMap(prepared.Destination, "plugin_type", s.pluginType)
	if response.PluginType != "" && response.PluginType != expectedPluginType {
		return fmt.Errorf("external sink handshake plugin mismatch: got %q, want %q", response.PluginType, expectedPluginType)
	}
	return nil
}

func (s *Sink) invoke(ctx context.Context, prepared delivery.PreparedDispatch, action string) (externalapi.Response, error) {
	argv := stringSliceFromMap(prepared.Destination, "argv")
	if len(argv) == 0 {
		return externalapi.Response{}, delivery.NewPermanentError(errors.New("external sink sealed batch missing argv"))
	}
	timeout := durationFromMap(prepared.Destination, "timeout_ms", 30*time.Second)
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request := externalapi.Request{
		ProtocolVersion: externalapi.ProtocolVersion,
		Action:          action,
		PluginType:      stringFromMap(prepared.Destination, "plugin_type", s.pluginType),
		SinkConfig:      s.cfg,
	}
	if action == externalapi.ActionSend {
		request.Prepared = &externalapi.PreparedDispatch{
			SinkID:          prepared.SinkID,
			BatchID:         prepared.BatchID,
			Payload:         prepared.Payload,
			PayloadSHA256:   prepared.PayloadSHA256,
			ContentType:     prepared.ContentType,
			Headers:         prepared.Headers,
			PayloadFormat:   prepared.PayloadFormat,
			LedgerMinOffset: prepared.LedgerMinOffset,
			LedgerMaxOffset: prepared.LedgerMaxOffset,
			EventCount:      prepared.EventCount,
			CreatedAt:       prepared.CreatedAt.UTC().Format(time.RFC3339Nano),
			Destination:     prepared.Destination,
		}
	}
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return externalapi.Response{}, err
	}

	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Stdin = bytes.NewReader(requestBytes)
	stdout := &limitedBuffer{max: int64(intFromMap(prepared.Destination, "max_output_bytes", 4096))}
	stderr := &limitedBuffer{max: int64(intFromMap(prepared.Destination, "max_output_bytes", 4096))}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if runCtx.Err() == context.DeadlineExceeded {
			return externalapi.Response{}, fmt.Errorf("external sink timed out after %s: %s", timeout, stderr.String())
		}
		return externalapi.Response{}, fmt.Errorf("external sink command failed: %v: %s", err, stderr.String())
	}
	var response externalapi.Response
	if err := json.Unmarshal(stdout.buf.Bytes(), &response); err != nil {
		return externalapi.Response{}, fmt.Errorf("external sink returned invalid JSON: %w", err)
	}
	return response, nil
}

type limitedBuffer struct {
	max int64
	buf bytes.Buffer
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.max <= 0 {
		return len(p), nil
	}
	if int64(b.buf.Len()) >= b.max {
		return len(p), nil
	}
	remaining := int(b.max - int64(b.buf.Len()))
	if remaining < len(p) {
		p = p[:remaining]
	}
	return b.buf.Write(p)
}

func (b *limitedBuffer) String() string {
	return strings.TrimSpace(b.buf.String())
}

func stringSliceFromMap(values map[string]any, key string) []string {
	raw, ok := values[key]
	if !ok {
		return nil
	}
	switch typed := raw.(type) {
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func stringFromMap(values map[string]any, key, fallback string) string {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fallback
}

func intFromMap(values map[string]any, key string, fallback int) int {
	raw, ok := values[key]
	if !ok {
		return fallback
	}
	switch typed := raw.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	default:
		return fallback
	}
}

func durationFromMap(values map[string]any, key string, fallback time.Duration) time.Duration {
	return time.Duration(intFromMap(values, key, int(fallback/time.Millisecond))) * time.Millisecond
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
