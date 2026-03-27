package httpsink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestSendPreparedClassifiesPermanentAndRetryableResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   int
		wantPerm bool
	}{
		{name: "bad request is permanent", status: http.StatusBadRequest, wantPerm: true},
		{name: "server error retries", status: http.StatusInternalServerError, wantPerm: false},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("boom"))
			}))
			defer server.Close()

			sink := New(sinkapi.Config{
				ID:   "remote",
				Type: "http",
				Settings: map[string]any{
					"url": server.URL,
				},
			})
			if err := sink.Init(context.Background()); err != nil {
				t.Fatal(err)
			}

			_, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
				SinkID:          "remote",
				BatchID:         "batch-1",
				Payload:         []byte(`{"events":[]}`),
				PayloadSHA256:   schema.HashBytes([]byte(`{"events":[]}`)),
				ContentType:     "application/json",
				Headers:         map[string]string{"Content-Type": "application/json"},
				PayloadFormat:   "oas_batch_json",
				LedgerMinOffset: 1,
				LedgerMaxOffset: 1,
				EventCount:      1,
				CreatedAt:       time.Now().UTC(),
			})
			if err == nil {
				t.Fatal("expected send error")
			}
			var permanent *delivery.PermanentError
			gotPerm := errors.As(err, &permanent)
			if gotPerm != tc.wantPerm {
				t.Fatalf("permanent=%v, want %v (err=%v)", gotPerm, tc.wantPerm, err)
			}
		})
	}
}

func TestHealthFailsWhenBearerTokenEnvIsUnset(t *testing.T) {
	t.Parallel()

	sink := New(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"url":              "https://example.com/ingest",
			"bearer_token_env": "OAS_MISSING_TOKEN",
		},
	})
	if err := sink.Health(context.Background()); err == nil {
		t.Fatal("expected missing bearer token env to fail health")
	}
}

func TestHealthUsesOptionalProbeURL(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			t.Fatalf("probe path=%q, want /ready", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink := New(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"url":       server.URL + "/ingest",
			"probe_url": server.URL + "/ready",
		},
	})
	if err := sink.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
}

func TestSendPreparedUsesBearerTokenRefAndRecordsAuthMetrics(t *testing.T) {
	t.Setenv("OAS_REMOTE_TOKEN", "secret-token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.Header.Get("Authorization"), "Bearer secret-token"; got != want {
			t.Fatalf("Authorization = %q, want %q", got, want)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sink := New(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"url":              server.URL,
			"bearer_token_ref": "env://OAS_REMOTE_TOKEN",
		},
	})
	if err := sink.Init(context.Background()); err != nil {
		t.Fatal(err)
	}

	result, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
		SinkID:          "remote",
		BatchID:         "batch-1",
		Payload:         []byte(`{"events":[]}`),
		PayloadSHA256:   schema.HashBytes([]byte(`{"events":[]}`)),
		ContentType:     "application/json",
		Headers:         map[string]string{"Content-Type": "application/json"},
		PayloadFormat:   "oas_batch_json",
		LedgerMinOffset: 1,
		LedgerMaxOffset: 1,
		EventCount:      1,
		CreatedAt:       time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("SendPrepared() error = %v", err)
	}
	if got, want := len(result.AuthMetrics), 1; got != want {
		t.Fatalf("len(AuthMetrics) = %d, want %d", got, want)
	}
	if got, want := result.AuthMetrics[0].Provider, "env"; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
}

func TestSendPreparedClassifiesAuthBlockedResponses(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		wantKind delivery.BlockedKind
		wantCode string
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantKind: delivery.BlockedKindAuth, wantCode: "auth_denied"},
		{name: "forbidden", status: http.StatusForbidden, wantKind: delivery.BlockedKindConfig, wantCode: "sink_access_denied"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OAS_REMOTE_TOKEN", "secret-token")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer server.Close()

			sink := New(sinkapi.Config{
				ID:   "remote",
				Type: "http",
				Settings: map[string]any{
					"url":              server.URL,
					"bearer_token_ref": "env://OAS_REMOTE_TOKEN",
				},
			})
			if err := sink.Init(context.Background()); err != nil {
				t.Fatal(err)
			}

			_, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
				SinkID:          "remote",
				BatchID:         "batch-1",
				Payload:         []byte(`{"events":[]}`),
				PayloadSHA256:   schema.HashBytes([]byte(`{"events":[]}`)),
				ContentType:     "application/json",
				Headers:         map[string]string{"Content-Type": "application/json"},
				PayloadFormat:   "oas_batch_json",
				LedgerMinOffset: 1,
				LedgerMaxOffset: 1,
				EventCount:      1,
				CreatedAt:       time.Now().UTC(),
			})
			if err == nil {
				t.Fatal("expected blocked send error")
			}
			var blocked *delivery.BlockedError
			if !errors.As(err, &blocked) {
				t.Fatalf("SendPrepared() error = %T, want *BlockedError", err)
			}
			if got, want := blocked.Kind, tc.wantKind; got != want {
				t.Fatalf("Blocked kind = %q, want %q", got, want)
			}
			if got, want := blocked.Code, tc.wantCode; got != want {
				t.Fatalf("Blocked code = %q, want %q", got, want)
			}
		})
	}
}
