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
