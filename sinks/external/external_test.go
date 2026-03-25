package external

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/open-agent-stream/open-agent-stream/internal/delivery"
	"github.com/open-agent-stream/open-agent-stream/pkg/externalapi"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestHealthHandshakesWithExternalSink(t *testing.T) {
	t.Parallel()

	sink := New(testExternalConfig(t, "success"))
	if err := sink.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
}

func TestSendPreparedClassifiesPermanentFailures(t *testing.T) {
	t.Parallel()

	sink := New(testExternalConfig(t, "permanent"))
	if err := sink.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := sink.SendPrepared(context.Background(), delivery.PreparedDispatch{
		SinkID:      "vendor",
		BatchID:     "batch-1",
		Payload:     []byte("payload"),
		ContentType: "application/json",
		EventCount:  1,
		CreatedAt:   time.Now().UTC(),
		Destination: map[string]any{
			"argv":             helperArgv(t, "permanent"),
			"plugin_type":      "vendor-plugin",
			"timeout_ms":       int64(30000),
			"max_output_bytes": 4096,
		},
	})
	if err == nil {
		t.Fatal("expected permanent failure")
	}
	var permanent *delivery.PermanentError
	if !errors.As(err, &permanent) {
		t.Fatalf("err=%v, want permanent error", err)
	}
}

func TestExternalHelperProcess(t *testing.T) {
	if !helperProcessInvocation() {
		return
	}
	mode := "success"
	for idx, arg := range os.Args {
		if arg == "--" && idx+1 < len(os.Args) {
			mode = os.Args[idx+1]
			break
		}
	}
	var request externalapi.Request
	if err := json.NewDecoder(os.Stdin).Decode(&request); err != nil {
		panic(err)
	}
	response := externalapi.Response{
		ProtocolVersion: externalapi.ProtocolVersion,
		Status:          externalapi.StatusOK,
		PluginType:      request.PluginType,
		SupportedActions: []string{
			externalapi.ActionHandshake,
			externalapi.ActionHealth,
			externalapi.ActionSend,
		},
	}
	if request.Action == externalapi.ActionSend {
		switch mode {
		case "permanent":
			response.Status = externalapi.StatusPermanent
			response.Message = "reject"
		case "retry":
			response.Status = externalapi.StatusRetry
			response.Message = "retry later"
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		panic(err)
	}
	os.Exit(0)
}

func testExternalConfig(t *testing.T, mode string) sinkapi.Config {
	t.Helper()
	return sinkapi.Config{
		ID:   "vendor",
		Type: "external",
		Settings: map[string]any{
			"argv":        helperArgv(t, mode),
			"plugin_type": "vendor-plugin",
			"format":      "oas_batch_json",
		},
	}
}

func helperArgv(t *testing.T, mode string) []string {
	t.Helper()
	return []string{os.Args[0], "-test.run=TestExternalHelperProcess", "--", mode}
}

func helperProcessInvocation() bool {
	hasRun := false
	hasMarker := false
	for _, arg := range os.Args {
		if arg == "-test.run=TestExternalHelperProcess" {
			hasRun = true
		}
		if arg == "--" {
			hasMarker = true
		}
	}
	return hasRun && hasMarker
}
