package command

import (
	"context"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestHealthValidatesExecutableAndStagingDir(t *testing.T) {
	t.Parallel()

	sink := New(sinkapi.Config{
		ID:   "local-copy",
		Type: "command",
		Settings: map[string]any{
			"argv":        []string{"/bin/cp", "{payload_path}", "/tmp/out"},
			"staging_dir": t.TempDir(),
		},
	})
	if err := sink.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
}
