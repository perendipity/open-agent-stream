package supervisor

import (
	"bytes"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/internal/health"
)

func TestWriteChecksTableProducesReadableOutput(t *testing.T) {
	checks := []health.Check{
		{Name: "state_path", Status: "ok", Detail: "/tmp/state.db"},
		{Name: "storage_guard", Status: "warn", Detail: "usage_bytes=100 max_storage_bytes=80"},
	}

	var out bytes.Buffer
	if err := WriteChecksTable(&out, checks); err != nil {
		t.Fatalf("WriteChecksTable() error = %v", err)
	}

	text := out.String()
	for _, want := range []string{
		"NAME",
		"STATUS",
		"DETAIL",
		"state_path",
		"storage_guard",
		"warn",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("table output missing %q:\n%s", want, text)
		}
	}
}
