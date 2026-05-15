package hermes

import (
	"reflect"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sourceapi"
)

func TestNewTypeAndCapabilities(t *testing.T) {
	adapter := New()

	if got, want := adapter.Type(), "hermes_local"; got != want {
		t.Fatalf("Type() = %q, want %q", got, want)
	}

	want := []sourceapi.Capability{
		sourceapi.CapabilityMessages,
		sourceapi.CapabilityCommands,
		sourceapi.CapabilityToolCalls,
		sourceapi.CapabilityFileOps,
		sourceapi.CapabilityUsage,
	}
	if got := adapter.Capabilities(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Capabilities() = %#v, want %#v", got, want)
	}
}
