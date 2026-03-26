package ledger

import (
	"path/filepath"
	"testing"
)

func TestMetadataPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")

	first, err := Open(path)
	if err != nil {
		t.Fatalf("Open(first) error = %v", err)
	}
	firstMeta := first.Metadata()
	if firstMeta.InstanceID == "" {
		t.Fatal("first metadata instance id unexpectedly empty")
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(first) error = %v", err)
	}

	second, err := Open(path)
	if err != nil {
		t.Fatalf("Open(second) error = %v", err)
	}
	defer second.Close()
	secondMeta := second.Metadata()
	if secondMeta.InstanceID != firstMeta.InstanceID {
		t.Fatalf("InstanceID = %q, want %q", secondMeta.InstanceID, firstMeta.InstanceID)
	}
	if !secondMeta.CreatedAt.Equal(firstMeta.CreatedAt) {
		t.Fatalf("CreatedAt = %s, want %s", secondMeta.CreatedAt, firstMeta.CreatedAt)
	}
}
