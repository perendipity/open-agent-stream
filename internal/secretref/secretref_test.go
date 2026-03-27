package secretref

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseRejectsExperimentalProvidersByDefault(t *testing.T) {
	if _, err := Parse("keychain://oas/service-token"); err == nil {
		t.Fatal("expected keychain ref to be rejected without experimental flag")
	} else {
		var refErr *Error
		if !errors.As(err, &refErr) {
			t.Fatalf("Parse() error = %T, want *Error", err)
		}
		if got, want := refErr.Kind, KindInvalidRef; got != want {
			t.Fatalf("Kind = %q, want %q", got, want)
		}
	}

	t.Setenv(experimentalProvidersEnv, "1")
	ref, err := Parse("pass://shared/oas/s3")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if got, want := ref.Provider, ProviderPass; got != want {
		t.Fatalf("Provider = %q, want %q", got, want)
	}
	if got, want := ref.Target, "shared/oas/s3"; got != want {
		t.Fatalf("Target = %q, want %q", got, want)
	}
}

func TestResolveEnvCachesSuccessWithoutNegativeCaching(t *testing.T) {
	resolver := NewResolver(time.Hour)
	t.Setenv("OAS_SECRETREF_CACHE", "alpha")

	first, err := resolver.Resolve(context.Background(), "env://OAS_SECRETREF_CACHE")
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got, want := string(first.Value), "alpha"; got != want {
		t.Fatalf("resolved value = %q, want %q", got, want)
	}
	first.Value[0] = 'X'

	second, err := resolver.Resolve(context.Background(), "env://OAS_SECRETREF_CACHE")
	if err != nil {
		t.Fatalf("second Resolve() error = %v", err)
	}
	if got, want := string(second.Value), "alpha"; got != want {
		t.Fatalf("cached value = %q, want %q", got, want)
	}

	if _, err := resolver.Resolve(context.Background(), "env://OAS_SECRETREF_MISSING"); err == nil {
		t.Fatal("expected missing env ref to fail")
	}

	t.Setenv("OAS_SECRETREF_MISSING", "beta")
	resolved, err := resolver.Resolve(context.Background(), "env://OAS_SECRETREF_MISSING")
	if err != nil {
		t.Fatalf("Resolve() after env set error = %v", err)
	}
	if got, want := string(resolved.Value), "beta"; got != want {
		t.Fatalf("resolved value after miss = %q, want %q", got, want)
	}
}

func TestInspectStaticFileRefWarnsForGroupAccessAndWorktreeAndRejectsWorldAccess(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	worktreeRoot := filepath.Join(base, "repo")
	if err := os.MkdirAll(filepath.Join(worktreeRoot, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	resolvedWorktreeRoot, err := filepath.EvalSymlinks(worktreeRoot)
	if err != nil {
		t.Fatal(err)
	}

	warnPath := filepath.Join(worktreeRoot, "secrets", "token.txt")
	if err := os.MkdirAll(filepath.Dir(warnPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(warnPath, []byte("secret"), 0o640); err != nil {
		t.Fatal(err)
	}

	inspection, err := InspectStatic("file://"+warnPath, StaticOptions{WorktreeRoot: resolvedWorktreeRoot})
	if err != nil {
		t.Fatalf("InspectStatic() error = %v", err)
	}
	warnings := strings.Join(inspection.Warnings, " | ")
	for _, want := range []string{"group access", "inside the current worktree"} {
		if !strings.Contains(warnings, want) {
			t.Fatalf("warnings = %q, want substring %q", warnings, want)
		}
	}

	badPath := filepath.Join(base, "world-readable.txt")
	if err := os.WriteFile(badPath, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = InspectStatic("file://"+badPath, StaticOptions{})
	if err == nil {
		t.Fatal("expected world-readable file ref to fail")
	}
	var refErr *Error
	if !errors.As(err, &refErr) {
		t.Fatalf("InspectStatic() error = %T, want *Error", err)
	}
	if got, want := refErr.Kind, KindInsecureSecretFile; got != want {
		t.Fatalf("Kind = %q, want %q", got, want)
	}
	if strings.Contains(err.Error(), badPath) {
		t.Fatalf("error leaked raw path: %v", err)
	}
}
