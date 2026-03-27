package sinkauth

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func TestHTTPSupportsSecretRefAndLegacyEnv(t *testing.T) {
	t.Parallel()

	settings, err := HTTP(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"bearer_token_env": "OAS_REMOTE_TOKEN",
		},
	})
	if err != nil {
		t.Fatalf("HTTP() error = %v", err)
	}
	if got, want := settings.BearerTokenRef, "env://OAS_REMOTE_TOKEN"; got != want {
		t.Fatalf("BearerTokenRef = %q, want %q", got, want)
	}
	if !settings.LegacyEnv {
		t.Fatal("LegacyEnv = false, want true")
	}

	settings, err = HTTP(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"bearer_token_ref": "op://vault/oas/token",
		},
	})
	if err != nil {
		t.Fatalf("HTTP() with secret ref error = %v", err)
	}
	if settings.LegacyEnv {
		t.Fatal("LegacyEnv = true, want false")
	}
}

func TestHTTPRejectsMutuallyExclusiveTokenFields(t *testing.T) {
	t.Parallel()

	_, err := HTTP(sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"bearer_token_ref": "env://OAS_REMOTE_TOKEN",
			"bearer_token_env": "OAS_REMOTE_TOKEN",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("HTTP() error = %v, want mutually exclusive validation", err)
	}
}

func TestS3DefaultsAndAuthModeValidation(t *testing.T) {
	t.Parallel()

	settings, err := S3(sinkapi.Config{
		ID:   "archive",
		Type: "s3",
	})
	if err != nil {
		t.Fatalf("S3() default error = %v", err)
	}
	if settings.Present {
		t.Fatal("Present = true, want false")
	}
	if !settings.ImplicitDefaultMode {
		t.Fatal("ImplicitDefaultMode = false, want true")
	}

	tests := []struct {
		name     string
		settings map[string]any
		wantErr  string
	}{
		{
			name:     "missing mode",
			settings: map[string]any{"auth": map[string]any{}},
			wantErr:  "settings.auth.mode is required",
		},
		{
			name: "default chain disallows extras",
			settings: map[string]any{
				"auth": map[string]any{
					"mode":    S3AuthModeDefaultChain,
					"profile": "shared",
				},
			},
			wantErr: "does not allow additional auth fields",
		},
		{
			name: "profile requires profile name",
			settings: map[string]any{
				"auth": map[string]any{
					"mode": S3AuthModeProfile,
				},
			},
			wantErr: "settings.auth.profile is required",
		},
		{
			name: "secret refs require access key pair",
			settings: map[string]any{
				"auth": map[string]any{
					"mode": S3AuthModeSecretRefs,
				},
			},
			wantErr: "requires access_key_id_ref and secret_access_key_ref",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := S3(sinkapi.Config{
				ID:       "archive",
				Type:     "s3",
				Settings: tc.settings,
			})
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("S3() error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestStaticChecksSanitizeFileRefDetails(t *testing.T) {
	t.Parallel()

	worktree := t.TempDir()
	refPath := filepath.Join(worktree, "secrets", "token.txt")
	if err := os.MkdirAll(filepath.Dir(refPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(refPath, []byte("secret"), 0o640); err != nil {
		t.Fatal(err)
	}

	checks, err := StaticChecks(context.Background(), sinkapi.Config{
		ID:   "remote",
		Type: "http",
		Settings: map[string]any{
			"bearer_token_ref": "file://" + refPath,
		},
	}, worktree)
	if err != nil {
		t.Fatalf("StaticChecks() error = %v", err)
	}
	if got, want := len(checks), 1; got != want {
		t.Fatalf("len(checks) = %d, want %d", got, want)
	}
	if got, want := checks[0].Status, "warn"; got != want {
		t.Fatalf("Status = %q, want %q", got, want)
	}
	if !strings.Contains(checks[0].Detail, "provider=file") {
		t.Fatalf("detail = %q, want provider marker", checks[0].Detail)
	}
	if strings.Contains(checks[0].Detail, refPath) {
		t.Fatalf("detail leaked raw path: %q", checks[0].Detail)
	}
}
