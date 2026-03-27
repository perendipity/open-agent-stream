package sinkauth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/health"
	"github.com/open-agent-stream/open-agent-stream/internal/secretref"
	"github.com/open-agent-stream/open-agent-stream/internal/sinkutil"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

const (
	S3AuthModeDefaultChain = "default_chain"
	S3AuthModeProfile      = "profile"
	S3AuthModeSecretRefs   = "secret_refs"
)

type HTTPSettings struct {
	BearerTokenRef string
	LegacyEnv      bool
}

type S3Settings struct {
	Present             bool
	Mode                string
	Profile             string
	CredentialsFileRef  string
	ConfigFileRef       string
	AccessKeyIDRef      string
	SecretAccessKeyRef  string
	SessionTokenRef     string
	ImplicitDefaultMode bool
}

type RefUsage struct {
	Field string
	Ref   string
}

func Validate(cfg sinkapi.Config) error {
	switch cfg.Type {
	case "http", "webhook":
		_, err := HTTP(cfg)
		return err
	case "s3":
		_, err := S3(cfg)
		return err
	default:
		return nil
	}
}

func HTTP(cfg sinkapi.Config) (HTTPSettings, error) {
	ref := strings.TrimSpace(sinkutil.String(cfg, "bearer_token_ref"))
	legacyEnv := strings.TrimSpace(sinkutil.String(cfg, "bearer_token_env"))
	if ref != "" && legacyEnv != "" {
		return HTTPSettings{}, errors.New("bearer_token_ref and bearer_token_env are mutually exclusive")
	}
	if ref != "" {
		if _, err := secretref.Parse(ref); err != nil {
			return HTTPSettings{}, fmt.Errorf("bearer_token_ref: %w", err)
		}
		return HTTPSettings{BearerTokenRef: ref}, nil
	}
	if legacyEnv == "" {
		return HTTPSettings{}, nil
	}
	ref = "env://" + legacyEnv
	if _, err := secretref.Parse(ref); err != nil {
		return HTTPSettings{}, fmt.Errorf("bearer_token_env: %w", err)
	}
	return HTTPSettings{BearerTokenRef: ref, LegacyEnv: true}, nil
}

func S3(cfg sinkapi.Config) (S3Settings, error) {
	settings := S3Settings{ImplicitDefaultMode: true}
	auth := sinkutil.Map(cfg, "auth")
	if auth == nil {
		return settings, nil
	}
	settings.Present = true
	settings.ImplicitDefaultMode = false
	settings.Mode = strings.TrimSpace(sinkutil.StringFromMap(auth, "mode"))
	settings.Profile = strings.TrimSpace(sinkutil.StringFromMap(auth, "profile"))
	settings.CredentialsFileRef = strings.TrimSpace(sinkutil.StringFromMap(auth, "credentials_file_ref"))
	settings.ConfigFileRef = strings.TrimSpace(sinkutil.StringFromMap(auth, "config_file_ref"))
	settings.AccessKeyIDRef = strings.TrimSpace(sinkutil.StringFromMap(auth, "access_key_id_ref"))
	settings.SecretAccessKeyRef = strings.TrimSpace(sinkutil.StringFromMap(auth, "secret_access_key_ref"))
	settings.SessionTokenRef = strings.TrimSpace(sinkutil.StringFromMap(auth, "session_token_ref"))

	if settings.Mode == "" {
		return S3Settings{}, errors.New("settings.auth.mode is required when settings.auth is present")
	}
	switch settings.Mode {
	case S3AuthModeDefaultChain:
		if settings.Profile != "" || settings.CredentialsFileRef != "" || settings.ConfigFileRef != "" || settings.AccessKeyIDRef != "" || settings.SecretAccessKeyRef != "" || settings.SessionTokenRef != "" {
			return S3Settings{}, errors.New("settings.auth.mode=default_chain does not allow additional auth fields")
		}
	case S3AuthModeProfile:
		if settings.Profile == "" {
			return S3Settings{}, errors.New("settings.auth.profile is required when settings.auth.mode=profile")
		}
		if settings.AccessKeyIDRef != "" || settings.SecretAccessKeyRef != "" || settings.SessionTokenRef != "" {
			return S3Settings{}, errors.New("settings.auth.mode=profile does not allow secret_refs fields")
		}
		for field, ref := range map[string]string{
			"credentials_file_ref": settings.CredentialsFileRef,
			"config_file_ref":      settings.ConfigFileRef,
		} {
			if ref == "" {
				continue
			}
			if _, err := secretref.Parse(ref); err != nil {
				return S3Settings{}, fmt.Errorf("settings.auth.%s: %w", field, err)
			}
		}
	case S3AuthModeSecretRefs:
		if settings.Profile != "" || settings.CredentialsFileRef != "" || settings.ConfigFileRef != "" {
			return S3Settings{}, errors.New("settings.auth.mode=secret_refs does not allow profile fields")
		}
		if settings.AccessKeyIDRef == "" || settings.SecretAccessKeyRef == "" {
			return S3Settings{}, errors.New("settings.auth.mode=secret_refs requires access_key_id_ref and secret_access_key_ref")
		}
		for field, ref := range map[string]string{
			"access_key_id_ref":     settings.AccessKeyIDRef,
			"secret_access_key_ref": settings.SecretAccessKeyRef,
			"session_token_ref":     settings.SessionTokenRef,
		} {
			if ref == "" {
				continue
			}
			if _, err := secretref.Parse(ref); err != nil {
				return S3Settings{}, fmt.Errorf("settings.auth.%s: %w", field, err)
			}
		}
	default:
		return S3Settings{}, errors.New(`settings.auth.mode must be "default_chain", "profile", or "secret_refs"`)
	}
	return settings, nil
}

func StaticChecks(_ context.Context, cfg sinkapi.Config, worktreeRoot string) ([]health.Check, error) {
	switch cfg.Type {
	case "http", "webhook":
		httpCfg, err := HTTP(cfg)
		if err != nil {
			return []health.Check{{
				Name:   "sink-auth-config:" + cfg.ID,
				Status: "fail",
				Detail: err.Error(),
			}}, nil
		}
		if httpCfg.BearerTokenRef == "" {
			return nil, nil
		}
		return checksForRefs(cfg.ID, "http token ref", []RefUsage{{Field: "bearer_token_ref", Ref: httpCfg.BearerTokenRef}}, worktreeRoot)
	case "s3":
		s3Cfg, err := S3(cfg)
		if err != nil {
			return []health.Check{{
				Name:   "sink-auth-config:" + cfg.ID,
				Status: "fail",
				Detail: err.Error(),
			}}, nil
		}
		mode := S3AuthModeDefaultChain
		if s3Cfg.Present {
			mode = s3Cfg.Mode
		}
		checks := []health.Check{{
			Name:   "sink-auth-config:" + cfg.ID,
			Status: "ok",
			Detail: "mode=" + mode,
		}}
		var refs []RefUsage
		switch mode {
		case S3AuthModeProfile:
			if s3Cfg.CredentialsFileRef != "" {
				refs = append(refs, RefUsage{Field: "credentials_file_ref", Ref: s3Cfg.CredentialsFileRef})
			}
			if s3Cfg.ConfigFileRef != "" {
				refs = append(refs, RefUsage{Field: "config_file_ref", Ref: s3Cfg.ConfigFileRef})
			}
		case S3AuthModeSecretRefs:
			refs = append(refs,
				RefUsage{Field: "access_key_id_ref", Ref: s3Cfg.AccessKeyIDRef},
				RefUsage{Field: "secret_access_key_ref", Ref: s3Cfg.SecretAccessKeyRef},
			)
			if s3Cfg.SessionTokenRef != "" {
				refs = append(refs, RefUsage{Field: "session_token_ref", Ref: s3Cfg.SessionTokenRef})
			}
		}
		refChecks, err := checksForRefs(cfg.ID, "s3 auth ref", refs, worktreeRoot)
		if err != nil {
			return nil, err
		}
		checks = append(checks, refChecks...)
		return checks, nil
	default:
		return nil, nil
	}
}

func checksForRefs(sinkID, prefix string, refs []RefUsage, worktreeRoot string) ([]health.Check, error) {
	checks := make([]health.Check, 0, len(refs))
	for _, usage := range refs {
		inspection, err := secretref.InspectStatic(usage.Ref, secretref.StaticOptions{WorktreeRoot: worktreeRoot})
		if err != nil {
			checks = append(checks, health.Check{
				Name:   "sink-auth-ref:" + sinkID + ":" + usage.Field,
				Status: "fail",
				Detail: err.Error(),
			})
			continue
		}
		status := "ok"
		detail := prefix + " " + usage.Field + " provider=" + inspection.Ref.Provider + " ref=" + secretref.Display(inspection.Ref.Provider, inspection.Ref.Fingerprint)
		if len(inspection.Warnings) > 0 {
			status = "warn"
			detail += " warnings=" + strings.Join(inspection.Warnings, "; ")
		}
		checks = append(checks, health.Check{
			Name:   "sink-auth-ref:" + sinkID + ":" + usage.Field,
			Status: status,
			Detail: detail,
		})
	}
	return checks, nil
}
