package secretref

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	ProviderEnv      = "env"
	ProviderFile     = "file"
	ProviderOP       = "op"
	ProviderKeychain = "keychain"
	ProviderPass     = "pass"
)

const (
	KindInvalidRef           = "invalid_ref"
	KindMissingSecret        = "missing_secret"
	KindInsecureSecretFile   = "insecure_secret_file"
	KindProviderNotInstalled = "provider_not_installed"
	KindProviderUnavailable  = "provider_unavailable"
	KindProviderLocked       = "provider_locked"
	KindAuthExpired          = "auth_expired"
	KindAuthDenied           = "auth_denied"
	KindTransientProviderErr = "transient_provider_error"
)

const experimentalProvidersEnv = "OAS_EXPERIMENTAL_SECRET_PROVIDERS"

type Ref struct {
	Raw         string
	Canonical   string
	Provider    string
	Fingerprint string
	Target      string
	Host        string
	Path        string
}

type ResolvedSecret struct {
	Value          []byte
	Provider       string
	ResolvedAt     time.Time
	ExpiresAt      time.Time
	RefFingerprint string
}

type Inspection struct {
	Ref      Ref
	Warnings []string
}

type StaticOptions struct {
	WorktreeRoot string
}

type Error struct {
	Kind           string
	Provider       string
	RefFingerprint string
	Err            error
}

func (e *Error) Error() string {
	if e == nil {
		return "secret reference error"
	}
	label := e.Provider
	if label == "" {
		label = "secret"
	}
	if e.RefFingerprint != "" {
		label += ":" + e.RefFingerprint
	}
	switch e.Kind {
	case KindInvalidRef:
		return label + ": invalid secret reference"
	case KindMissingSecret:
		return label + ": secret is missing"
	case KindInsecureSecretFile:
		return label + ": file-backed secret has insecure permissions"
	case KindProviderNotInstalled:
		return label + ": provider executable is unavailable"
	case KindProviderUnavailable:
		return label + ": provider is unavailable"
	case KindProviderLocked:
		return label + ": provider is locked or denied access"
	case KindAuthExpired:
		return label + ": authentication has expired"
	case KindAuthDenied:
		return label + ": authentication was denied"
	case KindTransientProviderErr:
		return label + ": transient provider failure"
	default:
		return label + ": secret reference error"
	}
}

func (e *Error) Unwrap() error { return e.Err }

func (e *Error) BlockedKind() string {
	switch e.Kind {
	case KindInvalidRef, KindMissingSecret, KindInsecureSecretFile, KindProviderNotInstalled:
		return "config"
	case KindProviderUnavailable, KindProviderLocked, KindAuthExpired, KindAuthDenied:
		return "auth"
	default:
		return ""
	}
}

func (e *Error) ProviderWide() bool {
	switch e.Kind {
	case KindProviderUnavailable, KindProviderLocked, KindAuthExpired, KindAuthDenied:
		return true
	default:
		return false
	}
}

type Resolver struct {
	ttl      time.Duration
	mu       sync.Mutex
	cache    map[string]cacheEntry
	inflight map[string]*inflightCall
}

type cacheEntry struct {
	value          []byte
	provider       string
	resolvedAt     time.Time
	expiresAt      time.Time
	refFingerprint string
}

type inflightCall struct {
	done chan struct{}
	res  ResolvedSecret
	err  error
}

var (
	defaultResolverOnce sync.Once
	defaultResolver     *Resolver
)

func Default() *Resolver {
	defaultResolverOnce.Do(func() {
		defaultResolver = NewResolver(60 * time.Second)
	})
	return defaultResolver
}

func NewResolver(ttl time.Duration) *Resolver {
	return &Resolver{
		ttl:      ttl,
		cache:    map[string]cacheEntry{},
		inflight: map[string]*inflightCall{},
	}
}

func Parse(raw string) (Ref, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Ref{}, &Error{Kind: KindInvalidRef}
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Ref{}, &Error{Kind: KindInvalidRef, Err: err}
	}
	ref := Ref{
		Raw:       raw,
		Canonical: raw,
		Provider:  strings.ToLower(strings.TrimSpace(u.Scheme)),
	}
	ref.Fingerprint = fingerprint(raw)
	switch ref.Provider {
	case ProviderEnv:
		target := strings.TrimSpace(u.Host)
		if target == "" {
			target = strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
		}
		if target == "" || strings.Contains(target, "/") {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		ref.Target = target
		ref.Canonical = "env://" + target
	case ProviderFile:
		if u.Host != "" && u.Host != "localhost" {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		path := filepath.Clean(u.Path)
		if !filepath.IsAbs(path) {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		ref.Target = path
		ref.Path = path
		ref.Canonical = "file://" + path
	case ProviderOP:
		if u.Host == "" || strings.Trim(u.Path, "/") == "" {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		ref.Host = u.Host
		ref.Path = strings.TrimPrefix(u.Path, "/")
		ref.Target = ref.Host + "/" + ref.Path
	case ProviderKeychain:
		if !experimentalEnabled() {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		if u.Host == "" || strings.Trim(u.Path, "/") == "" {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		ref.Host = u.Host
		ref.Path = strings.TrimPrefix(u.Path, "/")
		ref.Target = ref.Host + "/" + ref.Path
	case ProviderPass:
		if !experimentalEnabled() {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		target := strings.Trim(strings.TrimSpace(u.Host+u.Path), "/")
		if target == "" {
			return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
		}
		ref.Target = target
		ref.Path = target
	default:
		return Ref{}, &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
	}
	ref.Fingerprint = fingerprint(ref.Canonical)
	return ref, nil
}

func InspectStatic(raw string, opts StaticOptions) (Inspection, error) {
	ref, err := Parse(raw)
	if err != nil {
		return Inspection{}, err
	}
	inspection := Inspection{Ref: ref}
	switch ref.Provider {
	case ProviderFile:
		resolvedPath, warnings, err := inspectFileRef(ref.Target, opts)
		if err != nil {
			return Inspection{}, wrap(ref, err)
		}
		inspection.Ref.Target = resolvedPath
		inspection.Ref.Path = resolvedPath
		inspection.Warnings = append(inspection.Warnings, warnings...)
	case ProviderOP:
		if _, err := exec.LookPath("op"); err != nil {
			return Inspection{}, wrap(ref, &Error{Kind: KindProviderNotInstalled, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err})
		}
	case ProviderKeychain:
		if _, err := exec.LookPath("security"); err != nil {
			return Inspection{}, wrap(ref, &Error{Kind: KindProviderNotInstalled, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err})
		}
	case ProviderPass:
		if _, err := exec.LookPath("pass"); err != nil {
			return Inspection{}, wrap(ref, &Error{Kind: KindProviderNotInstalled, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err})
		}
	}
	return inspection, nil
}

func (r *Resolver) Resolve(ctx context.Context, raw string) (ResolvedSecret, error) {
	ref, err := Parse(raw)
	if err != nil {
		return ResolvedSecret{}, err
	}
	now := time.Now().UTC()
	r.mu.Lock()
	if entry, ok := r.cache[ref.Canonical]; ok && now.Sub(entry.resolvedAt) < r.ttl {
		value := cloneBytes(entry.value)
		r.mu.Unlock()
		return ResolvedSecret{
			Value:          value,
			Provider:       entry.provider,
			ResolvedAt:     entry.resolvedAt,
			ExpiresAt:      entry.expiresAt,
			RefFingerprint: entry.refFingerprint,
		}, nil
	}
	if call, ok := r.inflight[ref.Canonical]; ok {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return ResolvedSecret{}, ctx.Err()
		case <-call.done:
			if call.err != nil {
				return ResolvedSecret{}, call.err
			}
			call.res.Value = cloneBytes(call.res.Value)
			return call.res, nil
		}
	}
	call := &inflightCall{done: make(chan struct{})}
	r.inflight[ref.Canonical] = call
	r.mu.Unlock()

	resolved, err := r.resolveUncached(ctx, ref)

	r.mu.Lock()
	delete(r.inflight, ref.Canonical)
	if err == nil {
		r.cache[ref.Canonical] = cacheEntry{
			value:          cloneBytes(resolved.Value),
			provider:       resolved.Provider,
			resolvedAt:     resolved.ResolvedAt,
			expiresAt:      resolved.ExpiresAt,
			refFingerprint: resolved.RefFingerprint,
		}
	} else {
		var refErr *Error
		if errors.As(err, &refErr) {
			if refErr.ProviderWide() {
				r.invalidateProviderLocked(refErr.Provider)
			} else {
				r.invalidateRefLocked(ref.Canonical)
			}
		} else {
			r.invalidateRefLocked(ref.Canonical)
		}
	}
	call.res = resolved
	call.err = err
	close(call.done)
	r.mu.Unlock()

	if err != nil {
		return ResolvedSecret{}, err
	}
	resolved.Value = cloneBytes(resolved.Value)
	return resolved, nil
}

func (r *Resolver) resolveUncached(ctx context.Context, ref Ref) (ResolvedSecret, error) {
	var (
		value []byte
		err   error
	)
	switch ref.Provider {
	case ProviderEnv:
		value, err = resolveEnv(ref)
	case ProviderFile:
		value, err = resolveFile(ref)
	case ProviderOP:
		value, err = resolveCommand(ctx, ref, "op", "read", ref.Canonical)
	case ProviderKeychain:
		value, err = resolveCommand(ctx, ref, "security", "find-generic-password", "-s", ref.Host, "-a", ref.Path, "-w")
	case ProviderPass:
		value, err = resolveCommand(ctx, ref, "pass", "show", ref.Path)
		if err == nil {
			value = []byte(firstLine(string(value)))
		}
	default:
		err = &Error{Kind: KindInvalidRef, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
	}
	if err != nil {
		return ResolvedSecret{}, err
	}
	return ResolvedSecret{
		Value:          value,
		Provider:       ref.Provider,
		ResolvedAt:     time.Now().UTC(),
		RefFingerprint: ref.Fingerprint,
	}, nil
}

func (r *Resolver) invalidateProviderLocked(provider string) {
	for key, entry := range r.cache {
		if entry.provider != provider {
			continue
		}
		zero(entry.value)
		delete(r.cache, key)
	}
}

func (r *Resolver) invalidateRefLocked(canonical string) {
	if entry, ok := r.cache[canonical]; ok {
		zero(entry.value)
		delete(r.cache, canonical)
	}
}

func resolveEnv(ref Ref) ([]byte, error) {
	value, ok := os.LookupEnv(ref.Target)
	if !ok {
		return nil, &Error{Kind: KindMissingSecret, Provider: ref.Provider, RefFingerprint: ref.Fingerprint}
	}
	return []byte(value), nil
}

func resolveFile(ref Ref) ([]byte, error) {
	resolvedPath, _, err := inspectFileRef(ref.Target, StaticOptions{})
	if err != nil {
		return nil, wrap(ref, err)
	}
	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &Error{Kind: KindMissingSecret, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		}
		return nil, &Error{Kind: KindProviderUnavailable, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
	}
	return data, nil
}

func resolveCommand(ctx context.Context, ref Ref, name string, args ...string) ([]byte, error) {
	if _, err := exec.LookPath(name); err != nil {
		return nil, &Error{Kind: KindProviderNotInstalled, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
	}
	cmd := exec.CommandContext(ctx, name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, classifyCommandError(ref, err, strings.TrimSpace(string(output)))
	}
	return []byte(strings.TrimRight(string(output), "\r\n")), nil
}

func classifyCommandError(ref Ref, err error, output string) error {
	message := strings.ToLower(output)
	if message == "" {
		message = strings.ToLower(err.Error())
	}
	switch ref.Provider {
	case ProviderOP:
		switch {
		case containsAny(message, "not currently signed in", "session expired", "session has expired", "please sign in", "signin"):
			return &Error{Kind: KindAuthExpired, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		case containsAny(message, "permission denied", "forbidden"):
			return &Error{Kind: KindAuthDenied, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		case containsAny(message, "not found"):
			return &Error{Kind: KindMissingSecret, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		}
	case ProviderKeychain:
		switch {
		case containsAny(message, "could not be found", "item could not be found"):
			return &Error{Kind: KindMissingSecret, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		case containsAny(message, "user interaction is not allowed", "authorization", "interaction not allowed"):
			return &Error{Kind: KindProviderLocked, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		}
	case ProviderPass:
		switch {
		case containsAny(message, "not in the password store"):
			return &Error{Kind: KindMissingSecret, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		case containsAny(message, "decryption failed", "no secret key", "bad passphrase", "gpg"):
			return &Error{Kind: KindProviderLocked, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
		}
	}
	if containsAny(message, "timeout", "timed out", "temporarily unavailable") {
		return &Error{Kind: KindTransientProviderErr, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
	}
	return &Error{Kind: KindProviderUnavailable, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
}

func inspectFileRef(path string, opts StaticOptions) (string, []string, error) {
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, &Error{Kind: KindMissingSecret, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path), Err: err}
		}
		return "", nil, &Error{Kind: KindProviderUnavailable, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path), Err: err}
	}
	info, err := os.Stat(resolvedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil, &Error{Kind: KindMissingSecret, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path), Err: err}
		}
		return "", nil, &Error{Kind: KindProviderUnavailable, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path), Err: err}
	}
	if !info.Mode().IsRegular() {
		return "", nil, &Error{Kind: KindInvalidRef, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path)}
	}
	mode := info.Mode().Perm()
	if mode&0o007 != 0 {
		return "", nil, &Error{Kind: KindInsecureSecretFile, Provider: ProviderFile, RefFingerprint: fingerprint("file://" + path)}
	}
	var warnings []string
	if mode&0o070 != 0 && !allowedGroupReadablePath(resolvedPath) {
		warnings = append(warnings, "file permissions allow group access")
	}
	if opts.WorktreeRoot != "" {
		root := filepath.Clean(opts.WorktreeRoot)
		if rel, err := filepath.Rel(root, resolvedPath); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			warnings = append(warnings, "file path is inside the current worktree")
		}
	}
	return resolvedPath, warnings, nil
}

func allowedGroupReadablePath(path string) bool {
	allowedRoots := []string{
		"/run/secrets",
		"/var/run/secrets",
	}
	if runtimeDir := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR")); runtimeDir != "" {
		allowedRoots = append(allowedRoots, runtimeDir)
	}
	cleanPath := filepath.Clean(path)
	for _, root := range allowedRoots {
		root = filepath.Clean(root)
		if rel, err := filepath.Rel(root, cleanPath); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func Display(provider, fingerprint string) string {
	if fingerprint == "" {
		return provider
	}
	return provider + ":" + fingerprint
}

func FindWorktreeRoot(start string) string {
	start = strings.TrimSpace(start)
	if start == "" {
		return ""
	}
	current := filepath.Clean(start)
	for {
		if _, err := os.Stat(filepath.Join(current, ".git")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return ""
		}
		current = parent
	}
}

func experimentalEnabled() bool {
	return strings.TrimSpace(os.Getenv(experimentalProvidersEnv)) == "1"
}

func wrap(ref Ref, err error) error {
	var refErr *Error
	if errors.As(err, &refErr) {
		if refErr.Provider == "" {
			refErr.Provider = ref.Provider
		}
		if refErr.RefFingerprint == "" {
			refErr.RefFingerprint = ref.Fingerprint
		}
		return refErr
	}
	return &Error{Kind: KindProviderUnavailable, Provider: ref.Provider, RefFingerprint: ref.Fingerprint, Err: err}
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:6])
}

func containsAny(value string, needles ...string) bool {
	for _, needle := range needles {
		if strings.Contains(value, needle) {
			return true
		}
	}
	return false
}

func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func zero(src []byte) {
	for i := range src {
		src[i] = 0
	}
}

func firstLine(value string) string {
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return strings.TrimSpace(value)
}
