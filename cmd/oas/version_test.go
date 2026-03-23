package main

import (
	"runtime/debug"
	"testing"
)

func TestVersionStringFallsBackToDev(t *testing.T) {
	if got := versionStringForBuildInfo(nil); got != "oas dev" {
		t.Fatalf("versionStringForBuildInfo(nil) = %q, want %q", got, "oas dev")
	}
}

func TestVersionStringUsesModuleVersionWhenAvailable(t *testing.T) {
	info := &debug.BuildInfo{}
	info.Main.Version = "v0.2.0"

	if got := versionStringForBuildInfo(info); got != "oas v0.2.0" {
		t.Fatalf("versionStringForBuildInfo(info) = %q, want %q", got, "oas v0.2.0")
	}
}
