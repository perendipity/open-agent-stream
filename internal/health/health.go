package health

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func CheckReadablePath(_ context.Context, name, path string) Check {
	info, err := os.Stat(path)
	if err != nil {
		return Check{Name: name, Status: "fail", Detail: err.Error()}
	}
	return Check{Name: name, Status: "ok", Detail: fmt.Sprintf("%s (%d bytes)", filepath.Base(path), info.Size())}
}

func CheckWritablePath(_ context.Context, name, path string) Check {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Check{Name: name, Status: "fail", Detail: err.Error()}
	}
	testFile := filepath.Join(dir, ".oas-healthcheck")
	if err := os.WriteFile(testFile, []byte("ok"), 0o644); err != nil {
		return Check{Name: name, Status: "fail", Detail: err.Error()}
	}
	_ = os.Remove(testFile)
	return Check{Name: name, Status: "ok", Detail: dir}
}
