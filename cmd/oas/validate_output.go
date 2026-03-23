package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/internal/config"
	"github.com/open-agent-stream/open-agent-stream/pkg/conformance"
)

type validationReport struct {
	ConfigPath    string
	RootPath      string
	ConfigIssues  []string
	FixtureIssues []string
}

func (r validationReport) OK() bool {
	return len(r.ConfigIssues) == 0 && len(r.FixtureIssues) == 0
}

func buildValidationReport(configPath, rootPath string) validationReport {
	report := validationReport{}

	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		report.ConfigIssues = append(report.ConfigIssues, "config path is required")
	} else {
		absConfigPath, err := filepath.Abs(configPath)
		if err != nil {
			report.ConfigIssues = append(report.ConfigIssues, fmt.Sprintf("resolve config path: %v", err))
		} else {
			report.ConfigPath = absConfigPath
			if _, err := config.Load(absConfigPath); err != nil {
				report.ConfigIssues = append(report.ConfigIssues, flattenValidationIssues(err)...)
			}
		}
	}

	absRoot, err := filepath.Abs(strings.TrimSpace(rootPath))
	if err != nil {
		report.FixtureIssues = append(report.FixtureIssues, fmt.Sprintf("resolve repository root: %v", err))
		return report
	}
	report.RootPath = absRoot
	if err := conformance.ValidateFixtures(absRoot); err != nil {
		report.FixtureIssues = append(report.FixtureIssues, flattenValidationIssues(err)...)
	}

	return report
}

func formatValidationFailure(report validationReport) string {
	var b strings.Builder
	b.WriteString("validation failed\n\n")
	if report.ConfigPath != "" {
		fmt.Fprintf(&b, "Config: %s\n", report.ConfigPath)
	}
	if report.RootPath != "" {
		fmt.Fprintf(&b, "Repository root: %s\n", report.RootPath)
	}
	if report.ConfigPath != "" || report.RootPath != "" {
		b.WriteString("\n")
	}
	appendBulletSection(&b, "Config issues", report.ConfigIssues)
	appendBulletSection(&b, "Fixture issues", report.FixtureIssues)
	return strings.TrimRight(b.String(), "\n")
}

func appendBulletSection(builder *strings.Builder, title string, items []string) {
	if len(items) == 0 {
		return
	}
	builder.WriteString(title)
	builder.WriteString(":\n")
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		fmt.Fprintf(builder, "  - %s\n", item)
	}
	builder.WriteString("\n")
}

func flattenValidationIssues(err error) []string {
	if err == nil {
		return nil
	}
	type multiUnwrapper interface {
		Unwrap() []error
	}
	if joined, ok := err.(multiUnwrapper); ok {
		var out []string
		for _, child := range joined.Unwrap() {
			out = append(out, flattenValidationIssues(child)...)
		}
		return out
	}
	text := strings.TrimSpace(err.Error())
	if text == "" {
		return nil
	}
	return []string{text}
}
