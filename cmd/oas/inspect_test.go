package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestInspectSessionBuildsReviewerFriendlyView(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"message.user","actor":{"kind":"user","name":"Peren"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{"text":"Please run the test suite"},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":3,"timestamp":"2026-03-22T10:02:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{"command":"go test ./..."},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
{"event_version":1,"event_id":"evt_4","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":4,"timestamp":"2026-03-22T10:03:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{"command":"go test ./...","exit_code":1},"raw_ref":{"ledger_offset":4,"envelope_id":"env_4"}}
{"event_version":1,"event_id":"evt_5","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":5,"timestamp":"2026-03-22T10:04:00Z","kind":"tool.failed","actor":{"kind":"tool","name":"web_search"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{"tool_name":"web_search","command":"query docs","exit_code":2,"reason":"network timeout"},"raw_ref":{"ledger_offset":5,"envelope_id":"env_5"}}
{"event_version":1,"event_id":"evt_6","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":6,"timestamp":"2026-03-22T10:05:00Z","kind":"message.agent","actor":{"kind":"agent","name":"codex_local"},"context":{"project_key":"proj_1","project_locator":"/tmp/project","source_session_key":"abc"},"payload":{"text":"The tests failed and web search timed out."},"raw_ref":{"ledger_offset":6,"envelope_id":"env_6"}}
{"event_version":1,"event_id":"evt_7","source_type":"claude_local","source_instance_id":"claude-b","session_key":"sess_b","sequence":1,"timestamp":"2026-03-22T11:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{},"payload":{},"raw_ref":{"ledger_offset":7,"envelope_id":"env_7"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 10, 10)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := inspection.ProjectLocator, "/tmp/project"; got != want {
		t.Fatalf("ProjectLocator = %q, want %q", got, want)
	}
	if got, want := inspection.EventCount, 6; got != want {
		t.Fatalf("EventCount = %d, want %d", got, want)
	}
	if got, want := len(inspection.Commands), 1; got != want {
		t.Fatalf("len(Commands) = %d, want %d", got, want)
	}
	if got, want := inspection.Commands[0].Status, "completed"; got != want {
		t.Fatalf("Commands[0].Status = %q, want %q", got, want)
	}
	if inspection.Commands[0].StartSequence == nil || *inspection.Commands[0].StartSequence != 3 {
		t.Fatalf("Commands[0].StartSequence = %v, want 3", inspection.Commands[0].StartSequence)
	}
	if inspection.Commands[0].FinishSequence == nil || *inspection.Commands[0].FinishSequence != 4 {
		t.Fatalf("Commands[0].FinishSequence = %v, want 4", inspection.Commands[0].FinishSequence)
	}
	if inspection.Commands[0].DurationMillis == nil || *inspection.Commands[0].DurationMillis != 60000 {
		t.Fatalf("Commands[0].DurationMillis = %v, want 60000", inspection.Commands[0].DurationMillis)
	}
	if got, want := len(inspection.ToolFailures), 1; got != want {
		t.Fatalf("len(ToolFailures) = %d, want %d", got, want)
	}
	if got, want := inspection.ToolFailures[0].ToolName, "web_search"; got != want {
		t.Fatalf("ToolFailures[0].ToolName = %q, want %q", got, want)
	}
	if got, want := len(inspection.AttentionEvents), 2; got != want {
		t.Fatalf("len(AttentionEvents) = %d, want %d", got, want)
	}
	if got, want := inspection.AttentionEvents[0].Kind, "command.finished"; got != want {
		t.Fatalf("AttentionEvents[0].Kind = %q, want %q", got, want)
	}
	if got, want := inspection.AttentionEvents[1].Kind, "tool.failed"; got != want {
		t.Fatalf("AttentionEvents[1].Kind = %q, want %q", got, want)
	}
	if got, want := inspection.KindCounts[0].Kind, "command.finished"; got != want {
		t.Fatalf("KindCounts[0].Kind = %q, want %q", got, want)
	}
	if got, want := len(inspection.Timeline), 6; got != want {
		t.Fatalf("len(Timeline) = %d, want %d", got, want)
	}
}

func TestInspectSessionTruncatesRepresentativeTimeline(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{},"payload":{},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"message.user","actor":{"kind":"user","name":"Peren"},"context":{},"payload":{"text":"one"},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":3,"timestamp":"2026-03-22T10:02:00Z","kind":"message.agent","actor":{"kind":"agent","name":"codex"},"context":{},"payload":{"text":"two"},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
{"event_version":1,"event_id":"evt_4","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":4,"timestamp":"2026-03-22T10:03:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"echo hi","exit_code":0},"raw_ref":{"ledger_offset":4,"envelope_id":"env_4"}}
{"event_version":1,"event_id":"evt_5","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":5,"timestamp":"2026-03-22T10:04:00Z","kind":"tool.failed","actor":{"kind":"tool","name":"web"},"context":{},"payload":{"tool_name":"web","reason":"timeout"},"raw_ref":{"ledger_offset":5,"envelope_id":"env_5"}}
{"event_version":1,"event_id":"evt_6","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":6,"timestamp":"2026-03-22T10:05:00Z","kind":"message.agent","actor":{"kind":"agent","name":"codex"},"context":{},"payload":{"text":"done"},"raw_ref":{"ledger_offset":6,"envelope_id":"env_6"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 4)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := inspection.OmittedTimelineEvents, 2; got != want {
		t.Fatalf("OmittedTimelineEvents = %d, want %d", got, want)
	}
	if got, want := len(inspection.Timeline), 4; got != want {
		t.Fatalf("len(Timeline) = %d, want %d", got, want)
	}
	if got, want := inspection.timelineHeadCount, 2; got != want {
		t.Fatalf("timelineHeadCount = %d, want %d", got, want)
	}
}

func TestInspectSessionTruncatesCommandRows(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"one"},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"one","exit_code":0},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":3,"timestamp":"2026-03-22T10:02:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"two"},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
{"event_version":1,"event_id":"evt_4","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":4,"timestamp":"2026-03-22T10:03:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"two","exit_code":0},"raw_ref":{"ledger_offset":4,"envelope_id":"env_4"}}
{"event_version":1,"event_id":"evt_5","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":5,"timestamp":"2026-03-22T10:04:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"three"},"raw_ref":{"ledger_offset":5,"envelope_id":"env_5"}}
{"event_version":1,"event_id":"evt_6","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":6,"timestamp":"2026-03-22T10:05:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"three","exit_code":0},"raw_ref":{"ledger_offset":6,"envelope_id":"env_6"}}
{"event_version":1,"event_id":"evt_7","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":7,"timestamp":"2026-03-22T10:06:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"four"},"raw_ref":{"ledger_offset":7,"envelope_id":"env_7"}}
{"event_version":1,"event_id":"evt_8","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":8,"timestamp":"2026-03-22T10:07:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"four","exit_code":0},"raw_ref":{"ledger_offset":8,"envelope_id":"env_8"}}
{"event_version":1,"event_id":"evt_9","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":9,"timestamp":"2026-03-22T10:08:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"five"},"raw_ref":{"ledger_offset":9,"envelope_id":"env_9"}}
{"event_version":1,"event_id":"evt_10","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":10,"timestamp":"2026-03-22T10:09:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"five","exit_code":0},"raw_ref":{"ledger_offset":10,"envelope_id":"env_10"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 4, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := inspection.OmittedCommands, 1; got != want {
		t.Fatalf("OmittedCommands = %d, want %d", got, want)
	}
	if got, want := len(inspection.Commands), 4; got != want {
		t.Fatalf("len(Commands) = %d, want %d", got, want)
	}
	if got, want := inspection.commandHeadCount, 2; got != want {
		t.Fatalf("commandHeadCount = %d, want %d", got, want)
	}
	if got, want := inspection.Commands[0].Command, "one"; got != want {
		t.Fatalf("Commands[0].Command = %q, want %q", got, want)
	}
	if got, want := inspection.Commands[len(inspection.Commands)-1].Command, "five"; got != want {
		t.Fatalf("Commands[last].Command = %q, want %q", got, want)
	}
}

func TestInspectSessionKeepsIncompleteCommandRowsVisible(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"one"},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"two","exit_code":1},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := len(inspection.Commands), 2; got != want {
		t.Fatalf("len(Commands) = %d, want %d", got, want)
	}
	if got, want := inspection.Commands[0].Status, "started_only"; got != want {
		t.Fatalf("Commands[0].Status = %q, want %q", got, want)
	}
	if got, want := inspection.Commands[1].Status, "finished_only"; got != want {
		t.Fatalf("Commands[1].Status = %q, want %q", got, want)
	}
	if inspection.Commands[0].FinishSequence != nil {
		t.Fatalf("Commands[0].FinishSequence = %v, want nil", inspection.Commands[0].FinishSequence)
	}
	if inspection.Commands[1].StartSequence != nil {
		t.Fatalf("Commands[1].StartSequence = %v, want nil", inspection.Commands[1].StartSequence)
	}
}

func TestInspectSessionFiltersCommandRowsByAttention(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"ok"},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"ok","exit_code":0},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":3,"timestamp":"2026-03-22T10:02:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"bad"},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
{"event_version":1,"event_id":"evt_4","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":4,"timestamp":"2026-03-22T10:03:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"bad","exit_code":2},"raw_ref":{"ledger_offset":4,"envelope_id":"env_4"}}
{"event_version":1,"event_id":"evt_5","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":5,"timestamp":"2026-03-22T10:04:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"stuck"},"raw_ref":{"ledger_offset":5,"envelope_id":"env_5"}}
`))

	inspection, err := inspectSession(input, "sess_a", "attention", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := inspection.CommandFilter, "attention"; got != want {
		t.Fatalf("CommandFilter = %q, want %q", got, want)
	}
	if got, want := len(inspection.Commands), 2; got != want {
		t.Fatalf("len(Commands) = %d, want %d", got, want)
	}
	if got, want := inspection.Commands[0].Command, "bad"; got != want {
		t.Fatalf("Commands[0].Command = %q, want %q", got, want)
	}
	if got, want := inspection.Commands[0].Status, "completed"; got != want {
		t.Fatalf("Commands[0].Status = %q, want %q", got, want)
	}
	if inspection.Commands[0].ExitCode == nil || *inspection.Commands[0].ExitCode != 2 {
		t.Fatalf("Commands[0].ExitCode = %v, want 2", inspection.Commands[0].ExitCode)
	}
	if got, want := inspection.Commands[1].Command, "stuck"; got != want {
		t.Fatalf("Commands[1].Command = %q, want %q", got, want)
	}
	if got, want := inspection.Commands[1].Status, "started_only"; got != want {
		t.Fatalf("Commands[1].Status = %q, want %q", got, want)
	}
}

func TestInspectSessionRejectsInvalidCommandFilter(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"session.started","actor":{"kind":"system","name":"oas"},"context":{},"payload":{},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
`))

	_, err := inspectSession(input, "sess_a", "weird", 0, 0)
	if err == nil {
		t.Fatal("inspectSession unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "invalid -command-status") {
		t.Fatalf("inspectSession error = %q, want invalid -command-status", err)
	}
}

func TestInspectSessionAttentionEventsOnlyIncludeFailures(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"ok","exit_code":0},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:01:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"bad","exit_code":2},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
{"event_version":1,"event_id":"evt_3","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":3,"timestamp":"2026-03-22T10:02:00Z","kind":"tool.finished","actor":{"kind":"tool","name":"web"},"context":{},"payload":{"tool_name":"web"},"raw_ref":{"ledger_offset":3,"envelope_id":"env_3"}}
{"event_version":1,"event_id":"evt_4","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":4,"timestamp":"2026-03-22T10:03:00Z","kind":"tool.failed","actor":{"kind":"tool","name":"web"},"context":{},"payload":{"tool_name":"web","reason":"timeout"},"raw_ref":{"ledger_offset":4,"envelope_id":"env_4"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := len(inspection.AttentionEvents), 2; got != want {
		t.Fatalf("len(AttentionEvents) = %d, want %d", got, want)
	}
	if got, want := inspection.AttentionEvents[0].Summary, "bad (exit 2)"; got != want {
		t.Fatalf("AttentionEvents[0].Summary = %q, want %q", got, want)
	}
	if got, want := inspection.AttentionEvents[1].Summary, "web — timeout"; got != want {
		t.Fatalf("AttentionEvents[1].Summary = %q, want %q", got, want)
	}
	if got, want := inspection.AttentionEvents[1].Detail, "timeout"; got != want {
		t.Fatalf("AttentionEvents[1].Detail = %q, want %q", got, want)
	}
}

func TestInspectSessionAttentionEventsExtractCommandFailureDetail(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:03:00Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"exit_code":1,"payload":{"output":"Command: /bin/bash -lc ./build.sh\nChunk ID: 4c5113\nWall time: 0.0781 seconds\nProcess exited with code 1\nOriginal token count: 40\nOutput:\nBuilding control binary: ./goated\ncmd/goated/cli/helpers.go:9:2: no required module provides package golang.org/x/term; to add it:\n\tgo get golang.org/x/term\n","type":"function_call_output"}},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := len(inspection.AttentionEvents), 1; got != want {
		t.Fatalf("len(AttentionEvents) = %d, want %d", got, want)
	}
	if got, want := inspection.AttentionEvents[0].Summary, "./build.sh (exit 1)"; got != want {
		t.Fatalf("AttentionEvents[0].Summary = %q, want %q", got, want)
	}
	if got := inspection.AttentionEvents[0].Detail; !strings.Contains(got, "no required module provides package golang.org/x/term") {
		t.Fatalf("AttentionEvents[0].Detail = %q, want compiler error excerpt", got)
	}
	if strings.Contains(inspection.AttentionEvents[0].Detail, "Chunk ID:") {
		t.Fatalf("AttentionEvents[0].Detail = %q, metadata should have been stripped", inspection.AttentionEvents[0].Detail)
	}
	if got, want := inspection.Commands[0].Command, "./build.sh"; got != want {
		t.Fatalf("Commands[0].Command = %q, want %q", got, want)
	}
}

func TestInspectSessionAttentionEventsExtractToolFailureDetail(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:03:00Z","kind":"tool.failed","actor":{"kind":"tool","name":"write_stdin"},"context":{},"payload":{"tool_name":"write_stdin","command":"cd self/tools/playwright-tool && npx playwright install-deps chromium","exit_code":1,"payload":{"output":"Command: /bin/bash -lc 'cd self/tools/playwright-tool && npx playwright install-deps chromium'\nChunk ID: 8e9955\nWall time: 0.6820 seconds\nProcess exited with code 1\nOriginal token count: 82\nOutput:\nInstalling dependencies...\nSwitching to root user to install dependencies...\nsudo: a terminal is required to read the password; either use the -S option to read from standard input or configure an askpass helper\nsudo: a password is required\nFailed to install browser dependencies\nError: Installation process exited with code: 1\n","type":"function_call_output"}},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := len(inspection.AttentionEvents), 1; got != want {
		t.Fatalf("len(AttentionEvents) = %d, want %d", got, want)
	}
	if got := inspection.AttentionEvents[0].Detail; !strings.Contains(got, "sudo: a terminal is required") {
		t.Fatalf("AttentionEvents[0].Detail = %q, want tool failure excerpt", got)
	}
	if strings.Contains(inspection.AttentionEvents[0].Detail, "Chunk ID:") {
		t.Fatalf("AttentionEvents[0].Detail = %q, metadata should have been stripped", inspection.AttentionEvents[0].Detail)
	}
}

func TestInspectSessionBackfillsCommandContextFromStartedCallID(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:00:00Z","kind":"command.started","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"command":"./bootstrap.sh","payload":{"call_id":"call_1","name":"exec_command","type":"function_call"}},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
{"event_version":1,"event_id":"evt_2","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":2,"timestamp":"2026-03-22T10:00:01Z","kind":"command.finished","actor":{"kind":"tool","name":"shell"},"context":{},"payload":{"exit_code":1,"payload":{"call_id":"call_1","output":"Command: /bin/bash -lc ./bootstrap.sh\nChunk ID: e50ed8\nWall time: 0.0000 seconds\nProcess exited with code 1\nOriginal token count: 38\nOutput:\nGo toolchain not found on PATH.\n","type":"function_call_output"}},"raw_ref":{"ledger_offset":2,"envelope_id":"env_2"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := len(inspection.Commands), 1; got != want {
		t.Fatalf("len(Commands) = %d, want %d", got, want)
	}
	if got, want := inspection.Commands[0].Command, "./bootstrap.sh"; got != want {
		t.Fatalf("Commands[0].Command = %q, want %q", got, want)
	}
	if got, want := inspection.AttentionEvents[0].Summary, "./bootstrap.sh (exit 1)"; got != want {
		t.Fatalf("AttentionEvents[0].Summary = %q, want %q", got, want)
	}
}

func TestInspectSessionBackfillsToolFailureCommandFromOutputHeader(t *testing.T) {
	input := strings.NewReader(strings.TrimSpace(`
{"event_version":1,"event_id":"evt_1","source_type":"codex_local","source_instance_id":"codex-a","session_key":"sess_a","sequence":1,"timestamp":"2026-03-22T10:03:00Z","kind":"tool.failed","actor":{"kind":"tool","name":"write_stdin"},"context":{},"payload":{"tool_name":"write_stdin","exit_code":124,"payload":{"output":"Command: /bin/bash -lc 'timeout 20 ./goated daemon run'\nChunk ID: f261d0\nWall time: 7.4467 seconds\nProcess exited with code 124\nOriginal token count: 0\nOutput:\n","type":"function_call_output"}},"raw_ref":{"ledger_offset":1,"envelope_id":"env_1"}}
`))

	inspection, err := inspectSession(input, "sess_a", "all", 0, 0)
	if err != nil {
		t.Fatalf("inspectSession error: %v", err)
	}
	if got, want := inspection.ToolFailures[0].Command, "timeout 20 ./goated daemon run"; got != want {
		t.Fatalf("ToolFailures[0].Command = %q, want %q", got, want)
	}
	if got, want := inspection.ToolFailures[0].Summary, "write_stdin — timeout 20 ./goated daemon run — exit 124"; got != want {
		t.Fatalf("ToolFailures[0].Summary = %q, want %q", got, want)
	}
	if got, want := inspection.AttentionEvents[0].Summary, "write_stdin — timeout 20 ./goated daemon run — exit 124"; got != want {
		t.Fatalf("AttentionEvents[0].Summary = %q, want %q", got, want)
	}
}

func TestWriteSessionInspectionProducesReadableReport(t *testing.T) {
	inspection := sessionInspection{
		SessionKey:       "sess_a",
		SourceType:       "codex_local",
		SourceInstanceID: "codex-a",
		ProjectLocator:   "/tmp/project",
		EventCount:       3,
		FirstTimestamp:   mustParseTime("2026-03-22T10:00:00Z"),
		LastTimestamp:    mustParseTime("2026-03-22T10:05:00Z"),
		KindCounts: []sessionKindCount{
			{Kind: "message.user", Count: 1},
			{Kind: "command.finished", Count: 1},
		},
		Commands: []inspectedCommand{
			{
				Sequence:        2,
				Status:          "completed",
				Command:         "go test ./...",
				ExitCode:        intPtr(1),
				StartSequence:   intPtr(2),
				StartTimestamp:  timePtr(mustParseTime("2026-03-22T10:02:00Z")),
				FinishSequence:  intPtr(3),
				FinishTimestamp: timePtr(mustParseTime("2026-03-22T10:03:00Z")),
				DurationMillis:  intPtr(60000),
			},
		},
		ToolFailures: []inspectedToolFailure{
			{Sequence: 3, Timestamp: mustParseTime("2026-03-22T10:03:00Z"), ToolName: "web_search", Summary: "web_search — exit 2"},
		},
		AttentionEvents: []sessionTimelineEvent{
			{Sequence: 2, Timestamp: mustParseTime("2026-03-22T10:02:00Z"), Kind: "command.finished", ActorKind: "tool", ActorName: "shell", Summary: "go test ./... (exit 1)", Detail: "FAIL: package ./..."},
			{Sequence: 3, Timestamp: mustParseTime("2026-03-22T10:03:00Z"), Kind: "tool.failed", ActorKind: "tool", ActorName: "web_search", Summary: "web_search — exit 2"},
		},
		Timeline: []sessionTimelineEvent{
			{Sequence: 1, Timestamp: mustParseTime("2026-03-22T10:00:00Z"), Kind: "session.started", ActorKind: "system", ActorName: "oas"},
			{Sequence: 2, Timestamp: mustParseTime("2026-03-22T10:02:00Z"), Kind: "command.finished", ActorKind: "tool", ActorName: "shell", Summary: "go test ./... (exit 1)"},
		},
	}

	var out bytes.Buffer
	if err := writeSessionInspection(&out, inspection); err != nil {
		t.Fatalf("writeSessionInspection error: %v", err)
	}

	text := out.String()
	for _, want := range []string{
		"SESSION",
		"sess_a",
		"/tmp/project",
		"COMMANDS",
		"go test ./...",
		"TOOL FAILURES",
		"web_search",
		"ATTENTION EVENTS",
		"DETAIL",
		"FAIL: package ./...",
		"TIMELINE",
		"command.finished",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("report missing %q:\n%s", want, text)
		}
	}
}

func mustParseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}
