package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
)

func GenerateMachineID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "machine-unknown"
	}
	return "machine-" + hex.EncodeToString(buf)
}

func EffectiveMachineID(envelopeMachineID, configuredMachineID string) (string, error) {
	envelopeMachineID = strings.TrimSpace(envelopeMachineID)
	configuredMachineID = strings.TrimSpace(configuredMachineID)
	switch {
	case envelopeMachineID == "" && configuredMachineID == "":
		return "", errors.New("machine_id is missing from both the retained envelope and config")
	case envelopeMachineID == "":
		return configuredMachineID, nil
	case configuredMachineID == "":
		return envelopeMachineID, nil
	case envelopeMachineID != configuredMachineID:
		return "", errors.New("captured envelope machine_id does not match config.machine_id")
	default:
		return envelopeMachineID, nil
	}
}

func MachineIDWarnings(machineID string) []string {
	machineID = strings.TrimSpace(machineID)
	if machineID == "" {
		return []string{"config.machine_id is empty; multi-machine routing and portable v2 exports require a stable opaque machine id"}
	}
	lower := strings.ToLower(machineID)
	switch {
	case lower == "local-dev":
		return []string{"config.machine_id uses the old local-dev placeholder; generate a unique opaque id before sending data to shared destinations"}
	case strings.HasPrefix(lower, "example"):
		return []string{"config.machine_id looks like an example placeholder; generate a unique opaque id before running OAS on a real machine"}
	case strings.HasPrefix(lower, "test"):
		return []string{"config.machine_id looks like a test placeholder; generate a unique opaque id before running OAS on a real machine"}
	case lower == "changeme", lower == "replace-me", lower == "todo":
		return []string{"config.machine_id looks like a placeholder; generate a unique opaque id before running OAS on a real machine"}
	default:
		return nil
	}
}

func EffectiveSinkEventSpecVersion(sinkVersion string) string {
	if normalized := schema.NormalizeEventSpecVersion(sinkVersion); normalized != "" {
		return normalized
	}
	return schema.EventSpecV1
}
