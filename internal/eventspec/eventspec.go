package eventspec

import (
	"fmt"

	"github.com/open-agent-stream/open-agent-stream/pkg/schema"
	"github.com/open-agent-stream/open-agent-stream/pkg/sinkapi"
)

func Batch(batch sinkapi.Batch, version string) (sinkapi.Batch, error) {
	normalized := schema.NormalizeEventSpecVersion(version)
	if normalized == "" {
		return sinkapi.Batch{}, fmt.Errorf("unsupported event spec version: %s", version)
	}
	if len(batch.Events) == 0 {
		return batch, nil
	}

	machineIDs := machineIDsForBatch(batch)
	out := sinkapi.Batch{
		Events:       make([]schema.CanonicalEvent, 0, len(batch.Events)),
		RawEnvelopes: append([]schema.RawEnvelope(nil), batch.RawEnvelopes...),
	}
	for idx, event := range batch.Events {
		machineID := machineIDs[event.RawRef.EnvelopeID]
		if machineID == "" && idx < len(batch.RawEnvelopes) {
			machineID = batch.RawEnvelopes[idx].MachineID
		}
		out.Events = append(out.Events, transformEvent(event, normalized, machineID))
	}
	return out, nil
}

func machineIDsForBatch(batch sinkapi.Batch) map[string]string {
	out := make(map[string]string, len(batch.RawEnvelopes))
	for _, envelope := range batch.RawEnvelopes {
		if envelope.EnvelopeID == "" {
			continue
		}
		out[envelope.EnvelopeID] = envelope.MachineID
	}
	return out
}

func transformEvent(event schema.CanonicalEvent, version, machineID string) schema.CanonicalEvent {
	cloned := event
	cloned.EventID = ""
	cloned.EventVersion = schema.CanonicalEventVersionForSpec(version)
	if version == schema.EventSpecV2 {
		cloned.MachineID = machineID
		cloned.SessionKey = schema.StableSessionKeyForVersion(version, machineID, cloned.SourceInstanceID, cloned.Context.SourceSessionKey)
	} else {
		cloned.MachineID = ""
		cloned.SessionKey = schema.StableSessionKeyForVersion(version, "", cloned.SourceInstanceID, cloned.Context.SourceSessionKey)
	}
	return schema.EnsureEventID(cloned)
}
