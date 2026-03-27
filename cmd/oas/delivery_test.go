package main

import "testing"

func TestParseDeliveryStatusesSupportsBlocked(t *testing.T) {
	statuses, err := parseDeliveryStatuses(" pending,blocked,quarantined,blocked ")
	if err != nil {
		t.Fatalf("parseDeliveryStatuses() error = %v", err)
	}
	want := []string{"blocked", "pending", "quarantined"}
	if len(statuses) != len(want) {
		t.Fatalf("len(statuses) = %d, want %d (%v)", len(statuses), len(want), statuses)
	}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("statuses[%d] = %q, want %q (all=%v)", i, statuses[i], want[i], statuses)
		}
	}
}
