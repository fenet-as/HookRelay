package id_test

import (
	"strings"
	"testing"

	"github.com/fenet-as/hookrelay/internal/id"
)

func TestNewEventID(t *testing.T) {
	seen := make(map[string]bool)
	const count = 1000

	var last string
	for i := 0; i < count; i++ {
		evtID := id.NewEventID()

		if !strings.HasPrefix(evtID, id.EventPrefix) {
			t.Fatalf("expected prefix %s, got %s", id.EventPrefix, evtID)
		}

		if seen[evtID] {
			t.Fatalf("generated duplicate ID: %s", evtID)
		}
		seen[evtID] = true

		if last != "" && evtID < last {
			t.Errorf("expected IDs to be lexicographically/chronologically monotonic: %s < %s", evtID, last)
		}
		last = evtID
	}
}
