package id

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	EventPrefix = "evt_"
)

var seq uint32

// NewEventID generates a time-sortable unique identifier for events
// formatted as: evt_<12_hex_timestamp_ms><4_hex_seq><8_hex_random>.
// Example: evt_0192f1b4a3c8000198d2e14f
func NewEventID() string {
	nowMs := uint64(time.Now().UnixMilli())
	s := atomic.AddUint32(&seq, 1) & 0xffff
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%s%012x%04x%08d", EventPrefix, nowMs, s, time.Now().UnixNano()%100000000)
	}
	return fmt.Sprintf("%s%012x%04x%s", EventPrefix, nowMs, s, hex.EncodeToString(b))
}
