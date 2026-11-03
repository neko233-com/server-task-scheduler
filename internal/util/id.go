// Package util holds small shared helpers with no external dependencies.
package util

import (
	"crypto/rand"
	"encoding/hex"
)

// newID returns a random 16-byte hex identifier (32 chars).
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand should never fail on supported platforms; fall back to
		// a timestamp-based id to keep the caller moving.
		return hexNow()
	}
	return hex.EncodeToString(b)
}
