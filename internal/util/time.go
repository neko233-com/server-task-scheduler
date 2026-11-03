package util

import (
	"encoding/hex"
	"strconv"
	"time"
)

func hexNow() string {
	return strconv.FormatInt(time.Now().UnixNano(), 16)
}

// Hex is a tiny helper kept for callers that want to encode raw bytes.
func Hex(b []byte) string { return hex.EncodeToString(b) }
