package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newTaskID() string    { return fmt.Sprintf("t_%s", randomHex(6)) }
func newExecID() string    { return fmt.Sprintf("ex_%s", randomHex(6)) }
func newEndpointID() string { return fmt.Sprintf("ep_%s", randomHex(6)) }
func newEventID() string   { return fmt.Sprintf("ev_%s", randomHex(6)) }
