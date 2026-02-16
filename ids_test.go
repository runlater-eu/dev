package main

import (
	"strings"
	"testing"
)

func TestNewTaskID(t *testing.T) {
	id := newTaskID()
	if !strings.HasPrefix(id, "t_") {
		t.Errorf("task ID should start with t_, got %s", id)
	}
	if len(id) != 14 { // "t_" + 12 hex chars
		t.Errorf("task ID should be 14 chars, got %d: %s", len(id), id)
	}
}

func TestNewExecID(t *testing.T) {
	id := newExecID()
	if !strings.HasPrefix(id, "ex_") {
		t.Errorf("exec ID should start with ex_, got %s", id)
	}
}

func TestNewEndpointID(t *testing.T) {
	id := newEndpointID()
	if !strings.HasPrefix(id, "ep_") {
		t.Errorf("endpoint ID should start with ep_, got %s", id)
	}
}

func TestNewEventID(t *testing.T) {
	id := newEventID()
	if !strings.HasPrefix(id, "ev_") {
		t.Errorf("event ID should start with ev_, got %s", id)
	}
}

func TestIDsAreUnique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := newTaskID()
		if seen[id] {
			t.Errorf("duplicate ID generated: %s", id)
		}
		seen[id] = true
	}
}
