package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExecuteTaskSuccess(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("X-Custom") != "test" {
			t.Errorf("expected header X-Custom=test, got %s", r.Header.Get("X-Custom"))
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"key":"value"}` {
			t.Errorf("unexpected body: %s", body)
		}
		w.WriteHeader(200)
	}))
	defer target.Close()

	store := NewStore()
	body := `{"key":"value"}`
	task := &Task{
		ID: "t_test1", URL: target.URL, Method: "POST",
		Headers:    map[string]string{"X-Custom": "test"},
		Body:       &body,
		TimeoutMs:  5000,
		InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	store.CreateTask(task)

	exec := &Execution{ID: "ex_test1", Status: "pending", ScheduledFor: nowISO(), Attempt: 1}
	store.AddExecution(task.ID, exec)

	executeTask(store, task, "ex_test1", ExecutorConfig{SimulatedDelay: 0})

	execs := store.ListExecutions(task.ID)
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution, got %d", len(execs))
	}
	if execs[0].Status != "success" {
		t.Errorf("expected success, got %s", execs[0].Status)
	}
	if execs[0].StatusCode == nil || *execs[0].StatusCode != 200 {
		t.Errorf("expected status code 200")
	}
	if execs[0].DurationMs == nil {
		t.Error("expected duration to be set")
	}
	if execs[0].StartedAt == nil {
		t.Error("expected started_at to be set")
	}
	if execs[0].FinishedAt == nil {
		t.Error("expected finished_at to be set")
	}
}

func TestExecuteTaskServerError(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer target.Close()

	store := NewStore()
	task := &Task{
		ID: "t_test1", URL: target.URL, Method: "GET",
		TimeoutMs: 5000, InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	store.CreateTask(task)

	exec := &Execution{ID: "ex_test1", Status: "pending", ScheduledFor: nowISO(), Attempt: 1}
	store.AddExecution(task.ID, exec)

	executeTask(store, task, "ex_test1", ExecutorConfig{SimulatedDelay: 0})

	execs := store.ListExecutions(task.ID)
	if execs[0].Status != "failed" {
		t.Errorf("expected failed, got %s", execs[0].Status)
	}
	if execs[0].StatusCode == nil || *execs[0].StatusCode != 500 {
		t.Errorf("expected status code 500")
	}
}

func TestExecuteTaskUnreachable(t *testing.T) {
	store := NewStore()
	task := &Task{
		ID: "t_test1", URL: "http://127.0.0.1:1", Method: "GET",
		TimeoutMs: 1000, InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	store.CreateTask(task)

	exec := &Execution{ID: "ex_test1", Status: "pending", ScheduledFor: nowISO(), Attempt: 1}
	store.AddExecution(task.ID, exec)

	executeTask(store, task, "ex_test1", ExecutorConfig{SimulatedDelay: 0})

	execs := store.ListExecutions(task.ID)
	if execs[0].Status != "failed" {
		t.Errorf("expected failed, got %s", execs[0].Status)
	}
	if execs[0].ErrorMessage == nil {
		t.Error("expected error message")
	}
}

func TestExecuteForwardSuccess(t *testing.T) {
	var receivedMethod string
	var receivedBody string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.WriteHeader(200)
	}))
	defer target.Close()

	headers := http.Header{"Content-Type": []string{"application/json"}}
	body := []byte(`{"event":"test"}`)

	code, err := executeForward("POST", target.URL, headers, body, ExecutorConfig{SimulatedDelay: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != 200 {
		t.Errorf("expected 200, got %d", code)
	}
	if receivedMethod != "POST" {
		t.Errorf("expected POST, got %s", receivedMethod)
	}
	if receivedBody != `{"event":"test"}` {
		t.Errorf("unexpected body: %s", receivedBody)
	}
}

func TestSimulatedDelay(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	store := NewStore()
	task := &Task{
		ID: "t_test1", URL: target.URL, Method: "GET",
		TimeoutMs: 5000, InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	store.CreateTask(task)
	exec := &Execution{ID: "ex_test1", Status: "pending", ScheduledFor: nowISO(), Attempt: 1}
	store.AddExecution(task.ID, exec)

	start := time.Now()
	executeTask(store, task, "ex_test1", ExecutorConfig{SimulatedDelay: 50 * time.Millisecond})
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Errorf("expected at least 50ms delay, got %v", elapsed)
	}

	_ = json.Marshal // ensure json import is used
}
