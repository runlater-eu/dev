package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupTaskServer() (*httptest.Server, *Store) {
	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerTaskRoutes(mux, store, execCfg)
	server := httptest.NewServer(mux)
	return server, store
}

func TestCreateImmediateTask(t *testing.T) {
	// Target server to receive the webhook
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{
		"url":    target.URL,
		"method": "POST",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data := result["data"].(map[string]interface{})
	if data["task_id"] == nil {
		t.Error("expected task_id in response")
	}
	if data["execution_id"] == nil {
		t.Error("expected execution_id in response")
	}
	if data["status"] != "pending" {
		t.Errorf("expected status pending, got %v", data["status"])
	}
	if result["message"] != "Task queued for execution" {
		t.Errorf("unexpected message: %v", result["message"])
	}
}

func TestCreateCronTask(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{
		"url":  "http://example.com/webhook",
		"cron": "*/5 * * * *",
		"name": "My Cron Job",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Errorf("expected 201, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data := result["data"].(map[string]interface{})
	if data["id"] == nil {
		t.Error("expected id in response")
	}
	if data["cron"] != "*/5 * * * *" {
		t.Errorf("expected cron expression, got %v", data["cron"])
	}
	if data["name"] != "My Cron Job" {
		t.Errorf("expected name My Cron Job, got %v", data["name"])
	}
	if result["message"] != "Cron task created" {
		t.Errorf("unexpected message: %v", result["message"])
	}
}

func TestCreateDelayedTask(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{
		"url":   target.URL,
		"delay": "30s",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	data := result["data"].(map[string]interface{})
	if data["task_id"] == nil {
		t.Error("expected task_id in response")
	}
}

func TestCreateScheduledTask(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	server, _ := setupTaskServer()
	defer server.Close()

	runAt := time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	body := map[string]interface{}{
		"url":    target.URL,
		"run_at": runAt,
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202 {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}
}

func TestCreateTaskMissingURL(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{"method": "POST"}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 422 {
		t.Errorf("expected 422, got %d", resp.StatusCode)
	}
}

func TestCreateTaskInvalidDelay(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{
		"url":   "http://example.com",
		"delay": "invalid",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestCreateTaskInvalidRunAt(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	body := map[string]interface{}{
		"url":    "http://example.com",
		"run_at": "not-a-date",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestTaskCRUDFlow(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	// Create cron task
	body := map[string]interface{}{
		"url":  "http://example.com/webhook",
		"cron": "0 * * * *",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	var createResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	taskID := createResult["data"].(map[string]interface{})["id"].(string)

	// Get task
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID)
	if resp.StatusCode != 200 {
		t.Errorf("GET expected 200, got %d", resp.StatusCode)
	}
	var getResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&getResult)
	resp.Body.Close()

	data := getResult["data"].(map[string]interface{})
	if data["url"] != "http://example.com/webhook" {
		t.Errorf("unexpected url: %v", data["url"])
	}
	if data["schedule_type"] != "cron" {
		t.Errorf("expected schedule_type cron, got %v", data["schedule_type"])
	}

	// List tasks
	resp, _ = http.Get(server.URL + "/api/v1/tasks")
	if resp.StatusCode != 200 {
		t.Errorf("LIST expected 200, got %d", resp.StatusCode)
	}
	var listResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&listResult)
	resp.Body.Close()

	tasks := listResult["data"].([]interface{})
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}

	// Delete task
	req, _ := http.NewRequest("DELETE", server.URL+"/api/v1/tasks/"+taskID, nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 204 {
		t.Errorf("DELETE expected 204, got %d", resp.StatusCode)
	}

	// Get after delete → 404
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID)
	if resp.StatusCode != 404 {
		t.Errorf("GET after delete expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestTriggerTask(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	server, store := setupTaskServer()
	defer server.Close()

	// Create a cron task (not auto-executed)
	body := map[string]interface{}{
		"url":  target.URL,
		"cron": "0 * * * *",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	var createResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	taskID := createResult["data"].(map[string]interface{})["id"].(string)

	// Trigger it
	resp, _ = http.Post(server.URL+"/api/v1/tasks/"+taskID+"/trigger", "application/json", nil)
	if resp.StatusCode != 202 {
		t.Errorf("expected 202, got %d", resp.StatusCode)
	}
	var triggerResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&triggerResult)
	resp.Body.Close()

	if triggerResult["message"] != "Task triggered successfully" {
		t.Errorf("unexpected message: %v", triggerResult["message"])
	}

	// Wait for execution to complete
	time.Sleep(100 * time.Millisecond)

	// Check executions
	execs := store.ListExecutions(taskID)
	if len(execs) < 1 {
		t.Fatal("expected at least 1 execution")
	}
}

func TestTriggerNonexistentTask(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	resp, _ := http.Post(server.URL+"/api/v1/tasks/t_nonexistent/trigger", "application/json", nil)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestListExecutions(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	server, _ := setupTaskServer()
	defer server.Close()

	// Create immediate task
	body := map[string]interface{}{
		"url":    target.URL,
		"method": "POST",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	var createResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	taskID := createResult["data"].(map[string]interface{})["task_id"].(string)

	// Wait for execution
	time.Sleep(100 * time.Millisecond)

	// List executions
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID + "/executions")
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	execs := result["data"].([]interface{})
	if len(execs) < 1 {
		t.Fatal("expected at least 1 execution")
	}

	exec := execs[0].(map[string]interface{})
	if exec["status"] != "success" {
		t.Errorf("expected success, got %v", exec["status"])
	}
	if exec["attempt"] != float64(1) {
		t.Errorf("expected attempt 1, got %v", exec["attempt"])
	}
}

func TestListExecutionsNonexistentTask(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	resp, _ := http.Get(server.URL + "/api/v1/tasks/t_nonexistent/executions")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestTaskDefaultMethod(t *testing.T) {
	server, _ := setupTaskServer()
	defer server.Close()

	// Cron default → GET
	body := map[string]interface{}{
		"url":  "http://example.com",
		"cron": "0 * * * *",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	taskID := result["data"].(map[string]interface{})["id"].(string)
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID)
	var getResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&getResult)
	resp.Body.Close()

	if getResult["data"].(map[string]interface{})["method"] != "GET" {
		t.Error("cron task default method should be GET")
	}
}

func TestParseDelay(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected int
		wantErr  bool
	}{
		{"30s", 30, false},
		{"5m", 300, false},
		{"2h", 7200, false},
		{"1d", 86400, false},
		{float64(60), 60, false},
		{"invalid", 0, true},
		{float64(-1), 0, true},
		{"0x", 0, true},
	}

	for _, tt := range tests {
		got, err := parseDelay(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseDelay(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			continue
		}
		if got != tt.expected {
			t.Errorf("parseDelay(%v) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}
