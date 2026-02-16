package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func loadTestValidator(t *testing.T) *SchemaValidator {
	t.Helper()
	v, err := LoadValidator("testdata/openapi.json")
	if err != nil {
		t.Fatalf("failed to load validator: %v", err)
	}
	return v
}

func TestSpecNotStale(t *testing.T) {
	info, err := os.Stat("testdata/openapi.json")
	if err != nil {
		t.Skip("openapi.json not found, skipping staleness check")
	}
	if time.Since(info.ModTime()) > 90*24*time.Hour {
		t.Log("WARNING: OpenAPI snapshot is >90 days old, consider refreshing")
	}
}

func TestValidatorLoads(t *testing.T) {
	v := loadTestValidator(t)

	expectedSchemas := []string{"Task", "Execution", "Endpoint", "InboundEvent"}
	for _, name := range expectedSchemas {
		if !v.HasSchema(name) {
			t.Errorf("expected schema %q to exist", name)
		}
	}
}

func TestTaskResponseMatchesSpec(t *testing.T) {
	v := loadTestValidator(t)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerTaskRoutes(mux, store, execCfg)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create cron task
	body := map[string]interface{}{
		"url":  target.URL,
		"cron": "0 * * * *",
		"name": "Test cron",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/tasks", "application/json", bytes.NewReader(b))
	var createResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	taskID := createResult["data"].(map[string]interface{})["id"].(string)

	// GET task and validate against Task schema
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var envelope map[string]json.RawMessage
	json.Unmarshal(respBody, &envelope)

	v.ValidateResponse(t, "Task", envelope["data"])
}

func TestExecutionResponseMatchesSpec(t *testing.T) {
	v := loadTestValidator(t)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer target.Close()

	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerTaskRoutes(mux, store, execCfg)
	server := httptest.NewServer(mux)
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

	// GET executions and validate
	resp, _ = http.Get(server.URL + "/api/v1/tasks/" + taskID + "/executions")
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	json.Unmarshal(respBody, &envelope)

	if len(envelope.Data) == 0 {
		t.Fatal("expected at least 1 execution")
	}
	v.ValidateResponse(t, "Execution", envelope.Data[0])
}

func TestEndpointResponseMatchesSpec(t *testing.T) {
	v := loadTestValidator(t)

	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerEndpointRoutes(mux, store, "localhost:8080", execCfg)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create endpoint
	body := map[string]interface{}{
		"name":        "Test EP",
		"slug":        "test-ep",
		"forward_url": "http://localhost:3000/hook",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var envelope map[string]json.RawMessage
	json.Unmarshal(respBody, &envelope)

	v.ValidateResponse(t, "Endpoint", envelope["data"])
}

func TestInboundEventResponseMatchesSpec(t *testing.T) {
	v := loadTestValidator(t)

	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerEndpointRoutes(mux, store, "localhost:8080", execCfg)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create endpoint directly in store
	ep := &Endpoint{
		ID: "ep_test1", Name: "Test", Slug: "test-slug",
		ForwardURL: "http://localhost:9999", Enabled: true,
		InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	store.CreateEndpoint(ep)

	// Add event directly
	store.AddEvent(ep.ID, &InboundEvent{
		ID: "ev_test1", Method: "POST", SourceIP: "127.0.0.1", ReceivedAt: nowISO(),
	})

	// GET events
	resp, _ := http.Get(server.URL + "/api/v1/endpoints/" + ep.ID + "/events")
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	json.Unmarshal(respBody, &envelope)

	if len(envelope.Data) == 0 {
		t.Fatal("expected at least 1 event")
	}
	v.ValidateResponse(t, "InboundEvent", envelope.Data[0])
}
