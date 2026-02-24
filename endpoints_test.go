package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func setupEndpointServer() (*httptest.Server, *Store) {
	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	// We need to know the server host for inbound_url, use placeholder
	registerEndpointRoutes(mux, store, "localhost:8080", execCfg)
	server := httptest.NewServer(mux)
	return server, store
}

func TestCreateEndpoint(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	body := map[string]interface{}{
		"name":        "Stripe Webhook",
		"slug":        "stripe-test",
		"forward_url": "http://localhost:3000/webhook",
	}
	b, _ := json.Marshal(body)

	resp, err := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
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
	if data["name"] != "Stripe Webhook" {
		t.Errorf("expected name Stripe Webhook, got %v", data["name"])
	}
	if data["slug"] != "stripe-test" {
		t.Errorf("expected slug stripe-test, got %v", data["slug"])
	}
	forwardURLs, ok := data["forward_urls"].([]interface{})
	if !ok || len(forwardURLs) != 1 || forwardURLs[0] != "http://localhost:3000/webhook" {
		t.Errorf("unexpected forward_urls: %v", data["forward_urls"])
	}
	if data["inbound_url"] != "http://localhost:8080/in/stripe-test" {
		t.Errorf("unexpected inbound_url: %v", data["inbound_url"])
	}
	if data["enabled"] != true {
		t.Error("expected enabled to be true by default")
	}
}

func TestCreateEndpointMissingForwardURL(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	body := map[string]interface{}{"name": "Test"}
	b, _ := json.Marshal(body)

	resp, _ := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	if resp.StatusCode != 422 {
		t.Errorf("expected 422, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestCreateEndpointDuplicateSlug(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	body := map[string]interface{}{
		"slug":        "my-slug",
		"forward_url": "http://localhost:3000/webhook",
	}
	b, _ := json.Marshal(body)

	resp, _ := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("first create expected 201, got %d", resp.StatusCode)
	}

	b, _ = json.Marshal(body)
	resp, _ = http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	if resp.StatusCode != 422 {
		t.Errorf("duplicate slug expected 422, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestEndpointCRUDFlow(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	// Create
	body := map[string]interface{}{
		"name":        "Test EP",
		"slug":        "test-ep",
		"forward_url": "http://localhost:3000/hook",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	var createResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&createResult)
	resp.Body.Close()

	epID := createResult["data"].(map[string]interface{})["id"].(string)

	// Get
	resp, _ = http.Get(server.URL + "/api/v1/endpoints/" + epID)
	if resp.StatusCode != 200 {
		t.Errorf("GET expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// List
	resp, _ = http.Get(server.URL + "/api/v1/endpoints")
	if resp.StatusCode != 200 {
		t.Errorf("LIST expected 200, got %d", resp.StatusCode)
	}
	var listResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&listResult)
	resp.Body.Close()

	eps := listResult["data"].([]interface{})
	if len(eps) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(eps))
	}

	// Delete
	req, _ := http.NewRequest("DELETE", server.URL+"/api/v1/endpoints/"+epID, nil)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 204 {
		t.Errorf("DELETE expected 204, got %d", resp.StatusCode)
	}

	// Get after delete → 404
	resp, _ = http.Get(server.URL + "/api/v1/endpoints/" + epID)
	if resp.StatusCode != 404 {
		t.Errorf("GET after delete expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestInboundWebhookForward(t *testing.T) {
	// Target that receives forwarded webhook
	var receivedBody string
	var receivedMethod string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		receivedBody = string(b)
		w.WriteHeader(200)
	}))
	defer target.Close()

	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerEndpointRoutes(mux, store, "localhost:8080", execCfg)
	server := httptest.NewServer(mux)
	defer server.Close()

	// Create endpoint pointing to target
	ep := &Endpoint{
		ID:         newEndpointID(),
		Name:       "Test",
		Slug:       "stripe-hook",
		ForwardURLs: []string{target.URL},
		Enabled:    true,
		InsertedAt: nowISO(),
		UpdatedAt:  nowISO(),
	}
	store.CreateEndpoint(ep)

	// Send inbound webhook
	webhookBody := `{"type":"payment_intent.succeeded"}`
	resp, err := http.Post(server.URL+"/in/stripe-hook", "application/json",
		bytes.NewReader([]byte(webhookBody)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["status"] != "received" {
		t.Errorf("expected status received, got %v", result["status"])
	}

	// Wait for forward
	time.Sleep(100 * time.Millisecond)

	if receivedMethod != "POST" {
		t.Errorf("expected POST forward, got %s", receivedMethod)
	}
	if receivedBody != webhookBody {
		t.Errorf("expected body %s, got %s", webhookBody, receivedBody)
	}

	// Check event was stored
	events := store.ListEvents(ep.ID)
	if len(events) != 1 {
		t.Errorf("expected 1 event, got %d", len(events))
	}
}

func TestInboundWebhookNotFound(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	resp, _ := http.Post(server.URL+"/in/nonexistent", "application/json", nil)
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestInboundWebhookDisabled(t *testing.T) {
	store := NewStore()
	mux := http.NewServeMux()
	execCfg := ExecutorConfig{SimulatedDelay: 0}
	registerEndpointRoutes(mux, store, "localhost:8080", execCfg)
	server := httptest.NewServer(mux)
	defer server.Close()

	ep := &Endpoint{
		ID:         newEndpointID(),
		Name:       "Disabled",
		Slug:       "disabled-ep",
		ForwardURLs: []string{"http://localhost:3000"},
		Enabled:    false,
		InsertedAt: nowISO(),
		UpdatedAt:  nowISO(),
	}
	store.CreateEndpoint(ep)

	resp, _ := http.Post(server.URL+"/in/disabled-ep", "application/json", nil)
	if resp.StatusCode != 410 {
		t.Errorf("expected 410, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestListEvents(t *testing.T) {
	server, store := setupEndpointServer()
	defer server.Close()

	// Create endpoint via store directly
	ep := &Endpoint{
		ID:         "ep_test1",
		Name:       "Test",
		Slug:       "test-events",
		ForwardURLs: []string{"http://localhost:3000"},
		Enabled:    true,
		InsertedAt: nowISO(),
		UpdatedAt:  nowISO(),
	}
	store.CreateEndpoint(ep)

	// Add some events
	store.AddEvent(ep.ID, &InboundEvent{
		ID: "ev_1", Method: "POST", SourceIP: "127.0.0.1", ReceivedAt: nowISO(),
	})
	store.AddEvent(ep.ID, &InboundEvent{
		ID: "ev_2", Method: "POST", SourceIP: "127.0.0.1", ReceivedAt: nowISO(),
	})

	resp, _ := http.Get(server.URL + "/api/v1/endpoints/" + ep.ID + "/events")
	if resp.StatusCode != 200 {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	events := result["data"].([]interface{})
	if len(events) != 2 {
		t.Errorf("expected 2 events, got %d", len(events))
	}
}

func TestListEventsNonexistentEndpoint(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	resp, _ := http.Get(server.URL + "/api/v1/endpoints/ep_nonexistent/events")
	if resp.StatusCode != 404 {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestEndpointAutoSlug(t *testing.T) {
	server, _ := setupEndpointServer()
	defer server.Close()

	body := map[string]interface{}{
		"forward_url": "http://localhost:3000/hook",
	}
	b, _ := json.Marshal(body)
	resp, _ := http.Post(server.URL+"/api/v1/endpoints", "application/json", bytes.NewReader(b))
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	slug := result["data"].(map[string]interface{})["slug"].(string)
	if slug == "" {
		t.Error("expected auto-generated slug")
	}
}
