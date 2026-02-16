package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

func registerEndpointRoutes(mux *http.ServeMux, store *Store, host string, execCfg ExecutorConfig) {
	mux.HandleFunc("POST /api/v1/endpoints", handleCreateEndpoint(store, host))
	mux.HandleFunc("GET /api/v1/endpoints", handleListEndpoints(store))
	mux.HandleFunc("GET /api/v1/endpoints/{id}", handleGetEndpoint(store))
	mux.HandleFunc("DELETE /api/v1/endpoints/{id}", handleDeleteEndpoint(store))
	mux.HandleFunc("GET /api/v1/endpoints/{id}/events", handleListEvents(store))

	mux.HandleFunc("POST /in/{slug}", handleInbound(store, execCfg))
}

type endpointCreateRequest struct {
	Name             string `json:"name"`
	Slug             string `json:"slug"`
	ForwardURL       string `json:"forward_url"`
	Enabled          *bool  `json:"enabled"`
	RetryAttempts    *int   `json:"retry_attempts"`
	UseQueue         *bool  `json:"use_queue"`
	NotifyOnFailure  *bool  `json:"notify_on_failure"`
	NotifyOnRecovery *bool  `json:"notify_on_recovery"`
}

func handleCreateEndpoint(store *Store, host string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req endpointCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		if req.ForwardURL == "" {
			writeError(w, 422, "validation_error", "forward_url is required")
			return
		}

		now := nowISO()
		slug := req.Slug
		if slug == "" {
			slug = randomHex(6)
		}
		name := req.Name
		if name == "" {
			name = fmt.Sprintf("Endpoint: %s", slug)
		}
		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		retryAttempts := 0
		if req.RetryAttempts != nil {
			retryAttempts = *req.RetryAttempts
		}
		useQueue := false
		if req.UseQueue != nil {
			useQueue = *req.UseQueue
		}

		// Check slug uniqueness
		if store.GetEndpointBySlug(slug) != nil {
			writeError(w, 422, "validation_error", "slug already taken")
			return
		}

		ep := &Endpoint{
			ID:               newEndpointID(),
			Name:             name,
			Slug:             slug,
			InboundURL:       fmt.Sprintf("http://%s/in/%s", host, slug),
			ForwardURL:       req.ForwardURL,
			Enabled:          enabled,
			RetryAttempts:    retryAttempts,
			UseQueue:         useQueue,
			NotifyOnFailure:  req.NotifyOnFailure,
			NotifyOnRecovery: req.NotifyOnRecovery,
			InsertedAt:       now,
			UpdatedAt:        now,
		}
		store.CreateEndpoint(ep)

		logInbound("POST", "/api/v1/endpoints", fmt.Sprintf("[%s created, slug=%s]", ep.ID, ep.Slug))

		writeJSON(w, 201, map[string]interface{}{
			"data": endpointToJSON(ep),
		})
	}
}

func handleListEndpoints(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		endpoints := store.ListEndpoints()

		limit := 50
		offset := 0
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 {
				limit = v
			}
		}
		if o := r.URL.Query().Get("offset"); o != "" {
			if v, err := strconv.Atoi(o); err == nil && v >= 0 {
				offset = v
			}
		}

		total := len(endpoints)
		if offset > total {
			offset = total
		}
		end := offset + limit
		if end > total {
			end = total
		}
		page := endpoints[offset:end]

		data := make([]map[string]interface{}, len(page))
		for i, ep := range page {
			data[i] = endpointToJSON(ep)
		}

		logInbound("GET", "/api/v1/endpoints", fmt.Sprintf("[%d endpoints]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data":     data,
			"has_more": end < total,
			"limit":    limit,
			"offset":   offset,
		})
	}
}

func handleGetEndpoint(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ep := store.GetEndpoint(id)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}

		logInbound("GET", "/api/v1/endpoints/"+id, "")

		writeJSON(w, 200, map[string]interface{}{
			"data": endpointToJSON(ep),
		})
	}
}

func handleDeleteEndpoint(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !store.DeleteEndpoint(id) {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}

		logInbound("DELETE", "/api/v1/endpoints/"+id, "[deleted]")
		w.WriteHeader(204)
	}
}

func handleListEvents(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ep := store.GetEndpoint(id)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}

		events := store.ListEvents(id)
		data := make([]map[string]interface{}, len(events))
		for i, ev := range events {
			data[i] = eventToJSON(ev)
		}

		logInbound("GET", "/api/v1/endpoints/"+id+"/events", fmt.Sprintf("[%d events]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data":     data,
			"has_more": false,
			"limit":    50,
			"offset":   0,
		})
	}
}

func handleInbound(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		ep := store.GetEndpointBySlug(slug)
		if ep == nil {
			writeError(w, 404, "not_found", "Not found")
			return
		}

		if !ep.Enabled {
			writeJSON(w, 410, map[string]interface{}{
				"error": "Endpoint disabled",
			})
			return
		}

		body, _ := io.ReadAll(r.Body)
		sourceIP := r.RemoteAddr
		if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
			sourceIP = fwd
		}

		event := &InboundEvent{
			ID:         newEventID(),
			Method:     r.Method,
			SourceIP:   sourceIP,
			ReceivedAt: nowISO(),
		}
		store.AddEvent(ep.ID, event)

		logInbound("POST", "/in/"+slug, fmt.Sprintf("[%s forwarding -> %s]", event.ID, ep.ForwardURL))

		// Forward in background
		go executeForward(r.Method, ep.ForwardURL, r.Header, body, execCfg)

		writeJSON(w, 200, map[string]interface{}{
			"id":     event.ID,
			"status": "received",
		})
	}
}

func endpointToJSON(ep *Endpoint) map[string]interface{} {
	return map[string]interface{}{
		"id":                ep.ID,
		"name":              ep.Name,
		"slug":              ep.Slug,
		"inbound_url":       ep.InboundURL,
		"forward_url":       ep.ForwardURL,
		"enabled":           ep.Enabled,
		"retry_attempts":    ep.RetryAttempts,
		"use_queue":         ep.UseQueue,
		"notify_on_failure": ep.NotifyOnFailure,
		"notify_on_recovery": ep.NotifyOnRecovery,
		"inserted_at":       ep.InsertedAt,
		"updated_at":        ep.UpdatedAt,
	}
}

func eventToJSON(ev *InboundEvent) map[string]interface{} {
	return map[string]interface{}{
		"id":               ev.ID,
		"method":           ev.Method,
		"source_ip":        ev.SourceIP,
		"received_at":      ev.ReceivedAt,
		"execution_id":     ev.ExecutionID,
		"execution_status": ev.ExecutionStatus,
	}
}
