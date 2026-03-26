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
	mux.HandleFunc("PUT /api/v1/endpoints/{id}", handleUpdateEndpoint(store, host))
	mux.HandleFunc("DELETE /api/v1/endpoints/{id}", handleDeleteEndpoint(store))
	mux.HandleFunc("GET /api/v1/endpoints/{id}/events", handleListEvents(store))
	mux.HandleFunc("POST /api/v1/endpoints/{id}/events/{event_id}/replay", handleReplayEvent(store, execCfg))
	mux.HandleFunc("POST /api/v1/endpoints/{id}/pause", handlePauseEndpoint(store))
	mux.HandleFunc("POST /api/v1/endpoints/{id}/resume", handleResumeEndpoint(store))

	mux.HandleFunc("/in/{slug}", handleInbound(store, execCfg))
}

type endpointCreateRequest struct {
	Name             string            `json:"name"`
	Slug             string            `json:"slug"`
	ForwardURLs      []string          `json:"forward_urls"`
	ForwardURL       string            `json:"forward_url"` // backward compat
	Enabled          *bool             `json:"enabled"`
	RetryAttempts    *int              `json:"retry_attempts"`
	UseLane          *bool             `json:"use_lane"`
	Paused           *bool             `json:"paused"`
	NotifyOnFailure  *bool             `json:"notify_on_failure"`
	NotifyOnRecovery *bool             `json:"notify_on_recovery"`
	Script           *string           `json:"script"`
	Secrets          map[string]string `json:"secrets"`
}

func normalizeForwardURLs(req *endpointCreateRequest) []string {
	if len(req.ForwardURLs) > 0 {
		return req.ForwardURLs
	}
	if req.ForwardURL != "" {
		return []string{req.ForwardURL}
	}
	return nil
}

func handleCreateEndpoint(store *Store, host string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req endpointCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		forwardURLs := normalizeForwardURLs(&req)
		if len(forwardURLs) == 0 {
			writeError(w, 422, "validation_error", "forward_urls is required")
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
		retryAttempts := 5
		if req.RetryAttempts != nil {
			retryAttempts = *req.RetryAttempts
		}
		useLane := true
		if req.UseLane != nil {
			useLane = *req.UseLane
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
			ForwardURLs:      forwardURLs,
			Enabled:          enabled,
			RetryAttempts:    retryAttempts,
			UseLane:         useLane,
			NotifyOnFailure:  req.NotifyOnFailure,
			NotifyOnRecovery: req.NotifyOnRecovery,
			Script:           req.Script,
			Secrets:          req.Secrets,
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

func handleUpdateEndpoint(store *Store, host string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ep := store.GetEndpoint(id)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}

		var req endpointCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		store.UpdateEndpoint(id, func(ep *Endpoint) {
			if req.Name != "" {
				ep.Name = req.Name
			}
			forwardURLs := normalizeForwardURLs(&req)
			if len(forwardURLs) > 0 {
				ep.ForwardURLs = forwardURLs
			}
			if req.Enabled != nil {
				ep.Enabled = *req.Enabled
			}
			if req.RetryAttempts != nil {
				ep.RetryAttempts = *req.RetryAttempts
			}
			if req.UseLane != nil {
				ep.UseLane = *req.UseLane
			}
			if req.Paused != nil {
				ep.Paused = *req.Paused
			}
			if req.NotifyOnFailure != nil {
				ep.NotifyOnFailure = req.NotifyOnFailure
			}
			if req.NotifyOnRecovery != nil {
				ep.NotifyOnRecovery = req.NotifyOnRecovery
			}
			if req.Script != nil {
				ep.Script = req.Script
			}
			if req.Secrets != nil {
				if ep.Secrets == nil {
					ep.Secrets = make(map[string]string)
				}
				for k, v := range req.Secrets {
					if v == "" {
						delete(ep.Secrets, k)
					} else {
						ep.Secrets[k] = v
					}
				}
			}
			ep.UpdatedAt = nowISO()
		})

		ep = store.GetEndpoint(id)
		logInbound("PUT", "/api/v1/endpoints/"+id, "[updated]")

		writeJSON(w, 200, map[string]interface{}{
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

		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
				limit = v
			}
		}

		events := store.ListEvents(id)
		// Return most recent first, limited
		if len(events) > limit {
			events = events[len(events)-limit:]
		}

		data := make([]map[string]interface{}, len(events))
		for i, ev := range events {
			data[i] = eventToJSON(ev)
		}

		logInbound("GET", "/api/v1/endpoints/"+id+"/events", fmt.Sprintf("[%d events]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data":     data,
			"has_more": false,
			"limit":    limit,
			"offset":   0,
		})
	}
}

func handleReplayEvent(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		epID := r.PathValue("id")
		eventID := r.PathValue("event_id")

		ep := store.GetEndpoint(epID)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}

		event := store.GetEvent(epID, eventID)
		if event == nil {
			writeError(w, 404, "not_found", "Event not found")
			return
		}

		logInbound("POST", fmt.Sprintf("/api/v1/endpoints/%s/events/%s/replay", epID, eventID), "[replaying]")

		// Create new forwarding executions for each forward URL
		type execResult struct {
			ExecutionID  string `json:"execution_id"`
			Status       string `json:"status"`
			ScheduledFor string `json:"scheduled_for"`
		}
		var results []execResult

		for _, fwdURL := range ep.ForwardURLs {
			now := nowISO()
			taskID := newTaskID()
			exec := &Execution{
				ID:           newExecID(),
				Status:       "pending",
				ScheduledFor: now,
				Attempt:      1,
			}

			// Create a temporary task for the forward
			task := &Task{
				ID:       taskID,
				Name:     fmt.Sprintf("Replay: %s -> %s", event.ID, fwdURL),
				URL:      fwdURL,
				Method:   event.Method,
				Enabled:  true,
				TimeoutMs: 30000,
				InsertedAt: now,
				UpdatedAt:  now,
			}
			if task.Headers == nil {
				task.Headers = map[string]string{}
			}
			store.CreateTask(task)
			store.AddExecution(taskID, exec)

			go executeTask(store, task, exec.ID, execCfg)

			results = append(results, execResult{
				ExecutionID:  exec.ID,
				Status:       "pending",
				ScheduledFor: now,
			})
		}

		writeJSON(w, 202, map[string]interface{}{
			"data": map[string]interface{}{
				"executions": results,
			},
			"message": fmt.Sprintf("Event replayed to %d destination(s)", len(results)),
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

		// Create task IDs for each forward URL
		var taskIDs []string
		for _, fwdURL := range ep.ForwardURLs {
			taskID := newTaskID()
			taskIDs = append(taskIDs, taskID)

			logInbound(r.Method, "/in/"+slug, fmt.Sprintf("[%s forwarding -> %s]", taskID, fwdURL))

			// Forward in background
			go executeForward(r.Method, fwdURL, r.Header, body, execCfg)
		}

		// Determine aggregate status
		status := "pending"

		event := &InboundEvent{
			ID:         newEventID(),
			Method:     r.Method,
			SourceIP:   sourceIP,
			ReceivedAt: nowISO(),
			TaskIDs:    taskIDs,
			Status:     &status,
		}
		store.AddEvent(ep.ID, event)

		// Determine response status based on pause state
		responseStatus := "received"
		if ep.Paused {
			responseStatus = "queued"
		}

		writeJSON(w, 200, map[string]interface{}{
			"id":     event.ID,
			"status": responseStatus,
		})
	}
}

func handlePauseEndpoint(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ep := store.GetEndpoint(id)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}
		store.UpdateEndpoint(id, func(ep *Endpoint) {
			ep.Paused = true
			ep.UseLane = true
			ep.UpdatedAt = nowISO()
		})
		ep = store.GetEndpoint(id)
		logInbound("POST", "/api/v1/endpoints/"+id+"/pause", "[paused]")
		writeJSON(w, 200, map[string]interface{}{
			"data": endpointToJSON(ep),
		})
	}
}

func handleResumeEndpoint(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		ep := store.GetEndpoint(id)
		if ep == nil {
			writeError(w, 404, "not_found", "Endpoint not found")
			return
		}
		store.UpdateEndpoint(id, func(ep *Endpoint) {
			ep.Paused = false
			ep.UpdatedAt = nowISO()
		})
		ep = store.GetEndpoint(id)
		logInbound("POST", "/api/v1/endpoints/"+id+"/resume", "[resumed]")
		writeJSON(w, 200, map[string]interface{}{
			"data": endpointToJSON(ep),
		})
	}
}

func endpointToJSON(ep *Endpoint) map[string]interface{} {
	secretsKeys := make([]string, 0)
	for k := range ep.Secrets {
		secretsKeys = append(secretsKeys, k)
	}

	result := map[string]interface{}{
		"id":                 ep.ID,
		"name":               ep.Name,
		"slug":               ep.Slug,
		"inbound_url":        ep.InboundURL,
		"forward_urls":       ep.ForwardURLs,
		"enabled":            ep.Enabled,
		"paused":             ep.Paused,
		"retry_attempts":     ep.RetryAttempts,
		"use_lane":          ep.UseLane,
		"notify_on_failure":  ep.NotifyOnFailure,
		"notify_on_recovery": ep.NotifyOnRecovery,
		"script":             ep.Script,
		"secrets_keys":       secretsKeys,
		"inserted_at":        ep.InsertedAt,
		"updated_at":         ep.UpdatedAt,
	}
	return result
}

func eventToJSON(ev *InboundEvent) map[string]interface{} {
	return map[string]interface{}{
		"id":          ev.ID,
		"method":      ev.Method,
		"source_ip":   ev.SourceIP,
		"received_at": ev.ReceivedAt,
		"task_ids":    ev.TaskIDs,
		"status":      ev.Status,
	}
}
