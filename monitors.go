package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func registerMonitorRoutes(mux *http.ServeMux, store *Store, host string) {
	mux.HandleFunc("POST /api/v1/monitors", handleCreateMonitor(store, host))
	mux.HandleFunc("GET /api/v1/monitors", handleListMonitors(store))
	mux.HandleFunc("GET /api/v1/monitors/{id}", handleGetMonitor(store))
	mux.HandleFunc("PUT /api/v1/monitors/{id}", handleUpdateMonitor(store))
	mux.HandleFunc("DELETE /api/v1/monitors/{id}", handleDeleteMonitor(store))
	mux.HandleFunc("GET /api/v1/monitors/{id}/pings", handleListPings(store))

	// Public ping endpoints (no auth)
	mux.HandleFunc("GET /ping/{token}", handlePing(store))
	mux.HandleFunc("POST /ping/{token}", handlePing(store))
}

type monitorCreateRequest struct {
	Name               string  `json:"name"`
	ScheduleType       string  `json:"schedule_type"`
	CronExpression     *string `json:"cron_expression"`
	IntervalSeconds    *int    `json:"interval_seconds"`
	GracePeriodSeconds *int    `json:"grace_period_seconds"`
	Enabled            *bool   `json:"enabled"`
	NotifyOnFailure    *bool   `json:"notify_on_failure"`
	NotifyOnRecovery   *bool   `json:"notify_on_recovery"`
}

func handleCreateMonitor(store *Store, host string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req monitorCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		if req.Name == "" {
			writeError(w, 422, "validation_error", "name is required")
			return
		}
		if req.ScheduleType == "" {
			writeError(w, 422, "validation_error", "schedule_type is required (cron or interval)")
			return
		}
		if req.ScheduleType == "cron" && req.CronExpression == nil {
			writeError(w, 422, "validation_error", "cron_expression is required for cron monitors")
			return
		}
		if req.ScheduleType == "interval" && req.IntervalSeconds == nil {
			writeError(w, 422, "validation_error", "interval_seconds is required for interval monitors")
			return
		}

		now := nowISO()
		enabled := true
		if req.Enabled != nil {
			enabled = *req.Enabled
		}
		gracePeriod := 300
		if req.GracePeriodSeconds != nil {
			gracePeriod = *req.GracePeriodSeconds
		}

		token := newPingToken()

		m := &Monitor{
			ID:                 newMonitorID(),
			Name:               req.Name,
			ScheduleType:       req.ScheduleType,
			CronExpression:     req.CronExpression,
			IntervalSeconds:    req.IntervalSeconds,
			GracePeriodSeconds: gracePeriod,
			Enabled:            enabled,
			NotifyOnFailure:    req.NotifyOnFailure,
			NotifyOnRecovery:   req.NotifyOnRecovery,
			PingToken:          token,
			PingURL:            fmt.Sprintf("http://%s/ping/%s", host, token),
			Status:             "new",
			InsertedAt:         now,
			UpdatedAt:          now,
		}
		store.CreateMonitor(m)

		logInbound("POST", "/api/v1/monitors", fmt.Sprintf("[%s created, %s]", m.ID, m.ScheduleType))

		writeJSON(w, 201, map[string]interface{}{
			"data": monitorToJSON(m),
		})
	}
}

func handleListMonitors(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		monitors := store.ListMonitors()

		data := make([]map[string]interface{}, len(monitors))
		for i, m := range monitors {
			data[i] = monitorToJSON(m)
		}

		logInbound("GET", "/api/v1/monitors", fmt.Sprintf("[%d monitors]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data": data,
		})
	}
}

func handleGetMonitor(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		m := store.GetMonitor(id)
		if m == nil {
			writeError(w, 404, "not_found", "Monitor not found")
			return
		}

		logInbound("GET", "/api/v1/monitors/"+id, "")

		writeJSON(w, 200, map[string]interface{}{
			"data": monitorToJSON(m),
		})
	}
}

func handleUpdateMonitor(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		m := store.GetMonitor(id)
		if m == nil {
			writeError(w, 404, "not_found", "Monitor not found")
			return
		}

		var req monitorCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		store.UpdateMonitor(id, func(m *Monitor) {
			if req.Name != "" {
				m.Name = req.Name
			}
			if req.ScheduleType != "" {
				m.ScheduleType = req.ScheduleType
			}
			if req.CronExpression != nil {
				m.CronExpression = req.CronExpression
			}
			if req.IntervalSeconds != nil {
				m.IntervalSeconds = req.IntervalSeconds
			}
			if req.GracePeriodSeconds != nil {
				m.GracePeriodSeconds = *req.GracePeriodSeconds
			}
			if req.Enabled != nil {
				m.Enabled = *req.Enabled
			}
			if req.NotifyOnFailure != nil {
				m.NotifyOnFailure = req.NotifyOnFailure
			}
			if req.NotifyOnRecovery != nil {
				m.NotifyOnRecovery = req.NotifyOnRecovery
			}
			m.UpdatedAt = nowISO()
		})

		m = store.GetMonitor(id)
		logInbound("PUT", "/api/v1/monitors/"+id, "[updated]")

		writeJSON(w, 200, map[string]interface{}{
			"data": monitorToJSON(m),
		})
	}
}

func handleDeleteMonitor(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !store.DeleteMonitor(id) {
			writeError(w, 404, "not_found", "Monitor not found")
			return
		}

		logInbound("DELETE", "/api/v1/monitors/"+id, "[deleted]")
		w.WriteHeader(204)
	}
}

func handleListPings(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		m := store.GetMonitor(id)
		if m == nil {
			writeError(w, 404, "not_found", "Monitor not found")
			return
		}

		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.Atoi(l); err == nil && v > 0 && v <= 100 {
				limit = v
			}
		}

		pings := store.ListPings(id)
		if len(pings) > limit {
			pings = pings[len(pings)-limit:]
		}

		data := make([]map[string]interface{}, len(pings))
		for i, p := range pings {
			data[i] = map[string]interface{}{
				"id":          p.ID,
				"received_at": p.ReceivedAt,
			}
		}

		logInbound("GET", "/api/v1/monitors/"+id+"/pings", fmt.Sprintf("[%d pings]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data": data,
		})
	}
}

func handlePing(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.PathValue("token")
		m := store.GetMonitorByToken(token)
		if m == nil {
			writeError(w, 404, "not_found", "Monitor not found")
			return
		}

		now := nowISO()
		ping := &MonitorPing{
			ID:         newPingID(),
			ReceivedAt: now,
		}
		store.AddPing(m.ID, ping)

		// Update monitor status
		store.UpdateMonitor(m.ID, func(m *Monitor) {
			m.Status = "up"
			m.LastPingAt = &now
			m.UpdatedAt = now
		})

		logInbound(r.Method, "/ping/"+token[:8]+"...", fmt.Sprintf("[%s pinged, status=up]", m.ID))

		writeJSON(w, 200, map[string]interface{}{
			"ok": true,
		})
	}
}

func monitorToJSON(m *Monitor) map[string]interface{} {
	return map[string]interface{}{
		"id":                   m.ID,
		"name":                 m.Name,
		"schedule_type":        m.ScheduleType,
		"cron_expression":      m.CronExpression,
		"interval_seconds":     m.IntervalSeconds,
		"grace_period_seconds": m.GracePeriodSeconds,
		"enabled":              m.Enabled,
		"notify_on_failure":    m.NotifyOnFailure,
		"notify_on_recovery":   m.NotifyOnRecovery,
		"ping_token":           m.PingToken,
		"ping_url":             m.PingURL,
		"status":               m.Status,
		"last_ping_at":         m.LastPingAt,
		"next_expected_at":     m.NextExpectedAt,
		"inserted_at":          m.InsertedAt,
		"updated_at":           m.UpdatedAt,
	}
}
