package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func registerBatchRoutes(mux *http.ServeMux, store *Store, execCfg ExecutorConfig) {
	mux.HandleFunc("POST /api/v1/tasks/batch", handleBatchCreate(store, execCfg))
}

type batchCreateRequest struct {
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers"`
	Queue         string            `json:"queue"`
	RunAt         *string           `json:"run_at"`
	Delay         interface{}       `json:"delay"`
	RetryAttempts *int              `json:"retry_attempts"`
	TimeoutMs     *int              `json:"timeout_ms"`
	CallbackURL   *string           `json:"callback_url"`
	Items         []json.RawMessage `json:"items"`
}

func handleBatchCreate(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req batchCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		if req.URL == "" {
			writeError(w, 422, "validation_error", "url is required")
			return
		}
		if req.Queue == "" {
			writeError(w, 422, "validation_error", "queue is required")
			return
		}
		if len(req.Items) == 0 {
			writeError(w, 422, "validation_error", "items is required and must not be empty")
			return
		}
		if len(req.Items) > 1000 {
			writeError(w, 422, "validation_error", "items must not exceed 1000")
			return
		}

		method := "POST"
		if req.Method != "" {
			method = strings.ToUpper(req.Method)
		}
		timeoutMs := 30000
		if req.TimeoutMs != nil {
			timeoutMs = *req.TimeoutMs
		}
		retryAttempts := 5
		if req.RetryAttempts != nil {
			retryAttempts = *req.RetryAttempts
		}

		// Determine scheduled_for
		var scheduledFor string
		if req.Delay != nil {
			delaySec, err := parseDelay(req.Delay)
			if err != nil {
				writeError(w, 400, "invalid_delay", err.Error())
				return
			}
			scheduledFor = time.Now().UTC().Add(time.Duration(delaySec) * time.Second).Format(time.RFC3339)
		} else if req.RunAt != nil {
			_, err := time.Parse(time.RFC3339, *req.RunAt)
			if err != nil {
				writeError(w, 400, "invalid_run_at", "run_at must be a valid ISO 8601 datetime")
				return
			}
			scheduledFor = *req.RunAt
		} else {
			scheduledFor = nowISO()
		}

		queue := req.Queue
		created := 0

		for _, item := range req.Items {
			bodyStr := string(item)
			now := nowISO()

			task := &Task{
				ID:            newTaskID(),
				Name:          fmt.Sprintf("Batch: %s", req.URL),
				URL:           req.URL,
				Method:        method,
				Headers:       req.Headers,
				Body:          &bodyStr,
				ScheduleType:  "once",
				Enabled:       true,
				TimeoutMs:     timeoutMs,
				RetryAttempts: retryAttempts,
				Queue:         &queue,
				CallbackURL:   req.CallbackURL,
				InsertedAt:    now,
				UpdatedAt:     now,
			}
			if task.Headers == nil {
				task.Headers = map[string]string{}
			}

			exec := &Execution{
				ID:           newExecID(),
				Status:       "pending",
				ScheduledFor: scheduledFor,
				Attempt:      1,
			}

			store.CreateTask(task)
			store.AddExecution(task.ID, exec)

			go executeTask(store, task, exec.ID, execCfg)
			created++
		}

		logInbound("POST", "/api/v1/tasks/batch", fmt.Sprintf("[%d tasks created in queue %q]", created, queue))

		writeJSON(w, 201, map[string]interface{}{
			"data": map[string]interface{}{
				"queue":         queue,
				"created":       created,
				"scheduled_for": scheduledFor,
			},
			"message": fmt.Sprintf("Created %d task(s)", created),
		})
	}
}
