package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func registerBatchRoutes(mux *http.ServeMux, store *Store, execCfg ExecutorConfig) {
	mux.HandleFunc("POST /api/v1/tasks/batch", handleBatchCreate(store, execCfg))
}

type batchTask struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    *string           `json:"body"`
	Name    string            `json:"name"`
}

type batchCreateRequest struct {
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers"`
	Lane          string            `json:"lane"`
	RunAt         *string           `json:"run_at"`
	Delay         interface{}       `json:"delay"`
	RetryAttempts *int              `json:"retry_attempts"`
	TimeoutMs     *int              `json:"timeout_ms"`
	CallbackURL   *string           `json:"callback_url"`
	Tasks         []batchTask       `json:"tasks"`
}

func handleBatchCreate(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req batchCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		if req.Lane == "" {
			writeError(w, 422, "validation_error", "lane is required")
			return
		}
		if len(req.Tasks) == 0 {
			writeError(w, 422, "validation_error", "tasks is required and must not be empty")
			return
		}
		if len(req.Tasks) > 1000 {
			writeError(w, 422, "validation_error", "tasks must not exceed 1000")
			return
		}

		// Validate each task has a valid URL
		for i, t := range req.Tasks {
			if t.URL == "" {
				writeError(w, 422, "validation_error", fmt.Sprintf("task at index %d: url is required", i))
				return
			}
			u, err := url.Parse(t.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				writeError(w, 422, "validation_error", fmt.Sprintf("task at index %d has an invalid url (must be HTTP or HTTPS)", i))
				return
			}
		}

		defaultMethod := "POST"
		if req.Method != "" {
			defaultMethod = strings.ToUpper(req.Method)
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

		lane := req.Lane
		created := 0

		for _, bt := range req.Tasks {
			method := defaultMethod
			if bt.Method != "" {
				method = strings.ToUpper(bt.Method)
			}

			headers := make(map[string]string)
			for k, v := range req.Headers {
				headers[k] = v
			}
			for k, v := range bt.Headers {
				headers[k] = v
			}

			taskName := bt.Name
			if taskName == "" {
				taskName = fmt.Sprintf("Batch: %s", bt.URL)
			}

			now := nowISO()

			task := &Task{
				ID:            newTaskID(),
				Name:          taskName,
				URL:           bt.URL,
				Method:        method,
				Headers:       headers,
				Body:          bt.Body,
				ScheduleType:  "once",
				Enabled:       true,
				TimeoutMs:     timeoutMs,
				RetryAttempts: retryAttempts,
				Lane:          &lane,
				CallbackURL:   req.CallbackURL,
				InsertedAt:    now,
				UpdatedAt:     now,
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

		logInbound("POST", "/api/v1/tasks/batch", fmt.Sprintf("[%d tasks created in lane %q]", created, lane))

		writeJSON(w, 201, map[string]interface{}{
			"data": map[string]interface{}{
				"lane":          lane,
				"created":       created,
				"scheduled_for": scheduledFor,
			},
			"message": fmt.Sprintf("Created %d task(s)", created),
		})
	}
}
