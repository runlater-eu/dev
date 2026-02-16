package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var delayRegex = regexp.MustCompile(`^(\d+)(s|m|h|d)$`)

func registerTaskRoutes(mux *http.ServeMux, store *Store, execCfg ExecutorConfig) {
	mux.HandleFunc("POST /api/v1/tasks", handleCreateTask(store, execCfg))
	mux.HandleFunc("GET /api/v1/tasks", handleListTasks(store))
	mux.HandleFunc("GET /api/v1/tasks/{id}", handleGetTask(store))
	mux.HandleFunc("DELETE /api/v1/tasks/{id}", handleDeleteTask(store))
	mux.HandleFunc("POST /api/v1/tasks/{id}/trigger", handleTriggerTask(store, execCfg))
	mux.HandleFunc("GET /api/v1/tasks/{id}/executions", handleListExecutions(store))
}

type taskCreateRequest struct {
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Headers       map[string]string `json:"headers"`
	Body          *string           `json:"body"`
	TimeoutMs     *int              `json:"timeout_ms"`
	RetryAttempts *int              `json:"retry_attempts"`
	Queue         *string           `json:"queue"`
	CallbackURL   *string           `json:"callback_url"`
	Cron          *string           `json:"cron"`
	Delay         interface{}       `json:"delay"`
	RunAt         *string           `json:"run_at"`
	Name          *string           `json:"name"`
	Enabled       *bool             `json:"enabled"`
}

func handleCreateTask(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req taskCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}

		if req.URL == "" {
			writeError(w, 422, "validation_error", "url is required")
			return
		}

		// Type inference: cron > delay > run_at > immediate
		if req.Cron != nil {
			createCronTask(w, store, &req)
		} else if req.Delay != nil {
			createDelayedTask(w, store, &req, execCfg)
		} else if req.RunAt != nil {
			createScheduledTask(w, store, &req, execCfg)
		} else {
			createImmediateTask(w, store, &req, execCfg)
		}
	}
}

func createCronTask(w http.ResponseWriter, store *Store, req *taskCreateRequest) {
	now := nowISO()
	name := fmt.Sprintf("Cron: %s", req.URL)
	if req.Name != nil {
		name = *req.Name
	}
	method := "GET"
	if req.Method != "" {
		method = strings.ToUpper(req.Method)
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	timeoutMs := 30000
	if req.TimeoutMs != nil {
		timeoutMs = *req.TimeoutMs
	}
	retryAttempts := 3
	if req.RetryAttempts != nil {
		retryAttempts = *req.RetryAttempts
	}

	task := &Task{
		ID:             newTaskID(),
		Name:           name,
		URL:            req.URL,
		Method:         method,
		Headers:        req.Headers,
		Body:           req.Body,
		ScheduleType:   "cron",
		CronExpression: req.Cron,
		Enabled:        enabled,
		TimeoutMs:      timeoutMs,
		RetryAttempts:  retryAttempts,
		Queue:          req.Queue,
		CallbackURL:    req.CallbackURL,
		InsertedAt:     now,
		UpdatedAt:      now,
	}
	if task.Headers == nil {
		task.Headers = map[string]string{}
	}

	store.CreateTask(task)

	logInbound("POST", "/api/v1/tasks", fmt.Sprintf("[%s created, cron]", task.ID))

	writeJSON(w, 201, map[string]interface{}{
		"data": map[string]interface{}{
			"id":              task.ID,
			"name":            task.Name,
			"url":             task.URL,
			"method":          task.Method,
			"cron_expression": task.CronExpression,
			"enabled":         task.Enabled,
			"next_run_at":     task.NextRunAt,
			"inserted_at":     task.InsertedAt,
			"updated_at":      task.UpdatedAt,
		},
		"message": "Cron task created",
	})
}

func createDelayedTask(w http.ResponseWriter, store *Store, req *taskCreateRequest, execCfg ExecutorConfig) {
	delaySec, err := parseDelay(req.Delay)
	if err != nil {
		writeError(w, 400, "invalid_delay", err.Error())
		return
	}

	scheduledAt := time.Now().UTC().Add(time.Duration(delaySec) * time.Second)
	scheduledStr := scheduledAt.Format(time.RFC3339)

	task, exec := buildOnceTask(store, req, &scheduledStr)
	store.CreateTask(task)
	store.AddExecution(task.ID, exec)

	logInbound("POST", "/api/v1/tasks", fmt.Sprintf("[%s created, delayed %ds]", task.ID, delaySec))

	// In dev emulator, execute immediately regardless of delay
	go executeTask(store, task, exec.ID, execCfg)

	writeJSON(w, 202, map[string]interface{}{
		"data": map[string]interface{}{
			"task_id":       task.ID,
			"execution_id":  exec.ID,
			"status":        "pending",
			"scheduled_for": exec.ScheduledFor,
		},
		"message": "Task queued for execution",
	})
}

func createScheduledTask(w http.ResponseWriter, store *Store, req *taskCreateRequest, execCfg ExecutorConfig) {
	_, err := time.Parse(time.RFC3339, *req.RunAt)
	if err != nil {
		writeError(w, 400, "invalid_run_at", "run_at must be a valid ISO 8601 datetime")
		return
	}

	task, exec := buildOnceTask(store, req, req.RunAt)
	store.CreateTask(task)
	store.AddExecution(task.ID, exec)

	logInbound("POST", "/api/v1/tasks", fmt.Sprintf("[%s created, scheduled]", task.ID))

	go executeTask(store, task, exec.ID, execCfg)

	writeJSON(w, 202, map[string]interface{}{
		"data": map[string]interface{}{
			"task_id":       task.ID,
			"execution_id":  exec.ID,
			"status":        "pending",
			"scheduled_for": exec.ScheduledFor,
		},
		"message": "Task queued for execution",
	})
}

func createImmediateTask(w http.ResponseWriter, store *Store, req *taskCreateRequest, execCfg ExecutorConfig) {
	task, exec := buildOnceTask(store, req, nil)
	store.CreateTask(task)
	store.AddExecution(task.ID, exec)

	logInbound("POST", "/api/v1/tasks", fmt.Sprintf("[%s created, immediate]", task.ID))

	go executeTask(store, task, exec.ID, execCfg)

	writeJSON(w, 202, map[string]interface{}{
		"data": map[string]interface{}{
			"task_id":       task.ID,
			"execution_id":  exec.ID,
			"status":        "pending",
			"scheduled_for": exec.ScheduledFor,
		},
		"message": "Task queued for execution",
	})
}

func buildOnceTask(store *Store, req *taskCreateRequest, scheduledAt *string) (*Task, *Execution) {
	now := nowISO()
	method := "POST"
	if req.Method != "" {
		method = strings.ToUpper(req.Method)
	}
	name := fmt.Sprintf("Task: %s", req.URL)
	if req.Name != nil {
		name = *req.Name
	}
	timeoutMs := 30000
	if req.TimeoutMs != nil {
		timeoutMs = *req.TimeoutMs
	}
	retryAttempts := 5
	if req.RetryAttempts != nil {
		retryAttempts = *req.RetryAttempts
	}

	var scheduledAtStr *string
	if scheduledAt != nil {
		scheduledAtStr = scheduledAt
	}

	task := &Task{
		ID:            newTaskID(),
		Name:          name,
		URL:           req.URL,
		Method:        method,
		Headers:       req.Headers,
		Body:          req.Body,
		ScheduleType:  "once",
		ScheduledAt:   scheduledAtStr,
		Enabled:       true,
		TimeoutMs:     timeoutMs,
		RetryAttempts: retryAttempts,
		Queue:         req.Queue,
		CallbackURL:   req.CallbackURL,
		InsertedAt:    now,
		UpdatedAt:     now,
	}
	if task.Headers == nil {
		task.Headers = map[string]string{}
	}

	schedFor := now
	if scheduledAt != nil {
		schedFor = *scheduledAt
	}

	exec := &Execution{
		ID:           newExecID(),
		Status:       "pending",
		ScheduledFor: schedFor,
		Attempt:      1,
	}

	return task, exec
}

func handleListTasks(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tasks := store.ListTasks()

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

		total := len(tasks)
		if offset > total {
			offset = total
		}
		end := offset + limit
		if end > total {
			end = total
		}
		page := tasks[offset:end]

		data := make([]map[string]interface{}, len(page))
		for i, t := range page {
			data[i] = taskToJSON(t)
		}

		logInbound("GET", "/api/v1/tasks", fmt.Sprintf("[%d tasks]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data":     data,
			"has_more": end < total,
			"limit":    limit,
			"offset":   offset,
		})
	}
}

func handleGetTask(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		task := store.GetTask(id)
		if task == nil {
			writeError(w, 404, "not_found", "Task not found")
			return
		}

		logInbound("GET", "/api/v1/tasks/"+id, "")

		writeJSON(w, 200, map[string]interface{}{
			"data": taskToJSON(task),
		})
	}
}

func handleDeleteTask(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if !store.DeleteTask(id) {
			writeError(w, 404, "not_found", "Task not found")
			return
		}

		logInbound("DELETE", "/api/v1/tasks/"+id, "[deleted]")
		w.WriteHeader(204)
	}
}

func handleTriggerTask(store *Store, execCfg ExecutorConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		task := store.GetTask(id)
		if task == nil {
			writeError(w, 404, "not_found", "Task not found")
			return
		}

		now := nowISO()
		exec := &Execution{
			ID:           newExecID(),
			Status:       "pending",
			ScheduledFor: now,
			Attempt:      1,
		}
		store.AddExecution(task.ID, exec)

		logInbound("POST", "/api/v1/tasks/"+id+"/trigger", fmt.Sprintf("[%s triggered]", exec.ID))

		go executeTask(store, task, exec.ID, execCfg)

		writeJSON(w, 202, map[string]interface{}{
			"data": map[string]interface{}{
				"execution_id":  exec.ID,
				"status":        "pending",
				"scheduled_for": exec.ScheduledFor,
			},
			"message": "Task triggered successfully",
		})
	}
}

func handleListExecutions(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		task := store.GetTask(id)
		if task == nil {
			writeError(w, 404, "not_found", "Task not found")
			return
		}

		execs := store.ListExecutions(id)
		data := make([]map[string]interface{}, len(execs))
		for i, e := range execs {
			data[i] = executionToJSON(e)
		}

		logInbound("GET", "/api/v1/tasks/"+id+"/executions", fmt.Sprintf("[%d executions]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data":     data,
			"has_more": false,
			"limit":    50,
			"offset":   0,
		})
	}
}

func taskToJSON(t *Task) map[string]interface{} {
	return map[string]interface{}{
		"id":                t.ID,
		"name":              t.Name,
		"url":               t.URL,
		"method":            t.Method,
		"headers":           t.Headers,
		"body":              t.Body,
		"schedule_type":     t.ScheduleType,
		"cron_expression":   t.CronExpression,
		"scheduled_at":      t.ScheduledAt,
		"enabled":           t.Enabled,
		"timeout_ms":        t.TimeoutMs,
		"retry_attempts":    t.RetryAttempts,
		"queue":             t.Queue,
		"notify_on_failure": t.NotifyOnFailure,
		"notify_on_recovery": t.NotifyOnRecovery,
		"next_run_at":       t.NextRunAt,
		"inserted_at":       t.InsertedAt,
		"updated_at":        t.UpdatedAt,
	}
}

func executionToJSON(e *Execution) map[string]interface{} {
	return map[string]interface{}{
		"id":            e.ID,
		"status":        e.Status,
		"scheduled_for": e.ScheduledFor,
		"started_at":    e.StartedAt,
		"finished_at":   e.FinishedAt,
		"status_code":   e.StatusCode,
		"duration_ms":   e.DurationMs,
		"error_message": e.ErrorMessage,
		"attempt":       e.Attempt,
	}
}

func parseDelay(delay interface{}) (int, error) {
	switch v := delay.(type) {
	case float64:
		if v <= 0 {
			return 0, fmt.Errorf("delay must be a positive number")
		}
		return int(v), nil
	case string:
		matches := delayRegex.FindStringSubmatch(v)
		if matches == nil {
			return 0, fmt.Errorf("invalid delay format: use '30s', '5m', '2h', or '1d'")
		}
		num, _ := strconv.Atoi(matches[1])
		switch matches[2] {
		case "s":
			return num, nil
		case "m":
			return num * 60, nil
		case "h":
			return num * 3600, nil
		case "d":
			return num * 86400, nil
		}
	}
	return 0, fmt.Errorf("delay must be a string (e.g., '30s') or a positive integer (seconds)")
}

// JSON helpers

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	})
}
