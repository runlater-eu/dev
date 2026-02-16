package main

import (
	"io"
	"net/http"
	"strings"
	"time"
)

// ExecutorConfig controls execution behavior.
type ExecutorConfig struct {
	SimulatedDelay time.Duration // delay before making the HTTP request
}

var defaultExecutorConfig = ExecutorConfig{
	SimulatedDelay: 1 * time.Second,
}

// executeTask makes an HTTP request to the task's URL and updates the execution record.
func executeTask(store *Store, task *Task, execID string, cfg ExecutorConfig) {
	time.Sleep(cfg.SimulatedDelay)

	now := nowISO()
	store.UpdateExecution(task.ID, execID, func(e *Execution) {
		e.Status = "running"
		e.StartedAt = &now
	})

	logOutbound(task.Method, task.URL)

	start := time.Now()
	statusCode, errMsg := doHTTPRequest(task)
	durationMs := int(time.Since(start).Milliseconds())

	finished := nowISO()
	status := "success"
	if errMsg != nil {
		status = "failed"
	} else if statusCode != nil && *statusCode >= 400 {
		status = "failed"
	}

	store.UpdateExecution(task.ID, execID, func(e *Execution) {
		e.Status = status
		e.FinishedAt = &finished
		e.StatusCode = statusCode
		e.DurationMs = &durationMs
		e.ErrorMessage = errMsg
	})

	if statusCode != nil {
		logResult(*statusCode, durationMs)
	} else {
		logError("request failed: " + *errMsg)
	}
}

func doHTTPRequest(task *Task) (statusCode *int, errMsg *string) {
	var bodyReader io.Reader
	if task.Body != nil {
		bodyReader = strings.NewReader(*task.Body)
	}

	req, err := http.NewRequest(task.Method, task.URL, bodyReader)
	if err != nil {
		msg := err.Error()
		return nil, &msg
	}

	for k, v := range task.Headers {
		req.Header.Set(k, v)
	}

	timeout := time.Duration(task.TimeoutMs) * time.Millisecond
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{Timeout: timeout}

	resp, err := client.Do(req)
	if err != nil {
		msg := err.Error()
		return nil, &msg
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return &resp.StatusCode, nil
}

// executeForward forwards an inbound webhook to the endpoint's forward URL.
func executeForward(method, forwardURL string, headers http.Header, body []byte, cfg ExecutorConfig) (int, error) {
	time.Sleep(cfg.SimulatedDelay)

	logOutbound(method, forwardURL)

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = strings.NewReader(string(body))
	}

	req, err := http.NewRequest(method, forwardURL, bodyReader)
	if err != nil {
		return 0, err
	}

	for k, v := range headers {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}

	client := &http.Client{Timeout: 30 * time.Second}
	start := time.Now()
	resp, err := client.Do(req)
	durationMs := int(time.Since(start).Milliseconds())
	if err != nil {
		logError("forward failed: " + err.Error())
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	logResult(resp.StatusCode, durationMs)
	return resp.StatusCode, nil
}
