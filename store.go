package main

import (
	"sync"
	"time"
)

// Task represents a scheduled or immediate task.
type Task struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	URL                string            `json:"url"`
	Method             string            `json:"method"`
	Headers            map[string]string `json:"headers"`
	Body               *string           `json:"body"`
	ScheduleType       string            `json:"schedule_type"`
	CronExpression     *string           `json:"cron_expression"`
	ScheduledAt        *string           `json:"scheduled_at"`
	Enabled            bool              `json:"enabled"`
	TimeoutMs          int               `json:"timeout_ms"`
	RetryAttempts      int               `json:"retry_attempts"`
	Queue              *string           `json:"queue"`
	CallbackURL        *string           `json:"callback_url"`
	NotifyOnFailure    *bool             `json:"notify_on_failure"`
	NotifyOnRecovery   *bool             `json:"notify_on_recovery"`
	NextRunAt          *string           `json:"next_run_at"`
	InsertedAt         string            `json:"inserted_at"`
	UpdatedAt          string            `json:"updated_at"`
}

// Execution represents a single run of a task.
type Execution struct {
	ID           string  `json:"id"`
	TaskID       string  `json:"-"`
	Status       string  `json:"status"`
	ScheduledFor string  `json:"scheduled_for"`
	StartedAt    *string `json:"started_at"`
	FinishedAt   *string `json:"finished_at"`
	StatusCode   *int    `json:"status_code"`
	DurationMs   *int    `json:"duration_ms"`
	ErrorMessage *string `json:"error_message"`
	Attempt      int     `json:"attempt"`
}

// Endpoint represents an inbound webhook endpoint.
type Endpoint struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Slug            string `json:"slug"`
	InboundURL      string `json:"inbound_url"`
	ForwardURL      string `json:"forward_url"`
	Enabled         bool   `json:"enabled"`
	RetryAttempts   int    `json:"retry_attempts"`
	UseQueue        bool   `json:"use_queue"`
	NotifyOnFailure *bool  `json:"notify_on_failure"`
	NotifyOnRecovery *bool `json:"notify_on_recovery"`
	InsertedAt      string `json:"inserted_at"`
	UpdatedAt       string `json:"updated_at"`
}

// InboundEvent represents a received inbound webhook event.
type InboundEvent struct {
	ID              string  `json:"id"`
	EndpointID      string  `json:"-"`
	Method          string  `json:"method"`
	SourceIP        string  `json:"source_ip"`
	ReceivedAt      string  `json:"received_at"`
	ExecutionID     *string `json:"execution_id"`
	ExecutionStatus *string `json:"execution_status"`
}

// Store is a thread-safe in-memory store for all data.
type Store struct {
	mu         sync.RWMutex
	tasks      map[string]*Task
	taskOrder  []string
	executions map[string][]*Execution // taskID -> executions
	endpoints  map[string]*Endpoint
	epOrder    []string
	epBySlugs  map[string]string // slug -> endpointID
	events     map[string][]*InboundEvent // endpointID -> events
}

func NewStore() *Store {
	return &Store{
		tasks:      make(map[string]*Task),
		executions: make(map[string][]*Execution),
		endpoints:  make(map[string]*Endpoint),
		epBySlugs:  make(map[string]string),
		events:     make(map[string][]*InboundEvent),
	}
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// Task operations

func (s *Store) CreateTask(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.ID] = task
	s.taskOrder = append(s.taskOrder, task.ID)
}

func (s *Store) GetTask(id string) *Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[id]
}

func (s *Store) ListTasks() []*Task {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := make([]*Task, 0, len(s.taskOrder))
	for _, id := range s.taskOrder {
		if t, ok := s.tasks[id]; ok {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (s *Store) DeleteTask(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tasks[id]; !ok {
		return false
	}
	delete(s.tasks, id)
	delete(s.executions, id)
	for i, tid := range s.taskOrder {
		if tid == id {
			s.taskOrder = append(s.taskOrder[:i], s.taskOrder[i+1:]...)
			break
		}
	}
	return true
}

// Execution operations

func (s *Store) AddExecution(taskID string, exec *Execution) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exec.TaskID = taskID
	s.executions[taskID] = append(s.executions[taskID], exec)
}

func (s *Store) ListExecutions(taskID string) []*Execution {
	s.mu.RLock()
	defer s.mu.RUnlock()
	execs := s.executions[taskID]
	result := make([]*Execution, len(execs))
	copy(result, execs)
	return result
}

func (s *Store) UpdateExecution(taskID, execID string, fn func(*Execution)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ex := range s.executions[taskID] {
		if ex.ID == execID {
			fn(ex)
			return
		}
	}
}

// Endpoint operations

func (s *Store) CreateEndpoint(ep *Endpoint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints[ep.ID] = ep
	s.epOrder = append(s.epOrder, ep.ID)
	s.epBySlugs[ep.Slug] = ep.ID
}

func (s *Store) GetEndpoint(id string) *Endpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.endpoints[id]
}

func (s *Store) GetEndpointBySlug(slug string) *Endpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.epBySlugs[slug]
	if !ok {
		return nil
	}
	return s.endpoints[id]
}

func (s *Store) ListEndpoints() []*Endpoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	eps := make([]*Endpoint, 0, len(s.epOrder))
	for _, id := range s.epOrder {
		if ep, ok := s.endpoints[id]; ok {
			eps = append(eps, ep)
		}
	}
	return eps
}

func (s *Store) DeleteEndpoint(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.endpoints[id]
	if !ok {
		return false
	}
	delete(s.epBySlugs, ep.Slug)
	delete(s.endpoints, id)
	delete(s.events, id)
	for i, eid := range s.epOrder {
		if eid == id {
			s.epOrder = append(s.epOrder[:i], s.epOrder[i+1:]...)
			break
		}
	}
	return true
}

// Event operations

func (s *Store) AddEvent(endpointID string, event *InboundEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.EndpointID = endpointID
	s.events[endpointID] = append(s.events[endpointID], event)
}

func (s *Store) ListEvents(endpointID string) []*InboundEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	evts := s.events[endpointID]
	result := make([]*InboundEvent, len(evts))
	copy(result, evts)
	return result
}
