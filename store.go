package main

import (
	"sync"
	"time"
)

// Task represents a scheduled or immediate task.
type Task struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	URL                 string            `json:"url"`
	Method              string            `json:"method"`
	Headers             map[string]string `json:"headers"`
	Body                *string           `json:"body"`
	ScheduleType        string            `json:"schedule_type"`
	CronExpression      *string           `json:"cron"`
	ScheduledAt         *string           `json:"run_at"`
	Enabled             bool              `json:"enabled"`
	TimeoutMs           int               `json:"timeout_ms"`
	RetryAttempts       int               `json:"retry_attempts"`
	Lane                *string           `json:"lane"`
	CallbackURL         *string           `json:"callback_url"`
	NotifyOnFailure     *bool             `json:"notify_on_failure"`
	NotifyOnRecovery    *bool             `json:"notify_on_recovery"`
	OnFailureURL        *string           `json:"on_failure_url"`
	OnRecoveryURL       *string           `json:"on_recovery_url"`
	ExpectedStatusCodes *string           `json:"expected_status_codes"`
	ExpectedBodyPattern *string           `json:"expected_body_pattern"`
	Script              *string           `json:"script"`
	NextRunAt           *string           `json:"next_run_at"`
	InsertedAt          string            `json:"inserted_at"`
	UpdatedAt           string            `json:"updated_at"`
}

// Execution represents a single run of a task.
type Execution struct {
	ID           string   `json:"id"`
	TaskID       string   `json:"-"`
	Status       string   `json:"status"`
	ScheduledFor string   `json:"scheduled_for"`
	StartedAt    *string  `json:"started_at"`
	FinishedAt   *string  `json:"finished_at"`
	StatusCode   *int     `json:"status_code"`
	DurationMs   *int     `json:"duration_ms"`
	ErrorMessage *string  `json:"error_message"`
	Attempt      int      `json:"attempt"`
	ScriptLogs   []string `json:"script_logs"`
	DebounceKey  *string  `json:"debounce_key"`
}

// Endpoint represents an inbound webhook endpoint.
type Endpoint struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Slug             string            `json:"slug"`
	InboundURL       string            `json:"inbound_url"`
	ForwardURLs      []string          `json:"forward_urls"`
	Enabled          bool              `json:"enabled"`
	RetryAttempts    int               `json:"retry_attempts"`
	UseLane          bool              `json:"use_lane"`
	Paused           bool              `json:"paused"`
	NotifyOnFailure  *bool             `json:"notify_on_failure"`
	NotifyOnRecovery *bool             `json:"notify_on_recovery"`
	OnFailureURL     *string           `json:"on_failure_url"`
	OnRecoveryURL    *string           `json:"on_recovery_url"`
	ForwardHeaders   map[string]string `json:"forward_headers"`
	ForwardBody      *string           `json:"forward_body"`
	ForwardMethod    *string           `json:"forward_method"`
	Script           *string           `json:"script"`
	Secrets          map[string]string `json:"-"`
	InsertedAt       string            `json:"inserted_at"`
	UpdatedAt        string            `json:"updated_at"`
}

// InboundEvent represents a received inbound webhook event.
type InboundEvent struct {
	ID         string   `json:"id"`
	EndpointID string   `json:"-"`
	Method     string   `json:"method"`
	SourceIP   string   `json:"source_ip"`
	ReceivedAt string   `json:"received_at"`
	TaskIDs    []string `json:"task_ids"`
	Status     *string  `json:"status"`
}

// Monitor represents a heartbeat/dead-man's-switch monitor.
type Monitor struct {
	ID                  string  `json:"id"`
	Name                string  `json:"name"`
	ScheduleType        string  `json:"schedule_type"`
	CronExpression      *string `json:"cron_expression"`
	IntervalSeconds     *int    `json:"interval_seconds"`
	GracePeriodSeconds  int     `json:"grace_period_seconds"`
	Enabled             bool    `json:"enabled"`
	NotifyOnFailure     *bool   `json:"notify_on_failure"`
	NotifyOnRecovery    *bool   `json:"notify_on_recovery"`
	OnFailureURL        *string `json:"on_failure_url"`
	OnRecoveryURL       *string `json:"on_recovery_url"`
	PingToken           string  `json:"ping_token"`
	PingURL             string  `json:"ping_url"`
	Status              string  `json:"status"`
	LastPingAt          *string `json:"last_ping_at"`
	NextExpectedAt      *string `json:"next_expected_at"`
	InsertedAt          string  `json:"inserted_at"`
	UpdatedAt           string  `json:"updated_at"`
}

// MonitorPing represents a single ping received by a monitor.
type MonitorPing struct {
	ID         string  `json:"id"`
	ReceivedAt string  `json:"received_at"`
	Status     string  `json:"status"`
	Message    *string `json:"message"`
}

// Store is a thread-safe in-memory store for all data.
type Store struct {
	mu           sync.RWMutex
	tasks        map[string]*Task
	taskOrder    []string
	executions   map[string][]*Execution // taskID -> executions
	endpoints    map[string]*Endpoint
	epOrder      []string
	epBySlugs    map[string]string // slug -> endpointID
	events       map[string][]*InboundEvent // endpointID -> events
	monitors     map[string]*Monitor
	monitorOrder []string
	monByTokens  map[string]string // pingToken -> monitorID
	pings        map[string][]*MonitorPing // monitorID -> pings
	pausedLanes  map[string]bool // lane name -> paused
}

func NewStore() *Store {
	return &Store{
		tasks:        make(map[string]*Task),
		executions:   make(map[string][]*Execution),
		endpoints:    make(map[string]*Endpoint),
		epBySlugs:    make(map[string]string),
		events:       make(map[string][]*InboundEvent),
		monitors:     make(map[string]*Monitor),
		monByTokens:  make(map[string]string),
		pings:        make(map[string][]*MonitorPing),
		pausedLanes:  make(map[string]bool),
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

func (s *Store) DeleteTasksByLane(lane string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	var remaining []string
	for _, id := range s.taskOrder {
		t := s.tasks[id]
		if t.Lane != nil && *t.Lane == lane {
			delete(s.tasks, id)
			delete(s.executions, id)
			deleted++
		} else {
			remaining = append(remaining, id)
		}
	}
	s.taskOrder = remaining
	return deleted
}

func (s *Store) UpdateTask(id string, fn func(*Task)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.tasks[id]
	if !ok {
		return false
	}
	fn(t)
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

// DeletePendingDebounced removes pending executions with the given debounce key
// across all tasks in the given lane. Returns the number deleted.
func (s *Store) DeletePendingDebounced(lane, debounceKey string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for taskID, execs := range s.executions {
		task := s.tasks[taskID]
		if task == nil || task.Lane == nil || *task.Lane != lane {
			continue
		}
		var kept []*Execution
		for _, ex := range execs {
			if ex.DebounceKey != nil && *ex.DebounceKey == debounceKey && ex.Status == "pending" {
				deleted++
			} else {
				kept = append(kept, ex)
			}
		}
		s.executions[taskID] = kept
	}
	return deleted
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

func (s *Store) UpdateEndpoint(id string, fn func(*Endpoint)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ep, ok := s.endpoints[id]
	if !ok {
		return false
	}
	fn(ep)
	return true
}

// Event operations

func (s *Store) AddEvent(endpointID string, event *InboundEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.EndpointID = endpointID
	s.events[endpointID] = append(s.events[endpointID], event)
}

func (s *Store) GetEvent(endpointID, eventID string) *InboundEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ev := range s.events[endpointID] {
		if ev.ID == eventID {
			return ev
		}
	}
	return nil
}

func (s *Store) ListEvents(endpointID string) []*InboundEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	evts := s.events[endpointID]
	result := make([]*InboundEvent, len(evts))
	copy(result, evts)
	return result
}

// Monitor operations

func (s *Store) CreateMonitor(m *Monitor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.monitors[m.ID] = m
	s.monitorOrder = append(s.monitorOrder, m.ID)
	s.monByTokens[m.PingToken] = m.ID
}

func (s *Store) GetMonitor(id string) *Monitor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.monitors[id]
}

func (s *Store) GetMonitorByToken(token string) *Monitor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.monByTokens[token]
	if !ok {
		return nil
	}
	return s.monitors[id]
}

func (s *Store) ListMonitors() []*Monitor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mons := make([]*Monitor, 0, len(s.monitorOrder))
	for _, id := range s.monitorOrder {
		if m, ok := s.monitors[id]; ok {
			mons = append(mons, m)
		}
	}
	return mons
}

func (s *Store) UpdateMonitor(id string, fn func(*Monitor)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.monitors[id]
	if !ok {
		return false
	}
	fn(m)
	return true
}

func (s *Store) DeleteMonitor(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.monitors[id]
	if !ok {
		return false
	}
	delete(s.monByTokens, m.PingToken)
	delete(s.monitors, id)
	delete(s.pings, id)
	for i, mid := range s.monitorOrder {
		if mid == id {
			s.monitorOrder = append(s.monitorOrder[:i], s.monitorOrder[i+1:]...)
			break
		}
	}
	return true
}

// Ping operations

func (s *Store) AddPing(monitorID string, ping *MonitorPing) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pings[monitorID] = append(s.pings[monitorID], ping)
}

func (s *Store) ListPings(monitorID string) []*MonitorPing {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ps := s.pings[monitorID]
	result := make([]*MonitorPing, len(ps))
	copy(result, ps)
	return result
}

// Lane operations

func (s *Store) PauseLane(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pausedLanes[name] = true
}

func (s *Store) ResumeLane(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pausedLanes, name)
}

func (s *Store) IsLanePaused(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pausedLanes[name]
}

func (s *Store) ListLanes() map[string]bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Collect all lane names from tasks
	lanes := make(map[string]bool)
	for _, t := range s.tasks {
		if t.Lane != nil && *t.Lane != "" {
			lanes[*t.Lane] = s.pausedLanes[*t.Lane]
		}
	}
	// Also include explicitly paused lanes
	for name := range s.pausedLanes {
		lanes[name] = true
	}
	return lanes
}
