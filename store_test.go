package main

import (
	"sync"
	"testing"
)

func TestStoreTaskCRUD(t *testing.T) {
	s := NewStore()

	task := &Task{
		ID: "t_test1", Name: "Test", URL: "http://example.com",
		Method: "GET", ScheduleType: "once", Enabled: true,
		TimeoutMs: 30000, InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	s.CreateTask(task)

	got := s.GetTask("t_test1")
	if got == nil {
		t.Fatal("expected task, got nil")
	}
	if got.Name != "Test" {
		t.Errorf("expected name Test, got %s", got.Name)
	}

	tasks := s.ListTasks()
	if len(tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(tasks))
	}

	if !s.DeleteTask("t_test1") {
		t.Error("expected delete to return true")
	}
	if s.GetTask("t_test1") != nil {
		t.Error("expected task to be deleted")
	}
	if s.DeleteTask("t_test1") {
		t.Error("expected delete of nonexistent to return false")
	}
}

func TestStoreExecutions(t *testing.T) {
	s := NewStore()

	task := &Task{ID: "t_test1", URL: "http://example.com", InsertedAt: nowISO(), UpdatedAt: nowISO()}
	s.CreateTask(task)

	exec := &Execution{ID: "ex_test1", Status: "pending", ScheduledFor: nowISO(), Attempt: 1}
	s.AddExecution("t_test1", exec)

	execs := s.ListExecutions("t_test1")
	if len(execs) != 1 {
		t.Fatalf("expected 1 execution, got %d", len(execs))
	}
	if execs[0].Status != "pending" {
		t.Errorf("expected status pending, got %s", execs[0].Status)
	}

	s.UpdateExecution("t_test1", "ex_test1", func(e *Execution) {
		e.Status = "success"
	})
	execs = s.ListExecutions("t_test1")
	if execs[0].Status != "success" {
		t.Errorf("expected status success after update, got %s", execs[0].Status)
	}
}

func TestStoreEndpointCRUD(t *testing.T) {
	s := NewStore()

	ep := &Endpoint{
		ID: "ep_test1", Name: "Stripe", Slug: "stripe-abc",
		ForwardURL: "http://localhost:3000/webhook", Enabled: true,
		InsertedAt: nowISO(), UpdatedAt: nowISO(),
	}
	s.CreateEndpoint(ep)

	got := s.GetEndpoint("ep_test1")
	if got == nil {
		t.Fatal("expected endpoint, got nil")
	}

	bySlug := s.GetEndpointBySlug("stripe-abc")
	if bySlug == nil {
		t.Fatal("expected endpoint by slug, got nil")
	}
	if bySlug.ID != "ep_test1" {
		t.Errorf("expected ep_test1, got %s", bySlug.ID)
	}

	eps := s.ListEndpoints()
	if len(eps) != 1 {
		t.Errorf("expected 1 endpoint, got %d", len(eps))
	}

	if !s.DeleteEndpoint("ep_test1") {
		t.Error("expected delete to return true")
	}
	if s.GetEndpointBySlug("stripe-abc") != nil {
		t.Error("expected slug lookup to fail after delete")
	}
}

func TestStoreEvents(t *testing.T) {
	s := NewStore()

	ep := &Endpoint{ID: "ep_test1", Slug: "test", InsertedAt: nowISO(), UpdatedAt: nowISO()}
	s.CreateEndpoint(ep)

	event := &InboundEvent{ID: "ev_test1", Method: "POST", SourceIP: "127.0.0.1", ReceivedAt: nowISO()}
	s.AddEvent("ep_test1", event)

	events := s.ListEvents("ep_test1")
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Method != "POST" {
		t.Errorf("expected method POST, got %s", events[0].Method)
	}
}

func TestStoreConcurrentAccess(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := newTaskID()
			s.CreateTask(&Task{
				ID: id, URL: "http://example.com",
				InsertedAt: nowISO(), UpdatedAt: nowISO(),
			})
			s.GetTask(id)
			s.ListTasks()
		}()
	}
	wg.Wait()

	tasks := s.ListTasks()
	if len(tasks) != 100 {
		t.Errorf("expected 100 tasks, got %d", len(tasks))
	}
}

func TestStoreDeleteTaskCleansExecutions(t *testing.T) {
	s := NewStore()

	task := &Task{ID: "t_test1", URL: "http://example.com", InsertedAt: nowISO(), UpdatedAt: nowISO()}
	s.CreateTask(task)
	s.AddExecution("t_test1", &Execution{ID: "ex_1", Status: "success", ScheduledFor: nowISO(), Attempt: 1})

	s.DeleteTask("t_test1")

	execs := s.ListExecutions("t_test1")
	if len(execs) != 0 {
		t.Errorf("expected 0 executions after task delete, got %d", len(execs))
	}
}
