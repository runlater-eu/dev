package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MQ types

// MqQueue represents a message queue.
type MqQueue struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	Slug                    string `json:"slug"`
	VisibilityTimeoutSeconds int   `json:"visibility_timeout_seconds"`
	MaxReceives             int    `json:"max_receives"`
	Paused                  bool   `json:"paused"`
	InsertedAt              string `json:"inserted_at"`
	UpdatedAt               string `json:"updated_at"`
}

// MqMessage represents a message in a queue.
type MqMessage struct {
	ID             string      `json:"id"`
	QueueID        string      `json:"-"`
	Body           interface{} `json:"body"`
	Status         string      `json:"status"`
	RunAt          *string     `json:"run_at"`
	ReceiptHandle  *string     `json:"receipt_handle"`
	ReceiveCount   int         `json:"receive_count"`
	LockedUntil    *string     `json:"locked_until"`
	FirstReceivedAt *string    `json:"first_received_at"`
	CompletedAt    *string     `json:"completed_at"`
	InsertedAt     string      `json:"inserted_at"`
}

// MQ Store methods

type MqStore struct {
	mu         sync.RWMutex
	queues     map[string]*MqQueue
	queueOrder []string
	queueSlugs map[string]string // slug -> queueID
	messages   map[string][]*MqMessage // queueID -> messages
}

func NewMqStore() *MqStore {
	return &MqStore{
		queues:     make(map[string]*MqQueue),
		queueSlugs: make(map[string]string),
		messages:   make(map[string][]*MqMessage),
	}
}

func newQueueID() string   { return fmt.Sprintf("mq_%s", randomHex(6)) }
func newMessageID() string { return fmt.Sprintf("msg_%s", randomHex(6)) }

func newReceiptHandle() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func (s *MqStore) CreateQueue(q *MqQueue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[q.ID] = q
	s.queueOrder = append(s.queueOrder, q.ID)
	s.queueSlugs[q.Slug] = q.ID
}

func (s *MqStore) GetQueueBySlug(slug string) *MqQueue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.queueSlugs[slug]
	if !ok {
		return nil
	}
	return s.queues[id]
}

func (s *MqStore) ListQueues() []*MqQueue {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*MqQueue, 0, len(s.queueOrder))
	for _, id := range s.queueOrder {
		if q, ok := s.queues[id]; ok {
			result = append(result, q)
		}
	}
	return result
}

func (s *MqStore) UpdateQueue(id string, fn func(*MqQueue)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[id]
	if !ok {
		return false
	}
	fn(q)
	return true
}

func (s *MqStore) DeleteQueue(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, ok := s.queues[id]
	if !ok {
		return false
	}
	delete(s.queueSlugs, q.Slug)
	delete(s.queues, id)
	delete(s.messages, id)
	for i, qid := range s.queueOrder {
		if qid == id {
			s.queueOrder = append(s.queueOrder[:i], s.queueOrder[i+1:]...)
			break
		}
	}
	return true
}

func (s *MqStore) AddMessage(queueID string, msg *MqMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.QueueID = queueID
	s.messages[queueID] = append(s.messages[queueID], msg)
}

func (s *MqStore) GetMessage(queueID, msgID string) *MqMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.messages[queueID] {
		if m.ID == msgID {
			return m
		}
	}
	return nil
}

func (s *MqStore) ListMessages(queueID string, status *string, limit, offset int) []*MqMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var filtered []*MqMessage
	for _, m := range s.messages[queueID] {
		if status != nil && m.Status != *status {
			continue
		}
		filtered = append(filtered, m)
	}
	// Reverse order (newest first for peek)
	for i, j := 0, len(filtered)-1; i < j; i, j = i+1, j-1 {
		filtered[i], filtered[j] = filtered[j], filtered[i]
	}
	if offset >= len(filtered) {
		return nil
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[offset:end]
}

func (s *MqStore) ClaimMessages(queueID string, max int, visibilityTimeout int) []*MqMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	var claimed []*MqMessage

	for _, m := range s.messages[queueID] {
		if len(claimed) >= max {
			break
		}

		available := false
		if m.Status == "available" {
			if m.RunAt == nil {
				available = true
			} else if runAt, err := time.Parse(time.RFC3339, *m.RunAt); err == nil {
				available = !runAt.After(now)
			}
		} else if m.Status == "locked" && m.LockedUntil != nil {
			if lu, err := time.Parse(time.RFC3339, *m.LockedUntil); err == nil {
				available = !lu.After(now)
			}
		}

		if !available {
			continue
		}

		lockedUntil := now.Add(time.Duration(visibilityTimeout) * time.Second).Format(time.RFC3339)
		receipt := newReceiptHandle()
		m.Status = "locked"
		m.LockedUntil = &lockedUntil
		m.ReceiptHandle = &receipt
		m.ReceiveCount++
		if m.FirstReceivedAt == nil {
			nowStr := now.Format(time.RFC3339)
			m.FirstReceivedAt = &nowStr
		}
		claimed = append(claimed, m)
	}
	return claimed
}

func (s *MqStore) AckMessage(queueID, receiptHandle string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages[queueID] {
		if m.ReceiptHandle != nil && *m.ReceiptHandle == receiptHandle && m.Status == "locked" {
			now := nowISO()
			m.Status = "completed"
			m.CompletedAt = &now
			m.LockedUntil = nil
			return true
		}
	}
	return false
}

func (s *MqStore) NackMessage(queueID, receiptHandle string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages[queueID] {
		if m.ReceiptHandle != nil && *m.ReceiptHandle == receiptHandle && m.Status == "locked" {
			m.Status = "available"
			m.LockedUntil = nil
			m.ReceiptHandle = nil
			return true
		}
	}
	return false
}

func (s *MqStore) DeleteMessage(queueID, msgID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := s.messages[queueID]
	for i, m := range msgs {
		if m.ID == msgID {
			s.messages[queueID] = append(msgs[:i], msgs[i+1:]...)
			return true
		}
	}
	return false
}

func (s *MqStore) GetStats(queueID string) map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[string]int{"available": 0, "locked": 0, "completed": 0, "dead": 0}
	total := 0
	for _, m := range s.messages[queueID] {
		counts[m.Status]++
		total++
	}
	return map[string]interface{}{
		"available":                   counts["available"],
		"locked":                      counts["locked"],
		"completed":                   counts["completed"],
		"dead":                        counts["dead"],
		"total":                       total,
		"avg_time_in_queue_seconds":   nil,
		"avg_processing_time_seconds": nil,
	}
}

func (s *MqStore) PurgeCompleted(queueID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var kept []*MqMessage
	purged := 0
	for _, m := range s.messages[queueID] {
		if m.Status == "completed" {
			purged++
		} else {
			kept = append(kept, m)
		}
	}
	s.messages[queueID] = kept
	return purged
}

func (s *MqStore) MoveToDeadLetter(queueID, msgID string) (*MqMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages[queueID] {
		if m.ID == msgID {
			if m.Status == "completed" || m.Status == "dead" {
				return nil, false
			}
			m.Status = "dead"
			m.LockedUntil = nil
			return m, true
		}
	}
	return nil, false
}

func (s *MqStore) RetryDeadMessage(queueID, msgID string) (*MqMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages[queueID] {
		if m.ID == msgID {
			if m.Status != "dead" {
				return nil, false
			}
			m.Status = "available"
			m.ReceiveCount = 0
			m.LockedUntil = nil
			m.ReceiptHandle = nil
			m.CompletedAt = nil
			return m, true
		}
	}
	return nil, false
}

// HTTP handlers

func registerMqRoutes(mux *http.ServeMux, mqStore *MqStore) {
	mux.HandleFunc("GET /api/v1/mq", handleListQueues(mqStore))
	mux.HandleFunc("POST /api/v1/mq", handleCreateQueue(mqStore))
	mux.HandleFunc("GET /api/v1/mq/{slug}", handleGetQueue(mqStore))
	mux.HandleFunc("PATCH /api/v1/mq/{slug}", handleUpdateQueue(mqStore))
	mux.HandleFunc("DELETE /api/v1/mq/{slug}", handleDeleteQueue(mqStore))
	mux.HandleFunc("GET /api/v1/mq/{slug}/stats", handleQueueStats(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/pause", handlePauseQueue(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/resume", handleResumeQueue(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/messages", handleEnqueue(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/messages/batch", handleBatchEnqueue(mqStore))
	mux.HandleFunc("GET /api/v1/mq/{slug}/receive", handleReceive(mqStore))
	mux.HandleFunc("DELETE /api/v1/mq/{slug}/ack/{receipt}", handleAck(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/nack/{receipt}", handleNack(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/ack/batch", handleBatchAck(mqStore))
	mux.HandleFunc("GET /api/v1/mq/{slug}/messages", handleListMessages(mqStore))
	mux.HandleFunc("GET /api/v1/mq/{slug}/messages/{id}", handleGetMessage(mqStore))
	mux.HandleFunc("DELETE /api/v1/mq/{slug}/messages/{id}", handleDeleteMessage(mqStore))
	mux.HandleFunc("DELETE /api/v1/mq/{slug}/messages", handlePurgeCompleted(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/messages/{id}/dead-letter", handleMoveToDLQ(mqStore))
	mux.HandleFunc("POST /api/v1/mq/{slug}/messages/{id}/retry", handleRetryMessage(mqStore))
}

func handleListQueues(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queues := s.ListQueues()
		writeJSON(w, 200, map[string]interface{}{"data": queues})
	}
}

type mqCreateRequest struct {
	Name                    string `json:"name"`
	VisibilityTimeoutSeconds *int  `json:"visibility_timeout_seconds"`
	MaxReceives             *int   `json:"max_receives"`
}

func handleCreateQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req mqCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}
		if req.Name == "" {
			writeError(w, 422, "validation_error", "name is required")
			return
		}

		slug := strings.ToLower(strings.ReplaceAll(req.Name, " ", "-"))
		if s.GetQueueBySlug(slug) != nil {
			writeError(w, 422, "validation_error", "a queue with this name already exists")
			return
		}

		vt := 30
		if req.VisibilityTimeoutSeconds != nil {
			vt = *req.VisibilityTimeoutSeconds
		}
		mr := 5
		if req.MaxReceives != nil {
			mr = *req.MaxReceives
		}
		now := nowISO()
		q := &MqQueue{
			ID:                       newQueueID(),
			Name:                     req.Name,
			Slug:                     slug,
			VisibilityTimeoutSeconds: vt,
			MaxReceives:              mr,
			InsertedAt:               now,
			UpdatedAt:                now,
		}
		s.CreateQueue(q)
		logInbound("POST", "/api/v1/mq", fmt.Sprintf("[%s created]", q.Slug))
		writeJSON(w, 201, map[string]interface{}{"data": q})
	}
}

func handleGetQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": q})
	}
}

func handleUpdateQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)

		s.UpdateQueue(q.ID, func(q *MqQueue) {
			if name, ok := body["name"].(string); ok {
				q.Name = name
			}
			if vt, ok := body["visibility_timeout_seconds"].(float64); ok {
				q.VisibilityTimeoutSeconds = int(vt)
			}
			if mr, ok := body["max_receives"].(float64); ok {
				q.MaxReceives = int(mr)
			}
			q.UpdatedAt = nowISO()
		})
		writeJSON(w, 200, map[string]interface{}{"data": q})
	}
}

func handleDeleteQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		s.DeleteQueue(q.ID)
		w.WriteHeader(204)
	}
}

func handleQueueStats(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": s.GetStats(q.ID)})
	}
}

func handlePauseQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		s.UpdateQueue(q.ID, func(q *MqQueue) {
			q.Paused = true
			q.UpdatedAt = nowISO()
		})
		writeJSON(w, 200, map[string]interface{}{"data": q})
	}
}

func handleResumeQueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		s.UpdateQueue(q.ID, func(q *MqQueue) {
			q.Paused = false
			q.UpdatedAt = nowISO()
		})
		writeJSON(w, 200, map[string]interface{}{"data": q})
	}
}

type mqEnqueueRequest struct {
	Body  interface{} `json:"body"`
	Delay interface{} `json:"delay"`
	RunAt *string     `json:"run_at"`
}

const maxDelaySeconds = 7 * 86400

func handleEnqueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}

		var req mqEnqueueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}
		if req.Body == nil {
			writeError(w, 422, "validation_error", "body is required")
			return
		}

		runAt, err := resolveRunAt(req.Delay, req.RunAt)
		if err != nil {
			writeError(w, 422, "validation_error", err.Error())
			return
		}

		msg := &MqMessage{
			ID:           newMessageID(),
			Body:         req.Body,
			Status:       "available",
			RunAt:        runAt,
			ReceiveCount: 0,
			InsertedAt:   nowISO(),
		}
		s.AddMessage(q.ID, msg)
		logInbound("POST", fmt.Sprintf("/api/v1/mq/%s/messages", slug), fmt.Sprintf("[%s enqueued]", msg.ID))
		writeJSON(w, 201, map[string]interface{}{"data": msg})
	}
}

func handleBatchEnqueue(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}

		var body struct {
			Messages []mqEnqueueRequest `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeError(w, 400, "invalid_json", "Invalid JSON body")
			return
		}
		if len(body.Messages) > 10 {
			writeError(w, 422, "batch_too_large", "Maximum 10 messages per batch")
			return
		}

		now := nowISO()
		for _, m := range body.Messages {
			runAt, err := resolveRunAt(m.Delay, m.RunAt)
			if err != nil {
				writeError(w, 422, "validation_error", err.Error())
				return
			}
			msg := &MqMessage{
				ID:           newMessageID(),
				Body:         m.Body,
				Status:       "available",
				RunAt:        runAt,
				ReceiveCount: 0,
				InsertedAt:   now,
			}
			s.AddMessage(q.ID, msg)
		}
		logInbound("POST", fmt.Sprintf("/api/v1/mq/%s/messages/batch", slug), fmt.Sprintf("[%d enqueued]", len(body.Messages)))
		writeJSON(w, 201, map[string]interface{}{"data": map[string]int{"enqueued": len(body.Messages)}})
	}
}

func handleReceive(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		if q.Paused {
			writeJSON(w, 200, map[string]interface{}{"data": []interface{}{}})
			return
		}

		max := 1
		if v := r.URL.Query().Get("max"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 10 {
				max = n
			}
		}

		messages := s.ClaimMessages(q.ID, max, q.VisibilityTimeoutSeconds)
		if messages == nil {
			messages = []*MqMessage{}
		}
		writeJSON(w, 200, map[string]interface{}{"data": messages})
	}
}

func handleAck(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		receipt := r.PathValue("receipt")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		if !s.AckMessage(q.ID, receipt) {
			writeError(w, 404, "not_found", "Message not found or not locked")
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": map[string]bool{"acknowledged": true}})
	}
}

func handleNack(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		receipt := r.PathValue("receipt")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		if !s.NackMessage(q.ID, receipt) {
			writeError(w, 404, "not_found", "Message not found or not locked")
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": map[string]bool{"nacked": true}})
	}
}

func handleBatchAck(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		var body struct {
			ReceiptHandles []string `json:"receipt_handles"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		successful := 0
		var failed []string
		for _, handle := range body.ReceiptHandles {
			if s.AckMessage(q.ID, handle) {
				successful++
			} else {
				failed = append(failed, handle)
			}
		}
		writeJSON(w, 200, map[string]interface{}{
			"data": map[string]interface{}{"acknowledged": successful, "failed": failed},
		})
	}
}

func handleListMessages(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}

		limit := 20
		offset := 0
		var status *string
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= 100 {
				limit = n
			}
		}
		if v := r.URL.Query().Get("offset"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				offset = n
			}
		}
		if v := r.URL.Query().Get("status"); v != "" {
			status = &v
		}
		messages := s.ListMessages(q.ID, status, limit, offset)
		if messages == nil {
			messages = []*MqMessage{}
		}
		writeJSON(w, 200, map[string]interface{}{"data": messages})
	}
}

func handleGetMessage(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		msgID := r.PathValue("id")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		msg := s.GetMessage(q.ID, msgID)
		if msg == nil {
			writeError(w, 404, "not_found", "Message not found")
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": msg})
	}
}

func handleDeleteMessage(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		msgID := r.PathValue("id")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		if !s.DeleteMessage(q.ID, msgID) {
			writeError(w, 404, "not_found", "Message not found")
			return
		}
		w.WriteHeader(204)
	}
}

func handlePurgeCompleted(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		count := s.PurgeCompleted(q.ID)
		writeJSON(w, 200, map[string]interface{}{"data": map[string]int{"purged": count}})
	}
}

func handleMoveToDLQ(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		msgID := r.PathValue("id")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		msg, ok := s.MoveToDeadLetter(q.ID, msgID)
		if msg == nil && !ok {
			// Could be not found or invalid status
			m := s.GetMessage(q.ID, msgID)
			if m == nil {
				writeError(w, 404, "not_found", "Message not found")
			} else {
				writeError(w, 422, "invalid_status", "Message cannot be moved to dead letter from its current status")
			}
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": msg})
	}
}

func handleRetryMessage(s *MqStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slug := r.PathValue("slug")
		msgID := r.PathValue("id")
		q := s.GetQueueBySlug(slug)
		if q == nil {
			writeError(w, 404, "not_found", "Queue not found")
			return
		}
		msg, ok := s.RetryDeadMessage(q.ID, msgID)
		if msg == nil && !ok {
			m := s.GetMessage(q.ID, msgID)
			if m == nil {
				writeError(w, 404, "not_found", "Message not found")
			} else {
				writeError(w, 422, "not_dead", "Message is not in dead letter state")
			}
			return
		}
		writeJSON(w, 200, map[string]interface{}{"data": msg})
	}
}

// resolveRunAt converts delay/run_at params into a *string (RFC3339) or nil.
func resolveRunAt(delay interface{}, runAt *string) (*string, error) {
	hasDelay := delay != nil
	hasRunAt := runAt != nil

	if hasDelay && hasRunAt {
		return nil, fmt.Errorf("delay and run_at are mutually exclusive")
	}

	if hasDelay {
		seconds, err := parseDelay(delay)
		if err != nil {
			return nil, err
		}
		if seconds > maxDelaySeconds {
			return nil, fmt.Errorf("delay cannot exceed 7 days")
		}
		t := time.Now().UTC().Add(time.Duration(seconds) * time.Second).Format(time.RFC3339)
		return &t, nil
	}

	if hasRunAt {
		t, err := time.Parse(time.RFC3339, *runAt)
		if err != nil {
			return nil, fmt.Errorf("invalid run_at format, expected ISO 8601 datetime")
		}
		now := time.Now().UTC()
		if !t.After(now) {
			return nil, fmt.Errorf("run_at must be in the future")
		}
		if t.Sub(now).Seconds() > float64(maxDelaySeconds) {
			return nil, fmt.Errorf("run_at cannot be more than 7 days in the future")
		}
		return runAt, nil
	}

	return nil, nil
}
