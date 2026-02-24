package main

import (
	"fmt"
	"net/http"
)

func registerQueueRoutes(mux *http.ServeMux, store *Store) {
	mux.HandleFunc("GET /api/v1/queues", handleListQueues(store))
	mux.HandleFunc("POST /api/v1/queues/{name}/pause", handlePauseQueue(store))
	mux.HandleFunc("POST /api/v1/queues/{name}/resume", handleResumeQueue(store))
}

func handleListQueues(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		queues := store.ListQueues()

		data := make([]map[string]interface{}, 0, len(queues))
		for name, paused := range queues {
			data = append(data, map[string]interface{}{
				"name":   name,
				"paused": paused,
			})
		}

		logInbound("GET", "/api/v1/queues", fmt.Sprintf("[%d queues]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data": data,
		})
	}
}

func handlePauseQueue(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		store.PauseQueue(name)

		logInbound("POST", "/api/v1/queues/"+name+"/pause", "[paused]")

		writeJSON(w, 200, map[string]interface{}{
			"data": map[string]interface{}{
				"name":   name,
				"paused": true,
			},
			"message": fmt.Sprintf("Queue %q paused", name),
		})
	}
}

func handleResumeQueue(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		store.ResumeQueue(name)

		logInbound("POST", "/api/v1/queues/"+name+"/resume", "[resumed]")

		writeJSON(w, 200, map[string]interface{}{
			"data": map[string]interface{}{
				"name":   name,
				"paused": false,
			},
			"message": fmt.Sprintf("Queue %q resumed", name),
		})
	}
}
