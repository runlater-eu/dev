package main

import (
	"fmt"
	"net/http"
)

func registerLaneRoutes(mux *http.ServeMux, store *Store) {
	mux.HandleFunc("GET /api/v1/lanes", handleListLanes(store))
	mux.HandleFunc("POST /api/v1/lanes/{name}/pause", handlePauseLane(store))
	mux.HandleFunc("POST /api/v1/lanes/{name}/resume", handleResumeLane(store))
}

func handleListLanes(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lanes := store.ListLanes()

		data := make([]map[string]interface{}, 0, len(lanes))
		for name, paused := range lanes {
			data = append(data, map[string]interface{}{
				"name":   name,
				"paused": paused,
			})
		}

		logInbound("GET", "/api/v1/lanes", fmt.Sprintf("[%d lanes]", len(data)))

		writeJSON(w, 200, map[string]interface{}{
			"data": data,
		})
	}
}

func handlePauseLane(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		store.PauseLane(name)

		logInbound("POST", "/api/v1/lanes/"+name+"/pause", "[paused]")

		writeJSON(w, 200, map[string]interface{}{
			"data": map[string]interface{}{
				"name":   name,
				"paused": true,
			},
			"message": fmt.Sprintf("Lane %q paused", name),
		})
	}
}

func handleResumeLane(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		store.ResumeLane(name)

		logInbound("POST", "/api/v1/lanes/"+name+"/resume", "[resumed]")

		writeJSON(w, 200, map[string]interface{}{
			"data": map[string]interface{}{
				"name":   name,
				"paused": false,
			},
			"message": fmt.Sprintf("Lane %q resumed", name),
		})
	}
}
