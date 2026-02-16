package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
)

const banner = `
  ┌─────────────────────────────────────┐
  │   runlater dev emulator             │
  │   Local development server          │
  └─────────────────────────────────────┘
`

func main() {
	port := flag.Int("port", 8080, "Port to listen on")
	host := flag.String("host", "0.0.0.0", "Host to bind to")
	disableColor := flag.Bool("no-color", false, "Disable colored output")
	flag.Parse()

	noColor = *disableColor

	if os.Getenv("NO_COLOR") != "" {
		noColor = true
	}

	store := NewStore()
	mux := buildMux(store, fmt.Sprintf("localhost:%d", *port), defaultExecutorConfig)

	addr := fmt.Sprintf("%s:%d", *host, *port)

	fmt.Fprint(os.Stderr, c(colorGreen, banner))
	fmt.Fprintf(os.Stderr, "  %s http://%s\n", c(colorDim, "Listening on"), addr)
	fmt.Fprintf(os.Stderr, "  %s http://%s/in/{slug}\n", c(colorDim, "Inbound URL"), addr)
	fmt.Fprintf(os.Stderr, "  %s any value accepted\n\n", c(colorDim, "API key"))

	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func buildMux(store *Store, host string, execCfg ExecutorConfig) *http.ServeMux {
	mux := http.NewServeMux()

	registerTaskRoutes(mux, store, execCfg)
	registerEndpointRoutes(mux, store, host, execCfg)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})

	return mux
}
