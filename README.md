# Runlater Dev Emulator

A lightweight local development server that mimics the Runlater API. Test your webhook integrations without deploying or using ngrok.

## Quick Start

```bash
docker run -p 8080:8080 ghcr.io/runlater-eu/dev
```

The server starts on `http://localhost:8080`.

Or run from source:

```bash
go run .
```

## Usage with the SDK

Point the Runlater SDK at your local emulator:

```typescript
import Runlater from "runlater";

const rl = new Runlater({
  apiKey: "dev",  // any value works
  baseUrl: "http://localhost:8080",
});

// Send a task — executes immediately against localhost
await rl.send("http://localhost:3000/api/webhook", {
  method: "POST",
  body: { event: "user.created", userId: "123" },
});

// Create a cron task — stored but not auto-scheduled
await rl.cron({
  url: "http://localhost:3000/api/cleanup",
  schedule: "0 * * * *",
  name: "Hourly cleanup",
});
```

## CLI Flags

```
--port 8080    Port to listen on (default: 8080)
--host 0.0.0.0 Host to bind to (default: 0.0.0.0)
--no-color     Disable colored terminal output
```

Set `NO_COLOR=1` environment variable to disable colors.

## API Routes

### Tasks
```
POST   /api/v1/tasks                  Create task (immediate, delayed, scheduled, or cron)
GET    /api/v1/tasks                  List tasks (filter: ?queue=name)
GET    /api/v1/tasks/{id}             Get task
PUT    /api/v1/tasks/{id}             Update task
DELETE /api/v1/tasks/{id}             Delete task
DELETE /api/v1/tasks?queue=name       Cancel all tasks in a queue
POST   /api/v1/tasks/{id}/trigger     Trigger task execution
GET    /api/v1/tasks/{id}/executions  List executions
```

### Batch
```
POST   /api/v1/tasks/batch            Create up to 1000 tasks at once
```

### Endpoints (Inbound Webhooks)
```
POST   /api/v1/endpoints                              Create endpoint
GET    /api/v1/endpoints                              List endpoints
GET    /api/v1/endpoints/{id}                         Get endpoint
PUT    /api/v1/endpoints/{id}                         Update endpoint
DELETE /api/v1/endpoints/{id}                         Delete endpoint
GET    /api/v1/endpoints/{id}/events                  List inbound events
POST   /api/v1/endpoints/{id}/events/{event_id}/replay  Replay an event
```

### Queues
```
GET    /api/v1/queues                  List queues with pause status
POST   /api/v1/queues/{name}/pause     Pause a queue
POST   /api/v1/queues/{name}/resume    Resume a queue
```

### Monitors (Heartbeat / Dead Man's Switch)
```
POST   /api/v1/monitors               Create monitor
GET    /api/v1/monitors                List monitors
GET    /api/v1/monitors/{id}           Get monitor
PUT    /api/v1/monitors/{id}           Update monitor
DELETE /api/v1/monitors/{id}           Delete monitor
GET    /api/v1/monitors/{id}/pings     List pings
```

### Inbound & Ping (Public, No Auth)
```
*      /in/{slug}                      Receive inbound webhook (any HTTP method)
GET    /ping/{token}                   Ping a monitor
POST   /ping/{token}                   Ping a monitor
```

### Health
```
GET    /health                         Health check (200 OK)
```

## Task Type Inference

The emulator uses the same type inference as production:

| Request fields | Task type | Status code | Behavior |
|---------------|-----------|-------------|----------|
| `cron` | Recurring | 201 | Stored, use `/trigger` to test |
| `delay` | Delayed | 202 | Executed immediately |
| `run_at` | Scheduled | 202 | Executed immediately |
| _(none)_ | Immediate | 202 | Executed immediately |

All non-cron tasks execute immediately with a 1-second simulated delay.

## Inbound Webhooks

Create an endpoint with one or more forward URLs, then send webhooks to the inbound URL:

```bash
# Create endpoint (fan-out to multiple URLs)
curl -X POST http://localhost:8080/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -d '{"name": "Stripe", "slug": "stripe", "forward_urls": ["http://localhost:3000/webhooks/stripe"]}'

# Send test webhook (simulating Stripe)
curl -X POST http://localhost:8080/in/stripe \
  -H "Content-Type: application/json" \
  -d '{"type": "payment_intent.succeeded"}'
```

## Batch Task Creation

Create multiple tasks sharing the same URL, method, and headers:

```bash
curl -X POST http://localhost:8080/api/v1/tasks/batch \
  -H "Content-Type: application/json" \
  -d '{
    "url": "http://localhost:3000/api/process",
    "method": "POST",
    "queue": "emails",
    "items": [
      {"to": "user1@example.com"},
      {"to": "user2@example.com"},
      {"to": "user3@example.com"}
    ]
  }'
```

## Monitors

Create a heartbeat monitor and ping it:

```bash
# Create monitor
curl -X POST http://localhost:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -d '{"name": "Nightly backup", "schedule_type": "cron", "cron_expression": "0 2 * * *"}'

# Ping it (using the ping_token from the response)
curl http://localhost:8080/ping/{token}
```

## Queue Management

Pause and resume queues to control task processing:

```bash
# List queues
curl http://localhost:8080/api/v1/queues

# Pause a queue
curl -X POST http://localhost:8080/api/v1/queues/emails/pause

# Resume a queue
curl -X POST http://localhost:8080/api/v1/queues/emails/resume
```

## Terminal Output

The emulator shows colored stripe-listen-style logs:

```
14:30:05 --> POST   /api/v1/tasks                    [t_abc123 created, immediate]
14:30:06  -> POST   http://localhost:3000/webhook
14:30:06  <- 200    [245ms]
14:30:10 --> POST   /in/stripe                       [t_def456 forwarding -> localhost:3000]
14:30:10  <- 200    [12ms]
```

## Docker

```bash
# From the published image
docker run -p 8080:8080 ghcr.io/runlater-eu/dev

# Or build locally
docker build -t runlater-dev dev-emulator/
docker run -p 8080:8080 runlater-dev
```

Image is multi-arch (amd64 + arm64), ~13MB Alpine-based.

Pushed automatically on every merge to `main` that touches `dev-emulator/`.

## What's Different from Production

- **No auth** — any API key (or none) is accepted
- **No persistence** — data resets on restart
- **No retries** — tasks execute once
- **No scheduling** — cron tasks are stored but never auto-triggered
- **No monitor alerting** — pings update status but monitors never go "down"
- **1s simulated delay** — mimics network latency
- **No rate limiting, SSRF checks, or tier limits**
- **No sync API** — declarative sync (`PUT /api/v1/sync`) is not emulated

## Development

```bash
go test ./...     # Run all tests
go build          # Build binary
go vet ./...      # Check for issues
```

Zero external dependencies — standard library only.
