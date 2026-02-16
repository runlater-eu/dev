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
GET    /api/v1/tasks                  List tasks
GET    /api/v1/tasks/{id}             Get task
DELETE /api/v1/tasks/{id}             Delete task
POST   /api/v1/tasks/{id}/trigger     Trigger task execution
GET    /api/v1/tasks/{id}/executions  List executions
```

### Endpoints (Inbound Webhooks)
```
POST   /api/v1/endpoints              Create endpoint
GET    /api/v1/endpoints              List endpoints
GET    /api/v1/endpoints/{id}         Get endpoint
DELETE /api/v1/endpoints/{id}         Delete endpoint
GET    /api/v1/endpoints/{id}/events  List inbound events
```

### Inbound Webhooks
```
POST   /in/{slug}                     Receive inbound webhook → forwards to forward_url
```

### Health
```
GET    /health                        Health check (200 OK)
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

Create an endpoint, then send webhooks to the inbound URL:

```bash
# Create endpoint
curl -X POST http://localhost:8080/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -d '{"name": "Stripe", "slug": "stripe", "forward_url": "http://localhost:3000/webhooks/stripe"}'

# Send test webhook (simulating Stripe)
curl -X POST http://localhost:8080/in/stripe \
  -H "Content-Type: application/json" \
  -d '{"type": "payment_intent.succeeded"}'
```

## Terminal Output

The emulator shows colored stripe-listen-style logs:

```
14:30:05 --> POST   /api/v1/tasks                    [t_abc123 created, immediate]
14:30:06  -> POST   http://localhost:3000/webhook
14:30:06  <- 200    [245ms]
14:30:10 --> POST   /in/stripe                       [ev_def456 forwarding -> localhost:3000]
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
- **1s simulated delay** — mimics network latency
- **No rate limiting, SSRF checks, or tier limits**

## Development

```bash
go test ./...     # Run all tests
go build          # Build binary
go vet ./...      # Check for issues
```

Zero external dependencies — standard library only.
