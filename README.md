# API Reliability Proxy

A lightweight local proxy that sits between your application and an OpenAI-compatible API and automatically handles transient API and network failures.

It is designed for long-running applications such as AI coding agents, API clients, SDKs, and custom applications where a temporary upstream failure should not immediately become an application-visible failure.

---

# 1. What does it do?

Normally, an application connects directly to an API:

```text
Application
     |
     v
   API
```

If the API temporarily returns:

```text
500
502
503
504
429
connection reset
unexpected EOF
TLS timeout
empty response
malformed JSON
```

the application has to deal with the failure itself.

With API Reliability Proxy:

```text
Application
     |
     v
API Reliability Proxy
     |
     v
   API
```

The proxy automatically retries transient failures before returning an error to the application.

For streaming APIs, it also protects against an important failure mode where the upstream claims to be streaming but never sends a valid streaming event.

---

# 2. Example architecture

For example, you can run:

```text
Claude Code
     |
     | HTTP
     v
127.0.0.1:20218
     |
     | Reliability Proxy
     |
     v
127.0.0.1:10808
     |
     | Outbound proxy
     |
     v
9Router / OpenRouter / API Gateway
```

The application only needs to know about:

```text
http://127.0.0.1:20218/v1
```

It does not need to know about the retry logic.

---

# 3. Main features

The proxy can automatically retry:

* `429 Too Many Requests`
* `500 Internal Server Error`
* `502 Bad Gateway`
* `503 Service Unavailable`
* `504 Gateway Timeout`
* connection failures
* connection resets
* unexpected EOF
* TLS/network failures
* empty successful responses
* malformed JSON
* streaming responses that are not SSE
* streams that produce no first SSE event
* first-event timeouts

For streaming requests, the proxy follows a strict rule:

> It only retries before valid streaming data has been delivered to the client.

Once the first valid SSE event reaches the client, the request is considered committed and will not be retried.

This prevents duplicate generated output.

---

# 4. Requirements

You need:

* Go 1.23+
* Windows, Linux, or macOS
* An OpenAI-compatible API endpoint

The proxy uses only the Go standard library.

No external Go packages are required.

---

# 5. Download the project

Clone the repository:

```bash
git clone https://github.com/YOUR_USERNAME/api-reliability-proxy.git
cd api-reliability-proxy
```

Replace:

```text
YOUR_USERNAME
```

with the GitHub username that owns the repository.

---

# 6. Build on Linux

Run:

```bash
go build -o api-reliability-proxy .
```

You should now have:

```text
api-reliability-proxy
```

Start it:

```bash
./api-reliability-proxy
```

You should see something similar to:

```text
================================================
 API Reliability Proxy
================================================
Listen:          127.0.0.1:20218
Upstream:        https://example.com
Outbound proxy:  http://127.0.0.1:10808
Max attempts:    8
First SSE event: 30s
Status:          http://127.0.0.1:20218/__proxy/status
================================================
```

---

# 7. Build on Windows

Open PowerShell:

```powershell
git clone https://github.com/YOUR_USERNAME/api-reliability-proxy.git
cd api-reliability-proxy
```

Build:

```powershell
go build -o api-reliability-proxy.exe .
```

Run:

```powershell
.\api-reliability-proxy.exe
```

The proxy will listen on:

```text
http://127.0.0.1:20218
```

---

# 8. Configure the upstream

There are two ways to configure the proxy.

## Option A — Environment variables

Linux:

```bash
export PROXY_LISTEN="127.0.0.1:20218"
export UPSTREAM_URL="https://your-api.example/v1"
export OUTBOUND_PROXY="http://127.0.0.1:10808"

./api-reliability-proxy
```

Windows PowerShell:

```powershell
$env:PROXY_LISTEN="127.0.0.1:20218"
$env:UPSTREAM_URL="https://your-api.example/v1"
$env:OUTBOUND_PROXY="http://127.0.0.1:10808"

.\api-reliability-proxy.exe
```

---

# 9. Configuration file

Alternatively, create:

```text
config.json
```

Example:

```json
{
  "listen_addr": "127.0.0.1:20218",
  "upstream": "https://your-api.example/v1",
  "outbound_proxy": "http://127.0.0.1:10808",
  "max_attempts": 8,
  "first_event_timeout_seconds": 30,
  "retry_delay_seconds": 1,
  "max_retry_delay_seconds": 8,
  "max_body_mb": 50
}
```

Then Linux:

```bash
export RELIABILITY_PROXY_CONFIG="$PWD/config.json"
./api-reliability-proxy
```

Windows:

```powershell
$env:RELIABILITY_PROXY_CONFIG="$PWD\config.json"
.\api-reliability-proxy.exe
```

---

# 10. Example: using an outbound proxy

Suppose your network setup is:

```text
127.0.0.1:10808
```

and your API gateway is:

```text
https://9router-production-b45b.up.railway.app
```

Configure:

```json
{
  "listen_addr": "127.0.0.1:20218",
  "upstream": "https://9router-production-b45b.up.railway.app",
  "outbound_proxy": "http://127.0.0.1:10808",
  "max_attempts": 8,
  "first_event_timeout_seconds": 30,
  "retry_delay_seconds": 1,
  "max_retry_delay_seconds": 8,
  "max_body_mb": 50
}
```

The resulting network path is:

```text
Claude Code
     |
     | http://127.0.0.1:20218/v1
     v
Reliability Proxy
     |
     | http://127.0.0.1:10808
     v
Outbound Proxy
     |
     | HTTPS
     v
9Router
```

---

# 11. Example: Claude Code

Configure your client/API environment to use:

```text
http://127.0.0.1:20218/v1
```

Instead of connecting directly to:

```text
https://your-api-gateway/v1
```

The important difference is:

```text
BEFORE

Claude Code
     |
     v
API Gateway
     |
     X transient failure
     |
     v
Claude sees error


AFTER

Claude Code
     |
     v
Reliability Proxy
     |
     X transient failure
     |
     v
Proxy retries
     |
     v
API Gateway succeeds
     |
     v
Claude receives normal response
```

The application does not need to manually retry the failed request.

---

# 12. Example: normal request

Suppose the client sends:

```http
POST /v1/messages
```

The proxy receives:

```text
[REQ 1] POST /v1/messages stream=false body=12000 bytes
```

The upstream returns:

```text
HTTP 200
```

The proxy passes the response to the client.

Log:

```text
[REQ 1] attempt 1/8
```

---

# 13. Example: upstream returns 502

Suppose the first request receives:

```text
HTTP 502 Bad Gateway
```

The proxy does:

```text
[REQ 2] attempt 1/8
[REQ 2] attempt 1 retryable HTTP status=502
retrying after 1s
[REQ 2] attempt 2/8
```

If attempt 2 succeeds:

```text
[REQ 2] attempt 2/8
```

the client receives the successful response.

The client never needs to handle the first `502`.

---

# 14. Example: network failure

Suppose the connection fails:

```text
TLS handshake timeout
```

The proxy logs:

```text
[REQ 3] attempt 1/8
[REQ 3] attempt 1 network error: TLS handshake timeout
retrying after 1s
[REQ 3] attempt 2/8
```

If the second attempt succeeds, the client receives the normal response.

---

# 15. Example: streaming request

Suppose the client sends:

```json
{
  "stream": true
}
```

The proxy recognizes that this is a streaming request.

It expects:

```text
Content-Type: text/event-stream
```

and waits for the first valid SSE event.

---

# 16. Streaming failure example

Suppose the upstream incorrectly returns:

```text
HTTP 200
Content-Type: application/json
```

instead of SSE.

The proxy detects the problem:

```text
[REQ 4] stream attempt 1/8
[REQ 4] stream attempt 1 returned non-SSE Content-Type="application/json"
retrying after 1s
```

It then retries:

```text
[REQ 4] stream attempt 2/8
```

If the second request returns valid SSE:

```text
[REQ 4] first valid SSE event received; response committed
```

the proxy starts streaming to the client.

---

# 17. Why the first SSE event matters

Consider:

```text
Request
   |
   v
Upstream
   |
   | no response
   |
   X connection closes
```

There is no useful output to the client.

The proxy can safely retry.

But consider:

```text
Request
   |
   v
Upstream
   |
   | SSE event
   v
Proxy
   |
   v
Client
```

At this point the client has already received part of the model response.

Retrying could produce:

```text
Hello, I will...
Hello, I will...
```

Therefore the proxy never retries after the stream has been committed.

---

# 18. Checking whether the proxy is running

Use:

```bash
curl http://127.0.0.1:20218/__proxy/health
```

Expected:

```json
{
  "status": "ok"
}
```

---

# 19. Checking proxy status

Run:

```bash
curl http://127.0.0.1:20218/__proxy/status
```

Example:

```json
{
  "enabled": true,
  "requests_seen": 17,
  "listen_addr": "127.0.0.1:20218",
  "upstream": "https://your-api.example/v1",
  "outbound_proxy": "http://127.0.0.1:10808",
  "max_attempts": 8,
  "first_event_s": 30
}
```

---

# 20. Temporarily disable reliability handling

Disable automatic retries:

```bash
curl -X POST http://127.0.0.1:20218/__proxy/disable
```

The proxy remains available, but requests are sent using one upstream attempt.

Check:

```bash
curl http://127.0.0.1:20218/__proxy/status
```

You should see:

```json
{
  "enabled": false
}
```

---

# 21. Enable reliability handling again

Run:

```bash
curl -X POST http://127.0.0.1:20218/__proxy/enable
```

Check:

```bash
curl http://127.0.0.1:20218/__proxy/status
```

You should see:

```json
{
  "enabled": true
}
```

---

# 22. Retry configuration

Default configuration:

```text
Maximum attempts:       8
Initial retry delay:    1 second
Maximum retry delay:    8 seconds
First SSE timeout:      30 seconds
Maximum request body:   50 MB
```

The retry delay grows approximately like:

```text
1 second
2 seconds
4 seconds
8 seconds
8 seconds
...
```

This prevents an unhealthy upstream from being hammered continuously.

---

# 23. Multiple Claude/API instances

The proxy supports multiple simultaneous clients.

For example:

```text
Claude #1 ─────┐
Claude #2 ─────┤
Claude #3 ─────┤
Claude #4 ─────┤──> Reliability Proxy
Claude #5 ─────┤
Claude #6 ─────┘
```

Each request has its own request ID:

```text
[REQ 21]
[REQ 22]
[REQ 23]
[REQ 24]
```

A retry of request 22 does not affect request 21.

---

# 24. What this proxy does NOT do

The proxy only handles API/network reliability.

It does not know whether an AI agent is:

* running Python
* compiling code
* running tests
* editing files
* executing a shell command
* waiting for a subprocess
* waiting for another tool
* thinking
* finished
* completely stopped

For example:

```text
Claude
  |
  | API request
  v
Proxy
  |
  v
Claude receives response
  |
  |---- Python execution ----|
  |                          |
  |                          |
  | no API request           |
  |                          |
  v                          v
next API request
```

During the Python execution, the proxy sees no API traffic.

Therefore:

> No API request does not mean that the application is stopped.

If process monitoring is required, use a separate supervisor.

---

# 25. Recommended architecture for AI agents

For advanced setups, use two separate components:

```text
                 AI Agent
                    |
          ┌─────────┴─────────┐
          |                   |
          v                   v
    Agent Supervisor    API Reliability Proxy
          |                   |
          |                   v
          |             Outbound Proxy
          |                   |
          |                   v
          |                API
          |
          +-- process state
          +-- CPU activity
          +-- terminal activity
          +-- completion detection
```

The responsibilities stay separate:

### Reliability Proxy

Handles:

```text
network
HTTP
timeouts
retries
SSE
upstream failures
```

### Agent Supervisor

Handles:

```text
process state
session state
CPU activity
terminal output
completion
stuck detection
```

This separation makes the system much easier to maintain.

---

# 26. Security

By default the proxy listens on:

```text
127.0.0.1:20218
```

This means it is accessible only from the local machine.

Do not expose the control endpoints publicly without adding authentication.

In particular, do not blindly expose:

```text
/__proxy/enable
/__proxy/disable
```

to the Internet.

---

# 27. Important warning about retries

Retries replay the original HTTP request.

This is desirable for AI generation requests where the first attempt failed before producing usable output.

However, users should understand that retrying arbitrary non-idempotent HTTP operations can potentially cause duplicate actions.

Use this proxy primarily for APIs where replaying failed requests is acceptable.

---

# 28. Troubleshooting

## Port already in use

Linux:

```bash
ss -ltnp | grep 20218
```

Windows:

```powershell
netstat -ano | findstr :20218
```

Change the listening port:

```bash
export PROXY_LISTEN="127.0.0.1:20219"
```

---

## Outbound proxy unavailable

Test:

```bash
curl -x http://127.0.0.1:10808 https://example.com
```

If this fails, the reliability proxy will also be unable to reach the upstream through that proxy.

---

## Upstream unavailable

Check the upstream directly:

```bash
curl https://your-api.example
```

Then check through the outbound proxy:

```bash
curl -x http://127.0.0.1:10808 https://your-api.example
```

Then check through the reliability proxy.

---

# 29. Minimal setup

If you just want the simplest possible setup:

```bash
git clone https://github.com/YOUR_USERNAME/api-reliability-proxy.git
cd api-reliability-proxy

go build -o api-reliability-proxy .

export UPSTREAM_URL="https://your-api.example/v1"
export OUTBOUND_PROXY="http://127.0.0.1:10808"

./api-reliability-proxy
```

Then configure your application to use:

```text
http://127.0.0.1:20218/v1
```

That's it.

---

# 30. Summary

The proxy provides a reliability layer:

```text
                    ┌─────────────────────┐
                    │     AI CLIENT       │
                    └──────────┬──────────┘
                               |
                               v
                    ┌─────────────────────┐
                    │ API Reliability     │
                    │ Proxy               │
                    │                     │
                    │ Retry               │
                    │ Backoff             │
                    │ SSE protection      │
                    │ Empty response      │
                    │ JSON validation     │
                    │ Network recovery    │
                    └──────────┬──────────┘
                               |
                               v
                    ┌─────────────────────┐
                    │ Optional Outbound   │
                    │ Proxy               │
                    └──────────┬──────────┘
                               |
                               v
                    ┌─────────────────────┐
                    │ Upstream API        │
                    └─────────────────────┘
```

The goal is simple:

> Let the application receive a valid response whenever a transient upstream failure can be recovered transparently.

For streaming APIs, recovery is only performed before the first valid event reaches the client. This prevents duplicate streamed output.
