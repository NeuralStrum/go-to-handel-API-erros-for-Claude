package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultListenAddr      = "127.0.0.1:20218"
	defaultUpstream        = "https://9router-production-b45b.up.railway.app"
	defaultOutboundProxy   = "http://127.0.0.1:10808"
	defaultMaxAttempts     = 8
	defaultFirstEvent      = 30 * time.Second
	defaultRetryDelay      = 1 * time.Second
	defaultMaxRetryDelay   = 8 * time.Second
	defaultReadBuffer      = 64 * 1024
	defaultMaxBody         = 50 * 1024 * 1024
	defaultHealthPath      = "/__proxy/health"
	defaultStatusPath      = "/__proxy/status"
	defaultEnablePath      = "/__proxy/enable"
	defaultDisablePath     = "/__proxy/disable"
)

type Config struct {
	ListenAddr        string `json:"listen_addr"`
	Upstream          string `json:"upstream"`
	OutboundProxy     string `json:"outbound_proxy"`
	MaxAttempts       int    `json:"max_attempts"`
	FirstEventTimeout int    `json:"first_event_timeout_seconds"`
	RetryDelay        int    `json:"retry_delay_seconds"`
	MaxRetryDelay     int    `json:"max_retry_delay_seconds"`
	MaxBodyMB         int    `json:"max_body_mb"`
}

type Proxy struct {
	cfg       Config
	client    *http.Client
	transport *http.Transport

	enabled atomic.Bool
	counter atomic.Uint64

	mu sync.RWMutex
}

type RequestBody struct {
	Data []byte
}

type AttemptResult struct {
	Response *http.Response
	Err      error
}

func main() {
	cfg := loadConfig()

	p, err := NewProxy(cfg)
	if err != nil {
		log.Fatalf("failed to initialize proxy: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc(defaultHealthPath, p.healthHandler)
	mux.HandleFunc(defaultStatusPath, p.statusHandler)
	mux.HandleFunc(defaultEnablePath, p.enableHandler)
	mux.HandleFunc(defaultDisablePath, p.disableHandler)

	mux.HandleFunc("/", p.proxyHandler)

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           loggingMiddleware(mux),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       0,
	}

	log.Println("================================================")
	log.Println(" API Reliability Proxy")
	log.Println("================================================")
	log.Printf("Listen:          %s", cfg.ListenAddr)
	log.Printf("Upstream:        %s", cfg.Upstream)
	log.Printf("Outbound proxy:  %s", cfg.OutboundProxy)
	log.Printf("Max attempts:    %d", cfg.MaxAttempts)
	log.Printf("First SSE event: %ds", cfg.FirstEventTimeout)
	log.Printf("Status:          http://%s%s", cfg.ListenAddr, defaultStatusPath)
	log.Println("================================================")

	log.Fatal(server.ListenAndServe())
}

func loadConfig() Config {
	cfg := Config{
		ListenAddr:        defaultListenAddr,
		Upstream:          defaultUpstream,
		OutboundProxy:     defaultOutboundProxy,
		MaxAttempts:       defaultMaxAttempts,
		FirstEventTimeout: int(defaultFirstEvent.Seconds()),
		RetryDelay:        int(defaultRetryDelay.Seconds()),
		MaxRetryDelay:     int(defaultMaxRetryDelay.Seconds()),
		MaxBodyMB:         50,
	}

	if path := os.Getenv("RELIABILITY_PROXY_CONFIG"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			log.Printf("warning: cannot read config %s: %v", path, err)
			return cfg
		}

		if err := json.Unmarshal(data, &cfg); err != nil {
			log.Printf("warning: cannot parse config %s: %v", path, err)
		}
	}

	if v := os.Getenv("PROXY_LISTEN"); v != "" {
		cfg.ListenAddr = v
	}

	if v := os.Getenv("UPSTREAM_URL"); v != "" {
		cfg.Upstream = v
	}

	if v := os.Getenv("OUTBOUND_PROXY"); v != "" {
		cfg.OutboundProxy = v
	}

	if v := os.Getenv("MAX_ATTEMPTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxAttempts = n
		}
	}

	if v := os.Getenv("FIRST_EVENT_TIMEOUT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.FirstEventTimeout = n
		}
	}

	if v := os.Getenv("RETRY_DELAY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			cfg.RetryDelay = n
		}
	}

	if v := os.Getenv("MAX_RETRY_DELAY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxRetryDelay = n
		}
	}

	if v := os.Getenv("MAX_BODY_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.MaxBodyMB = n
		}
	}

	return cfg
}

func NewProxy(cfg Config) (*Proxy, error) {
	proxyURL, err := url.Parse(cfg.OutboundProxy)
	if err != nil {
		return nil, fmt.Errorf("invalid outbound proxy: %w", err)
	}

	transport := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),

		ForceAttemptHTTP2: true,

		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		MaxConnsPerHost:     0,

		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 0,

		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},

		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}

	client := &http.Client{
		Transport: transport,

		// Streaming API calls can legitimately run for a long time.
		Timeout: 0,
	}

	p := &Proxy{
		cfg:       cfg,
		client:    client,
		transport: transport,
	}

	p.enabled.Store(true)

	return p, nil
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		next.ServeHTTP(w, r)

		log.Printf(
			"[HTTP] %s %s duration=%s",
			r.Method,
			r.URL.Path,
			time.Since(start).Round(time.Millisecond),
		)
	})
}

func (p *Proxy) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
	})
}

func (p *Proxy) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	p.mu.RLock()
	enabled := p.enabled.Load()
	p.mu.RUnlock()

	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled":        enabled,
		"requests_seen":  p.counter.Load(),
		"listen_addr":    p.cfg.ListenAddr,
		"upstream":       p.cfg.Upstream,
		"outbound_proxy": p.cfg.OutboundProxy,
		"max_attempts":   p.cfg.MaxAttempts,
		"first_event_s":  p.cfg.FirstEventTimeout,
	})
}

func (p *Proxy) enableHandler(w http.ResponseWriter, r *http.Request) {
	p.enabled.Store(true)

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled": true,
	})
}

func (p *Proxy) disableHandler(w http.ResponseWriter, r *http.Request) {
	p.enabled.Store(false)

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]any{
		"enabled": false,
	})
}

func (p *Proxy) proxyHandler(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/__proxy/") {
		http.NotFound(w, r)
		return
	}

	reqID := p.counter.Add(1)

	body, err := readRequestBody(r.Body, p.cfg.MaxBodyMB)
	if err != nil {
		http.Error(w, "request body error: "+err.Error(), http.StatusBadRequest)
		return
	}

	streaming := isStreamingRequest(body.Data)

	log.Printf(
		"[REQ %d] %s %s stream=%v body=%d bytes",
		reqID,
		r.Method,
		r.URL.Path,
		streaming,
		len(body.Data),
	)

	if !p.enabled.Load() {
		log.Printf("[REQ %d] reliability layer disabled; single upstream attempt", reqID)

		resp, err := p.sendAttempt(r.Context(), r, body.Data)

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}

		p.writeResponse(w, resp)
		return
	}

	if streaming {
		p.handleStreaming(w, r, body.Data, reqID)
		return
	}

	p.handleNormal(w, r, body.Data, reqID)
}

func readRequestBody(rc io.ReadCloser, maxMB int) (RequestBody, error) {
	defer rc.Close()

	maxBytes := int64(maxMB) * 1024 * 1024

	data, err := io.ReadAll(io.LimitReader(rc, maxBytes+1))
	if err != nil {
		return RequestBody{}, err
	}

	if int64(len(data)) > maxBytes {
		return RequestBody{}, fmt.Errorf("request body exceeds %d MB", maxMB)
	}

	return RequestBody{Data: data}, nil
}

func isStreamingRequest(body []byte) bool {
	var payload map[string]any

	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}

	value, ok := payload["stream"]

	if !ok {
		return false
	}

	stream, ok := value.(bool)

	return ok && stream
}

func (p *Proxy) handleNormal(
	w http.ResponseWriter,
	incoming *http.Request,
	body []byte,
	reqID uint64,
) {
	for attempt := 1; attempt <= p.cfg.MaxAttempts; attempt++ {
		log.Printf(
			"[REQ %d] attempt %d/%d",
			reqID,
			attempt,
			p.cfg.MaxAttempts,
		)

		resp, err := p.sendAttempt(
			incoming.Context(),
			incoming,
			body,
		)

		if err != nil {
			log.Printf(
				"[REQ %d] attempt %d network error: %v",
				reqID,
				attempt,
				err,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			http.Error(w, "upstream network failure: "+err.Error(), http.StatusBadGateway)
			return
		}

		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		if readErr != nil {
			log.Printf(
				"[REQ %d] attempt %d response read error: %v",
				reqID,
				attempt,
				readErr,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			http.Error(w, "upstream response read failure: "+readErr.Error(), http.StatusBadGateway)
			return
		}

		if shouldRetryStatus(resp.StatusCode) {
			log.Printf(
				"[REQ %d] attempt %d retryable HTTP status=%d",
				reqID,
				attempt,
				resp.StatusCode,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			writeBufferedResponse(w, resp, responseBody)
			return
		}

		if resp.StatusCode >= 200 &&
			resp.StatusCode < 300 &&
			len(bytes.TrimSpace(responseBody)) == 0 {

			log.Printf(
				"[REQ %d] attempt %d HTTP %d but EMPTY body",
				reqID,
				attempt,
				resp.StatusCode,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}
		}

		if resp.StatusCode >= 200 &&
			resp.StatusCode < 300 &&
			isJSONContentType(resp.Header.Get("Content-Type")) {

			if !validJSON(responseBody) {
				log.Printf(
					"[REQ %d] attempt %d HTTP %d malformed JSON",
					reqID,
					attempt,
					resp.StatusCode,
				)

				if attempt < p.cfg.MaxAttempts {
					p.sleepBackoff(incoming.Context(), attempt)
					continue
				}
			}
		}

		writeBufferedResponse(w, resp, responseBody)
		return
	}
}

func (p *Proxy) handleStreaming(
	w http.ResponseWriter,
	incoming *http.Request,
	body []byte,
	reqID uint64,
) {
	for attempt := 1; attempt <= p.cfg.MaxAttempts; attempt++ {
		log.Printf(
			"[REQ %d] stream attempt %d/%d",
			reqID,
			attempt,
			p.cfg.MaxAttempts,
		)

		resp, err := p.sendAttempt(
			incoming.Context(),
			incoming,
			body,
		)

		if err != nil {
			log.Printf(
				"[REQ %d] stream attempt %d network error: %v",
				reqID,
				attempt,
				err,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			http.Error(w, "upstream network failure: "+err.Error(), http.StatusBadGateway)
			return
		}

		contentType := strings.ToLower(resp.Header.Get("Content-Type"))

		if shouldRetryStatus(resp.StatusCode) {
			log.Printf(
				"[REQ %d] stream attempt %d retryable HTTP status=%d",
				reqID,
				attempt,
				resp.StatusCode,
			)

			resp.Body.Close()

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			http.Error(w, "upstream retryable failure", resp.StatusCode)
			return
		}

		// A normal client error such as 400/401/403 should be passed
		// through instead of endlessly retrying it.
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			log.Printf(
				"[REQ %d] stream client error status=%d; passing through",
				reqID,
				resp.StatusCode,
			)

			p.writeResponse(w, resp)
			return
		}

		// The request asked for streaming but upstream did not actually
		// return SSE. This is one of the important failure modes this
		// proxy protects against.
		if !strings.Contains(contentType, "text/event-stream") {
			data, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()

			if readErr != nil {
				log.Printf(
					"[REQ %d] non-SSE response read error: %v",
					reqID,
					readErr,
				)
			}

			log.Printf(
				"[REQ %d] stream attempt %d returned non-SSE Content-Type=%q body=%d bytes",
				reqID,
				attempt,
				contentType,
				len(data),
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			writeBufferedResponse(w, resp, data)
			return
		}

		firstEvent, err := waitForFirstSSEEvent(
			incoming.Context(),
			resp.Body,
			time.Duration(p.cfg.FirstEventTimeout)*time.Second,
		)

		if err != nil {
			resp.Body.Close()

			log.Printf(
				"[REQ %d] stream attempt %d failed before first SSE event: %v",
				reqID,
				attempt,
				err,
			)

			if attempt < p.cfg.MaxAttempts {
				p.sleepBackoff(incoming.Context(), attempt)
				continue
			}

			http.Error(
				w,
				"upstream stream produced no valid event: "+err.Error(),
				http.StatusBadGateway,
			)
			return
		}

		// IMPORTANT:
		// From this point onward, valid stream data is going to the client.
		// We must never retry after this point.
		copyHeaders(w.Header(), resp.Header)

		w.WriteHeader(resp.StatusCode)

		if flusher, ok := w.(http.Flusher); ok {
			_, _ = w.Write(firstEvent)
			flusher.Flush()
		} else {
			_, _ = w.Write(firstEvent)
		}

		log.Printf(
			"[REQ %d] first valid SSE event received; response committed",
			reqID,
		)

		p.streamRemaining(w, resp.Body, reqID)
		resp.Body.Close()

		return
	}
}

func waitForFirstSSEEvent(
	ctx context.Context,
	body io.Reader,
	timeout time.Duration,
) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}

	ch := make(chan result, 1)

	go func() {
		reader := bufio.NewReaderSize(body, defaultReadBuffer)

		var event bytes.Buffer
		sawSSEField := false

		for {
			line, err := reader.ReadBytes('\n')

			if len(line) > 0 {
				event.Write(line)

				trimmed := strings.TrimSpace(string(line))

				if strings.HasPrefix(trimmed, "data:") ||
					strings.HasPrefix(trimmed, "event:") {

					sawSSEField = true
				}

				// SSE event ends at an empty line.
				if trimmed == "" && sawSSEField {
					ch <- result{
						data: event.Bytes(),
						err:  nil,
					}
					return
				}
			}

			if err != nil {
				if errors.Is(err, io.EOF) {
					ch <- result{
						err: io.ErrUnexpectedEOF,
					}
				} else {
					ch <- result{err: err}
				}
				return
			}
		}
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-ch:
		return result.data, result.err

	case <-timer.C:
		return nil, fmt.Errorf("first SSE event timeout after %s", timeout)

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *Proxy) streamRemaining(
	w http.ResponseWriter,
	body io.Reader,
	reqID uint64,
) {
	flusher, canFlush := w.(http.Flusher)

	buffer := make([]byte, defaultReadBuffer)

	for {
		n, err := body.Read(buffer)

		if n > 0 {
			_, writeErr := w.Write(buffer[:n])

			if writeErr != nil {
				log.Printf(
					"[REQ %d] downstream write error: %v",
					reqID,
					writeErr,
				)
				return
			}

			if canFlush {
				flusher.Flush()
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("[REQ %d] stream completed", reqID)
			} else {
				log.Printf(
					"[REQ %d] stream ended with error: %v",
					reqID,
					err,
				)
			}

			return
		}
	}
}

func (p *Proxy) sendAttempt(
	ctx context.Context,
	incoming *http.Request,
	body []byte,
) (*http.Response, error) {
	target, err := url.Parse(p.cfg.Upstream)

	if err != nil {
		return nil, err
	}

	target.Path = joinURLPath(target.Path, incoming.URL.Path)
	target.RawQuery = incoming.URL.RawQuery

	req, err := http.NewRequestWithContext(
		ctx,
		incoming.Method,
		target.String(),
		bytes.NewReader(body),
	)

	if err != nil {
		return nil, err
	}

	copyRequestHeaders(req.Header, incoming.Header)

	// Avoid transparent compression because it complicates replay and
	// streaming handling.
	req.Header.Set("Accept-Encoding", "identity")

	// These are controlled by Go's HTTP transport.
	req.Header.Del("Host")
	req.Header.Del("Content-Length")
	req.Header.Del("Connection")
	req.Header.Del("Keep-Alive")
	req.Header.Del("Proxy-Connection")
	req.Header.Del("Transfer-Encoding")
	req.Header.Del("Upgrade")

	req.ContentLength = int64(len(body))

	req.Host = target.Host

	resp, err := p.client.Do(req)

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func copyRequestHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func writeBufferedResponse(
	w http.ResponseWriter,
	resp *http.Response,
	body []byte,
) {
	copyHeaders(w.Header(), resp.Header)

	w.WriteHeader(resp.StatusCode)

	_, _ = w.Write(body)
}

func (p *Proxy) writeResponse(
	w http.ResponseWriter,
	resp *http.Response,
) {
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)

	w.WriteHeader(resp.StatusCode)

	_, _ = io.Copy(w, resp.Body)
}

func shouldRetryStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true

	default:
		return false
	}
}

func isJSONContentType(contentType string) bool {
	contentType = strings.ToLower(contentType)

	return strings.Contains(contentType, "application/json") ||
		strings.Contains(contentType, "+json")
}

func validJSON(data []byte) bool {
	var value any

	return json.Unmarshal(data, &value) == nil
}

func (p *Proxy) sleepBackoff(
	ctx context.Context,
	attempt int,
) {
	delay := p.cfg.RetryDelay

	if delay <= 0 {
		return
	}

	for i := 1; i < attempt; i++ {
		delay *= 2

		if delay >= p.cfg.MaxRetryDelay {
			delay = p.cfg.MaxRetryDelay
			break
		}
	}

	if delay > p.cfg.MaxRetryDelay {
		delay = p.cfg.MaxRetryDelay
	}

	log.Printf("retrying after %ds", delay)

	timer := time.NewTimer(time.Duration(delay) * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func joinURLPath(base, incoming string) string {
	if base == "" {
		return incoming
	}

	if incoming == "" {
		return base
	}

	return strings.TrimRight(base, "/") +
		"/" +
		strings.TrimLeft(incoming, "/")
}
