package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
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

var (
	config Config

	enabled atomic.Bool
	reqID   atomic.Uint64
)

type Proxy struct {
	client *http.Client
	config Config
}

type AttemptResult struct {
	statusCode  int
	headers     http.Header
	body        []byte
	success     bool
	err         error
	retryReason string
}

func main() {
	config = loadConfig()

	if config.MaxAttempts < 1 {
		config.MaxAttempts = 1
	}

	if config.FirstEventTimeout <= 0 {
		config.FirstEventTimeout = 30
	}

	if config.RetryDelay <= 0 {
		config.RetryDelay = 1
	}

	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = 8
	}

	if config.MaxBodyMB <= 0 {
		config.MaxBodyMB = 50
	}

	enabled.Store(true)

	client, err := newHTTPClient(config)
	if err != nil {
		log.Fatalf("client initialization failed: %v", err)
	}

	proxy := &Proxy{
		client: client,
		config: config,
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/__proxy/health", proxy.health)
	mux.HandleFunc("/__proxy/status", proxy.status)
	mux.HandleFunc("/__proxy/enable", proxy.enable)
	mux.HandleFunc("/__proxy/disable", proxy.disable)

	// Everything else is proxied.
	mux.HandleFunc("/", proxy.handle)

	server := &http.Server{
		Addr:              config.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	log.Println("================================================")
	log.Println(" API Reliability Proxy")
	log.Println("================================================")
	log.Printf("Listen:          %s", config.ListenAddr)
	log.Printf("Upstream:        %s", config.Upstream)
	log.Printf("Outbound proxy:  %s", config.OutboundProxy)
	log.Printf("Max attempts:    %d", config.MaxAttempts)
	log.Printf("First event:     %ds", config.FirstEventTimeout)
	log.Printf("Max body:        %d MB", config.MaxBodyMB)
	log.Printf("Status:          ENABLED")
	log.Println("================================================")

	if err := server.ListenAndServe(); err != nil &&
		!errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func newHTTPClient(cfg Config) (*http.Client, error) {
	transport := &http.Transport{
		Proxy: nil,
	}

	// Optional outbound proxy.
	//
	// Example:
	// http://127.0.0.1:10808
	//
	// We do NOT need to know the API token.
	// Claude's Authorization header is forwarded unchanged.
	if cfg.OutboundProxy != "" {
		proxyURL, err := url.Parse(cfg.OutboundProxy)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid outbound proxy: %w",
				err,
			)
		}

		transport.Proxy = http.ProxyURL(proxyURL)
	}

	return &http.Client{
		Transport: transport,

		// IMPORTANT:
		// No global timeout.
		//
		// Claude streaming responses can legitimately take
		// a long time.
		Timeout: 0,
	}, nil
}

// ============================================================
// CONTROL ENDPOINTS
// ============================================================

func (p *Proxy) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"enabled": enabled.Load(),
	})
}

func (p *Proxy) status(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":           enabled.Load(),
		"listen_addr":       p.config.ListenAddr,
		"upstream":          p.config.Upstream,
		"outbound_proxy":    p.config.OutboundProxy,
		"max_attempts":      p.config.MaxAttempts,
		"first_event_timeout": p.config.FirstEventTimeout,
	})
}

func (p *Proxy) enable(w http.ResponseWriter, r *http.Request) {
	enabled.Store(true)

	log.Println("[CONTROL] Proxy ENABLED")

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled": true,
	})
}

func (p *Proxy) disable(w http.ResponseWriter, r *http.Request) {
	enabled.Store(false)

	log.Println("[CONTROL] Proxy DISABLED")

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled": false,
	})
}

// ============================================================
// MAIN REQUEST HANDLER
// ============================================================

func (p *Proxy) handle(w http.ResponseWriter, r *http.Request) {
	id := reqID.Add(1)

	log.Printf(
		"[REQ %d] %s %s",
		id,
		r.Method,
		r.URL.Path,
	)

	body, err := readRequestBody(
		r,
		p.config.MaxBodyMB,
	)

	if err != nil {
		log.Printf(
			"[REQ %d] request body read error: %v",
			id,
			err,
		)

		http.Error(
			w,
			"request body error",
			http.StatusBadRequest,
		)

		return
	}

	log.Printf(
		"[REQ %d] request body bytes: %d",
		id,
		len(body),
	)

	stream := detectStream(body)

	if stream {
		log.Printf(
			"[REQ %d] detected STREAMING request",
			id,
		)
	} else {
		log.Printf(
			"[REQ %d] detected NON-STREAMING request",
			id,
		)
	}

	// --------------------------------------------------------
	// DISABLED MODE
	// --------------------------------------------------------
	//
	// One upstream attempt only.
	// No retries.
	//
	// We still perform the request normally.
	// --------------------------------------------------------

	if !enabled.Load() {
		log.Printf(
			"[REQ %d] proxy disabled -> single upstream attempt",
			id,
		)

		result := p.attempt(
			r,
			body,
			stream,
			id,
			1,
		)

		if result.err != nil {
			log.Printf(
				"[REQ %d] disabled-mode upstream error: %v",
				id,
				result.err,
			)

			http.Error(
				w,
				"upstream request failed",
				http.StatusBadGateway,
			)

			return
		}

		result.writeTo(w)
		return
	}

	// --------------------------------------------------------
	// RELIABLE MODE
	// --------------------------------------------------------

	p.forwardWithRetry(
		w,
		r,
		body,
		stream,
		id,
	)
}

// ============================================================
// REQUEST BODY
// ============================================================

func readRequestBody(
	r *http.Request,
	maxMB int,
) ([]byte, error) {
	maxBytes := int64(maxMB) * 1024 * 1024

	reader := io.LimitReader(
		r.Body,
		maxBytes+1,
	)

	body, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf(
			"request body exceeds %d MB",
			maxMB,
		)
	}

	return body, nil
}

func detectStream(body []byte) bool {
	var v map[string]interface{}

	if err := json.Unmarshal(body, &v); err != nil {
		return false
	}

	stream, ok := v["stream"].(bool)

	return ok && stream
}

// ============================================================
// RETRY ENGINE
// ============================================================

func (p *Proxy) forwardWithRetry(
	w http.ResponseWriter,
	r *http.Request,
	body []byte,
	stream bool,
	id uint64,
) {
	delay := time.Duration(
		p.config.RetryDelay,
	) * time.Second

	for attempt := 1;
		attempt <= p.config.MaxAttempts;
		attempt++ {

		log.Printf(
			"[REQ %d] ============================================",
			id,
		)

		log.Printf(
			"[REQ %d] ATTEMPT %d/%d",
			id,
			attempt,
			p.config.MaxAttempts,
		)

		result := p.attempt(
			r,
			body,
			stream,
			id,
			attempt,
		)

		if result.err == nil && result.success {
			log.Printf(
				"[REQ %d] SUCCESS on attempt %d",
				id,
				attempt,
			)

			result.writeTo(w)
			return
		}

		if result.err != nil {
			log.Printf(
				"[REQ %d] attempt error: %v",
				id,
				result.err,
			)
		}

		if result.retryReason != "" {
			log.Printf(
				"[REQ %d] RETRY REASON: %s",
				id,
				result.retryReason,
			)
		}

		if attempt == p.config.MaxAttempts {
			log.Printf(
				"[REQ %d] ============================================",
				id,
			)

			log.Printf(
				"[REQ %d] ALL %d ATTEMPTS FAILED",
				id,
				p.config.MaxAttempts,
			)

			if result.statusCode != 0 {
				result.writeTo(w)
			} else {
				http.Error(
					w,
					"upstream request failed after retries",
					http.StatusBadGateway,
				)
			}

			return
		}

		log.Printf(
			"[REQ %d] retrying in %s",
			id,
			delay,
		)

		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
		case <-r.Context().Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			log.Printf(
				"[REQ %d] client disconnected during retry delay",
				id,
			)

			return
		}

		delay *= 2

		maxDelay := time.Duration(
			p.config.MaxRetryDelay,
		) * time.Second

		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// ============================================================
// SINGLE UPSTREAM ATTEMPT
// ============================================================

func (p *Proxy) attempt(
	r *http.Request,
	body []byte,
	stream bool,
	id uint64,
	attempt int,
) AttemptResult {
	upstreamURL := strings.TrimRight(
		p.config.Upstream,
		"/",
	) + r.URL.Path

	if r.URL.RawQuery != "" {
		upstreamURL += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(
		r.Context(),
		r.Method,
		upstreamURL,
		bytes.NewReader(body),
	)

	if err != nil {
		return AttemptResult{
			err:         err,
			retryReason: "request_creation_error",
		}
	}

	// Copy all safe headers, including Authorization.
	//
	// We NEVER print the Authorization header.
	copyHeaders(
		req.Header,
		r.Header,
	)

	// Prevent compression so that response-body diagnostics
	// remain deterministic.
	req.Header.Set(
		"Accept-Encoding",
		"identity",
	)

	parsedUpstream, err := url.Parse(
		p.config.Upstream,
	)

	if err == nil {
		req.Host = parsedUpstream.Host
	}

	start := time.Now()

	resp, err := p.client.Do(req)

	if err != nil {
		return AttemptResult{
			err:         err,
			retryReason: "transport_error",
		}
	}

	log.Printf(
		"[REQ %d] attempt %d HTTP %d after %s",
		id,
		attempt,
		resp.StatusCode,
		time.Since(start).Round(time.Millisecond),
	)

	log.Printf(
		"[REQ %d] response Content-Type: %s",
		id,
		resp.Header.Get("Content-Type"),
	)

	if stream {
		return p.handleStreamingResponse(
			resp,
			id,
			attempt,
		)
	}

	return p.handleNonStreamingResponse(
		resp,
		id,
		attempt,
	)
}

// ============================================================
// NON-STREAMING RESPONSE
// ============================================================

func (p *Proxy) handleNonStreamingResponse(
	resp *http.Response,
	id uint64,
	attempt int,
) AttemptResult {
	defer resp.Body.Close()

	maxBytes :=
		int64(p.config.MaxBodyMB)*1024*1024 + 1

	data, err := io.ReadAll(
		io.LimitReader(
			resp.Body,
			maxBytes,
		),
	)

	if err != nil {
		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			err:        err,
			retryReason: "response_body_read_error",
		}
	}

	log.Printf(
		"[REQ %d] response body bytes: %d",
		id,
		len(data),
	)

	// --------------------------------------------------------
	// EMPTY BODY
	// --------------------------------------------------------

	if len(data) == 0 {
		log.Printf(
			"[REQ %d] *** RESPONSE BODY IS EMPTY ***",
			id,
		)

		log.Printf(
			"[REQ %d] HTTP status was %d but body contained 0 bytes",
			id,
			resp.StatusCode,
		)

		if resp.StatusCode >= 200 &&
			resp.StatusCode < 300 {
			return AttemptResult{
				statusCode:  resp.StatusCode,
				headers:     resp.Header,
				body:        data,
				success:     false,
				retryReason: "empty_response_body",
			}
		}

		if isRetryableStatus(resp.StatusCode) {
			return AttemptResult{
				statusCode:  resp.StatusCode,
				headers:     resp.Header,
				body:        data,
				success:     false,
				retryReason: "empty_retryable_response",
			}
		}

		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			success:     true,
		}
	}

	// --------------------------------------------------------
	// RETRYABLE HTTP STATUS
	// --------------------------------------------------------

	if isRetryableStatus(resp.StatusCode) {
		log.Printf(
			"[REQ %d] retryable HTTP status %d",
			id,
			resp.StatusCode,
		)

		return AttemptResult{
			statusCode:  resp.StatusCode,
			headers:     resp.Header,
			body:        data,
			success:     false,
			retryReason: fmt.Sprintf(
				"retryable_http_%d",
				resp.StatusCode,
			),
		}
	}

	// --------------------------------------------------------
	// NON-2XX NON-RETRYABLE RESPONSE
	// --------------------------------------------------------

	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		log.Printf(
			"[REQ %d] non-retryable HTTP status %d",
			id,
			resp.StatusCode,
		)

		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			success:     true,
		}
	}

	// --------------------------------------------------------
	// JSON VALIDATION
	// --------------------------------------------------------

	contentType := strings.ToLower(
		resp.Header.Get("Content-Type"),
	)

	if strings.Contains(contentType, "json") {
		valid, reason := validateOpenAIResponse(
			data,
		)

		log.Printf(
			"[REQ %d] JSON validation: valid=%v reason=%s",
			id,
			valid,
			reason,
		)

		if !valid {
			return AttemptResult{
				statusCode:  resp.StatusCode,
				headers:     resp.Header,
				body:        data,
				success:     false,
				retryReason: reason,
			}
		}
	}

	return AttemptResult{
		statusCode: resp.StatusCode,
		headers:    resp.Header,
		body:       data,
		success:     true,
	}
}

// ============================================================
// OPENAI RESPONSE VALIDATION
// ============================================================

func validateOpenAIResponse(
	data []byte,
) (bool, string) {
	var root map[string]interface{}

	if err := json.Unmarshal(
		data,
		&root,
	); err != nil {
		log.Printf(
			"[VALIDATION] malformed JSON",
		)

		return false, "malformed_json"
	}

	choicesRaw, exists := root["choices"]

	// Not every JSON API response necessarily uses choices.
	//
	// Therefore don't reject arbitrary JSON responses merely
	// because choices isn't present.
	if !exists {
		return true, "no_choices_field"
	}

	choices, ok := choicesRaw.([]interface{})

	if !ok {
		return false, "choices_not_array"
	}

	if len(choices) == 0 {
		return false, "empty_choices"
	}

	choice, ok := choices[0].(map[string]interface{})

	if !ok {
		return false, "invalid_first_choice"
	}

	// --------------------------------------------------------
	// CHAT COMPLETION
	// --------------------------------------------------------

	if messageRaw, exists := choice["message"]; exists {
		message, ok := messageRaw.(map[string]interface{})

		if !ok {
			return false, "invalid_message"
		}

		// Text content.
		if content, exists := message["content"]; exists {
			switch value := content.(type) {

			case string:
				if strings.TrimSpace(value) != "" {
					return true, "message_content_present"
				}

			case []interface{}:
				if len(value) > 0 {
					return true, "structured_message_content_present"
				}
			}
		}

		// Tool calls are legitimate output.
		if toolCalls, exists := message["tool_calls"]; exists {
			if arr, ok := toolCalls.([]interface{}); ok &&
				len(arr) > 0 {

				return true, "tool_calls_present"
			}
		}

		// Older function-call format.
		if functionCall, exists := message["function_call"]; exists {
			if functionCall != nil {
				return true, "function_call_present"
			}
		}

		return false, "empty_message_content"
	}

	// --------------------------------------------------------
	// COMPLETION API
	// --------------------------------------------------------

	if text, exists := choice["text"]; exists {
		if s, ok := text.(string); ok &&
			strings.TrimSpace(s) != "" {

			return true, "completion_text_present"
		}

		return false, "empty_completion_text"
	}

	// --------------------------------------------------------
	// DELTA
	// --------------------------------------------------------

	if deltaRaw, exists := choice["delta"]; exists {
		delta, ok := deltaRaw.(map[string]interface{})

		if !ok {
			return false, "invalid_delta"
		}

		if content, exists := delta["content"]; exists {
			if s, ok := content.(string); ok &&
				strings.TrimSpace(s) != "" {

				return true, "delta_content_present"
			}
		}

		if toolCalls, exists := delta["tool_calls"]; exists {
			if arr, ok := toolCalls.([]interface{}); ok &&
				len(arr) > 0 {

				return true, "delta_tool_calls_present"
			}
		}

		if functionCall, exists := delta["function_call"]; exists {
			if functionCall != nil {
				return true, "delta_function_call_present"
			}
		}

		return false, "empty_delta"
	}

	// Unknown choices format.
	//
	// We don't reject it because a provider can extend the
	// OpenAI-compatible schema.
	return true, "choices_present_unknown_format"
}

// ============================================================
// STREAMING RESPONSE
// ============================================================

func (p *Proxy) handleStreamingResponse(
	resp *http.Response,
	id uint64,
	attempt int,
) AttemptResult {
	contentType := strings.ToLower(
		resp.Header.Get("Content-Type"),
	)

	// --------------------------------------------------------
	// RETRYABLE STATUS
	// --------------------------------------------------------

	if resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode >= 500 &&
			resp.StatusCode <= 599) {

		defer resp.Body.Close()

		data, _ := io.ReadAll(
			io.LimitReader(
				resp.Body,
				int64(p.config.MaxBodyMB)*1024*1024,
			),
		)

		log.Printf(
			"[REQ %d] streaming retryable HTTP status %d",
			id,
			resp.StatusCode,
		)

		log.Printf(
			"[REQ %d] error response bytes: %d",
			id,
			len(data),
		)

		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			success:    false,
			retryReason: fmt.Sprintf(
				"stream_retryable_http_%d",
				resp.StatusCode,
			),
		}
	}

	// --------------------------------------------------------
	// OTHER HTTP ERRORS
	// --------------------------------------------------------

	if resp.StatusCode < 200 ||
		resp.StatusCode >= 300 {

		defer resp.Body.Close()

		data, _ := io.ReadAll(
			io.LimitReader(
				resp.Body,
				int64(p.config.MaxBodyMB)*1024*1024,
			),
		)

		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			success:    true,
		}
	}

	// --------------------------------------------------------
	// EXPECT SSE
	// --------------------------------------------------------

	if !strings.Contains(
		contentType,
		"text/event-stream",
	) {
		defer resp.Body.Close()

		data, _ := io.ReadAll(
			io.LimitReader(
				resp.Body,
				int64(p.config.MaxBodyMB)*1024*1024+1,
			),
		)

		log.Printf(
			"[REQ %d] STREAM REQUEST GOT NON-SSE RESPONSE",
			id,
		)

		log.Printf(
			"[REQ %d] non-SSE body bytes: %d",
			id,
			len(data),
		)

		if len(data) == 0 {
			return AttemptResult{
				statusCode:  resp.StatusCode,
				headers:     resp.Header,
				success:     false,
				retryReason: "empty_non_sse_stream_response",
			}
		}

		valid, reason := validateOpenAIResponse(
			data,
		)

		if !valid {
			return AttemptResult{
				statusCode:  resp.StatusCode,
				headers:     resp.Header,
				body:       data,
				success:     false,
				retryReason: "non_sse_" + reason,
			}
		}

		return AttemptResult{
			statusCode: resp.StatusCode,
			headers:    resp.Header,
			body:       data,
			success:     true,
		}
	}

	return p.readAndValidateSSE(
		resp,
		id,
		attempt,
	)
}

// ============================================================
// SSE READER
// ============================================================

func (p *Proxy) readAndValidateSSE(
	resp *http.Response,
	id uint64,
	attempt int,
) AttemptResult {
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)

	// We need to wait for the first meaningful application
	// event without blocking forever.
	type firstEventResult struct {
		event []byte
		err   error
	}

	firstEventCh := make(chan firstEventResult, 1)

	go func() {
		event, err := readUntilMeaningfulSSEEvent(
			reader,
			id,
		)

		firstEventCh <- firstEventResult{
			event: event,
			err:   err,
		}
	}()

	timer := time.NewTimer(
		time.Duration(
			p.config.FirstEventTimeout,
		) * time.Second,
	)

	defer timer.Stop()

	var firstEvent []byte

	select {
	case result := <-firstEventCh:

		if result.err != nil {
			log.Printf(
				"[REQ %d] SSE ended before meaningful content: %v",
				id,
				result.err,
			)

			return AttemptResult{
				statusCode: resp.StatusCode,
				headers:    resp.Header,
				success:    false,
				retryReason: "sse_no_meaningful_first_event",
			}
		}

		firstEvent = result.event

	case <-timer.C:

		log.Printf(
			"[REQ %d] SSE FIRST MEANINGFUL EVENT TIMEOUT after %ds",
			id,
			p.config.FirstEventTimeout,
		)

		// Closing the response body unblocks the goroutine that
		// is currently reading it.
		_ = resp.Body.Close()

		return AttemptResult{
			statusCode:  resp.StatusCode,
			headers:     resp.Header,
			success:     false,
			retryReason: "sse_first_meaningful_event_timeout",
		}
	}

	if len(firstEvent) == 0 {
		return AttemptResult{
			statusCode:  resp.StatusCode,
			headers:     resp.Header,
			success:     false,
			retryReason: "empty_sse_first_event",
		}
	}

	var output bytes.Buffer

	_, _ = output.Write(firstEvent)

	log.Printf(
		"[REQ %d] FIRST MEANINGFUL SSE EVENT RECEIVED: %d bytes",
		id,
		len(firstEvent),
	)

	// IMPORTANT:
	//
	// From this point onward, actual application content has
	// been received.
	//
	// We MUST NOT retry a stream after this point because
	// Claude could already have received partial output.
	//
	// Continue until upstream closes the stream.
	n, err := io.Copy(
		&output,
		reader,
	)

	if err != nil {
		log.Printf(
			"[REQ %d] SSE stream read error AFTER content: %v",
			id,
			err,
		)
	}

	log.Printf(
		"[REQ %d] SSE bytes after first event: %d",
		id,
		n,
	)

	log.Printf(
		"[REQ %d] TOTAL SSE BYTES: %d",
		id,
		output.Len(),
	)

	return AttemptResult{
		statusCode: resp.StatusCode,
		headers:    resp.Header,
		body:       output.Bytes(),
		success:    true,
	}
}

// ============================================================
// READ FIRST MEANINGFUL SSE EVENT
// ============================================================

func readUntilMeaningfulSSEEvent(
	reader *bufio.Reader,
	id uint64,
) ([]byte, error) {
	var event bytes.Buffer

	for {
		line, err := reader.ReadBytes('\n')

		if len(line) > 0 {
			_, _ = event.Write(line)
		}

		if err != nil {
			if errors.Is(err, io.EOF) {

				if event.Len() == 0 {
					return nil, io.EOF
				}

				if isMeaningfulSSEEvent(
					event.Bytes(),
				) {
					return event.Bytes(), nil
				}

				return nil, io.EOF
			}

			return nil, err
		}

		// Blank line terminates an SSE event.
		normalized := strings.ReplaceAll(
			event.String(),
			"\r\n",
			"\n",
		)

		if strings.HasSuffix(
			normalized,
			"\n\n",
		) {

			data := event.Bytes()

			if isMeaningfulSSEEvent(data) {
				return data, nil
			}

			// Role-only / metadata-only / empty event.
			event.Reset()
		}
	}
}

// ============================================================
// MEANINGFUL SSE VALIDATION
// ============================================================

func isMeaningfulSSEEvent(
	event []byte,
) bool {
	text := strings.ReplaceAll(
		string(event),
		"\r\n",
		"\n",
	)

	lines := strings.Split(
		text,
		"\n",
	)

	for _, line := range lines {
		line = strings.TrimSpace(line)

		if !strings.HasPrefix(line, "data:") {
			continue
		}

		data := strings.TrimSpace(
			strings.TrimPrefix(
				line,
				"data:",
			),
		)

		if data == "" {
			continue
		}

		// [DONE] is not application content.
		if data == "[DONE]" {
			continue
		}

		// Try OpenAI-compatible JSON.
		var root map[string]interface{}

		if err := json.Unmarshal(
			[]byte(data),
			&root,
		); err != nil {

			// Non-JSON SSE data can still be legitimate
			// application data.
			return true
		}

		choicesRaw, exists := root["choices"]

		if !exists {
			// Unknown SSE JSON event.
			return true
		}

		choices, ok := choicesRaw.([]interface{})

		if !ok || len(choices) == 0 {
			continue
		}

		choice, ok := choices[0].(map[string]interface{})

		if !ok {
			continue
		}

		// ----------------------------------------------------
		// DELTA
		// ----------------------------------------------------

		if deltaRaw, exists := choice["delta"]; exists {

			delta, ok := deltaRaw.(map[string]interface{})

			if !ok {
				continue
			}

			// Actual generated text.
			if content, exists := delta["content"]; exists {

				if s, ok := content.(string); ok &&
					strings.TrimSpace(s) != "" {

					return true
				}
			}

			// Tool calls.
			if toolCalls, exists := delta["tool_calls"]; exists {

				if arr, ok := toolCalls.([]interface{}); ok &&
					len(arr) > 0 {

					return true
				}
			}

			// Older function call format.
			if functionCall, exists := delta["function_call"]; exists {

				if functionCall != nil {
					return true
				}
			}

			// A delta containing meaningful role-only metadata
			// is NOT enough.
			continue
		}

		// ----------------------------------------------------
		// TEXT COMPLETION
		// ----------------------------------------------------

		if textValue, exists := choice["text"]; exists {

			if s, ok := textValue.(string); ok &&
				strings.TrimSpace(s) != "" {

				return true
			}
		}

		// ----------------------------------------------------
		// MESSAGE
		// ----------------------------------------------------

		if messageRaw, exists := choice["message"]; exists {

			message, ok := messageRaw.(map[string]interface{})

			if !ok {
				continue
			}

			if content, exists := message["content"]; exists {

				if s, ok := content.(string); ok &&
					strings.TrimSpace(s) != "" {

					return true
				}

				if arr, ok := content.([]interface{}); ok &&
					len(arr) > 0 {

					return true
				}
			}

			if toolCalls, exists := message["tool_calls"]; exists {

				if arr, ok := toolCalls.([]interface{}); ok &&
					len(arr) > 0 {

					return true
				}
			}

			continue
		}
	}

	return false
}

// ============================================================
// HELPERS
// ============================================================

func isRetryableStatus(status int) bool {
	switch status {
	case 429, 500, 502, 503, 504:
		return true

	default:
		return false
	}
}

func (a AttemptResult) writeTo(
	w http.ResponseWriter,
) {
	for key, values := range a.headers {

		for _, value := range values {
			w.Header().Add(
				key,
				value,
			)
		}
	}

	if a.statusCode != 0 {
		w.WriteHeader(a.statusCode)
	}

	if len(a.body) > 0 {
		_, _ = w.Write(a.body)
	}
}

func copyHeaders(
	dst http.Header,
	src http.Header,
) {
	for key, values := range src {

		lower := strings.ToLower(key)

		// Hop-by-hop / transport-controlled headers.
		switch lower {

		case "host":
			continue

		case "content-length":
			continue

		case "connection":
			continue

		case "proxy-connection":
			continue

		case "keep-alive":
			continue

		case "transfer-encoding":
			continue

		case "upgrade":
			continue

		case "te":
			continue

		case "trailer":
			continue

		case "accept-encoding":
			continue
		}

		for _, value := range values {
			dst.Add(
				key,
				value,
			)
		}
	}
}

// ============================================================
// CONFIGURATION
// ============================================================

func loadConfig() Config {
	cfg := Config{
		ListenAddr:        "127.0.0.1:20218",
		Upstream:          "https://9router-production-b45b.up.railway.app",
		OutboundProxy:     "http://127.0.0.1:10808",
		MaxAttempts:       8,
		FirstEventTimeout: 30,
		RetryDelay:        1,
		MaxRetryDelay:     8,
		MaxBodyMB:         50,
	}

	// --------------------------------------------------------
	// JSON CONFIG
	// --------------------------------------------------------

	if path := os.Getenv(
		"RELIABILITY_PROXY_CONFIG",
	); path != "" {

		data, err := os.ReadFile(path)

		if err != nil {
			log.Printf(
				"WARNING: could not read config file: %v",
				err,
			)
		} else {

			if err := json.Unmarshal(
				data,
				&cfg,
			); err != nil {

				log.Printf(
					"WARNING: could not parse config file: %v",
					err,
				)
			}
		}
	}

	// --------------------------------------------------------
	// ENVIRONMENT OVERRIDES
	// --------------------------------------------------------

	if value := os.Getenv("PROXY_LISTEN"); value != "" {
		cfg.ListenAddr = value
	}

	if value := os.Getenv("UPSTREAM_URL"); value != "" {
		cfg.Upstream = value
	}

	if value := os.Getenv("OUTBOUND_PROXY"); value != "" {
		cfg.OutboundProxy = value
	}

	if value := os.Getenv("MAX_ATTEMPTS"); value != "" {

		if n, err := strconv.Atoi(value); err == nil {
			cfg.MaxAttempts = n
		}
	}

	if value := os.Getenv("FIRST_EVENT_TIMEOUT"); value != "" {

		if n, err := strconv.Atoi(value); err == nil {
			cfg.FirstEventTimeout = n
		}
	}

	if value := os.Getenv("RETRY_DELAY"); value != "" {

		if n, err := strconv.Atoi(value); err == nil {
			cfg.RetryDelay = n
		}
	}

	if value := os.Getenv("MAX_RETRY_DELAY"); value != "" {

		if n, err := strconv.Atoi(value); err == nil {
			cfg.MaxRetryDelay = n
		}
	}

	if value := os.Getenv("MAX_BODY_MB"); value != "" {

		if n, err := strconv.Atoi(value); err == nil {
			cfg.MaxBodyMB = n
		}
	}

	return cfg
}
