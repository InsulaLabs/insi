package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net" // Added for SplitHostPort and JoinHostPort
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/models" // Assuming models.Event is defined here
	"github.com/gorilla/websocket"
)

const (
	defaultTimeout = 10 * time.Second
)

type ConnectionType string

const (
	ConnectionTypeDirect ConnectionType = "direct"
	ConnectionTypeRandom ConnectionType = "random"
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrTokenNotFound  = errors.New("token not found")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrAPIKeyNotFound = errors.New("api key not found") // New distinct error
)

type Endpoint struct {
	HostPort     string
	ClientDomain string
	Logger       *slog.Logger
}

type Config struct {
	ConnectionType ConnectionType // Direct will use Endpoints[0] always
	Endpoints      []Endpoint
	ApiKey         string
	SkipVerify     bool
	Timeout        time.Duration
	Logger         *slog.Logger
}

type ErrorResponse struct {
	ErrorType string `json:"error_type"`
	Message   string `json:"message"`
}

// Client is the API client for the insi service.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	apiKey     string
	logger     *slog.Logger
}

// NewClient creates a new insi API client.
func NewClient(cfg *Config) (*Client, error) {

	for _, endpoint := range cfg.Endpoints {
		if endpoint.HostPort == "" {
			return nil, fmt.Errorf("hostPort cannot be empty")
		}
	}

	if cfg.ApiKey == "" {
		return nil, fmt.Errorf("apiKey cannot be empty")
	}
	clientLogger := cfg.Logger.WithGroup("insi_client")

	connectHost := ""
	connectPort := ""
	connectClientDomain := ""

	if cfg.ConnectionType == ConnectionTypeDirect {
		connectHost = cfg.Endpoints[0].HostPort
		connectClientDomain = cfg.Endpoints[0].ClientDomain
	} else if cfg.ConnectionType == ConnectionTypeRandom {
		connectHost = cfg.Endpoints[rand.Intn(len(cfg.Endpoints))].HostPort
		connectClientDomain = cfg.Endpoints[rand.Intn(len(cfg.Endpoints))].ClientDomain
	}

	// Parse port from HostPort. HostPort might be IP:port or Domain:port.
	_, parsedPort, err := net.SplitHostPort(connectHost)
	if err != nil {
		clientLogger.Error("Failed to parse port from HostPort", "hostPort", connectHost, "error", err)
		return nil, fmt.Errorf("failed to parse port from HostPort '%s': %w", connectHost, err)
	}
	connectPort = parsedPort

	// Determine the host for the connection URL.
	if connectClientDomain != "" {
		connectHost = connectClientDomain // Prefer ClientDomain if available
		clientLogger.Info("Using ClientDomain for connection URL host", "domain", connectClientDomain)
	} else {
		// Fallback to host from HostPort if ClientDomain is not set.
		hostFromHostPort, _, _ := net.SplitHostPort(connectHost) // Error already handled for port, ignore here for host.
		connectHost = hostFromHostPort
		clientLogger.Info("Using host from HostPort for connection URL (ClientDomain not provided)", "host", connectHost)
	}

	// We ENFORCE HTTPS - NEVER PERMIT HTTP
	finalConnectAddress := net.JoinHostPort(connectHost, connectPort)
	baseURLStr := fmt.Sprintf("https://%s", finalConnectAddress)
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		clientLogger.Error("Failed to parse base URL", "url", baseURLStr, "error", err)
		return nil, fmt.Errorf("failed to parse base URL '%s': %w", baseURLStr, err)
	}

	tlsClientCfg := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify,
	}

	if !cfg.SkipVerify {
		// ServerName for TLS verification should be the host we are connecting to.
		// By leaving tlsClientCfg.ServerName as "" (its zero value), the crypto/tls
		// package will automatically use the host from the dial address for SNI
		// and certificate validation. This works correctly across redirects.
		// Our `connectHost` (derived from ClientDomain or HostPort) is already used in the baseURL.
		// tlsClientCfg.ServerName = connectHost // REMOVED: Let crypto/tls handle it based on target host
		clientLogger.Info("TLS verification active. ServerName for SNI will be derived from target URL host.", "initial_target_host", connectHost)
	} else {
		clientLogger.Info("TLS verification is skipped.")
	}

	transport := &http.Transport{
		TLSClientConfig: tlsClientCfg,
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	clientLogger.Info("Insi client initialized", "base_url", baseURL.String(), "tls_skip_verify", cfg.SkipVerify /*, "explicit_tls_server_name_set", tlsClientCfg.ServerName != "" */) // tlsClientCfg.ServerName will be empty

	return &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
		apiKey:     cfg.ApiKey,
		logger:     clientLogger,
	}, nil
}

// internal request helper
func (c *Client) doRequest(method, path string, queryParams map[string]string, body interface{}, target interface{}) error {
	originalMethod := method // Save original method to preserve it across redirects

	// Construct the initial URL
	initialPathURL := &url.URL{Path: path}
	currentReqURL := c.baseURL.ResolveReference(initialPathURL)

	// Apply query parameters
	if len(queryParams) > 0 {
		q := currentReqURL.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		currentReqURL.RawQuery = q.Encode()
	}

	var reqBodyBytes []byte
	var err error
	if body != nil {
		reqBodyBytes, err = json.Marshal(body)
		if err != nil {
			c.logger.Error("Failed to marshal request body", "path", path, "method", originalMethod, "error", err)
			return fmt.Errorf("failed to marshal request body for %s %s: %w", originalMethod, path, err)
		}
	}

	var lastResp *http.Response // To store the response if loop finishes due to too many redirects

	for redirects := 0; redirects < 10; redirects++ { // Max 10 redirects
		reqBodyReader := bytes.NewBuffer(reqBodyBytes) // Can be re-read

		req, err := http.NewRequest(originalMethod, currentReqURL.String(), reqBodyReader)
		if err != nil {
			c.logger.Error("Failed to create new HTTP request", "method", originalMethod, "url", currentReqURL.String(), "error", err)
			return fmt.Errorf("failed to create request %s %s: %w", originalMethod, currentReqURL.String(), err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", c.apiKey)
		// req.Host is set by the transport based on req.URL.Host

		c.logger.Debug("Sending request", "method", originalMethod, "url", currentReqURL.String(), "attempt", redirects+1)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.logger.Error("HTTP request failed", "method", originalMethod, "url", currentReqURL.String(), "error", err)
			// Close previous response body if any, though `err` here means `resp` is likely nil
			if lastResp != nil && lastResp.Body != nil {
				lastResp.Body.Close()
			}
			return fmt.Errorf("http request %s %s failed: %w", originalMethod, currentReqURL.String(), err)
		}
		if lastResp != nil && lastResp.Body != nil { // Clean up body from previous iteration if any
			lastResp.Body.Close()
		}
		lastResp = resp // Store current response

		// Check for redirect status codes
		switch resp.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, // 301, 302, 303
			http.StatusTemporaryRedirect, http.StatusPermanentRedirect: // 307, 308

			loc := resp.Header.Get("Location")
			if loc == "" {
				resp.Body.Close() // Close current redirect response body
				c.logger.Error("Redirect response missing Location header", "status_code", resp.StatusCode, "url", currentReqURL.String())
				return fmt.Errorf("redirect (status %d) missing Location header from %s", resp.StatusCode, currentReqURL.String())
			}

			redirectURL, err := currentReqURL.Parse(loc) // Resolve Location relative to currentReqURL
			if err != nil {
				resp.Body.Close() // Close current redirect response body
				c.logger.Error("Failed to parse redirect Location header", "location", loc, "error", err)
				return fmt.Errorf("failed to parse redirect Location '%s': %w", loc, err)
			}

			c.logger.Info("Request redirected",
				"from_url", currentReqURL.String(),
				"to_url", redirectURL.String(),
				"status_code", resp.StatusCode,
				"method", originalMethod)

			currentReqURL = redirectURL // Update URL for the next iteration
			resp.Body.Close()           // Close current redirect response body before continuing
			continue                    // Continue to the next iteration of the loop for the redirect
		}

		// Not a redirect, or unhandled status by the loop; this is the final response to process.
		defer resp.Body.Close() // Ensure this final response body is closed when function returns

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.logger.Warn("Received non-2xx status code", "method", originalMethod, "url", currentReqURL.String(), "status_code", resp.StatusCode)

			// Attempt to read error body for more details
			var errorResp ErrorResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			if readErr == nil {
				if jsonErr := json.Unmarshal(bodyBytes, &errorResp); jsonErr == nil {
					c.logger.Debug("Parsed JSON error response from server", "error_type", errorResp.ErrorType, "message", errorResp.Message)
					if errorResp.ErrorType == "API_KEY_NOT_FOUND" { // Matches the error_type from authedPing (if it could send it)
						return ErrAPIKeyNotFound
					}
					// Return a generic error with the server's message if available
					return fmt.Errorf("server error (status %d): %s - %s", resp.StatusCode, errorResp.ErrorType, errorResp.Message)
				}
			}
			// Fallback if error body can't be parsed or isn't JSON
			return fmt.Errorf("server returned status %d for %s %s", resp.StatusCode, originalMethod, currentReqURL.String())
		}

		if target != nil {
			if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
				c.logger.Error("Failed to decode response body", "method", originalMethod, "url", currentReqURL.String(), "status_code", resp.StatusCode, "error", err)
				return fmt.Errorf("failed to decode response body for %s %s (status %d): %w", originalMethod, currentReqURL.String(), resp.StatusCode, err)
			}
		}
		c.logger.Debug("Request successful", "method", originalMethod, "url", currentReqURL.String(), "status_code", resp.StatusCode)
		return nil // Success
	}

	// If loop finishes, it means too many redirects
	if lastResp != nil && lastResp.Body != nil {
		lastResp.Body.Close()
	}
	c.logger.Error("Too many redirects", "final_url_attempt", currentReqURL.String(), "original_method", originalMethod)
	return fmt.Errorf("stopped after %d redirects, last URL: %s", 10, currentReqURL.String())
}

// --- Value Operations ---

// Get retrieves a value for a given key.
func (c *Client) Get(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key cannot be empty")
	}
	params := map[string]string{"key": key}
	var response struct {
		Data string `json:"data"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/get", params, nil, &response)
	if err != nil {
		if strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "key not found") {
			return "", ErrKeyNotFound
		}
		return "", err
	}
	return response.Data, nil
}

// Set sets a value for a given key.
func (c *Client) Set(key, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key, "value": value}
	return c.doRequest(http.MethodPost, "db/api/v1/set", nil, payload, nil)
}

// Delete removes a key and its value.
func (c *Client) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key}
	err := c.doRequest(http.MethodPost, "db/api/v1/delete", nil, payload, nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "key not found") {
			// If the key is not found, we return nil
			return nil
		}
		return err
	}
	return nil
}

// IterateByPrefix retrieves a list of keys matching a given prefix.
func (c *Client) IterateByPrefix(prefix string, offset, limit int) ([]string, error) {
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}
	params := map[string]string{
		"prefix": prefix,
		"offset": strconv.Itoa(offset),
		"limit":  strconv.Itoa(limit),
	}
	var response struct {
		Data []string `json:"data"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/iterate/prefix", params, nil, &response)
	if err != nil {
		if strings.Contains(err.Error(), "404") ||
			strings.Contains(err.Error(), "key not found") {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return response.Data, nil
}

// --- Cache Operations ---

// SetCache stores a key-value pair in the cache with a specific TTL.
func (c *Client) SetCache(key, value string, ttl time.Duration) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]interface{}{
		"key":   key,
		"value": value,
		"ttl":   ttl.Seconds(), // Server expects TTL in seconds
	}
	return c.doRequest(http.MethodPost, "db/api/v1/cache/set", nil, payload, nil)
}

// GetCache retrieves a value from the cache by its key.
func (c *Client) GetCache(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key cannot be empty")
	}
	params := map[string]string{"key": key}
	var response struct {
		Data string `json:"data"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/cache/get", params, nil, &response)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return "", ErrKeyNotFound
		}
		return "", err
	}
	return response.Data, nil
}

// DeleteCache removes a key-value pair from the cache.
func (c *Client) DeleteCache(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key}
	err := c.doRequest(http.MethodPost, "db/api/v1/cache/delete", nil, payload, nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return ErrKeyNotFound
		}
		return err
	}
	return nil
}

func (c *Client) PublishEvent(topic string, data any) error {
	payload := map[string]interface{}{
		"topic": topic,
		"data":  data,
	}
	return c.doRequest(http.MethodPost, "db/api/v1/events", nil, payload, nil)
}

// --- System Operations ---

// Join requests the target node (which must be a leader) to add a new follower to the Raft cluster.
func (c *Client) Join(followerID, followerAddr string) error {
	if followerID == "" {
		return fmt.Errorf("followerID cannot be empty")
	}
	if followerAddr == "" {
		return fmt.Errorf("followerAddr cannot be empty")
	}
	params := map[string]string{
		"followerId":   followerID, // Note: server uses followerId (lowercase d)
		"followerAddr": followerAddr,
	}
	// The /join endpoint is a GET request but needs auth and carries parameters in the query string.
	return c.doRequest(http.MethodGet, "db/api/v1/join", params, nil, nil)
}

// Ping sends a ping request to the server and returns the response.
func (c *Client) Ping() (map[string]string, error) {
	var response map[string]string
	err := c.doRequest(http.MethodGet, "db/api/v1/ping", nil, nil, &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

// SubscribeToEvents connects to the event subscription WebSocket endpoint and prints incoming events.
func (c *Client) SubscribeToEvents(topic string, ctx context.Context, onEvent func(data any)) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	// Construct WebSocket URL. Example: ws://localhost:8080/db/api/v1/events/subscribe?topic=MY_TOPIC&token=API_KEY
	// Note: The server must handle the "token" query parameter for authentication.
	wsScheme := "ws"
	if c.baseURL.Scheme == "https" {
		wsScheme = "wss"
	}

	// hostPort is derived from c.baseURL.Host which already contains host:port
	wsURL := url.URL{
		Scheme: wsScheme,
		Host:   c.baseURL.Host, // c.baseURL.Host already has host:port
		Path:   "/db/api/v1/events/subscribe",
	}
	query := wsURL.Query()
	query.Set("topic", topic)
	query.Set("token", c.apiKey) // Pass API key as a query parameter for WebSocket authentication
	wsURL.RawQuery = query.Encode()

	c.logger.Info("Attempting to connect to WebSocket for event subscription", "url", wsURL.String())

	// Prepare headers if needed, though gorilla/websocket might not use http.Client's headers directly
	// For token authentication via query param, headers might not be strictly necessary for WebSocket handshake itself
	// but server-side WebSocket upgrader can inspect the initial HTTP request.
	header := http.Header{}
	header.Set("Authorization", c.apiKey) // Standard Authorization header, server might check this during upgrade

	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: 10 * time.Second, // Example timeout
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: c.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify,
		},
	}

	conn, resp, err := dialer.Dial(wsURL.String(), header)
	if err != nil {
		errMsg := fmt.Sprintf("failed to dial websocket %s", wsURL.String())
		if resp != nil {
			// Try to read body for more details if connection was partially made
			// bodyBytes, readErr := io.ReadAll(resp.Body)
			// if readErr == nil {
			//  errMsg = fmt.Sprintf("%s: %s (status: %s)", errMsg, string(bodyBytes), resp.Status)
			// } else {
			errMsg = fmt.Sprintf("%s: (status: %s), error: %v", errMsg, resp.Status, err)
			// }
			// resp.Body.Close()
			c.logger.Error("WebSocket dial error with response", "url", wsURL.String(), "status", resp.Status, "error", err)
			return fmt.Errorf("%s: %w", errMsg, err)

		}
		c.logger.Error("WebSocket dial error", "url", wsURL.String(), "error", err)
		return fmt.Errorf("%s: %w", errMsg, err)
	}
	defer conn.Close()

	// Set a read deadline for the connection to prevent indefinite blocking if the server stops sending messages.
	// This is important for graceful shutdown and resource management.
	// conn.SetReadDeadline(time.Now().Add(readWait)) // readWait could be e.g. 60 seconds
	// To keep the connection open indefinitely without read timeouts (as requested for "does not timeout from inactivity"):
	// conn.SetReadDeadline(time.Time{}) // Zero time value means no deadline

	// Handle pong messages to keep the connection alive if the server sends pings
	conn.SetPongHandler(func(string) error {
		c.logger.Debug("Received pong from server")
		// conn.SetReadDeadline(time.Now().Add(pongWait)) // Reset read deadline after pong
		// For no timeout: conn.SetReadDeadline(time.Time{})
		return nil
	})

	// Periodically send pings to the server to keep the connection alive
	// This helps prevent proxies or the server itself from closing the connection due to inactivity.
	go func() {
		ticker := time.NewTicker(30 * time.Second) // Example: send a ping every 30 seconds
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.logger.Debug("Sending ping to server")
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					c.logger.Error("Error sending ping", "error", err)
					return // Stop pinging if there's an error
				}
			case <-ctx.Done(): // Listen for context cancellation
				c.logger.Info("Context cancelled, closing WebSocket ping loop.")
				// Attempt to send a close message to the server.
				err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					c.logger.Error("Error sending close message during ping loop shutdown", "error", err)
				}
				return
			}
		}
	}()

	c.logger.Info("Successfully connected to WebSocket. Listening for events...", "topic", topic)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled, closing WebSocket read loop.")
			// Attempt to send a close message to the server.
			err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			if err != nil {
				c.logger.Error("Error sending close message during read loop shutdown", "error", err)
			}
			return ctx.Err() // Return context error
		default:
			// Set a short read deadline to allow checking ctx.Done() periodically
			// This is a common pattern for interruptible blocking reads.
			// If we want NO timeout at all, this is trickier with select.
			// For now, let's proceed without a read deadline in the loop for simplicity,
			// relying on the ping/pong and server closing the connection if needed.
			// conn.SetReadDeadline(time.Now().Add(1 * time.Second)) // Example: 1 second timeout

			messageType, message, err := conn.ReadMessage()
			if err != nil {
				// Check if the error is due to context cancellation or a normal close
				if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) && err != context.Canceled {
					c.logger.Error("Error reading message from WebSocket", "error", err)
				} else {
					c.logger.Info("WebSocket connection closed gracefully or context cancelled.", "error", err)
				}
				return err // Exit loop on error or normal closure
			}

			if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				var event models.Event
				if err := json.Unmarshal(message, &event); err != nil {
					c.logger.Error("Failed to unmarshal event message", "error", err, "message", string(message))
					continue // Skip this message
				}
				if onEvent != nil {
					onEvent(event.Data)
				}
			}
		}
	}
}

// --- Batch Value Operations ---

// BatchSet sends a batch of key-value pairs to be set.
func (c *Client) BatchSet(items []models.KVPayload) error {
	if len(items) == 0 {
		return fmt.Errorf("items slice cannot be empty for BatchSet")
	}
	// The server endpoint for batch set is "/db/api/v1/batchset"
	// The payload should be `BatchSetRequest struct { Items []models.KVPayload }`
	// However, the FSM's BatchSet expects []models.KVPayload directly.
	// The routes_w.go batchSetHandler now expects a JSON body like:
	// `type BatchSetRequest struct { Items []models.KVPayload `json:"items"` }`

	requestPayload := struct {
		Items []models.KVPayload `json:"items"`
	}{Items: items}

	return c.doRequest(http.MethodPost, "db/api/v1/batchset", nil, requestPayload, nil)
}

// BatchDelete sends a batch of keys to be deleted.
func (c *Client) BatchDelete(keys []string) error {
	if len(keys) == 0 {
		return fmt.Errorf("keys slice cannot be empty for BatchDelete")
	}
	// The server endpoint for batch delete is "/db/api/v1/batchdelete"
	// The routes_w.go batchDeleteHandler expects a JSON body like:
	// `type BatchDeleteRequest struct { Keys []string `json:"keys"`	}`

	requestPayload := struct {
		Keys []string `json:"keys"`
	}{Keys: keys}
	return c.doRequest(http.MethodPost, "db/api/v1/batchdelete", nil, requestPayload, nil)
}

// --- Atomic Operations ---

// AtomicNew creates a new atomic counter or resets an existing one if overwrite is true.
func (c *Client) AtomicNew(key string, overwrite bool) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty for AtomicNew")
	}
	payload := models.AtomicNewRequest{
		Key:       key,
		Overwrite: overwrite,
	}
	err := c.doRequest(http.MethodPost, "db/api/v1/atomic/new", nil, payload, nil)
	if err != nil {
		if strings.Contains(err.Error(), "409") {
			return fmt.Errorf("atomic key '%s' likely already exists and overwrite was false: %w", key, err)
		}
		return err
	}
	return nil
}

// AtomicGet retrieves the current value of an atomic counter.
// Returns 0 if the key does not exist (server handles this by returning 0 value).
func (c *Client) AtomicGet(key string) (int64, error) {
	if key == "" {
		return 0, fmt.Errorf("key cannot be empty for AtomicGet")
	}
	params := map[string]string{"key": key}
	var response models.AtomicGetResponse
	err := c.doRequest(http.MethodGet, "db/api/v1/atomic/get", params, nil, &response)
	if err != nil {
		// Server returns 404 for non-existent keys in GET, but AtomicGet on server should return 0 for non-existent.
		// Server returns 409 for tkv.ErrInvalidState.
		if strings.Contains(err.Error(), "404") { // Should not happen if server adheres to AtomicGet spec (0 for not found)
			c.logger.Warn("AtomicGet received 404, expecting server to handle non-existent key by returning 0 value", "key", key)
			return 0, ErrKeyNotFound
		}
		if strings.Contains(err.Error(), "409") {
			return 0, fmt.Errorf("atomic key '%s' has invalid state (not an int64): %w", key, err)
		}
		return 0, err
	}
	return response.Value, nil
}

// AtomicAdd adds a delta to an atomic counter and returns the new value.
func (c *Client) AtomicAdd(key string, delta int64) (int64, error) {
	if key == "" {
		return 0, fmt.Errorf("key cannot be empty for AtomicAdd")
	}
	payload := models.AtomicAddRequest{
		Key:   key,
		Delta: delta,
	}
	var response models.AtomicAddResponse
	err := c.doRequest(http.MethodPost, "db/api/v1/atomic/add", nil, payload, &response)
	if err != nil {
		// Server returns 409 for tkv.ErrInvalidState.
		if strings.Contains(err.Error(), "409") {
			return 0, fmt.Errorf("atomic key '%s' has invalid state (not an int64) for add: %w", key, err)
		}
		return 0, err
	}
	return response.NewValue, nil
}

// AtomicDelete deletes an atomic counter.
func (c *Client) AtomicDelete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty for AtomicDelete")
	}
	payload := models.AtomicKeyPayload{Key: key} // Server expects a body with the key
	err := c.doRequest(http.MethodPost, "db/api/v1/atomic/delete", nil, payload, nil)
	if err != nil {
		// Server returns 200 OK even if key doesn't exist, so 404 shouldn't happen for "not found".
		// Any error here is likely a server-side processing issue or network problem.
		return err
	}
	return nil
}

// --- Queue Operations ---

// QueueNew creates a new in-memory queue on the server.
func (c *Client) QueueNew(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty for QueueNew")
	}
	payload := models.QueueNewRequest{Key: key}
	// Server endpoint: /db/api/v1/queue/new
	return c.doRequest(http.MethodPost, "db/api/v1/queue/new", nil, payload, nil)
}

// QueuePush pushes a value onto an in-memory queue and returns the new length of the queue.
func (c *Client) QueuePush(key string, value string) (int, error) {
	if key == "" {
		return 0, fmt.Errorf("key cannot be empty for QueuePush")
	}
	payload := models.QueuePushRequest{Key: key, Value: value}
	var response models.QueuePushResponse
	// Server endpoint: /db/api/v1/queue/push
	err := c.doRequest(http.MethodPost, "db/api/v1/queue/push", nil, payload, &response)
	if err != nil {
		if strings.Contains(err.Error(), "404") { // Assuming 404 for ErrQueueNotFound from server
			return 0, fmt.Errorf("queue '%s' not found: %w", key, ErrKeyNotFound) // Wrap with more specific error if desired
		}
		return 0, err
	}
	return response.NewLength, nil
}

// QueuePop removes and returns the first value from an in-memory queue.
func (c *Client) QueuePop(key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("key cannot be empty for QueuePop")
	}
	payload := models.QueueKeyPayload{Key: key} // Pop request uses QueueKeyPayload
	var response models.QueuePopResponse
	// Server endpoint: /db/api/v1/queue/pop
	err := c.doRequest(http.MethodPost, "db/api/v1/queue/pop", nil, payload, &response)
	if err != nil {
		// Server might return 404 for both ErrQueueNotFound and ErrQueueEmpty
		// The error message from the server (e.g., "queue 'X' not found" or "queue 'X' is empty")
		// will be part of the error returned by doRequest if it parses the server error correctly.
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "is empty") {
			// We can make this more specific if server guarantees distinct error messages or codes
			// For now, returning a generic error that includes the server message seems reasonable.
			return "", fmt.Errorf("failed to pop from queue '%s': %w", key, err)
		}
		return "", err
	}
	return response.Value, nil
}

// QueueDelete deletes an in-memory queue from the server.
func (c *Client) QueueDelete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty for QueueDelete")
	}
	payload := models.QueueDeleteRequest{Key: key}
	// Server endpoint: /db/api/v1/queue/delete
	return c.doRequest(http.MethodPost, "db/api/v1/queue/delete", nil, payload, nil)
}
