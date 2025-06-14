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

	"github.com/InsulaLabs/insi/db/models" // Assuming models.Event is defined here
	"github.com/gorilla/websocket"

	"crypto/sha256"
	"encoding/hex"
	"mime/multipart"
	"os"
	"path/filepath"
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
	ErrConflict       = errors.New("operation failed due to a conflict (e.g., key exists for setnx, or cas failed)")
	ErrTokenNotFound  = errors.New("token not found")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrAPIKeyNotFound = errors.New("api key not found") // New distinct error
)

// ObjectUploadResponse is the response from a successful object upload.
type ObjectUploadResponse struct {
	Status           string `json:"status,omitempty"`
	ObjectID         string `json:"objectID,omitempty"`
	Message          string `json:"message,omitempty"`
	ClientSha256     string `json:"clientSha256,omitempty"`
	CalculatedSha256 string `json:"calculatedSha256,omitempty"`
}

// ObjectHashResponse is the response from an object hash request.
type ObjectHashResponse struct {
	ObjectID string `json:"objectID"`
	Sha256   string `json:"sha256"`
}

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
		clientLogger.Debug("Using ClientDomain for connection URL host", "domain", connectClientDomain)
	} else {
		// Fallback to host from HostPort if ClientDomain is not set.
		hostFromHostPort, _, _ := net.SplitHostPort(connectHost) // Error already handled for port, ignore here for host.
		connectHost = hostFromHostPort
		clientLogger.Debug("Using host from HostPort for connection URL (ClientDomain not provided)", "host", connectHost)
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
		clientLogger.Debug("TLS verification active. ServerName for SNI will be derived from target URL host.", "initial_target_host", connectHost)
	} else {
		clientLogger.Debug("TLS verification is skipped.")
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

	clientLogger.Debug("Insi client initialized", "base_url", baseURL.String(), "tls_skip_verify", cfg.SkipVerify /*, "explicit_tls_server_name_set", tlsClientCfg.ServerName != "" */) // tlsClientCfg.ServerName will be empty

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

		if resp.StatusCode == http.StatusNotFound {
			// For specific GET operations, a 404 is a semantic "not found" for the resource.
			isDataGetOperation := method == http.MethodGet &&
				(strings.HasPrefix(path, "db/api/v1/get") ||
					strings.HasPrefix(path, "db/api/v1/cache/get") ||
					strings.HasPrefix(path, "db/api/v1/atomic/get") ||
					strings.HasPrefix(path, "db/api/v1/iterate"))

			if isDataGetOperation {
				return ErrKeyNotFound
			}

			// For all other operations (POST, DELETE, other GETs), a 404 implies the endpoint itself was not found.
			bodyBytes, _ := io.ReadAll(resp.Body) // Read body for context
			return fmt.Errorf("endpoint not found (404) at %s: %s", currentReqURL.String(), string(bodyBytes))
		}

		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusPreconditionFailed {
			bodyBytes, _ := io.ReadAll(resp.Body) // Read body for specific conflict reason
			c.logger.Warn("Conflict/Precondition failed response from server", "url", currentReqURL.String(), "status", resp.StatusCode, "body", string(bodyBytes))
			// The body contains a useful message like "key '...' already exists"
			// But for programmatic checking, we return a typed error.
			return ErrConflict
		}

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

// SetNX sets a value for a given key, but only if the key does not already exist.
func (c *Client) SetNX(key, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key, "value": value}
	return c.doRequest(http.MethodPost, "db/api/v1/setnx", nil, payload, nil)
}

// CompareAndSwap atomically swaps the value of a key from an old value to a new value.
// The operation fails if the current value does not match the old value.
func (c *Client) CompareAndSwap(key, oldValue, newValue string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := models.CASPayload{
		Key:      key,
		OldValue: oldValue,
		NewValue: newValue,
	}
	return c.doRequest(http.MethodPost, "db/api/v1/cas", nil, payload, nil)
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
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return response.Data, nil
}

// --- Cache Operations ---

// SetCache stores a key-value pair in the cache.
func (c *Client) SetCache(key, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]interface{}{
		"key":   key,
		"value": value,
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

// SetCacheNX sets a value in the cache, but only if the key does not already exist.
func (c *Client) SetCacheNX(key, value string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key, "value": value}
	return c.doRequest(http.MethodPost, "db/api/v1/cache/setnx", nil, payload, nil)
}

// CompareAndSwapCache atomically swaps the value of a key in the cache.
func (c *Client) CompareAndSwapCache(key, oldValue, newValue string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := models.CASPayload{
		Key:      key,
		OldValue: oldValue,
		NewValue: newValue,
	}
	return c.doRequest(http.MethodPost, "db/api/v1/cache/cas", nil, payload, nil)
}

// IterateCacheByPrefix retrieves a list of keys from the cache matching a given prefix.
func (c *Client) IterateCacheByPrefix(prefix string, offset, limit int) ([]string, error) {
	if prefix == "" {
		return nil, fmt.Errorf("prefix cannot be empty")
	}
	params := map[string]string{
		"prefix": prefix,
		"offset": strconv.Itoa(offset),
		"limit":  strconv.Itoa(limit),
	}
	var response []string
	err := c.doRequest(http.MethodGet, "db/api/v1/cache/iterate/prefix", params, nil, &response)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return response, nil
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

// --- Admin Operations ---

// CreateAPIKey requests the server to create a new API key.
// This operation typically requires root privileges.
func (c *Client) CreateAPIKey(keyName string) (*models.ApiKeyCreateResponse, error) {
	if keyName == "" {
		return nil, fmt.Errorf("keyName cannot be empty for CreateAPIKey")
	}
	requestPayload := models.ApiKeyCreateRequest{KeyName: keyName}
	var response models.ApiKeyCreateResponse

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/api/create", nil, requestPayload, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteAPIKey requests the server to delete an existing API key.
// This operation typically requires root privileges.
func (c *Client) DeleteAPIKey(apiKey string) error {
	if apiKey == "" {
		return fmt.Errorf("apiKey cannot be empty for DeleteAPIKey")
	}
	requestPayload := models.ApiKeyDeleteRequest{Key: apiKey}

	// Expects 200 OK on success, no specific response body to decode beyond error handling.
	err := c.doRequest(http.MethodPost, "db/api/v1/admin/api/delete", nil, requestPayload, nil)
	if err != nil {
		// If server returns 404 specifically for API key not found during delete,
		// we could translate it to ErrAPIKeyNotFound or similar, but generic error is also fine.
		return err
	}
	return nil
}

// GetLimits retrieves the usage limits and current usage for the API key used by the client.
func (c *Client) GetLimits() (*models.LimitsResponse, error) {
	var response models.LimitsResponse
	err := c.doRequest(http.MethodGet, "db/api/v1/limits", nil, nil, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// SetLimits sets the usage limits for a given API key.
// This operation typically requires root privileges.
func (c *Client) SetLimits(apiKey string, limits models.Limits) error {
	if apiKey == "" {
		return fmt.Errorf("apiKey cannot be empty for SetLimits")
	}
	requestPayload := models.SetLimitsRequest{
		ApiKey: apiKey,
		Limits: &limits,
	}

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/limits/set", nil, requestPayload, nil)
	if err != nil {
		return err
	}
	return nil
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

// --- Object Operations ---

// ObjectUpload uploads a file to the object storage.
func (c *Client) ObjectUpload(filePath string) (*ObjectUploadResponse, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %w", filePath, err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error getting file info for %s: %w", filePath, err)
	}
	fileSize := fileInfo.Size()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		file.Close()
		return nil, fmt.Errorf("error calculating SHA256 hash for %s: %w", filePath, err)
	}
	clientCalculatedSha256 := hex.EncodeToString(hasher.Sum(nil))
	c.logger.Info("Calculated SHA256", "file", filepath.Base(filePath), "sha256", clientCalculatedSha256)

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return nil, fmt.Errorf("error seeking to start of file %s: %w", filePath, err)
	}
	defer file.Close()

	pipeReader, pipeWriter := io.Pipe()
	mpWriter := multipart.NewWriter(pipeWriter)

	errChan := make(chan error, 1)

	go func() {
		defer pipeWriter.Close()
		defer mpWriter.Close()

		if err := mpWriter.WriteField("clientSha256", clientCalculatedSha256); err != nil {
			errChan <- fmt.Errorf("error writing clientSha256 field: %w", err)
			return
		}

		part, err := mpWriter.CreateFormFile("file", filepath.Base(filePath))
		if err != nil {
			errChan <- fmt.Errorf("error creating form file: %w", err)
			return
		}

		progressReader := newProgressReader(file, fileSize, "Uploading", filePath)
		if _, err = io.Copy(part, progressReader); err != nil {
			errChan <- fmt.Errorf("error copying file content to form: %w", err)
			return
		}
		errChan <- nil
	}()

	uploadURL := c.baseURL.ResolveReference(&url.URL{Path: "objects/upload"})
	req, err := http.NewRequest(http.MethodPost, uploadURL.String(), pipeReader)
	if err != nil {
		pipeWriter.CloseWithError(err)
		return nil, fmt.Errorf("error creating POST request: %w", err)
	}

	req.Header.Set("Authorization", c.apiKey)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())

	// Use a client with a longer timeout for uploads.
	uploadClient := *c.httpClient
	uploadClient.Timeout = 30 * time.Minute

	resp, err := uploadClient.Do(req)

	goroutineErr := <-errChan
	if goroutineErr != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, fmt.Errorf("error during upload processing: %w", goroutineErr)
	}

	if err != nil {
		return nil, fmt.Errorf("error sending request to %s: %w", uploadURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error uploading file. Server responded with %s: %s", resp.Status, string(bodyBytes))
	}

	var result ObjectUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("error decoding response: %w. Body: %s", err, string(bodyBytes))
	}

	return &result, nil
}

// ObjectDownload downloads a file from the object storage.
func (c *Client) ObjectDownload(objectUUID, outputPath string) error {
	downloadURL := c.baseURL.ResolveReference(&url.URL{
		Path:     "objects/download",
		RawQuery: "id=" + objectUUID,
	})

	req, err := http.NewRequest(http.MethodGet, downloadURL.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating GET request: %w", err)
	}
	req.Header.Set("Authorization", c.apiKey)

	c.logger.Info("Attempting to download object", "uuid", objectUUID, "url", downloadURL.String())

	// Use a client with a longer timeout for downloads.
	downloadClient := *c.httpClient
	downloadClient.Timeout = 30 * time.Minute

	resp, err := downloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request to %s: %w", downloadURL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("error downloading file from %s. Server responded with %s: %s", downloadURL.String(), resp.Status, string(bodyBytes))
	}

	if filepath.Dir(outputPath) != "." && filepath.Dir(outputPath) != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return fmt.Errorf("error creating output directory %s: %w", filepath.Dir(outputPath), err)
		}
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %w", outputPath, err)
	}
	defer outFile.Close()

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

	progressReader := newProgressReader(resp.Body, contentLength, "Downloading", outputPath)
	bytesCopied, err := io.Copy(outFile, progressReader)
	if err != nil {
		return fmt.Errorf("error writing downloaded content to file %s: %w", outputPath, err)
	}

	// Final newline is handled by progress reader's EOF condition
	c.logger.Info("File downloaded successfully", "path", outputPath, "size", formatBytes(bytesCopied))
	return nil
}

// ObjectGetHash retrieves the SHA256 hash of an object.
func (c *Client) ObjectGetHash(objectUUID string) (*ObjectHashResponse, error) {
	if objectUUID == "" {
		return nil, fmt.Errorf("objectUUID cannot be empty")
	}
	params := map[string]string{"id": objectUUID}
	var response ObjectHashResponse
	err := c.doRequest(http.MethodGet, "objects/hash", params, nil, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// --- Progress Reader ---

// progressReader is an io.Reader that reports progress.
type progressReader struct {
	reader         io.Reader
	total          int64
	current        int64
	lastReportTime time.Time
	operation      string // e.g., "Uploading" or "Downloading"
	fileName       string // Name of the file being processed
	isEOF          bool   // Flag to indicate if EOF has been reached
}

// newProgressReader creates a new progressReader.
func newProgressReader(reader io.Reader, total int64, operation, fileName string) *progressReader {
	return &progressReader{
		reader:         reader,
		total:          total,
		operation:      operation,
		fileName:       filepath.Base(fileName), // Use only the base name for display
		lastReportTime: time.Now(),
	}
}

// Read implements the io.Reader interface.
func (pr *progressReader) Read(p []byte) (n int, err error) {
	n, err = pr.reader.Read(p)
	pr.current += int64(n)

	if err == io.EOF {
		pr.isEOF = true
	}

	// Report progress every 500ms or on EOF
	if time.Since(pr.lastReportTime) > 500*time.Millisecond || err == io.EOF {
		pr.printProgress()
		pr.lastReportTime = time.Now()
	}

	return
}

func (pr *progressReader) printProgress() {
	var percentage float64
	var progressString string

	if pr.total > 0 {
		percentage = float64(pr.current) * 100 / float64(pr.total)
		progressString = fmt.Sprintf("%s %s: %s / %s (%.2f%%)",
			pr.operation,
			pr.fileName,
			formatBytes(pr.current),
			formatBytes(pr.total),
			percentage)
	} else {
		// If total size is unknown, just show current bytes
		progressString = fmt.Sprintf("%s %s: %s processed",
			pr.operation,
			pr.fileName,
			formatBytes(pr.current))
	}

	fmt.Fprintf(os.Stderr, "\r\033[K%s", progressString)

	if pr.isEOF {
		fmt.Fprintln(os.Stderr)
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
