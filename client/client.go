/*
	Bosley 2025-06-15

	This client handles all connections to the insi application.
	It provides a means to interface with the "objects" service, and automatically
	handles cluster write-redirection and read-balancing.

	Key/ Value Store
	  - Set
	  - Get
	  - Delete
	  - CompareAndSwap
	  - SetNX
	  - IterateByPrefix

	Cache Store
	  - Set
	  - Get
	  - Delete
	  - CompareAndSwap
	  - SetNX
	  - IterateByPrefix

	Event Emitter
	  - PublishEvent
	  - SubscribeToEvents

	[Heavily Rate Limited]
	  All operations below this are usually, and preferably, heavily rate limited
	  (available in the configuration)

	System
	  - Join      --> Root-only node-to-node operation
	  - Ping      --> Can use error code to establish key validity and aliveness
	  - GetLimits --> Gets limits for the key being used to make the request

	Admin (root key only)
	  - CreateAPIKey
	  - DeleteAPIKey
	  - SetLimits
	  - GetLimitsForKey

	Note on usage:

	   The client counts some specific events internally and offers access to some parts
	   of its state for the sake of debugging, monitoring, and helping systems that are utilizing
	   it to make intelligent decisions.

	   For instance: In this system we have the potential for redirects when attempting to write data
	    as only the leader can write to the cluster. This client will follow the redirects and count
		them internally.

    Where the client sites inside the system:
		The entire JS runtime utlizes this client. It is the "backend" to a JS client. Its also
		used in other tools to facilitate dialog with the system.
*/

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
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/db/models"
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
	MaximumRedirects      = 5 // Scale up if cluster > 5 nodes
	WebSocketTimeout      = 10 * time.Second
	WebSocketPingInterval = 30 * time.Second
	ObjectClientTimeout   = 10 * time.Minute
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrConflict       = errors.New("operation failed due to a conflict (e.g., key exists for setnx, or cas failed)")
	ErrTokenNotFound  = errors.New("token not found")
	ErrTokenInvalid   = errors.New("token invalid")
	ErrAPIKeyNotFound = errors.New("api key not found")
	ErrHashMismatch   = errors.New("downloaded file hash mismatch")
)

// ErrRateLimited is returned when a request is rate-limited (429).
type ErrRateLimited struct {
	RetryAfter time.Duration
	Limit      int
	Burst      int
	Message    string
}

func (e *ErrRateLimited) Error() string {
	return fmt.Sprintf("rate limited: %s. Try again in %v. (Limit: %d, Burst: %d)", e.Message, e.RetryAfter, e.Limit, e.Burst)
}

// ErrDiskLimitExceeded is returned when the disk usage limit is exceeded.
type ErrDiskLimitExceeded struct {
	CurrentUsage int64
	Limit        int64
	Message      string
}

func (e *ErrDiskLimitExceeded) Error() string {
	return fmt.Sprintf("disk limit exceeded: %s. Current: %d, Limit: %d", e.Message, e.CurrentUsage, e.Limit)
}

// ErrMemoryLimitExceeded is returned when the memory usage limit is exceeded.
type ErrMemoryLimitExceeded struct {
	CurrentUsage int64
	Limit        int64
	Message      string
}

func (e *ErrMemoryLimitExceeded) Error() string {
	return fmt.Sprintf("memory limit exceeded: %s. Current: %d, Limit: %d", e.Message, e.CurrentUsage, e.Limit)
}

// ErrEventsLimitExceeded is returned when the events limit is exceeded.
type ErrEventsLimitExceeded struct {
	CurrentUsage int64
	Limit        int64
	Message      string
}

func (e *ErrEventsLimitExceeded) Error() string {
	return fmt.Sprintf("events limit exceeded: %s. Current: %d, Limit: %d", e.Message, e.CurrentUsage, e.Limit)
}

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
	baseURL         *url.URL
	httpClient      *http.Client
	apiKey          string
	logger          *slog.Logger
	redirectCoutner atomic.Uint64
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
		clientLogger.Debug(
			"TLS verification active. ServerName for SNI will be derived from target URL host.",
			"initial_target_host", connectHost,
			"tls_skip_verify", cfg.SkipVerify,
		)
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

	clientLogger.Debug(
		"Insi client initialized",
		"base_url", baseURL.String(),
		"tls_skip_verify", cfg.SkipVerify,
	)

	return &Client{
		baseURL:         baseURL,
		httpClient:      httpClient,
		apiKey:          cfg.ApiKey,
		logger:          clientLogger,
		redirectCoutner: atomic.Uint64{},
	}, nil
}

// GetApiKey returns the API key used by the client.
func (c *Client) GetApiKey() string {
	return c.apiKey
}

func (c *Client) GetAccumulatedRedirects() uint64 {
	return c.redirectCoutner.Load()
}

func (c *Client) ResetAccumulatedRedirects() {
	c.redirectCoutner.Store(0)
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
			c.logger.Error(
				"Failed to marshal request body",
				"path", path,
				"method", originalMethod,
				"error", err,
			)
			return fmt.Errorf(
				"failed to marshal request body for %s %s: %w",
				originalMethod, path, err,
			)
		}
	}

	var lastResp *http.Response // To store the response if loop finishes due to too many redirects

	for redirects := 0; redirects < MaximumRedirects; redirects++ { // Max 10 redirects
		reqBodyReader := bytes.NewBuffer(reqBodyBytes) // Can be re-read

		req, err := http.NewRequest(originalMethod, currentReqURL.String(), reqBodyReader)
		if err != nil {
			c.logger.Error(
				"Failed to create new HTTP request",
				"method", originalMethod,
				"url", currentReqURL.String(),
				"error", err,
			)
			return fmt.Errorf(
				"failed to create request %s %s: %w", originalMethod, currentReqURL.String(), err,
			)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", c.apiKey)

		c.logger.Debug(
			"Sending request",
			"method", originalMethod,
			"url", currentReqURL.String(),
			"attempt", redirects+1,
		)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			c.logger.Error(
				"HTTP request failed",
				"method", originalMethod,
				"url", currentReqURL.String(),
				"error", err,
			)
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

			c.logger.Debug("Request redirected",
				"from_url", currentReqURL.String(),
				"to_url", redirectURL.String(),
				"status_code", resp.StatusCode,
				"method", originalMethod)

			if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
				c.redirectCoutner.Add(1)
			}

			currentReqURL = redirectURL // Update URL for the next iteration
			resp.Body.Close()           // Close current redirect response body before continuing
			continue                    // Continue to the next iteration of the loop for the redirect
		}

		// Not a redirect, or unhandled status by the loop; this is the final response to process.
		defer resp.Body.Close() // Ensure this final response body is closed when function returns

		// Check for rate limiting first
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfterStr := resp.Header.Get("Retry-After")
			limitStr := resp.Header.Get("X-RateLimit-Limit")
			burstStr := resp.Header.Get("X-RateLimit-Burst")

			var retryAfter time.Duration
			if seconds, err := strconv.ParseFloat(retryAfterStr, 64); err == nil {
				retryAfter = time.Duration(seconds * float64(time.Second))
			} else {
				// Fallback if parsing fails
				retryAfter = 1 * time.Second // Default to 1s if header is missing or invalid
				c.logger.Warn(
					"Could not parse Retry-After header, defaulting",
					"value", retryAfterStr,
					"default", retryAfter,
					"error", err,
				)
			}

			limit, _ := strconv.Atoi(limitStr)
			burst, _ := strconv.Atoi(burstStr)

			bodyBytes, _ := io.ReadAll(resp.Body)

			c.logger.Warn(
				"Request was rate limited",
				"url", currentReqURL.String(),
				"retry_after", retryAfter,
				"limit", limit,
				"burst", burst,
			)

			return &ErrRateLimited{
				RetryAfter: retryAfter,
				Limit:      limit,
				Burst:      burst,
				Message:    strings.TrimSpace(string(bodyBytes)),
			}
		}

		if resp.StatusCode == http.StatusBadRequest {
			bodyBytes, _ := io.ReadAll(resp.Body)

			// Check for resource limit headers first, as they are a specific type of bad request.

			/*
				Disk usage cap reached
			*/
			if currentDiskUsageStr := resp.Header.Get("X-Current-Disk-Usage"); currentDiskUsageStr != "" {
				limitStr := resp.Header.Get("X-Disk-Usage-Limit")
				current, _ := strconv.ParseInt(currentDiskUsageStr, 10, 64)
				limit, _ := strconv.ParseInt(limitStr, 10, 64)
				return &ErrDiskLimitExceeded{
					CurrentUsage: current,
					Limit:        limit,
					Message:      strings.TrimSpace(string(bodyBytes)),
				}
			}

			/*
				Memory usage cap reached
			*/
			if currentMemoryUsageStr := resp.Header.Get("X-Current-Memory-Usage"); currentMemoryUsageStr != "" {
				limitStr := resp.Header.Get("X-Memory-Usage-Limit")
				current, _ := strconv.ParseInt(currentMemoryUsageStr, 10, 64)
				limit, _ := strconv.ParseInt(limitStr, 10, 64)
				return &ErrMemoryLimitExceeded{
					CurrentUsage: current,
					Limit:        limit,
					Message:      strings.TrimSpace(string(bodyBytes)),
				}
			}

			/*
				Events emitter cap reached
			*/
			if currentEventsStr := resp.Header.Get("X-Current-Events"); currentEventsStr != "" {
				limitStr := resp.Header.Get("X-Events-Limit")
				current, _ := strconv.ParseInt(currentEventsStr, 10, 64)
				limit, _ := strconv.ParseInt(limitStr, 10, 64)
				return &ErrEventsLimitExceeded{
					CurrentUsage: current,
					Limit:        limit,
					Message:      strings.TrimSpace(string(bodyBytes)),
				}
			}

			/*
				Structured error response
			*/
			var errorResp ErrorResponse
			if jsonErr := json.Unmarshal(bodyBytes, &errorResp); jsonErr == nil {
				return fmt.Errorf("server error (status 400): %s - %s", errorResp.ErrorType, errorResp.Message)
			}

			/*
				Generic error response
			*/
			return fmt.Errorf("server returned status 400 Bad Request: %s", string(bodyBytes))
		}

		if resp.StatusCode == http.StatusNotFound {
			bodyBytes, _ := io.ReadAll(resp.Body)

			var errorResp ErrorResponse
			if jsonErr := json.Unmarshal(bodyBytes, &errorResp); jsonErr == nil {
				/*
					Check for specific error types returned with a 404 status
				*/
				if errorResp.ErrorType == "API_KEY_NOT_FOUND" {
					return ErrAPIKeyNotFound
				}
				return fmt.Errorf("server error (status 404): %s - %s", errorResp.ErrorType, errorResp.Message)
			}

			/*
				For specific GET operations, a 404 is a semantic "not found" for the resource key in the URL.

				Here, we convert 404 to ErrKeyNotFound to be more informative.
			*/
			isDataGetOperation := method == http.MethodGet &&
				(strings.HasPrefix(path, "db/api/v1/get") ||
					strings.HasPrefix(path, "db/api/v1/cache/get") ||
					strings.HasPrefix(path, "db/api/v1/iterate"))

			if isDataGetOperation {
				return ErrKeyNotFound
			}

			/*
				For all other operations (POST, DELETE, other GETs),
					a 404 implies the endpoint itself was not found.
			*/
			return fmt.Errorf("endpoint not found (404) at %s: %s", currentReqURL.String(), string(bodyBytes))
		}

		/*
			Atomic operations (CAS, SetNX) can fail with a 409 Conflict or 412 Precondition Failed.
			These are a semantic way to indicate that the operation failed due to a conflict.
			So we return ErrConflict in these cases.
		*/
		if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusPreconditionFailed {
			bodyBytes, _ := io.ReadAll(resp.Body)
			c.logger.Warn(
				"Conflict/Precondition failed response from server",
				"url", currentReqURL.String(),
				"status", resp.StatusCode,
				"body", string(bodyBytes),
			)
			return ErrConflict
		}

		/*
			All other non-2xx status codes are errors.
		*/
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.logger.Warn("Received non-2xx status code", "method", originalMethod, "url", currentReqURL.String(), "status_code", resp.StatusCode)
			/*
				Attempt to read error body for more details
			*/
			var errorResp ErrorResponse
			bodyBytes, readErr := io.ReadAll(resp.Body)
			if readErr == nil {
				if jsonErr := json.Unmarshal(bodyBytes, &errorResp); jsonErr == nil {
					c.logger.Debug("Parsed JSON error response from server", "error_type", errorResp.ErrorType, "message", errorResp.Message)
					/*
						We acheive an auth no-op by pinging a server with a value to check if its a valid key
						and/or to see if the server is online. To distinguish between the two
						we check real quick to see if the error response from the server indicates
						anything about the API key itself not being found vs us not being authorized.
					*/
					if errorResp.ErrorType == "API_KEY_NOT_FOUND" {
						return ErrAPIKeyNotFound
					}
					/*
						Return a generic error with the server's message if available
					*/
					return fmt.Errorf(
						"server error (status %d): %s - %s",
						resp.StatusCode,
						errorResp.ErrorType,
						errorResp.Message,
					)
				}
			}
			return fmt.Errorf(
				"server returned status %d for %s %s",
				resp.StatusCode,
				originalMethod,
				currentReqURL.String(),
			)
		}

		/*
			Decode the response body into the target interface if provided.
		*/
		if target != nil {
			if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
				c.logger.Error(
					"Failed to decode response body",
					"method", originalMethod,
					"url", currentReqURL.String(),
					"status_code", resp.StatusCode,
					"error", err,
				)
				return fmt.Errorf(
					"failed to decode response body for %s %s (status %d): %w",
					originalMethod, currentReqURL.String(), resp.StatusCode, err,
				)
			}
		}
		c.logger.Debug(
			"Request successful",
			"method", originalMethod,
			"url", currentReqURL.String(),
			"status_code", resp.StatusCode,
		)
		return nil
	}

	/*
		If loop finishes, it means too many redirects.
	*/
	if lastResp != nil && lastResp.Body != nil {
		lastResp.Body.Close()
	}
	c.logger.Error(
		"Too many redirects",
		"final_url_attempt", currentReqURL.String(),
		"original_method", originalMethod,
	)
	return fmt.Errorf(
		"stopped after %d redirects, last URL: %s", MaximumRedirects, currentReqURL.String(),
	)
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
// this can only be done via the root api key, derived from the configured, shared, node secret.
// Having access to this is why api keys are available to be created and limited
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
// Only the root api key can create other api keys.
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
// Only the root api key can delete other api keys.
func (c *Client) DeleteAPIKey(apiKey string) error {
	if apiKey == "" {
		return fmt.Errorf("apiKey cannot be empty for DeleteAPIKey")
	}
	requestPayload := models.ApiKeyDeleteRequest{Key: apiKey}

	// Expects 200 OK on success, no specific response body to decode beyond error handling.
	err := c.doRequest(http.MethodPost, "db/api/v1/admin/api/delete", nil, requestPayload, nil)
	if err != nil {
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
// Only the root api key can set limits for other api keys.
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

// GetLimitsForKey retrieves the usage limits and current usage for a specific API key.
// Only the root api key can get limits for other api keys.
func (c *Client) GetLimitsForKey(apiKey string) (*models.LimitsResponse, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("apiKey cannot be empty for GetLimitsForKey")
	}
	requestPayload := models.GetLimitsRequest{ApiKey: apiKey}
	var response models.LimitsResponse

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/limits/get", nil, requestPayload, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
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
	wsURL.RawQuery = query.Encode()

	c.logger.Info("Attempting to connect to WebSocket for event subscription", "url", wsURL.String())

	// The Authorization header is used for authentication on the WebSocket upgrade request.
	header := http.Header{}
	header.Set("Authorization", c.apiKey)

	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: WebSocketTimeout,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: c.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify,
		},
	}

	conn, resp, err := dialer.Dial(wsURL.String(), header)
	if err != nil {
		errMsg := fmt.Sprintf("failed to dial websocket %s", wsURL.String())
		if resp != nil {
			errMsg = fmt.Sprintf(
				"%s: (status: %s), error: %v",
				errMsg,
				resp.Status,
				err,
			)
			c.logger.Error(
				"WebSocket dial error with response",
				"url", wsURL.String(),
				"status", resp.Status,
				"error", err,
			)
			return fmt.Errorf("%s: %w", errMsg, err)
		}
		c.logger.Error("WebSocket dial error", "url", wsURL.String(), "error", err)
		return fmt.Errorf("%s: %w", errMsg, err)
	}
	defer conn.Close()

	/*
		Handle pong messages to keep the connection alive if the server sends pings
		This helps prevent proxies or the server itself from closing the connection due to inactivity.
	*/
	conn.SetPongHandler(func(string) error {
		c.logger.Debug("Received pong from server")
		return nil
	})

	/*
		Periodically send pings to the server to keep the connection alive
		This helps prevent proxies or the server itself from closing the connection due to inactivity.
	*/
	go func() {
		ticker := time.NewTicker(WebSocketPingInterval)
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
				err := conn.WriteMessage(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				)
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
			err := conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			)
			if err != nil {
				c.logger.Error("Error sending close message during read loop shutdown", "error", err)
			}
			return ctx.Err() // Return context error
		default:
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(
					err, websocket.CloseNormalClosure, websocket.CloseGoingAway,
				) && err != context.Canceled {
					c.logger.Error("Error reading message from WebSocket", "error", err)
				} else {
					c.logger.Info(
						"WebSocket connection closed gracefully or context cancelled.",
						"error", err,
					)
				}
				return err
			}

			if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				var event models.Event
				if err := json.Unmarshal(message, &event); err != nil {
					c.logger.Error(
						"Failed to unmarshal event message",
						"error", err,
						"message", string(message),
					)
					continue
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
	uploadClient.Timeout = ObjectClientTimeout

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
		return nil, fmt.Errorf(
			"error uploading file. Server responded with %s: %s",
			resp.Status, string(bodyBytes),
		)
	}

	var result ObjectUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"error decoding response: %w. Body: %s", err, string(bodyBytes),
		)
	}

	return &result, nil
}

// ObjectDownload downloads a file from the object storage and verifies its integrity.
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
	downloadClient.Timeout = ObjectClientTimeout

	resp, err := downloadClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request to %s: %w", downloadURL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf(
			"error downloading file from %s. Server responded with %s: %s",
			downloadURL.String(), resp.Status, string(bodyBytes),
		)
	}

	if filepath.Dir(outputPath) != "." && filepath.Dir(outputPath) != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			return fmt.Errorf(
				"error creating output directory %s: %w", filepath.Dir(outputPath), err,
			)
		}
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file %s: %w", outputPath, err)
	}

	contentLength, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)

	progressReader := newProgressReader(resp.Body, contentLength, "Downloading", outputPath)
	bytesCopied, err := io.Copy(outFile, progressReader)
	if err != nil {
		outFile.Close() // Close on error
		return fmt.Errorf("error writing downloaded content to file %s: %w", outputPath, err)
	}

	if err := outFile.Close(); err != nil {
		return fmt.Errorf("error closing file after download: %w", err)
	}

	// Final newline is handled by progress reader's EOF condition
	c.logger.Info(
		"File download completed, verifying hash...",
		"path", outputPath,
		"size", formatBytes(bytesCopied),
	)

	// --- Hash Verification ---
	hashResp, err := c.ObjectGetHash(objectUUID)
	if err != nil {
		os.Remove(outputPath) // Cleanup on verification failure
		return fmt.Errorf("could not retrieve object hash for verification: %w", err)
	}
	expectedSha256 := hashResp.Sha256

	downloadedFile, err := os.Open(outputPath)
	if err != nil {
		os.Remove(outputPath) // Cleanup
		return fmt.Errorf("could not reopen downloaded file for hashing: %w", err)
	}
	defer downloadedFile.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, downloadedFile); err != nil {
		os.Remove(outputPath) // Cleanup
		return fmt.Errorf(
			"error calculating SHA256 hash for downloaded file %s: %w", outputPath, err,
		)
	}
	calculatedSha256 := hex.EncodeToString(hasher.Sum(nil))

	if calculatedSha256 != expectedSha256 {
		os.Remove(outputPath) // Cleanup invalid file
		c.logger.Error(
			"Hash mismatch for downloaded file",
			"path", outputPath,
			"expected", expectedSha256,
			"got", calculatedSha256,
		)
		return ErrHashMismatch
	}

	c.logger.Info(
		"File hash verified successfully",
		"path", outputPath, "sha256", calculatedSha256,
	)
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
	operation      string // e.g "Uploading" or "Downloading"
	fileName       string
	isEOF          bool
}

// newProgressReader creates a new progressReader.
func newProgressReader(reader io.Reader, total int64, operation, fileName string) *progressReader {
	return &progressReader{
		reader:         reader,
		total:          total,
		operation:      operation,
		fileName:       filepath.Base(fileName),
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
