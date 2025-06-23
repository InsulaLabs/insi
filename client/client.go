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
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/InsulaLabs/insi/db/models"
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
	MaximumRedirects      = 5 // Scale up if cluster > 5 nodes
	WebSocketTimeout      = 10 * time.Second
	WebSocketPingInterval = 30 * time.Second
	ObjectClientTimeout   = 30 * time.Minute
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

// ErrSubscriberLimitExceeded is returned when the subscriber limit is exceeded.
type ErrSubscriberLimitExceeded struct {
	Message string
}

func (e *ErrSubscriberLimitExceeded) Error() string {
	return fmt.Sprintf("subscriber limit exceeded: %s", e.Message)
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
	PublicBinding  string
	PrivateBinding string
	ClientDomain   string
	Logger         *slog.Logger
}

type Config struct {
	ConnectionType         ConnectionType // Direct will use Endpoints[0] always
	Endpoints              []Endpoint
	ApiKey                 string
	SkipVerify             bool
	Timeout                time.Duration
	Logger                 *slog.Logger
	EnableLeaderStickiness bool // Set to true to enable the client to "stick" to a leader upon redirect. Defaults to false (stickiness disabled).
}

type ErrorResponse struct {
	ErrorType string `json:"error_type"`
	Message   string `json:"message"`
}

// Client is the API client for the insi service.
type Client struct {
	mu                     sync.RWMutex // To protect endpoint URLs
	publicBaseURL          *url.URL
	privateBaseURL         *url.URL
	httpClient             *http.Client
	objectClient           *http.Client
	apiKey                 string
	logger                 *slog.Logger
	redirectCoutner        atomic.Uint64
	endpoints              []Endpoint // Store all potential endpoints
	enableLeaderStickiness bool
}

func (c *Client) DeriveWithApiKey(name, apiKey string) *Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Create copies of the URLs to prevent pointer sharing. This is crucial for
	// leader stickiness, ensuring that a redirect for one derived client doesn't
	// affect the base URLs of the original client or other derived clients.
	publicURLCopy := *c.publicBaseURL
	privateURLCopy := *c.privateBaseURL

	return &Client{
		publicBaseURL:          &publicURLCopy,
		privateBaseURL:         &privateURLCopy,
		httpClient:             c.httpClient,
		objectClient:           c.objectClient,
		apiKey:                 apiKey,
		logger:                 c.logger.WithGroup(name),
		redirectCoutner:        atomic.Uint64{},
		endpoints:              c.endpoints,
		enableLeaderStickiness: c.enableLeaderStickiness,
	}
}

func (c *Client) DeriveWithEntity(entity *models.Entity) *Client {
	return c.DeriveWithApiKey(entity.DataScopeUUID, entity.RootApiKey)
}

func (c *Client) GetHttpClient() *http.Client {
	return c.httpClient
}

func (c *Client) GetObjectHttpClient() *http.Client {
	return c.objectClient
}

// NewClient creates a new insi API client.
func NewClient(cfg *Config) (*Client, error) {

	for _, endpoint := range cfg.Endpoints {
		if endpoint.PublicBinding == "" {
			return nil, fmt.Errorf("publicBinding cannot be empty")
		}
		if endpoint.PrivateBinding == "" {
			return nil, fmt.Errorf("privateBinding cannot be empty")
		}
	}

	if cfg.ApiKey == "" {
		return nil, fmt.Errorf("apiKey cannot be empty")
	}
	clientLogger := cfg.Logger.WithGroup("insi_client")

	var selectedEndpoint Endpoint
	if cfg.ConnectionType == ConnectionTypeDirect {
		selectedEndpoint = cfg.Endpoints[0]
	} else if cfg.ConnectionType == ConnectionTypeRandom {
		selectedEndpoint = cfg.Endpoints[rand.Intn(len(cfg.Endpoints))]
	}

	createBaseURL := func(binding, clientDomain string) (*url.URL, error) {
		host, port, err := net.SplitHostPort(binding)
		if err != nil {
			return nil, fmt.Errorf("failed to parse binding '%s': %w", binding, err)
		}

		connectHost := host
		if clientDomain != "" {
			connectHost = clientDomain
		}

		finalConnectAddress := net.JoinHostPort(connectHost, port)
		baseURLStr := fmt.Sprintf("https://%s", finalConnectAddress)
		return url.Parse(baseURLStr)
	}

	publicBaseURL, err := createBaseURL(selectedEndpoint.PublicBinding, selectedEndpoint.ClientDomain)
	if err != nil {
		clientLogger.Error("Failed to parse public base URL", "error", err)
		return nil, fmt.Errorf("failed to create public base URL: %w", err)
	}

	privateBaseURL, err := createBaseURL(selectedEndpoint.PrivateBinding, selectedEndpoint.ClientDomain)
	if err != nil {
		clientLogger.Error("Failed to parse private base URL", "error", err)
		return nil, fmt.Errorf("failed to create private base URL: %w", err)
	}

	tlsClientCfg := &tls.Config{
		InsecureSkipVerify: cfg.SkipVerify,
	}

	if !cfg.SkipVerify {
		clientLogger.Debug(
			"TLS verification active.",
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

	objectClient := &http.Client{
		Transport: transport,
		Timeout:   ObjectClientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	clientLogger.Debug(
		"Insi client initialized",
		"public_base_url", publicBaseURL.String(),
		"private_base_url", privateBaseURL.String(),
		"tls_skip_verify", cfg.SkipVerify,
	)

	return &Client{
		publicBaseURL:          publicBaseURL,
		privateBaseURL:         privateBaseURL,
		httpClient:             httpClient,
		objectClient:           objectClient,
		apiKey:                 cfg.ApiKey,
		logger:                 clientLogger,
		redirectCoutner:        atomic.Uint64{},
		endpoints:              cfg.Endpoints,
		enableLeaderStickiness: cfg.EnableLeaderStickiness,
	}, nil
}

// GetApiKey returns the API key used by the client.
func (c *Client) GetApiKey() string {
	return c.apiKey
}

func (c *Client) GetBaseURL() *url.URL {
	return c.publicBaseURL
}

func (c *Client) GetAccumulatedRedirects() uint64 {
	return c.redirectCoutner.Load()
}

func (c *Client) ResetAccumulatedRedirects() {
	c.redirectCoutner.Store(0)
}

func isPrivatePath(path string) bool {
	return strings.HasPrefix(path, "db/api/v1/join") || strings.HasPrefix(path, "db/api/v1/admin/")
}

// internal request helper
func (c *Client) doRequest(method, path string, queryParams map[string]string, body interface{}, target interface{}) error {
	originalMethod := method // Save original method to preserve it across redirects

	c.mu.RLock()
	var baseURL *url.URL
	if isPrivatePath(path) {
		baseURL = c.privateBaseURL
	} else {
		baseURL = c.publicBaseURL
	}
	c.mu.RUnlock()

	// Construct the initial URL
	initialPathURL := &url.URL{Path: path}
	currentReqURL := baseURL.ResolveReference(initialPathURL)

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
				// This is a leader redirect. Update the client to stick to the new leader for future requests.
				if c.enableLeaderStickiness {
					if err := c.setLeader(redirectURL); err != nil {
						// Log the error but continue; the redirect will be handled for this single request anyway.
						c.logger.Warn("Failed to set sticky leader from redirect", "error", err)
					}
				}
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
					strings.HasPrefix(path, "db/api/v1/iterate") ||
					strings.HasPrefix(path, "db/api/v1/blob/iterate"))

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
				// If JSON unmarshal fails, we have a raw text body.
				// We should return it.
				return fmt.Errorf(
					"server returned status %d for %s %s: %s",
					resp.StatusCode,
					originalMethod,
					currentReqURL.String(),
					strings.TrimSpace(string(bodyBytes)),
				)
			}
			return fmt.Errorf(
				"server returned status %d for %s %s (and could not read body)",
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

// Bump atomically increments the integer value of a key by a given amount.
// The key must hold a string representation of an integer.
// If the key does not exist, it is created with the bump value.
func (c *Client) Bump(key string, value int) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key, "value": strconv.Itoa(value)}
	return c.doRequest(http.MethodPost, "db/api/v1/bump", nil, payload, nil)
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

// GetOpsPerSecond retrieves the current operations-per-second metrics for the node.
// Only the root api key can get ops per second.
func (c *Client) GetOpsPerSecond() (*models.OpsPerSecondCounters, error) {
	var response models.OpsPerSecondCounters
	err := c.doRequest(http.MethodGet, "db/api/v1/admin/metrics/ops", nil, nil, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// GetEntity retrieves a single entity by its root API key.
// Only the root api key can perform this operation.
func (c *Client) GetEntity(rootApiKey string) (*models.Entity, error) {
	if rootApiKey == "" {
		return nil, fmt.Errorf("rootApiKey cannot be empty")
	}
	requestPayload := models.InsightRequestEntity{RootApiKey: rootApiKey}
	var response models.InsightResponseEntity

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/insight/entity", nil, requestPayload, &response)
	if err != nil {
		return nil, err
	}
	return &response.Entity, nil
}

// GetEntities retrieves a list of entities.
// Only the root api key can perform this operation.
func (c *Client) GetEntities(offset, limit int) ([]models.Entity, error) {
	requestPayload := models.InsightRequestEntities{Offset: offset, Limit: limit}
	var response models.InsightResponseEntities

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/insight/entities", nil, requestPayload, &response)
	if err != nil {
		return nil, err
	}
	return response.Entities, nil
}

// GetEntityByAlias retrieves a single entity by its alias.
// Only the root api key can perform this operation.
func (c *Client) GetEntityByAlias(alias string) (*models.Entity, error) {
	if alias == "" {
		return nil, fmt.Errorf("alias cannot be empty")
	}
	requestPayload := models.InsightRequestEntityByAlias{Alias: alias}
	var response models.InsightResponseEntityByAlias

	err := c.doRequest(http.MethodPost, "db/api/v1/admin/insight/entity_by_alias", nil, requestPayload, &response)
	if err != nil {
		return nil, err
	}
	return &response.Entity, nil
}

// SubscribeToEvents connects to the event subscription WebSocket endpoint and prints incoming events.
func (c *Client) SubscribeToEvents(topic string, ctx context.Context, onEvent func(data any)) error {
	if topic == "" {
		return fmt.Errorf("topic cannot be empty")
	}

	wsScheme := "ws"
	if c.publicBaseURL.Scheme == "https" {
		wsScheme = "wss"
	}

	c.mu.RLock()
	// Initial URL construction
	currentWsURL := &url.URL{
		Scheme: wsScheme,
		Host:   c.publicBaseURL.Host,
		Path:   "/db/api/v1/events/subscribe",
	}
	c.mu.RUnlock()

	query := currentWsURL.Query()
	query.Set("topic", topic)
	currentWsURL.RawQuery = query.Encode()

	header := http.Header{}
	header.Set("Authorization", c.apiKey)

	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: WebSocketTimeout,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: c.httpClient.Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify,
		},
	}

	var conn *websocket.Conn
	var err error

	for redirects := 0; redirects < MaximumRedirects; redirects++ {
		c.logger.Info("Attempting to connect to WebSocket for event subscription", "url", currentWsURL.String(), "attempt", redirects+1)
		var resp *http.Response
		conn, resp, err = dialer.Dial(currentWsURL.String(), header)
		if err == nil {
			// Success
			break
		}

		if resp == nil {
			c.logger.Error("WebSocket dial error with no response", "url", currentWsURL.String(), "error", err)
			return fmt.Errorf("failed to dial websocket %s: %w", currentWsURL.String(), err)
		}
		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			loc := resp.Header.Get("Location")
			if loc == "" {
				return fmt.Errorf("redirect response missing Location header from %s", currentWsURL.String())
			}

			redirectURL, parseErr := currentWsURL.Parse(loc)
			if parseErr != nil {
				return fmt.Errorf("failed to parse redirect Location '%s': %w", loc, parseErr)
			}

			c.logger.Debug("WebSocket subscription redirected",
				"from_url", currentWsURL.String(),
				"to_url", redirectURL.String(),
				"status_code", resp.StatusCode)

			if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
				c.redirectCoutner.Add(1)
				if c.enableLeaderStickiness {
					if stickErr := c.setLeader(redirectURL); stickErr != nil {
						c.logger.Warn("Failed to set sticky leader from WebSocket redirect", "error", stickErr)
					}
				}
			}

			*currentWsURL = *redirectURL
			if currentWsURL.Scheme == "http" {
				currentWsURL.Scheme = "ws"
			} else if currentWsURL.Scheme == "https" {
				currentWsURL.Scheme = "wss"
			}
			continue

		default:
			errMsg := fmt.Sprintf("failed to dial websocket %s", currentWsURL.String())
			if resp.StatusCode == http.StatusServiceUnavailable {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return &ErrSubscriberLimitExceeded{Message: strings.TrimSpace(string(bodyBytes))}
			}

			errMsg = fmt.Sprintf(
				"%s: (status: %s), error: %v",
				errMsg,
				resp.Status,
				err,
			)
			c.logger.Error(
				"WebSocket dial error with response",
				"url", currentWsURL.String(),
				"status", resp.Status,
				"error", err,
			)
			return fmt.Errorf("failed to dial websocket %s: %w", currentWsURL.String(), err)
		}
	}

	if err != nil {
		return fmt.Errorf("failed to connect to websocket after %d redirects, last url: %s, error: %w", MaximumRedirects, currentWsURL.String(), err)
	}

	defer conn.Close()

	fmt.Println("Successfully connected to event stream.")

	done := make(chan struct{})

	// Goroutine to read messages from the server
	go func() {
		defer close(done)
		for {
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
				return
			}

			if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				var payload models.EventPayload
				if err := json.Unmarshal(message, &payload); err != nil {
					// It might be an error message from the server instead of an event.
					var errorResp ErrorResponse
					if err2 := json.Unmarshal(message, &errorResp); err2 == nil {
						// It's a structured error. Return it so the client can exit.
						c.logger.Error("Received error message from WebSocket", "error_type", errorResp.ErrorType, "message", errorResp.Message)
						return
					}

					// It's neither an event nor a known error structure. Log and continue.
					c.logger.Error(
						"Failed to unmarshal event message",
						"error", err,
						"message", string(message),
					)
					continue
				}
				if onEvent != nil {
					onEvent(payload)
				}
			}
		}
	}()

	select {
	case <-done:
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
	}
}

// --- Blob Operations ---

// UploadBlob uploads a blob from an io.Reader. The caller is responsible for the reader.
// The filename is used in the multipart form, can be empty.
func (c *Client) UploadBlob(ctx context.Context, key string, data io.Reader, filename string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Prepare multipart body
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	if err := writer.WriteField("key", key); err != nil {
		return fmt.Errorf("failed to write key to multipart form: %w", err)
	}

	if filename == "" {
		filename = "blob" // a default filename
	}
	part, err := writer.CreateFormFile("blob", filename)
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}

	if _, err := io.Copy(part, data); err != nil {
		return fmt.Errorf("failed to copy blob data to form: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close multipart writer: %w", err)
	}

	// Prepare request
	baseURL := c.publicBaseURL
	pathURL := &url.URL{Path: "db/api/v1/blob/set"}
	currentReqURL := baseURL.ResolveReference(pathURL)
	contentType := writer.FormDataContentType()

	var lastResp *http.Response

	for redirects := 0; redirects < MaximumRedirects; redirects++ {
		// We need a new reader for the body on each request attempt
		bodyReader := bytes.NewReader(body.Bytes())
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, currentReqURL.String(), bodyReader)
		if err != nil {
			return fmt.Errorf("failed to create upload request: %w", err)
		}
		req.Header.Set("Authorization", c.apiKey)
		req.Header.Set("Content-Type", contentType)

		c.logger.Debug("Sending blob upload request", "url", currentReqURL.String())

		resp, err := c.objectClient.Do(req)
		if err != nil {
			if lastResp != nil && lastResp.Body != nil {
				lastResp.Body.Close()
			}
			return fmt.Errorf("http request for blob upload failed: %w", err)
		}
		if lastResp != nil && lastResp.Body != nil {
			lastResp.Body.Close()
		}
		lastResp = resp

		// Handle redirects
		switch resp.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			loc := resp.Header.Get("Location")
			if loc == "" {
				resp.Body.Close()
				return fmt.Errorf("redirect response missing Location header from %s", currentReqURL.String())
			}
			redirectURL, err := currentReqURL.Parse(loc)
			if err != nil {
				resp.Body.Close()
				return fmt.Errorf("failed to parse redirect Location '%s': %w", loc, err)
			}
			c.logger.Debug("Blob upload request redirected", "to", redirectURL.String())
			currentReqURL = redirectURL
			resp.Body.Close()
			if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
				c.redirectCoutner.Add(1)
				// This is a leader redirect. Update the client to stick to the new leader for future requests.
				if c.enableLeaderStickiness {
					if err := c.setLeader(redirectURL); err != nil {
						// Log the error but continue; the redirect will be handled for this single request anyway.
						c.logger.Warn("Failed to set sticky leader from blob redirect", "error", err)
					}
				}
			}
			continue
		}

		// Process final response
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil // Success
		}

		// Handle specific errors like in doRequest
		bodyBytes, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusBadRequest {
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
		}

		return fmt.Errorf("blob upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if lastResp != nil && lastResp.Body != nil {
		lastResp.Body.Close()
	}
	return fmt.Errorf("stopped after %d redirects for blob upload, last URL: %s", MaximumRedirects, currentReqURL.String())
}

// GetBlob retrieves a blob as an io.ReadCloser. The caller MUST close the reader.
func (c *Client) GetBlob(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == "" {
		return nil, fmt.Errorf("key cannot be empty")
	}

	baseURL := c.publicBaseURL
	pathURL := &url.URL{Path: "db/api/v1/blob/get"}
	currentReqURL := baseURL.ResolveReference(pathURL)
	q := currentReqURL.Query()
	q.Set("key", key)
	currentReqURL.RawQuery = q.Encode()

	var lastResp *http.Response

	for redirects := 0; redirects < MaximumRedirects; redirects++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, currentReqURL.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create get blob request: %w", err)
		}
		req.Header.Set("Authorization", c.apiKey)

		c.logger.Debug("Sending get blob request", "url", currentReqURL.String())

		resp, err := c.objectClient.Do(req)
		if err != nil {
			if lastResp != nil && lastResp.Body != nil {
				lastResp.Body.Close()
			}
			return nil, fmt.Errorf("http request for get blob failed: %w", err)
		}
		if lastResp != nil && lastResp.Body != nil {
			lastResp.Body.Close()
		}
		lastResp = resp

		switch resp.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			loc := resp.Header.Get("Location")
			if loc == "" {
				resp.Body.Close()
				return nil, fmt.Errorf("redirect response missing Location header from %s", currentReqURL.String())
			}
			redirectURL, err := currentReqURL.Parse(loc)
			if err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("failed to parse redirect Location '%s': %w", loc, err)
			}
			c.logger.Debug("Get blob redirected", "to", redirectURL.String())
			currentReqURL = redirectURL
			resp.Body.Close()
			if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusPermanentRedirect {
				c.redirectCoutner.Add(1)
				// This is a leader redirect. Update the client to stick to the new leader for future requests.
				if c.enableLeaderStickiness {
					if err := c.setLeader(redirectURL); err != nil {
						// Log the error but continue; the redirect will be handled for this single request anyway.
						c.logger.Warn("Failed to set sticky leader from blob redirect", "error", err)
					}
				}
			}
			continue
		}

		// Process final response
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp.Body, nil
		}

		// It's not a success status, so we need to handle it as an error.
		defer resp.Body.Close()
		bodyBytes, _ := io.ReadAll(resp.Body)

		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrKeyNotFound
		}

		return nil, fmt.Errorf("get blob failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if lastResp != nil && lastResp.Body != nil {
		lastResp.Body.Close()
	}
	return nil, fmt.Errorf("stopped after %d redirects for get blob, last URL: %s", MaximumRedirects, currentReqURL.String())
}

// DeleteBlob deletes a blob by key.
func (c *Client) DeleteBlob(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key}
	err := c.doRequest(http.MethodPost, "db/api/v1/blob/delete", nil, payload, nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return ErrKeyNotFound
		}
		return err
	}
	return nil
}

// IterateBlobKeysByPrefix retrieves a list of blob keys matching a given prefix.
func (c *Client) IterateBlobKeysByPrefix(prefix string, offset, limit int) ([]string, error) {
	params := map[string]string{
		"prefix": prefix,
		"offset": strconv.Itoa(offset),
		"limit":  strconv.Itoa(limit),
	}
	var response struct {
		Data []string `json:"data"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/blob/iterate/prefix", params, nil, &response)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return []string{}, nil
		}
		return nil, err
	}
	return response.Data, nil
}

// setLeader updates the client's public and private base URLs to point to the new leader.
// It finds the correct endpoint configuration by matching the host of the leader's URL.
func (c *Client) setLeader(leaderURL *url.URL) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we are already pointing to the correct leader to avoid unnecessary work.
	if c.publicBaseURL.Host == leaderURL.Host || c.privateBaseURL.Host == leaderURL.Host {
		return nil
	}

	// Helper function to create a base URL from binding information.
	createBaseURL := func(binding, clientDomain string) (*url.URL, error) {
		host, port, err := net.SplitHostPort(binding)
		if err != nil {
			return nil, fmt.Errorf("failed to parse binding '%s': %w", binding, err)
		}
		connectHost := host
		if clientDomain != "" {
			connectHost = clientDomain
		}
		finalConnectAddress := net.JoinHostPort(connectHost, port)
		baseURLStr := fmt.Sprintf("https://%s", finalConnectAddress)
		return url.Parse(baseURLStr)
	}

	for _, endpoint := range c.endpoints {
		// Generate the potential public and private URLs for the current endpoint config.
		potentialPublicURL, err := createBaseURL(endpoint.PublicBinding, endpoint.ClientDomain)
		if err != nil {
			continue // Skip malformed endpoints
		}
		potentialPrivateURL, err := createBaseURL(endpoint.PrivateBinding, endpoint.ClientDomain)
		if err != nil {
			continue
		}

		// If the host from the redirect matches either the public or private host of this endpoint, we've found our leader.
		if potentialPublicURL.Host == leaderURL.Host || potentialPrivateURL.Host == leaderURL.Host {
			c.logger.Info("Redirect detected. Sticking to new leader.", "leader_host", leaderURL.Host)
			c.publicBaseURL = potentialPublicURL
			c.privateBaseURL = potentialPrivateURL
			return nil
		}
	}

	return fmt.Errorf("could not find matching endpoint for leader host: %s", leaderURL.Host)
}

// --- Alias Operations ---

// SetAlias creates a new alias for the API key currently in use.
func (c *Client) SetAlias() (*models.SetAliasResponse, error) {
	var response models.SetAliasResponse
	err := c.doRequest(http.MethodPost, "db/api/v1/alias/set", nil, nil, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteAlias deletes a specific alias.
func (c *Client) DeleteAlias(alias string) error {
	if alias == "" {
		return fmt.Errorf("alias cannot be empty for DeleteAlias")
	}
	requestPayload := models.DeleteAliasRequest{Alias: alias}
	return c.doRequest(http.MethodPost, "db/api/v1/alias/delete", nil, requestPayload, nil)
}

// ListAliases lists all aliases for the current API key.
func (c *Client) ListAliases() (*models.ListAliasesResponse, error) {
	var response models.ListAliasesResponse
	err := c.doRequest(http.MethodGet, "db/api/v1/alias/list", nil, nil, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}
