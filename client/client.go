package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	defaultTimeout = 10 * time.Second
)

type Config struct {
	HostPort   string
	ApiKey     string
	SkipVerify bool
	Logger     *slog.Logger
	Timeout    time.Duration
}

// Client is the API client for the insi service.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	apiKey     string
	logger     *slog.Logger
}

// NewClient creates a new insi API client.
// hostPort is the target server, e.g., "127.0.0.1:8443".
// authToken is the access token for the cluster, used to authenticate system routes.
// clientSkipVerifySetting controls whether the client skips verification of the server's TLS certificate, read from config.
func NewClient(cfg *Config) (*Client, error) {
	if cfg.HostPort == "" {
		return nil, fmt.Errorf("hostPort cannot be empty")
	}
	if cfg.ApiKey == "" {
		return nil, fmt.Errorf("apiKey cannot be empty")
	}
	clientLogger := cfg.Logger.WithGroup("insi_client")

	// We ENFORCE HTTPS - NEVER PERMIT HTTP
	baseURLStr := fmt.Sprintf("https://%s", cfg.HostPort)
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		clientLogger.Error("Failed to parse base URL", "url", baseURLStr, "error", err)
		return nil, fmt.Errorf("failed to parse base URL '%s': %w", baseURLStr, err)
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.SkipVerify},
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultTimeout
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// Prevent client from following redirects automatically.
			// We will handle them manually in doRequest to preserve method and body.
			return http.ErrUseLastResponse
		},
	}

	clientLogger.Info("Insi client initialized", "base_url", baseURL.String(), "tls_skip_verify", cfg.SkipVerify, "manual_redirects", true)

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
				resp.Body.Close()
				c.logger.Error("Redirect response missing Location header", "status_code", resp.StatusCode, "url", currentReqURL.String())
				return fmt.Errorf("redirect (status %d) missing Location header from %s", resp.StatusCode, currentReqURL.String())
			}

			redirectURL, err := currentReqURL.Parse(loc) // Resolve Location relative to currentReqURL
			if err != nil {
				resp.Body.Close()
				c.logger.Error("Failed to parse redirect Location header", "location", loc, "error", err)
				return fmt.Errorf("failed to parse redirect Location '%s': %w", loc, err)
			}

			c.logger.Info("Request redirected",
				"from_url", currentReqURL.String(),
				"to_url", redirectURL.String(),
				"status_code", resp.StatusCode,
				"method", originalMethod)

			currentReqURL = redirectURL // Update URL for the next iteration

			// Note: We are preserving originalMethod for all these redirect codes.
			// For 303 See Other, the spec suggests changing to GET. If strict 303 behavior is needed,
			// this would require adjustment. For the current use case (e.g. "set" command), preserving
			// the method seems to be the desired outcome.

			continue // Continue to the next iteration of the loop for the redirect
		}

		// Not a redirect, or unhandled status by the loop; this is the final response to process.
		defer resp.Body.Close() // Ensure this final response body is closed when function returns

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.logger.Warn("Received non-2xx status code", "method", originalMethod, "url", currentReqURL.String(), "status_code", resp.StatusCode)
			// TODO: Attempt to read error body for more details (as was in original code comment)
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
	return c.doRequest(http.MethodPost, "db/api/v1/delete", nil, payload, nil)
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
		return nil, err
	}
	return response.Data, nil
}

// --- Tag Operations ---

// Tag associates a tag with a key.
func (c *Client) Tag(key, tag string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if tag == "" {
		return fmt.Errorf("tag cannot be empty")
	}
	payload := map[string]string{"key": key, "value": tag} // Server expects "value" for the tag
	return c.doRequest(http.MethodPost, "db/api/v1/tag", nil, payload, nil)
}

// Untag removes a tag association from a key.
func (c *Client) Untag(key, tag string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if tag == "" {
		return fmt.Errorf("tag cannot be empty")
	}
	// Server's untagHandler expects KVPayload {Key: key, Value: tag}
	payload := map[string]string{"key": key, "value": tag}
	return c.doRequest(http.MethodPost, "db/api/v1/untag", nil, payload, nil)
}

// IterateByTag retrieves a list of keys associated with a given tag.
func (c *Client) IterateByTag(tag string, offset, limit int) ([]string, error) {
	if tag == "" {
		return nil, fmt.Errorf("tag cannot be empty")
	}
	params := map[string]string{
		"tag":    tag,
		"offset": strconv.Itoa(offset),
		"limit":  strconv.Itoa(limit),
	}
	var response struct {
		Data []string `json:"data"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/iterate/tags", params, nil, &response)
	if err != nil {
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
	return c.doRequest(http.MethodPost, "db/api/v1/cache/delete", nil, payload, nil)
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

// NewAPIKey requests the server to generate a new API key for the given entity.
func (c *Client) NewAPIKey(entityName string) (string, error) {
	if entityName == "" {
		return "", fmt.Errorf("entityName cannot be empty")
	}
	params := map[string]string{"entity": entityName}
	var response struct {
		APIKey string `json:"apiKey"`
	}
	err := c.doRequest(http.MethodGet, "db/api/v1/new-api-key", params, nil, &response)
	if err != nil {
		return "", err
	}
	return response.APIKey, nil
}

// DeleteAPIKey requests the server to delete an existing API key.
func (c *Client) DeleteAPIKey(apiKey string) error {
	if apiKey == "" {
		return fmt.Errorf("apiKey cannot be empty")
	}
	params := map[string]string{"key": apiKey}
	return c.doRequest(http.MethodGet, "db/api/v1/delete-api-key", params, nil, nil)
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
