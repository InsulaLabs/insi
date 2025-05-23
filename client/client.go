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

// Client is the API client for the insi service.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	authToken  string
	logger     *slog.Logger
}

// NewClient creates a new insi API client.
// hostPort is the target server, e.g., "127.0.0.1:8443".
// authToken is the access token for the cluster, used to authenticate system routes.
// clientSkipVerifySetting controls whether the client skips verification of the server's TLS certificate, read from config.
func NewClient(hostPort string, authToken string, clientSkipVerifySetting bool, logger *slog.Logger) (*Client, error) {
	if hostPort == "" {
		return nil, fmt.Errorf("hostPort cannot be empty")
	}
	if authToken == "" {
		return nil, fmt.Errorf("authToken cannot be empty")
	}
	if logger == nil {
		logger = slog.Default()
	}
	clientLogger := logger.WithGroup("insi_client")

	// We ENFORCE HTTPS - NEVER PERMIT HTTP
	baseURLStr := fmt.Sprintf("https://%s", hostPort)
	baseURL, err := url.Parse(baseURLStr)
	if err != nil {
		clientLogger.Error("Failed to parse base URL", "url", baseURLStr, "error", err)
		return nil, fmt.Errorf("failed to parse base URL '%s': %w", baseURLStr, err)
	}

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: clientSkipVerifySetting},
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   defaultTimeout,
	}

	clientLogger.Info("Insi client initialized", "base_url", baseURL.String(), "tls_skip_verify", clientSkipVerifySetting)

	return &Client{
		baseURL:    baseURL,
		httpClient: httpClient,
		authToken:  authToken,
		logger:     clientLogger,
	}, nil
}

// internal request helper
func (c *Client) doRequest(method, path string, queryParams map[string]string, body interface{}, target interface{}) error {
	fullURL := c.baseURL.ResolveReference(&url.URL{Path: path})

	q := fullURL.Query()
	for k, v := range queryParams {
		q.Set(k, v)
	}
	fullURL.RawQuery = q.Encode()

	var reqBodyBytes []byte
	var err error
	if body != nil {
		reqBodyBytes, err = json.Marshal(body)
		if err != nil {
			c.logger.Error("Failed to marshal request body", "path", path, "error", err)
			return fmt.Errorf("failed to marshal request body for %s: %w", path, err)
		}
	}

	req, err := http.NewRequest(method, fullURL.String(), bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		c.logger.Error("Failed to create new HTTP request", "method", method, "url", fullURL.String(), "error", err)
		return fmt.Errorf("failed to create request %s %s: %w", method, fullURL.String(), err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.authToken)

	c.logger.Debug("Sending request", "method", method, "url", fullURL.String())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Error("HTTP request failed", "method", method, "url", fullURL.String(), "error", err)
		return fmt.Errorf("http request %s %s failed: %w", method, fullURL.String(), err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.logger.Warn("Received non-2xx status code", "method", method, "url", fullURL.String(), "status_code", resp.StatusCode)
		// TODO: Attempt to read error body for more details
		return fmt.Errorf("server returned status %d for %s %s", resp.StatusCode, method, fullURL.String())
	}

	if target != nil {
		if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
			c.logger.Error("Failed to decode response body", "method", method, "url", fullURL.String(), "status_code", resp.StatusCode, "error", err)
			return fmt.Errorf("failed to decode response body for %s %s (status %d): %w", method, fullURL.String(), resp.StatusCode, err)
		}
	}
	c.logger.Debug("Request successful", "method", method, "url", fullURL.String(), "status_code", resp.StatusCode)
	return nil
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
	err := c.doRequest(http.MethodGet, "/get", params, nil, &response)
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
	return c.doRequest(http.MethodPost, "/set", nil, payload, nil)
}

// Delete removes a key and its value.
func (c *Client) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	payload := map[string]string{"key": key}
	return c.doRequest(http.MethodPost, "/delete", nil, payload, nil)
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
	err := c.doRequest(http.MethodGet, "/iterate/prefix", params, nil, &response)
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
	return c.doRequest(http.MethodPost, "/tag", nil, payload, nil)
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
	return c.doRequest(http.MethodPost, "/untag", nil, payload, nil)
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
	err := c.doRequest(http.MethodGet, "/iterate/tags", params, nil, &response)
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
	return c.doRequest(http.MethodPost, "/cache/set", nil, payload, nil)
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
	err := c.doRequest(http.MethodGet, "/cache/get", params, nil, &response)
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
	return c.doRequest(http.MethodPost, "/cache/delete", nil, payload, nil)
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
	return c.doRequest(http.MethodGet, "/join", params, nil, nil)
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
	err := c.doRequest(http.MethodGet, "/new-api-key", params, nil, &response)
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
	return c.doRequest(http.MethodGet, "/delete-api-key", params, nil, nil)
}
