# Insi Go Client (`github.com/InsulaLabs/insi/client`)

This document provides instructions on how to integrate and use the official Go client for interacting with an InsiDB cluster.

## Getting the Client

To add the Insi Go client to your Go project, you'''ll need to configure your Go environment to access the private repository and then fetch the package.

Execute the following commands in your terminal:

```bash
export GOPRIVATE="github.com/InsulaLabs/*"
export GONOSUMDB="github.com/InsulaLabs/*"
go get -u github.com/InsulaLabs/insi/client
```

This will:
1.  Tell Go that repositories under `github.com/InsulaLabs/` are private and should be fetched directly (e.g., using your SSH keys if you have access).
2.  Tell Go to not verify checksums for these private modules against the public Go sum database.
3.  Fetch and install the latest version of the `insi/client` package.

Ensure you have appropriate access permissions (e.g., an SSH key configured with GitHub that has access to the `InsulaLabs/insi` repository).

## Client Initialization

To use the client, you first need to create a new client instance using `client.NewClient()`. This function takes a `client.Config` struct as an argument.

### `client.Config` Structure

```go
type Config struct {
    // HostPort is the address of the InsiDB node to connect to, including the port.
    // This is used to derive the port for the connection.
    // Example: "127.0.0.1:8443" or "db-0.insula.dev:443"
    HostPort     string

    // ClientDomain (Optional) specifies the domain name of the InsiDB node.
    // If provided, this domain will be used for constructing the connection URL
    // and for TLS certificate verification (as ServerName).
    // If empty, the host part of HostPort will be used.
    // Example: "db-0.insula.dev"
    ClientDomain string

    // ApiKey is the authentication token required to interact with the InsiDB API.
    ApiKey       string

    // SkipVerify (Optional) controls whether the client skips TLS certificate verification.
    // Defaults to false (verification is performed). Set to true for development or
    // if using self-signed certificates without a proper CA setup.
    // Warning: Setting this to true in production is insecure.
    SkipVerify   bool

    // Logger (Required) is an instance of *slog.Logger for client-side logging.
    // You should provide your application'''s logger, and the client will create a
    // sub-logger group "insi_client".
    Logger       *slog.Logger

    // Timeout (Optional) specifies the timeout for HTTP requests made by the client.
    // If not set, a default timeout (currently 10 seconds) will be used.
    Timeout      time.Duration
}
```

### Example Initialization

```go
import (
    "log/slog"
    "os"
    "time"

    "github.com/InsulaLabs/insi/client"
)

func main() {
    // Setup your logger
    logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

    clientCfg := &client.Config{
        HostPort:     "db-0.insula.dev:443", // Or an IP:port like "134.122.121.148:443"
        ClientDomain: "db-0.insula.dev",     // Recommended for TLS verification
        ApiKey:       "your_api_key_here",
        SkipVerify:   false,                 // Should be false in production
        Logger:       logger,
        Timeout:      15 * time.Second,      // Optional
    }

    c, err := client.NewClient(clientCfg)
    if err != nil {
        logger.Error("Failed to create Insi client", "error", err)
        // Handle error
        return
    }

    logger.Info("Insi client initialized successfully!")
    // Now you can use the client 'c' to interact with InsiDB
}
```

**Key points during initialization:**
-   The client enforces HTTPS.
-   If `ClientDomain` is provided in `client.Config`, it will be used as the host for the connection URL, and also as `ServerName` for TLS certificate validation. This is the recommended approach when dealing with certificates issued for domain names.
-   If `ClientDomain` is not provided, the host part from `HostPort` will be used for the URL and for `ServerName` in TLS. If this host is an IP address, TLS validation might fail if the server'''s certificate does not include that IP as a Subject Alternative Name (SAN).
-   `SkipVerify: true` should only be used in trusted development environments.

## Available Client Methods

The client provides methods for various operations, categorized below. All methods return an `error` as their last argument, which should always be checked.

### Value Operations

-   `Get(key string) (string, error)`: Retrieves a value for a given key.
-   `Set(key, value string) error`: Sets a value for a given key.
-   `Delete(key string) error`: Removes a key and its value.
-   `IterateByPrefix(prefix string, offset, limit int) ([]string, error)`: Retrieves a list of keys matching a given prefix, with pagination.

### Tag Operations

-   `Tag(key, tag string) error`: Associates a tag with a key.
-   `Untag(key, tag string) error`: Removes a tag association from a key.
-   `IterateByTag(tag string, offset, limit int) ([]string, error)`: Retrieves a list of keys associated with a given tag, with pagination.

### Cache Operations

-   `SetCache(key, value string, ttl time.Duration) error`: Stores a key-value pair in the cache with a specific TTL.
-   `GetCache(key string) (string, error)`: Retrieves a value from the cache by its key.
-   `DeleteCache(key string) error`: Removes a key-value pair from the cache.

### Event Operations

-   `PublishEvent(topic string, data any) error`: Publishes an event to a specific topic with arbitrary data.
-   `SubscribeToEvents(topic string, ctx context.Context, onEvent func(data any)) error`:
    Subscribes to events on a given topic via WebSocket.
    -   `topic`: The event topic to subscribe to.
    -   `ctx`: A `context.Context` to control the lifetime of the subscription. Cancelling the context will close the WebSocket connection.
    -   `onEvent`: A callback function `func(data any)` that will be invoked with the `data` field of each received `models.Event`.

### System Operations

These operations are typically used for cluster administration.

-   `Join(followerID, followerAddr string) error`:
    Requests the target node (which must be a leader) to add a new follower to the Raft cluster.
    -   `followerID`: The unique ID of the node to be added.
    -   `followerAddr`: The Raft binding address (host:port) of the follower node.
-   `NewAPIKey(entityName string) (string, error)`: Requests the server to generate a new API key for the given entity.
-   `DeleteAPIKey(apiKey string) error`: Requests the server to delete an existing API key.
-   `Ping() (map[string]string, error)`: Sends a ping request to the server. Returns a map containing server status information or an error if the ping fails.

## Error Handling

All client methods return an `error`. It is crucial to check this error to ensure the operation was successful. Errors can range from network issues, server-side problems, authentication failures, to invalid input.

The client logger will also output detailed information about requests and responses, which can be helpful for debugging.

## Example Usage: Set and Get a Value

```go
// Assuming 'c' is an initialized *client.Client and 'logger' is an *slog.Logger

key := "mykey"
value := "myvalue"

err = c.Set(key, value)
if err != nil {
    logger.Error("Failed to set value", "key", key, "error", err)
    return
}
logger.Info("Successfully set value", "key", key)

retrievedValue, err := c.Get(key)
if err != nil {
    logger.Error("Failed to get value", "key", key, "error", err)
    return
}
logger.Info("Successfully retrieved value", "key", key, "value", retrievedValue)

if retrievedValue != value {
    logger.Error("Retrieved value does not match set value!", "expected", value, "got", retrievedValue)
}
```

This markdown file should serve as a good starting point for understanding and using the `insi/client`.
