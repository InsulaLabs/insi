package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"log/slog"

	"github.com/InsulaLabs/insi/client"
)

var skipTLSVerify bool

// ProgressReader is an io.Reader that reports progress.
type ProgressReader struct {
	reader         io.Reader
	total          int64
	current        int64
	lastReportTime time.Time
	operation      string // e.g., "Uploading" or "Downloading"
	fileName       string // Name of the file being processed
	isEOF          bool   // Flag to indicate if EOF has been reached
}

// NewProgressReader creates a new ProgressReader.
func NewProgressReader(reader io.Reader, total int64, operation, fileName string) *ProgressReader {
	return &ProgressReader{
		reader:         reader,
		total:          total,
		operation:      operation,
		fileName:       filepath.Base(fileName), // Use only the base name for display
		lastReportTime: time.Now(),
	}
}

// getHTTPClient creates an HTTP client with appropriate TLS settings and timeout.
func getHTTPClient(skipVerify bool, timeout time.Duration) *http.Client {
	transport := &http.Transport{
		// Proxy: http.ProxyFromEnvironment, // Uncomment if proxy support is needed
		// DialContext: (&net.Dialer{ // More specific dialing options if needed
		//	Timeout:   30 * time.Second,
		//	KeepAlive: 30 * time.Second,
		// }).DialContext,
		// ForceAttemptHTTP2:     true,
		// MaxIdleConns:          100,
		// IdleConnTimeout:       90 * time.Second,
		// TLSHandshakeTimeout:   10 * time.Second,
		// ExpectContinueTimeout: 1 * time.Second,
	}
	if skipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
	}
}

// Read implements the io.Reader interface.
func (pr *ProgressReader) Read(p []byte) (n int, err error) {
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

	// No explicit newline here anymore, final printProgress handles it or a new log line will start fresh.
	return
}

func (pr *ProgressReader) printProgress() {
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
		progressString = fmt.Sprintf("%s %s: %s downloaded",
			pr.operation,
			pr.fileName,
			formatBytes(pr.current))
	}

	fmt.Fprintf(os.Stderr, "\r\033[K%s", progressString) // Print with carriage return and clear line

	// If EOF has been reached, this is the final progress update, so print a newline.
	if pr.isEOF {
		fmt.Fprintln(os.Stderr)
	}
}

// formatBytes converts bytes to a human-readable string.
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

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	// Process global flags and filter them out from command-specific args
	var commandArgs []string
	for _, arg := range os.Args[1:] { // Start from os.Args[1] to skip program name
		if arg == "--skip-tls-verify" {
			skipTLSVerify = true
		} else {
			commandArgs = append(commandArgs, arg)
		}
	}

	if len(commandArgs) == 0 {
		fmt.Fprintln(os.Stderr, "Error: No command specified.")
		printUsage()
		os.Exit(1)
	}

	apiKey := os.Getenv("INSI_API_KEY")
	if apiKey == "" {
		fmt.Fprintln(os.Stderr, "Error: INSI_API_KEY environment variable not set.")
		os.Exit(1)
	}

	insiNodeURL := os.Getenv("INSI_NODE_URL")
	if insiNodeURL == "" {
		fmt.Fprintln(os.Stderr, "Error: INSI_NODE_URL environment variable not set (e.g., http://localhost:8080).")
		os.Exit(1)
	}
	// Ensure no trailing slash for insiNodeURL
	insiNodeURL = strings.TrimRight(insiNodeURL, "/")

	// Create a new logger for the client
	logOpts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stderr, logOpts)
	logger := slog.New(handler)

	// Create a new client
	c, err := client.NewClient(&client.Config{
		ConnectionType: client.ConnectionTypeDirect,
		Endpoints: []client.Endpoint{
			{
				HostPort: insiNodeURL, // This might need parsing if it includes http://
			},
		},
		ApiKey:     apiKey,
		SkipVerify: skipTLSVerify,
		Logger:     logger,
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating client: %v\n", err)
		os.Exit(1)
	}

	command := commandArgs[0]
	switch command {
	case "put":
		if len(commandArgs) != 2 { // Expecting command + filepath
			fmt.Fprintln(os.Stderr, "Usage: ./insio [--skip-tls-verify] put <filepath>")
			os.Exit(1)
		}
		filePath := commandArgs[1]
		handlePut(c, filePath)
	case "get":
		if len(commandArgs) < 2 || len(commandArgs) > 3 { // Expecting command + UUID [+ outputpath]
			fmt.Fprintln(os.Stderr, "Usage: ./insio [--skip-tls-verify] get <UUID> [outputpath]")
			os.Exit(1)
		}
		objectUUID := commandArgs[1]
		outputPath := ""
		if len(commandArgs) == 3 {
			outputPath = commandArgs[2]
		} else {
			outputPath = objectUUID // Default output path is ./<UUID>
		}
		handleGet(c, objectUUID, outputPath)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: ./insio [--skip-tls-verify] <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("  put <filepath>         Uploads a file to the '/objects/upload' endpoint.")
	fmt.Println("  get <UUID> [outputpath] Downloads a file from '/objects/download' endpoint.")
	fmt.Println("                         Default output path if not specified is './<UUID>'.")
	fmt.Println("\nGlobal Flags:")
	fmt.Println("  --skip-tls-verify      Skip TLS certificate verification.")
	fmt.Println("\nEnvironment variables:")
	fmt.Println("  INSI_API_KEY:  Your API token for authentication.")
	fmt.Println("  INSI_NODE_URL: The base URL of the Insi node (e.g., http://localhost:8080).")
}

func handlePut(c *client.Client, filePath string) {
	resp, err := c.ObjectUpload(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error uploading file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("File uploaded successfully. ObjectID: %s\n", resp.ObjectID)
	if resp.Message != "" {
		fmt.Printf("Server message: %s\n", resp.Message)
	}
}

func handleGet(c *client.Client, objectUUID, outputPath string) {
	err := c.ObjectDownload(objectUUID, outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error downloading file: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("File downloaded successfully to %s\n", outputPath)
}
