package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

	command := commandArgs[0]
	switch command {
	case "put":
		if len(commandArgs) != 2 { // Expecting command + filepath
			fmt.Fprintln(os.Stderr, "Usage: ./insio [--skip-tls-verify] put <filepath>")
			os.Exit(1)
		}
		filePath := commandArgs[1]
		handlePut(insiNodeURL, apiKey, filePath)
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
		handleGet(insiNodeURL, apiKey, objectUUID, outputPath)
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

func handlePut(nodeURL, apiKey, filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", filePath, err)
		os.Exit(1)
	}
	// defer file.Close() will be handled after hashing and reopening/seeking for upload

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting file info for %s: %v\n", filePath, err)
		file.Close()
		os.Exit(1)
	}
	fileSize := fileInfo.Size()

	// Calculate SHA256 hash of the file before uploading
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		fmt.Fprintf(os.Stderr, "Error calculating SHA256 hash for %s: %v\n", filePath, err)
		file.Close()
		os.Exit(1)
	}
	clientCalculatedSha256 := hex.EncodeToString(hasher.Sum(nil))
	fmt.Fprintf(os.Stderr, "Calculated SHA256 for %s: %s\n", filepath.Base(filePath), clientCalculatedSha256)

	// We need to reset the file reader to the beginning for the upload
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error seeking to start of file %s: %v\n", filePath, err)
		file.Close()
		os.Exit(1)
	}
	// defer file.Close() should be active now for the upload part
	defer file.Close()

	pipeReader, pipeWriter := io.Pipe()
	mpWriter := multipart.NewWriter(pipeWriter) // Renamed to mpWriter to avoid conflict

	// Channel to capture errors from the goroutine
	errChan := make(chan error, 1)

	go func() {
		var opErr error
		// Ensure pipeWriter is closed. If opErr is not nil, close with error.
		defer func() {
			if opErr != nil {
				pipeWriter.CloseWithError(opErr)
			} else {
				// Normal closure: first multipart, then pipe.
				if err := mpWriter.Close(); err != nil {
					opErr = fmt.Errorf("error closing multipart writer: %w", err)
					pipeWriter.CloseWithError(opErr) // Close pipe with this error
					errChan <- opErr
					return
				}
				pipeWriter.Close()
			}
		}()

		// Add the clientCalculatedSha256 as a form field
		if err := mpWriter.WriteField("clientSha256", clientCalculatedSha256); err != nil {
			opErr = fmt.Errorf("error writing clientSha256 field: %w", err)
			errChan <- opErr
			return
		}

		part, err := mpWriter.CreateFormFile("file", filepath.Base(filePath))
		if err != nil {
			opErr = fmt.Errorf("error creating form file: %w", err)
			errChan <- opErr
			return
		}

		progressReader := NewProgressReader(file, fileSize, "Uploading", filePath)
		if _, err = io.Copy(part, progressReader); err != nil {
			opErr = fmt.Errorf("error copying file content to form: %w", err)
			errChan <- opErr
			return
		}
		errChan <- nil // Signal success or that mpWriter.Close() will be attempted
	}()

	uploadURL := fmt.Sprintf("%s/objects/upload", nodeURL)
	req, err := http.NewRequest("POST", uploadURL, pipeReader)
	if err != nil {
		// Ensure pipeWriter is closed to unblock goroutine if it's stuck writing
		pipeWriter.CloseWithError(fmt.Errorf("error creating POST request: %w", err))
		fmt.Fprintf(os.Stderr, "Error creating POST request: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", mpWriter.FormDataContentType())
	// We don't set ContentLength, allowing for chunked transfer encoding.

	client := getHTTPClient(skipTLSVerify, 30*time.Minute)
	resp, clientDoErr := client.Do(req)

	// Wait for the goroutine to finish and get its error status
	goroutineErr := <-errChan

	if clientDoErr != nil {
		fmt.Fprintf(os.Stderr, "Error sending request to %s: %v\n", uploadURL, clientDoErr)
		if goroutineErr != nil {
			fmt.Fprintf(os.Stderr, "Additional error from upload goroutine: %v\n", goroutineErr)
		}
		os.Exit(1)
	}
	// If client.Do was successful, but the goroutine had an error (e.g., closing multipart writer)
	if goroutineErr != nil {
		fmt.Fprintf(os.Stderr, "Error during upload processing (goroutine): %v\n", goroutineErr)
		// Close response body if open, as we are exiting.
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		os.Exit(1)
	}

	defer resp.Body.Close()

	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading response body: %v\n", err)
		os.Exit(1)
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error uploading file. Server responded with %s: %s\n", resp.Status, string(respBodyBytes))
		os.Exit(1)
	}

	var result map[string]string
	if err := json.Unmarshal(respBodyBytes, &result); err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding JSON response '%s': %v\n", string(respBodyBytes), err)
		os.Exit(1)
	}

	objectID, ok := result["objectID"]
	if !ok {
		fmt.Fprintf(os.Stderr, "Error: 'objectID' not found in response: %s\n", string(respBodyBytes))
		os.Exit(1)
	}

	fmt.Printf("File uploaded successfully. ObjectID: %s\n", objectID)

	// Print additional info from response if available
	if message, ok := result["message"]; ok {
		fmt.Printf("Server message: %s\n", message)
	}
	if respClientSha, ok := result["clientSha256"]; ok {
		fmt.Printf("Server received client SHA256: %s\n", respClientSha)
	}
	if respCalcSha, ok := result["calculatedSha256"]; ok {
		fmt.Printf("Server calculated SHA256: %s\n", respCalcSha)
	}
}

func handleGet(nodeURL, apiKey, objectUUID, outputPath string) {
	downloadURL := fmt.Sprintf("%s/objects/download?id=%s", nodeURL, objectUUID)

	client := getHTTPClient(skipTLSVerify, 10*time.Minute)

	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating GET request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)

	fmt.Printf("Attempting to download object %s from %s\n", objectUUID, downloadURL)

	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending request to %s: %v\n", downloadURL, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// After potential redirects, resp.Request.URL will contain the final URL.
	finalURL := resp.Request.URL.String()
	if finalURL != downloadURL {
		fmt.Printf("Request was redirected to: %s\n", finalURL)
	}

	// Get content length for progress bar
	contentLengthStr := resp.Header.Get("Content-Length")
	contentLength, err := strconv.ParseInt(contentLengthStr, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not parse Content-Length header ('%s'). Progress percentage will not be shown. Error: %v\n", contentLengthStr, err)
		contentLength = -1 // Indicate unknown size
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body) // Read body for error context
		fmt.Fprintf(os.Stderr, "Error downloading file from %s. Server responded with %s: %s\n", finalURL, resp.Status, string(bodyBytes))
		os.Exit(1)
	}

	// Ensure output directory exists if outputPath includes a path
	if filepath.Dir(outputPath) != "." && filepath.Dir(outputPath) != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output directory %s: %v\n", filepath.Dir(outputPath), err)
			os.Exit(1)
		}
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file %s: %v\n", outputPath, err)
		os.Exit(1)
	}
	defer outFile.Close()

	progressReader := NewProgressReader(resp.Body, contentLength, "Downloading", outputPath)
	bytesCopied, err := io.Copy(outFile, progressReader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing downloaded content to file %s: %v\n", outputPath, err)
		os.Exit(1)
	}

	fmt.Printf("File downloaded successfully to %s (%s).\n", outputPath, formatBytes(bytesCopied))

	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		fmt.Printf("Server suggested filename (Content-Disposition): %s\n", contentDisposition)
	}
}
