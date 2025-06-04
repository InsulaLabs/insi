package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
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
	insiNodeURL = strings.TrimRight(insiNodeURL, "/")

	command := os.Args[1]

	switch command {
	case "put":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "Usage: ./insio put <filepath>")
			os.Exit(1)
		}
		filePath := os.Args[2]
		handlePut(insiNodeURL, apiKey, filePath)
	case "get":
		if len(os.Args) < 3 || len(os.Args) > 4 {
			fmt.Fprintln(os.Stderr, "Usage: ./insio get <UUID> [outputpath]")
			os.Exit(1)
		}
		objectUUID := os.Args[2]
		outputPath := ""
		if len(os.Args) == 4 {
			outputPath = os.Args[3]
		} else {
			outputPath = objectUUID
		}
		handleGet(insiNodeURL, apiKey, objectUUID, outputPath)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: ./insio <command> [args]")
	fmt.Println("Commands:")
	fmt.Println("  put <filepath>         Uploads a file to the '/objects/upload' endpoint.")
	fmt.Println("  get <UUID> [outputpath] Downloads a file from '/objects/download' endpoint.")
	fmt.Println("                         Default output path if not specified is './<UUID>'.")
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
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating form file: %v\n", err)
		os.Exit(1)
	}
	_, err = io.Copy(part, file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error copying file content to form: %v\n", err)
		os.Exit(1)
	}
	err = writer.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error closing multipart writer: %v\n", err)
		os.Exit(1)
	}

	uploadURL := fmt.Sprintf("%s/objects/upload", nodeURL)
	req, err := http.NewRequest("POST", uploadURL, body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating POST request: %v\n", err)
		os.Exit(1)
	}

	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error sending request to %s: %v\n", uploadURL, err)
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
}

func handleGet(nodeURL, apiKey, objectUUID, outputPath string) {
	downloadURL := fmt.Sprintf("%s/objects/download?id=%s", nodeURL, objectUUID)

	client := &http.Client{
		Timeout: 10 * time.Minute,
	}

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

	finalURL := resp.Request.URL.String()
	if finalURL != downloadURL {
		fmt.Printf("Request was redirected to: %s\n", finalURL)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Error downloading file from %s. Server responded with %s: %s\n", finalURL, resp.Status, string(bodyBytes))
		os.Exit(1)
	}

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

	bytesCopied, err := io.Copy(outFile, resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing downloaded content to file %s: %v\n", outputPath, err)
		os.Exit(1)
	}

	fmt.Printf("File downloaded successfully to %s (%d bytes).\n", outputPath, bytesCopied)

	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		fmt.Printf("Server suggested filename (Content-Disposition): %s\n", contentDisposition)
	}
}
