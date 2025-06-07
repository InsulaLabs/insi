package docukit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/InsulaLabs/insi/ai/kit"

	"github.com/tmc/langchaingo/llms"
)

var (
	ErrInvalidTool      = fmt.Errorf("invalid tool name")
	ErrDocumentNotFound = fmt.Errorf("document not found")
)

type DocuKitTool string

const (
	DocuKitToolInsertRange    DocuKitTool = "insertRange"
	DocuKitToolDeleteRange    DocuKitTool = "deleteRange"
	DocuKitToolAddDocument    DocuKitTool = "addDocument"
	DocuKitToolListDocuments  DocuKitTool = "listDocuments"
	DocuKitToolGetDocument    DocuKitTool = "getDocument"
	DocuKitToolDeleteDocument DocuKitTool = "deleteDocument"
)

// DocuKit hosts a collection of in-memory documents and provides tools to interact with them.
type DocuKit struct {
	activeDocuments map[string]string
}

// NewDocuKit creates a new DocuKit.
func NewDocuKit() *DocuKit {
	return &DocuKit{
		activeDocuments: make(map[string]string),
	}
}

// WithDocument adds a document to the DocuKit's in-memory store.
func (d *DocuKit) WithDocument(name, content string) *DocuKit {
	d.activeDocuments[name] = content
	return d
}

// GetTool returns a tool that can be used with a kit.
func (d *DocuKit) GetTool(name DocuKitTool) (kit.Tool, error) {
	switch name {
	case DocuKitToolInsertRange:
		return &insertRangeTool{dk: d}, nil
	case DocuKitToolDeleteRange:
		return &deleteRangeTool{dk: d}, nil
	case DocuKitToolAddDocument:
		return &addDocumentTool{dk: d}, nil
	case DocuKitToolListDocuments:
		return &listDocumentsTool{dk: d}, nil
	case DocuKitToolGetDocument:
		return &getDocumentTool{dk: d}, nil
	case DocuKitToolDeleteDocument:
		return &deleteDocumentTool{dk: d}, nil
	}
	return nil, ErrInvalidTool
}

// --- Tool Implementations ---

// addDocumentTool adds a document.
type addDocumentTool struct{ dk *DocuKit }

type addDocumentArgs struct {
	DocumentName string `json:"documentName"`
	Content      string `json:"content"`
}

func (t *addDocumentTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "addDocument",
			Description: "Adds a new document to the in-memory store.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"documentName": map[string]any{
						"type":        "string",
						"description": "The name of the document to add.",
					},
					"content": map[string]any{
						"type":        "string",
						"description": "The initial content of the document.",
					},
				},
				"required": []string{"documentName", "content"},
			},
		},
	}
}

func (t *addDocumentTool) GetPrompt() string {
	return `
	Tool: addDocument
	Description: Adds a new document to the in-memory store.
	Use this tool to create a new document that you can work on.
	`
}

func (t *addDocumentTool) Execute(_ context.Context, arguments string) (string, error) {
	var args addDocumentArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}
	t.dk.activeDocuments[args.DocumentName] = args.Content
	return fmt.Sprintf("document %q added successfully", args.DocumentName), nil
}

// listDocumentsTool lists all documents.
type listDocumentsTool struct{ dk *DocuKit }

func (t *listDocumentsTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "listDocuments",
			Description: "Lists the names of all documents in the store.",
			Parameters:  map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}
}

func (t *listDocumentsTool) Execute(_ context.Context, _ string) (string, error) {
	names := make([]string, 0, len(t.dk.activeDocuments))
	for name := range t.dk.activeDocuments {
		names = append(names, name)
	}
	b, err := json.Marshal(names)
	if err != nil {
		return "", fmt.Errorf("failed to marshal document names: %w", err)
	}
	return string(b), nil
}

func (t *listDocumentsTool) GetPrompt() string {
	return `
	Tool: listDocuments
	Description: Lists the names of all documents in the store.
	Use this tool to see what documents are available to work on.
	`
}

// getDocumentTool retrieves a document.
type getDocumentTool struct{ dk *DocuKit }

type getDocumentArgs struct {
	DocumentName string `json:"documentName"`
}

func (t *getDocumentTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "getDocument",
			Description: "Gets the content of a specific document.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"documentName": map[string]any{
						"type":        "string",
						"description": "The name of the document to retrieve.",
					},
				},
				"required": []string{"documentName"},
			},
		},
	}
}

func (t *getDocumentTool) Execute(_ context.Context, arguments string) (string, error) {
	var args getDocumentArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}
	content, ok := t.dk.activeDocuments[args.DocumentName]
	if !ok {
		return "", ErrDocumentNotFound
	}
	return content, nil
}

func (t *getDocumentTool) GetPrompt() string {
	return `
	Tool: getDocument
	Description: Gets the content of a specific document.
	Use this tool to read the contents of a document.
	`
}

// deleteDocumentTool deletes a document.
type deleteDocumentTool struct{ dk *DocuKit }

type deleteDocumentArgs struct {
	DocumentName string `json:"documentName"`
}

func (t *deleteDocumentTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "deleteDocument",
			Description: "Deletes a document from the in-memory store.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"documentName": map[string]any{
						"type":        "string",
						"description": "The name of the document to delete.",
					},
				},
				"required": []string{"documentName"},
			},
		},
	}
}

func (t *deleteDocumentTool) Execute(_ context.Context, arguments string) (string, error) {
	var args deleteDocumentArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}
	if _, ok := t.dk.activeDocuments[args.DocumentName]; !ok {
		return "", ErrDocumentNotFound
	}
	delete(t.dk.activeDocuments, args.DocumentName)
	return fmt.Sprintf("document %q deleted successfully", args.DocumentName), nil
}

func (t *deleteDocumentTool) GetPrompt() string {
	return `
	Tool: deleteDocument
	Description: Deletes a document from the in-memory store.
	Use this tool to remove a document you no longer need.
	`
}

// insertRangeTool inserts or replaces a range of lines in a document.
type insertRangeTool struct{ dk *DocuKit }

type insertRangeArgs struct {
	DocumentName string `json:"documentName"`
	StartLine    int    `json:"startLine"`
	EndLine      int    `json:"endLine"`
	Content      string `json:"content"`
}

func (t *insertRangeTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "insertRange",
			Description: "Inserts or replaces a range of lines in a document. To insert, set startLine and endLine to the same line number.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"documentName": map[string]any{"type": "string", "description": "The name of the document to modify."},
					"startLine":    map[string]any{"type": "integer", "description": "The 1-indexed starting line number for the operation."},
					"endLine":      map[string]any{"type": "integer", "description": "The 1-indexed ending line number for the replacement range. For insertion, this should be the same as startLine."},
					"content":      map[string]any{"type": "string", "description": "The content to insert or use as replacement."},
				},
				"required": []string{"documentName", "startLine", "endLine", "content"},
			},
		},
	}
}

func (t *insertRangeTool) Execute(_ context.Context, arguments string) (string, error) {
	var args insertRangeArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}

	doc, ok := t.dk.activeDocuments[args.DocumentName]
	if !ok {
		return "", ErrDocumentNotFound
	}

	lines := strings.Split(doc, "\n")
	start := args.StartLine - 1
	end := args.EndLine - 1

	if start < 0 || start > len(lines) || end < 0 || end > len(lines) || start > end {
		return "", fmt.Errorf("invalid line range: start %d, end %d (document has %d lines)", args.StartLine, args.EndLine, len(lines))
	}

	contentLines := strings.Split(args.Content, "\n")

	var newLines []string
	newLines = append(newLines, lines[:start]...)
	newLines = append(newLines, contentLines...)
	newLines = append(newLines, lines[end:]...)

	t.dk.activeDocuments[args.DocumentName] = strings.Join(newLines, "\n")
	return fmt.Sprintf("successfully modified %q", args.DocumentName), nil
}

func (t *insertRangeTool) GetPrompt() string {
	return `
	Tool: insertRange
	Description: Inserts or replaces a range of lines in a document.
	Use this tool to add new content or modify existing lines in a document.
	`
}

// deleteRangeTool deletes a range of lines from a document.
type deleteRangeTool struct{ dk *DocuKit }

type deleteRangeArgs struct {
	DocumentName string `json:"documentName"`
	StartLine    int    `json:"startLine"`
	EndLine      int    `json:"endLine"`
}

func (t *deleteRangeTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "deleteRange",
			Description: "Deletes a range of lines from a document.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"documentName": map[string]any{"type": "string", "description": "The name of the document to modify."},
					"startLine":    map[string]any{"type": "integer", "description": "The 1-indexed starting line number of the range to delete."},
					"endLine":      map[string]any{"type": "integer", "description": "The 1-indexed ending line number of the range to delete."},
				},
				"required": []string{"documentName", "startLine", "endLine"},
			},
		},
	}
}

func (t *deleteRangeTool) Execute(_ context.Context, arguments string) (string, error) {
	var args deleteRangeArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}

	doc, ok := t.dk.activeDocuments[args.DocumentName]
	if !ok {
		return "", ErrDocumentNotFound
	}

	lines := strings.Split(doc, "\n")
	start := args.StartLine - 1
	end := args.EndLine

	if start < 0 || start >= len(lines) || end < 0 || end > len(lines) || start >= end {
		return "", fmt.Errorf("invalid line range: start %d, end %d (document has %d lines)", args.StartLine, args.EndLine, len(lines))
	}

	var newLines []string
	newLines = append(newLines, lines[:start]...)
	newLines = append(newLines, lines[end:]...)

	t.dk.activeDocuments[args.DocumentName] = strings.Join(newLines, "\n")
	return fmt.Sprintf("successfully deleted range from %q", args.DocumentName), nil
}

func (t *deleteRangeTool) GetPrompt() string {
	return `
	Tool: deleteRange
	Description: Deletes a range of lines from a document.
	Use this tool to remove lines from a document.
	`
}

func (d *DocuKit) GetAllTools() []kit.Tool {
	return []kit.Tool{
		&addDocumentTool{dk: d},
		&listDocumentsTool{dk: d},
		&getDocumentTool{dk: d},
		&deleteDocumentTool{dk: d},
		&insertRangeTool{dk: d},
		&deleteRangeTool{dk: d},
	}
}

func (d *DocuKit) GetAllDocuments() map[string]string {
	return d.activeDocuments
}
