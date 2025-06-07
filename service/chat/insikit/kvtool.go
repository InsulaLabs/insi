package insikit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/InsulaLabs/insi/ai/kit"
	"github.com/InsulaLabs/insi/client"
	"github.com/tmc/langchaingo/llms"
)

// The toolkit(s) the user can import DURING chat.

// KVKit provides tools for interacting with the Insi K/V store.
type KVKit struct {
	client *client.Client
}

// NewKVKit creates a new KVKit.
func NewKVKit(c *client.Client) *KVKit {
	return &KVKit{
		client: c,
	}
}

// KVKitTool defines the names for the K/V tools.
type KVKitTool string

const (
	KVKitToolSet           KVKitTool = "kvSet"
	KVKitToolGet           KVKitTool = "kvGet"
	KVKitToolDelete        KVKitTool = "kvDelete"
	KVKitToolIteratePrefix KVKitTool = "kvIterateByPrefix"
)

// GetTool returns a tool that can be used with a kit.
func (k *KVKit) GetTool(name KVKitTool) (kit.Tool, error) {
	switch name {
	case KVKitToolSet:
		return &kvSetTool{k: k}, nil
	case KVKitToolGet:
		return &kvGetTool{k: k}, nil
	case KVKitToolDelete:
		return &kvDeleteTool{k: k}, nil
	case KVKitToolIteratePrefix:
		return &kvIterateByPrefixTool{k: k}, nil
	}
	return nil, fmt.Errorf("invalid kvtool name: %s", name)
}

// GetAllTools returns all available K/V tools.
func (k *KVKit) GetAllTools() []kit.Tool {
	return []kit.Tool{
		&kvSetTool{k: k},
		&kvGetTool{k: k},
		&kvDeleteTool{k: k},
		&kvIterateByPrefixTool{k: k},
	}
}

// --- Tool Implementations ---

// kvSetTool sets a key-value pair.
type kvSetTool struct{ k *KVKit }

type kvSetArgs struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Definition returns the tool's definition.
func (t *kvSetTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        string(KVKitToolSet),
			Description: "Sets a value for a given key in the K/V store. This will overwrite an existing value.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{
						"type":        "string",
						"description": "The key to set.",
					},
					"value": map[string]any{
						"type":        "string",
						"description": "The value to associate with the key.",
					},
				},
				"required": []string{"key", "value"},
			},
		},
	}
}

// Execute runs the tool.
func (t *kvSetTool) Execute(_ context.Context, arguments string) (string, error) {
	var args kvSetArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return fmt.Sprintf("error: invalid arguments: %s", err.Error()), nil
	}
	if err := t.k.client.Set(args.Key, args.Value); err != nil {
		return fmt.Sprintf("error: failed to set key %q: %s", args.Key, err.Error()), nil
	}
	return fmt.Sprintf("key %q set successfully", args.Key), nil
}

// GetPrompt returns the prompt for the tool.
func (t *kvSetTool) GetPrompt() string {
	return `
	Tool: kvSet
	Description: Sets a value for a given key in the K/V store. This will overwrite an existing value.
	Use this tool to store or update a key-value pair.
	`
}

// kvGetTool gets a value for a key.
type kvGetTool struct{ k *KVKit }

type kvGetArgs struct {
	Key string `json:"key"`
}

// Definition returns the tool's definition.
func (t *kvGetTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        string(KVKitToolGet),
			Description: "Gets the value for a given key from the K/V store.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{
						"type":        "string",
						"description": "The key to get the value of.",
					},
				},
				"required": []string{"key"},
			},
		},
	}
}

// Execute runs the tool.
func (t *kvGetTool) Execute(_ context.Context, arguments string) (string, error) {
	var args kvGetArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return fmt.Sprintf("error: invalid arguments: %s", err.Error()), nil
	}
	value, err := t.k.client.Get(args.Key)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return fmt.Sprintf("error: key not found: %s", args.Key), nil
		}
		return fmt.Sprintf("error: failed to get key %q: %s", args.Key, err.Error()), nil
	}
	return value, nil
}

// GetPrompt returns the prompt for the tool.
func (t *kvGetTool) GetPrompt() string {
	return `
	Tool: kvGet
	Description: Gets the value for a given key from the K/V store.
	Use this tool to retrieve a value associated with a key.
	`
}

// kvDeleteTool deletes a key.
type kvDeleteTool struct{ k *KVKit }

type kvDeleteArgs struct {
	Key string `json:"key"`
}

// Definition returns the tool's definition.
func (t *kvDeleteTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        string(KVKitToolDelete),
			Description: "Deletes a key and its value from the K/V store.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"key": map[string]any{
						"type":        "string",
						"description": "The key to delete.",
					},
				},
				"required": []string{"key"},
			},
		},
	}
}

// Execute runs the tool.
func (t *kvDeleteTool) Execute(_ context.Context, arguments string) (string, error) {
	var args kvDeleteArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return fmt.Sprintf("error: invalid arguments: %s", err.Error()), nil
	}
	if err := t.k.client.Delete(args.Key); err != nil {
		return fmt.Sprintf("error: failed to delete key %q: %s", args.Key, err.Error()), nil
	}
	return fmt.Sprintf("key %q deleted successfully", args.Key), nil
}

// GetPrompt returns the prompt for the tool.
func (t *kvDeleteTool) GetPrompt() string {
	return `
	Tool: kvDelete
	Description: Deletes a key and its value from the K/V store.
	Use this tool to remove a key-value pair.
	`
}

// kvIterateByPrefixTool iterates keys by prefix.
type kvIterateByPrefixTool struct{ k *KVKit }

type kvIterateByPrefixArgs struct {
	Prefix string `json:"prefix"`
	Offset int    `json:"offset,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// Definition returns the tool's definition.
func (t *kvIterateByPrefixTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        string(KVKitToolIteratePrefix),
			Description: "Lists keys in the K/V store that match a given prefix.",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"prefix": map[string]any{
						"type":        "string",
						"description": "The prefix to search for keys.",
					},
					"offset": map[string]any{
						"type":        "integer",
						"description": "The number of keys to skip before returning results. Defaults to 0.",
					},
					"limit": map[string]any{
						"type":        "integer",
						"description": "The maximum number of keys to return. Defaults to 100.",
					},
				},
				"required": []string{"prefix"},
			},
		},
	}
}

// Execute runs the tool.
func (t *kvIterateByPrefixTool) Execute(_ context.Context, arguments string) (string, error) {
	var args kvIterateByPrefixArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return fmt.Sprintf("error: invalid arguments: %s", err.Error()), nil
	}
	if args.Limit == 0 {
		args.Limit = 100
	}
	keys, err := t.k.client.IterateByPrefix(args.Prefix, args.Offset, args.Limit)
	if err != nil {
		if errors.Is(err, client.ErrKeyNotFound) {
			return "[]", nil
		}
		return fmt.Sprintf("error: failed to iterate keys with prefix %q: %s", args.Prefix, err.Error()), nil
	}
	if len(keys) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(keys)
	if err != nil {
		return fmt.Sprintf("error: failed to marshal keys to JSON: %s", err.Error()), nil
	}
	return string(b), nil
}

// GetPrompt returns the prompt for the tool.
func (t *kvIterateByPrefixTool) GetPrompt() string {
	return `
	Tool: kvIterateByPrefix
	Description: Lists keys in the K/V store that match a given prefix.
	Use this tool to discover keys.
	`
}
