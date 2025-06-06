package chat

import "time"

// Message struct was here, will be replaced by ChatMessage or similar if needed,
// or removed if not used by the new API. For now, we are defining the new structures.

// ChatCompletionRequest is the structure for an OpenAI-compatible chat completion request.
type ChatCompletionRequest struct {
	Model            string        `json:"model"`
	Messages         []ChatMessage `json:"messages"`
	MaxTokens        int           `json:"max_tokens,omitempty"`
	Temperature      float32       `json:"temperature,omitempty"`
	TopP             float32       `json:"top_p,omitempty"`
	N                int           `json:"n,omitempty"`
	Stream           bool          `json:"stream,omitempty"`
	Stop             []string      `json:"stop,omitempty"`
	PresencePenalty  float32       `json:"presence_penalty,omitempty"`
	FrequencyPenalty float32       `json:"frequency_penalty,omitempty"`
	User             string        `json:"user,omitempty"`
}

// ChatMessage represents a single message in the chat history.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatCompletionResponse is the structure for a non-streaming OpenAI-compatible chat completion response.
type ChatCompletionResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// Choice represents a single choice in a non-streaming chat completion response.
type Choice struct {
	Index        int             `json:"index"`
	Message      ResponseMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

// ResponseMessage represents the message returned in a non-streaming choice.
type ResponseMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Usage represents the token usage statistics.
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// --- Streaming Structures ---

// ChatCompletionStreamResponse is the structure for a streaming OpenAI-compatible chat completion response.
// This is what will be sent for each chunk.
type ChatCompletionStreamResponse struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"` // e.g., "chat.completion.chunk"
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []StreamChoice `json:"choices"`
}

// StreamChoice represents a single choice in a streaming chat completion response.
type StreamChoice struct {
	Index        int          `json:"index"`
	Delta        DeltaMessage `json:"delta"`                   // The actual content difference
	FinishReason *string      `json:"finish_reason,omitempty"` // Pointer to allow null when not finished
}

// DeltaMessage represents the delta content in a streaming choice.
// Either Role or Content (or both if it's the first chunk for a message) can be present.
type DeltaMessage struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Helper to create a timestamp
func nowTimestamp() int64 {
	return time.Now().Unix()
}
