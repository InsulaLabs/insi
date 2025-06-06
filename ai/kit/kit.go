package kit

import (
	"context"
	"log/slog"
	"strings"

	"github.com/tmc/langchaingo/llms"
)

// Kit is a tool-using agent that can complete tasks and stream responses.
type Kit struct {
	llm    llms.Model
	log    *slog.Logger
	tools  map[string]Tool
	ctx    context.Context
	cancel context.CancelFunc
}

// Tool is an interface for tools that can be used by the Kit.
type Tool interface {
	Definition() llms.Tool
	Execute(ctx context.Context, arguments string) (string, error)

	GetPrompt() string
}

// NewKit creates a new Kit.
func NewKit(parentCtx context.Context, llm llms.Model, logger *slog.Logger) *Kit {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Kit{
		llm:    llm,
		log:    logger,
		tools:  make(map[string]Tool),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Ctx returns the Kit's managed context.
func (k *Kit) Ctx() context.Context {
	return k.ctx
}

// WithTool adds a tool to the Kit.
func (k *Kit) WithTool(tool Tool) *Kit {
	k.tools[tool.Definition().Function.Name] = tool
	k.log.Info("tool registered", "name", tool.Definition().Function.Name)
	return k
}

// WithStopTool adds a tool to the Kit that can be used by the LLM to signal task completion.
func (k *Kit) WithStopTool() *Kit {
	return k.WithTool(&StopTool{kit: k})
}

type completeOptions struct {
	forceToolUse  bool
	streamingFunc func(ctx context.Context, chunk []byte) error
}

// CompleteOption is a functional option for the Complete method.
type CompleteOption func(*completeOptions)

// WithForcedToolUse is an option to force the LLM to use a tool.
func WithForcedToolUse() CompleteOption {
	return func(o *completeOptions) {
		o.forceToolUse = true
	}
}

// WithStreamingFunc is an option to stream the final response.
func WithStreamingFunc(fn func(ctx context.Context, chunk []byte) error) CompleteOption {
	return func(o *completeOptions) {
		o.streamingFunc = fn
	}
}

// Complete executes a prompt and returns the final message history.
func (k *Kit) Complete(ctx context.Context, messages []llms.MessageContent, cOpts ...CompleteOption) []llms.MessageContent {
	opts := &completeOptions{}
	for _, o := range cOpts {
		o(opts)
	}
	return k.run(ctx, messages, opts)
}

func (k *Kit) GetToolForwardPrompt() string {

	prompt := `
	You are part of an automated workflow. This is a list of tools you have access to:
	`
	prompts := make([]string, 0, len(k.tools))
	for _, tool := range k.tools {
		prompts = append(prompts, tool.GetPrompt())
	}
	prompt += strings.Join(prompts, "\n\n")

	prompt += `
	You MUST use the tools provided to you to complete your task.
	You MUST NOT attempt to directly respond to the user.
	`
	return prompt
}

func (k *Kit) run(ctx context.Context, messages []llms.MessageContent, cOpts *completeOptions) []llms.MessageContent {
	toolDefs := k.getToolDefinitions()
	baseOpts := []llms.CallOption{
		llms.WithTools(toolDefs),
		llms.WithMaxTokens(8192),
	}

	firstCall := true

	for {
		callOpts := make([]llms.CallOption, len(baseOpts))
		copy(callOpts, baseOpts)

		if firstCall && cOpts.forceToolUse {
			callOpts = append(callOpts, llms.WithToolChoice("any"))
			firstCall = false
		}

		if cOpts.streamingFunc != nil {
			callOpts = append(callOpts, llms.WithStreamingFunc(cOpts.streamingFunc))
		}

		k.log.Debug("calling llm", "message_count", len(messages))
		resp, err := k.llm.GenerateContent(ctx, messages, callOpts...)
		if err != nil {
			k.log.Error("failed to generate content", "error", err)
			// Returning messages here allows the caller to inspect the history.
			return messages
		}

		aiResponse := k.toMessageContent(resp.Choices[0])
		messages = append(messages, aiResponse)
		k.log.Info("got llm response", "content", resp.Choices[0].Content, "tool_calls", len(resp.Choices[0].ToolCalls))

		if len(resp.Choices[0].ToolCalls) == 0 {
			return messages
		}

		toolResponseMessages := k.executeToolCalls(ctx, resp.Choices[0].ToolCalls)
		messages = append(messages, toolResponseMessages...)
		k.log.Info("executed tool calls", "response_count", len(toolResponseMessages))
	}
}

func (k *Kit) getToolDefinitions() []llms.Tool {
	defs := make([]llms.Tool, 0, len(k.tools))
	for _, tool := range k.tools {
		defs = append(defs, tool.Definition())
	}
	return defs
}

func (k *Kit) toMessageContent(choice *llms.ContentChoice) llms.MessageContent {
	msg := llms.MessageContent{Role: llms.ChatMessageTypeAI}
	if len(choice.ToolCalls) > 0 {
		parts := make([]llms.ContentPart, 0, len(choice.ToolCalls))
		for _, tc := range choice.ToolCalls {
			k.log.Debug("llm requested tool call", "tool", tc.FunctionCall.Name, "args", tc.FunctionCall.Arguments)
			parts = append(parts, llms.ToolCall{
				ID:   tc.ID,
				Type: tc.Type,
				FunctionCall: &llms.FunctionCall{
					Name:      tc.FunctionCall.Name,
					Arguments: tc.FunctionCall.Arguments,
				},
			})
		}
		msg.Parts = parts
	} else {
		msg.Parts = []llms.ContentPart{llms.TextContent{Text: choice.Content}}
	}
	return msg
}

func (k *Kit) executeToolCalls(ctx context.Context, toolCalls []llms.ToolCall) []llms.MessageContent {
	var toolResponseMessages []llms.MessageContent
	for _, tc := range toolCalls {
		tool, ok := k.tools[tc.FunctionCall.Name]
		if !ok {
			k.log.Error("llm requested unknown tool", "tool", tc.FunctionCall.Name)
			continue
		}

		k.log.Info("executing tool", "tool", tc.FunctionCall.Name, "args", tc.FunctionCall.Arguments)
		result, err := tool.Execute(ctx, tc.FunctionCall.Arguments)
		if err != nil {
			k.log.Error("tool execution failed", "tool", tc.FunctionCall.Name, "error", err)
			continue // Or append an error message for the LLM
		}
		k.log.Info("tool executed successfully", "tool", tc.FunctionCall.Name)

		toolResponseMessages = append(toolResponseMessages, llms.MessageContent{
			Role: llms.ChatMessageTypeTool,
			Parts: []llms.ContentPart{
				llms.ToolCallResponse{
					ToolCallID: tc.ID,
					Name:       tc.FunctionCall.Name,
					Content:    result,
				},
			},
		})
	}
	return toolResponseMessages
}

// StopTool is a tool that signals the completion of a task by canceling the Kit's context.
type StopTool struct {
	kit *Kit
}

// Definition returns the tool's definition.
func (t *StopTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "taskComplete",
			Description: "Call this tool to signal that you have completed the task or risk penalization.",
			Parameters:  map[string]any{"type": "object", "properties": map[string]any{}},
		},
	}
}

// Execute runs the tool.
func (t *StopTool) Execute(_ context.Context, arguments string) (string, error) {
	t.kit.log.Info("taskComplete tool called, canceling context")
	t.kit.cancel()
	return "Task marked as complete. Work context cancelled.", nil
}

func (t *StopTool) GetPrompt() string {
	return `
	IMPORTANT: The existence of this tool means you [the ai] are part of an AUTOMATED WORKFLOW.
	You MUST perform your directed task to the best of your ability until you are reasonably sure you have completed the task.
	Then you MUST call this tool to signal that you have completed the task or risk penalization.
	`
}
