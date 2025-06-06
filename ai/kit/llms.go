package kit

import (
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/anthropic"
	"github.com/tmc/langchaingo/llms/openai"
)

func GetOpenAILLM(apiKey string) (llms.Model, error) {
	return openai.New(
		openai.WithModel("gpt-4o-mini"),
		openai.WithBaseURL("https://api.openai.com/v1"),
		openai.WithToken(apiKey),
	)
}

func GetAnthropicLLM(apiKey string) (llms.Model, error) {
	return anthropic.New(
		anthropic.WithModel("claude-opus-4-20250514"),
		anthropic.WithBaseURL("https://api.anthropic.com/v1"),
		anthropic.WithToken(apiKey),
	)
}

func GetGrokLLM(apiKey string) (llms.Model, error) {
	return openai.New(
		openai.WithModel("grok-2-1212"),
		openai.WithBaseURL("https://api.x.ai/v1"),
		openai.WithToken(apiKey),
	)
}
