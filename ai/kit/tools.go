package kit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tmc/langchaingo/llms"
)

// WeatherDemoTool is a simple tool that returns the weather for a location.
type WeatherDemoTool struct{}

// NewWeatherDemoTool creates a new WeatherDemoTool.
func NewWeatherDemoTool() *WeatherDemoTool {
	return &WeatherDemoTool{}
}

// Definition returns the tool's definition.
func (t *WeatherDemoTool) Definition() llms.Tool {
	return llms.Tool{
		Type: "function",
		Function: &llms.FunctionDefinition{
			Name:        "getCurrentWeather",
			Description: "Get the current weather in a given location",
			Parameters: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"location": map[string]any{
						"type":        "string",
						"description": "The city and state, e.g. San Francisco, CA",
					},
					"unit": map[string]any{
						"type": "string",
						"enum": []string{"fahrenheit", "celsius"},
					},
				},
				"required": []string{"location"},
			},
		},
	}
}

type weatherArgs struct {
	Location string `json:"location"`
	Unit     string `json:"unit"`
}

// Execute runs the tool.
func (t *WeatherDemoTool) Execute(_ context.Context, arguments string) (string, error) {
	var args weatherArgs
	if err := json.Unmarshal([]byte(arguments), &args); err != nil {
		return "", fmt.Errorf("failed to unmarshal arguments: %w", err)
	}

	weatherResponses := map[string]string{
		"boston":  "72 and sunny",
		"chicago": "65 and windy",
	}

	loweredLocation := strings.ToLower(args.Location)

	var weatherInfo string
	found := false
	for key, value := range weatherResponses {
		if strings.Contains(loweredLocation, key) {
			weatherInfo = value
			found = true
			break
		}
	}

	if !found {
		return "", fmt.Errorf("no weather info for %q", args.Location)
	}

	b, err := json.Marshal(weatherInfo)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (t *WeatherDemoTool) GetPrompt() string {
	return `
	Tool: getCurrentWeather
	Description: Get the current weather in a given location.
	Use this tool when you need to find out the weather.
	`
}
