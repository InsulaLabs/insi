package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/ai/docukit"
	"github.com/InsulaLabs/insi/ai/kit"

	"github.com/tmc/langchaingo/llms"
)

func main() {

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var targetFile string
	var inquiry string
	flag.StringVar(&targetFile, "target", "", "The file to edit")
	flag.StringVar(&inquiry, "inquiry", "", "The inquiry to make")
	flag.Parse()

	if inquiry == "" {
		log.Error("inquiry is required")
		os.Exit(1)
	}

	if targetFile == "" {
		log.Error("target file is required")
		os.Exit(1)
	}

	data, err := os.ReadFile(targetFile)
	if err != nil {
		log.Error("failed to read target file", "error", err)
		os.Exit(1)
	}

	dk := docukit.NewDocuKit().
		WithDocument(targetFile, string(data))

	llm, err := kit.GetGrokLLM(os.Getenv("XAI_API_KEY"))
	if err != nil {
		log.Error("failed to get grok llm", "error", err)
		os.Exit(1)
	}

	k := kit.NewKit(context.Background(), llm, log)

	k.WithStopTool()

	for _, tool := range dk.GetAllTools() {
		k.WithTool(tool)
	}

	const maxIterations = 10
	forwardPrompt := k.GetToolForwardPrompt()
	log.Info("forward prompt", "prompt", forwardPrompt)

	initialPrompt := fmt.Sprintf(
		`
		You are an AI assistant that can modify documents. 
		Please follow the instructions in the 'task_instructions' document to modify the document named '%s'. 
		You have access to the following tools and are expected to automate the user task using them.
		Please use the stop tool once completed with your task. You have %d total refining steps.
		to perform the task: %s
		%s
		`,
		targetFile,
		maxIterations,
		inquiry,
		forwardPrompt,
	)
	messageHistory := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, initialPrompt),
		llms.TextParts(llms.ChatMessageTypeHuman, "[BEGIN TASK]"),
	}

	log.Info("starting work loop", "max_iterations", maxIterations)

	for i := 0; i < maxIterations; i++ {
		if k.Ctx().Err() != nil {
			log.Info("work context cancelled, exiting loop.")
			break
		}
		log.Info("running completion", "iteration", i+1)
		messageHistory = append(messageHistory, llms.TextParts(llms.ChatMessageTypeHuman, fmt.Sprintf(
			"You are on stage %d of %d. If the task is complete, please use the stop tool.", i+1, maxIterations)))
		messageHistory = k.Complete(k.Ctx(), messageHistory, kit.WithForcedToolUse())
	}
	log.Info("work loop finished.")

	for name, content := range dk.GetAllDocuments() {
		fmt.Println("--------------------------------")
		fmt.Println(name)
		fmt.Println(content)
	}

}
