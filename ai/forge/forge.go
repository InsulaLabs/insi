package forge

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/InsulaLabs/insi/ai/docukit"
	"github.com/InsulaLabs/insi/ai/kit"

	"github.com/tmc/langchaingo/llms"
)

type Config struct {
	Logger        *slog.Logger
	Task          string
	Llm           llms.Model
	MaxIterations int
	Documents     map[string]string
	Kit           *kit.Kit
}

type Forge struct {
	log           *slog.Logger
	kit           *kit.Kit
	task          string
	llm           llms.Model
	maxIterations int

	ctx    context.Context
	cancel context.CancelFunc

	dk *docukit.DocuKit
	k  *kit.Kit

	systemForwardPrompt string
}

func NewForge(config Config) (*Forge, error) {

	dk := docukit.NewDocuKit()
	config.Kit.WithStopTool()

	for name, content := range config.Documents {
		dk.WithDocument(name, content)
	}

	for _, tool := range dk.GetAllTools() {
		config.Kit.WithTool(tool)
	}

	systemForwardPrompt := config.Kit.GetToolForwardPrompt()

	return &Forge{
		log:                 config.Logger,
		kit:                 config.Kit,
		task:                config.Task,
		llm:                 config.Llm,
		dk:                  dk,
		k:                   config.Kit,
		ctx:                 config.Kit.Ctx(),
		cancel:              func() {}, // Handled by the kit
		maxIterations:       config.MaxIterations,
		systemForwardPrompt: systemForwardPrompt,
	}, nil
}

func (f *Forge) Run() (map[string]string, error) {
	return f.RunWithFeedback("")
}

func (f *Forge) RunWithFeedback(feedback string) (map[string]string, error) {

	docNames := []string{}
	for name := range f.dk.GetAllDocuments() {
		docNames = append(docNames, name)
	}

	initialPrompt := fmt.Sprintf(
		`
		You are an AI assistant that can modify documents. 
		You have access to the following documents: %s
		Your task is to: %s

		You have access to the following tools and are expected to automate the user task using them.
		Please use the stop tool once completed with your task. You have %d total refining steps.
		
		%s
		`,
		strings.Join(docNames, ", "),
		f.task,
		f.maxIterations,
		f.systemForwardPrompt,
	)

	if feedback != "" && feedback != "No feedback needed" {
		initialPrompt += fmt.Sprintf(
			`

		Please refine your work based on the following feedback:
%s
			`,
			feedback,
		)
	}

	messageHistory := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, initialPrompt),
		llms.TextParts(llms.ChatMessageTypeHuman, "[BEGIN TASK]"),
	}

	f.log.Info("starting work loop", "max_iterations", f.maxIterations)

	for i := 0; i < f.maxIterations; i++ {
		if f.kit.Ctx().Err() != nil {
			f.log.Info("work context cancelled, exiting loop.")
			break
		}
		f.log.Info("running completion", "iteration", i+1)
		messageHistory = append(messageHistory, llms.TextParts(llms.ChatMessageTypeHuman, fmt.Sprintf(
			"You are on stage %d of %d. If the task is complete, please use the stop tool.", i+1, f.maxIterations)))
		messageHistory = f.kit.Complete(f.kit.Ctx(), messageHistory, kit.WithForcedToolUse())
	}
	f.log.Info("work loop finished.")
	return f.dk.GetAllDocuments(), nil
}
