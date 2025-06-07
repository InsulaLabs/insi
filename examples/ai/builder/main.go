package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/InsulaLabs/insi/ai/crit"
	"github.com/InsulaLabs/insi/ai/forge"
	"github.com/InsulaLabs/insi/ai/kit"

	"github.com/tmc/langchaingo/llms"
)

func newForge(llm llms.Model, log *slog.Logger, task string, documents map[string]string) (*forge.Forge, error) {
	forgeKit := kit.NewKit(context.Background(), llm, log.WithGroup("forge-kit"))
	f, err := forge.NewForge(forge.Config{
		Logger:        log.WithGroup("forge"),
		Task:          task,
		Llm:           llm,
		MaxIterations: 10,
		Documents:     documents,
		Kit:           forgeKit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create forge: %w", err)
	}
	return f, nil
}

func newCrit(llm llms.Model, log *slog.Logger, task string, documents map[string]string) (*crit.Crit, error) {
	critKit := kit.NewKit(context.Background(), llm, log.WithGroup("crit-kit"))
	c, err := crit.NewCrit(crit.Config{
		Logger:        log.WithGroup("crit"),
		Task:          task,
		Llm:           llm,
		MaxIterations: 10,
		Documents:     documents,
		Kit:           critKit,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create crit: %w", err)
	}
	return c, nil
}

func main() {

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var targetFile string
	var inquiry string
	var iterations int
	flag.StringVar(&targetFile, "target", "", "The file to edit")
	flag.StringVar(&inquiry, "inquiry", "", "The inquiry to make")
	flag.IntVar(&iterations, "iterations", 2, "The number of refinement iterations")
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

	llm, err := kit.GetGrokLLM(os.Getenv("XAI_API_KEY"))
	if err != nil {
		log.Error("failed to get grok llm", "error", err)
		os.Exit(1)
	}

	currentDocuments := map[string]string{
		targetFile: string(data),
	}
	var feedback string

	log.Info("starting refinement loop", "iterations", iterations)

	for i := 0; i < iterations; i++ {

		// If we have feedback, it's for "working_document". We need to rename our
		// target file in the documents map to match what the feedback is for.
		if feedback != "" {
			if content, ok := currentDocuments[targetFile]; ok {
				delete(currentDocuments, targetFile)
				currentDocuments["working_document"] = content
			}
		}

		fmt.Println("--------------------------------")
		fmt.Println("iteration", i+1, "total_iterations", iterations)
		fmt.Println("--------------------------------")

		log.Info("running iteration", "iteration", i+1, "total_iterations", iterations)

		// 1. Run forge
		log.Info("running forge")
		forgeRunner, err := newForge(llm, log, inquiry, currentDocuments)
		if err != nil {
			log.Error("failed to create forge", "error", err)
			os.Exit(1)
		}

		fmt.Println("--------------------------------")
		fmt.Println("RUNNING FORGE WITH FEEDBACK: ", feedback)
		fmt.Println("--------------------------------")

		forgedDocs, err := forgeRunner.RunWithFeedback(feedback)
		if err != nil {
			log.Error("failed to run forge", "error", err)
			os.Exit(1)
		}
		log.Info("forge run complete")
		currentDocuments = forgedDocs // The result of this forge run becomes the input for the next, or the final result.

		// If feedback was applied, the document will be named "working_document".
		// We need to rename it back to the original target file name for the next
		// crit run and for saving the final output.
		if content, ok := currentDocuments["working_document"]; ok {
			delete(currentDocuments, "working_document")
			currentDocuments[targetFile] = content
		}

		// Prepare documents for crit.
		critDocs := forgedDocs

		cdPreview := ""
		for name, content := range critDocs {
			cdPreview += fmt.Sprintf("\n---\n%s\n---\n%s", name, content)
		}

		fmt.Println("--------------------------------")
		fmt.Println("RUNNING CRIT WITH DOCS: ", cdPreview)
		fmt.Println("--------------------------------")

		if i < iterations-1 {
			// 2. Run crit
			log.Info("running crit")
			critRunner, err := newCrit(llm, log, inquiry, critDocs)
			if err != nil {
				log.Error("failed to create crit", "error", err)
				os.Exit(1)
			}
			critResults, err := critRunner.Run()
			if err != nil {
				log.Error("failed to run crit", "error", err)
				os.Exit(1)
			}
			log.Info("crit run complete")

			// 3. Get feedback
			if fb, ok := critResults["feedback_document"]; ok {
				if fb != "No feedback needed" {
					feedback = fb
					log.Info("feedback received from crit for next iteration")
				} else {
					log.Info("crit provided no feedback, stopping iterations.")
					break
				}
			} else {
				log.Warn("feedback_document not found in crit results, continuing without feedback for next iteration")
				feedback = "" // reset feedback
			}
		}

	}

	// 5. Save the final result
	finalContent := currentDocuments[targetFile]
	outFileName := targetFile + ".out"
	log.Info("writing final result", "file", outFileName)
	err = os.WriteFile(outFileName, []byte(finalContent), 0644)
	if err != nil {
		log.Error("failed to write output file", "error", err)
		os.Exit(1)
	}

	log.Info("process complete")
}
