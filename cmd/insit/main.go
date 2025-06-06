package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"
)

func main() {

	streaming := false
	flag.BoolVar(&streaming, "streaming", false, "Enable streaming")
	flag.Parse()

	// Create a custom HTTP client that skips TLS verification
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: customTransport}

	llm, err := openai.New(
		openai.WithModel("test-island"),
		openai.WithToken(os.Getenv("INSI_API_KEY")),
		openai.WithBaseURL("https://localhost:8443"),
		openai.WithHTTPClient(httpClient), // Add the custom HTTP client
	)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	content := []llms.MessageContent{
		llms.TextParts(llms.ChatMessageTypeSystem, "You are a helpful assistant."),
		llms.TextParts(llms.ChatMessageTypeHuman, `
		(\'tl kvs)(\'help)
		Please use the tools provided to set the key "blue" to the value "HELL YEAH"`),
	}

	if streaming {
		llm.GenerateContent(ctx, content, llms.WithStreamingFunc(func(ctx context.Context, chunk []byte) error {
			fmt.Print(string(chunk))
			return nil
		}))
	} else {
		r, err := llm.GenerateContent(ctx, content, llms.WithMaxTokens(104))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(r.Choices[0].Content)
	}

	content = append(content, llms.TextParts(llms.ChatMessageTypeHuman, `(\'tl kvs) Now please use the tool to get the key 'blue' you MUST execute the tool?`))

	r, err := llm.GenerateContent(ctx, content, llms.WithMaxTokens(104))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(r.Choices[0].Content)

}
