# Colors and formatting
GREEN  := $(shell tput setaf 2)
YELLOW := $(shell tput setaf 3)
BLUE   := $(shell tput setaf 4)
PURPLE := $(shell tput setaf 5)
RESET  := $(shell tput sgr0)
BOLD   := $(shell tput bold)

# Build settings
BUILD_DIR := build
BINARY := insid
CONFIG := cluster.yaml

.PHONY: all clean build test

all: build

clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(RESET)"
	@rm -rf ${BUILD_DIR}
	@echo "$(GREEN)✨ Cleanup complete!$(RESET)"

build: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building $(BINARY)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code...$(RESET)"
	@go build -o ${BUILD_DIR}/${BINARY} cmd/insid/*.go
	@echo "$(PURPLE)   Copying configuration...$(RESET)"
	@cp ${CONFIG} ${BUILD_DIR}/
	@echo "$(GREEN)✅ Build complete! Binary available at ${BUILD_DIR}/${BINARY}$(RESET)"

test:
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@go test -v ./... || (echo "$(YELLOW)⚠️  Tests failed$(RESET)" && exit 1)
	@echo "$(GREEN)✅ All tests passed!$(RESET)"
