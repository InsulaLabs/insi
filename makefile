# Colors and formatting
GREEN  := $(shell tput setaf 2)
YELLOW := $(shell tput setaf 3)
BLUE   := $(shell tput setaf 4)
PURPLE := $(shell tput setaf 5)
RESET  := $(shell tput sgr0)
BOLD   := $(shell tput bold)

# Build settings
BUILD_DIR := build
BINARY_SERVER := insid
BINARY_CLIENT := insic
CONFIG := cluster.yaml

# Go private repository settings
GOPRIVATE_SETTING := GOPRIVATE=github.com/InsulaLabs

.PHONY: all clean server client test

all: server client
	@echo "$(GREEN)✅ All builds complete! Binaries available at ${BUILD_DIR}/$(RESET)"

clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(RESET)"
	@rm -rf ${BUILD_DIR}
	@echo "$(GREEN)✨ Cleanup complete!$(RESET)"

server: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building server $(BINARY_SERVER)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_SERVER)...$(RESET)"
	@$(GOPRIVATE_SETTING) go build -o ${BUILD_DIR}/${BINARY_SERVER} cmd/insid/*.go
	@echo "$(PURPLE)   Copying configuration for $(BINARY_SERVER)...$(RESET)"
	@cp ${CONFIG} ${BUILD_DIR}/
	@echo "$(GREEN)✅ Server $(BINARY_SERVER) build complete! Available at ${BUILD_DIR}/${BINARY_SERVER}$(RESET)"

client: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building client $(BINARY_CLIENT)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_CLIENT)...$(RESET)"
	@$(GOPRIVATE_SETTING) go build -o ${BUILD_DIR}/${BINARY_CLIENT} cmd/insic/*.go
	@echo "$(GREEN)✅ Client $(BINARY_CLIENT) build complete! Available at ${BUILD_DIR}/${BINARY_CLIENT}$(RESET)"

${BUILD_DIR}:
	@mkdir -p ${BUILD_DIR}

test:
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@$(GOPRIVATE_SETTING) go test -v ./... || (echo "$(YELLOW)⚠️  Tests failed$(RESET)" && exit 1)
	@echo "$(GREEN)✅ All tests passed!$(RESET)"
