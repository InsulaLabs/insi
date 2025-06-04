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
BINARY_OBJECTS := insio
CONFIG := cluster.yaml
CONFIG_MAC := cluster_mac.yaml

# Go private repository settings
GOPRIVATE_SETTING := GOPRIVATE=github.com/InsulaLabs

# Production build settings
LDFLAGS_PROD := -ldflags="-s -w"

.PHONY: all clean server client test prod server-prod client-prod objects

all: server client objects
	@echo "$(GREEN)✅ All builds complete! Binaries available at ${BUILD_DIR}/$(RESET)"

clean:
	@echo "$(YELLOW)🧹 Cleaning up...$(RESET)"
	@rm -rf ${BUILD_DIR}
	@echo "$(GREEN)✨ Cleanup complete!$(RESET)"

server: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building server $(BINARY_SERVER)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_SERVER)...$(RESET)"
	@$(GOPRIVATE_SETTING) GOGC=20 go build -o ${BUILD_DIR}/${BINARY_SERVER} cmd/insid/*.go
	@echo "$(PURPLE)   Copying configuration for $(BINARY_SERVER)...$(RESET)"
	@cp ${CONFIG} ${BUILD_DIR}/
	@cp ${CONFIG_MAC} ${BUILD_DIR}/
	@echo "$(GREEN)✅ Server $(BINARY_SERVER) build complete! Available at ${BUILD_DIR}/${BINARY_SERVER}$(RESET)"

client: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building client $(BINARY_CLIENT)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_CLIENT)...$(RESET)"
	@$(GOPRIVATE_SETTING) go build -o ${BUILD_DIR}/${BINARY_CLIENT} cmd/insic/*.go
	@echo "$(GREEN)✅ Client $(BINARY_CLIENT) build complete! Available at ${BUILD_DIR}/${BINARY_CLIENT}$(RESET)"

objects: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building insio...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for insio...$(RESET)"
	@$(GOPRIVATE_SETTING) go build -o ${BUILD_DIR}/${BINARY_OBJECTS} cmd/insio/*.go
	@echo "$(GREEN)✅ insio build complete! Available at ${BUILD_DIR}/${BINARY_OBJECTS}$(RESET)"

${BUILD_DIR}:
	@mkdir -p ${BUILD_DIR}

test:
	@echo "$(BLUE)🧪 Running tests...$(RESET)"
	@$(GOPRIVATE_SETTING) go test -v ./... || (echo "$(YELLOW)⚠️  Tests failed$(RESET)" && exit 1)
	@echo "$(GREEN)✅ All tests passed!$(RESET)"

prod: server-prod client-prod objects-prod
	@echo "$(GREEN)✅ All PRODUCTION builds complete! Binaries available at ${BUILD_DIR}/$(RESET)"

server-prod: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building PRODUCTION server $(BINARY_SERVER)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_SERVER) (production)...$(RESET)"
	@$(GOPRIVATE_SETTING) GOGC=20 go build $(LDFLAGS_PROD) -o ${BUILD_DIR}/${BINARY_SERVER} cmd/insid/*.go
	@echo "$(PURPLE)   Copying configuration for $(BINARY_SERVER)...$(RESET)"
	@cp ${CONFIG} ${BUILD_DIR}/
	@echo "$(GREEN)✅ PRODUCTION Server $(BINARY_SERVER) build complete! Available at ${BUILD_DIR}/${BINARY_SERVER}$(RESET)"

client-prod: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building PRODUCTION client $(BINARY_CLIENT)...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for $(BINARY_CLIENT) (production)...$(RESET)"
	@$(GOPRIVATE_SETTING) go build $(LDFLAGS_PROD) -o ${BUILD_DIR}/${BINARY_CLIENT} cmd/insic/*.go
	@echo "$(GREEN)✅ PRODUCTION Client $(BINARY_CLIENT) build complete! Available at ${BUILD_DIR}/${BINARY_CLIENT}$(RESET)"

objects-prod: ${BUILD_DIR}
	@echo "$(BLUE)🚀 Building PRODUCTION insio...$(RESET)"
	@echo "$(PURPLE)   Compiling Go code for insio (production)...$(RESET)"
	@$(GOPRIVATE_SETTING) go build $(LDFLAGS_PROD) -o ${BUILD_DIR}/${BINARY_OBJECTS} cmd/insio/*.go
	@echo "$(GREEN)✅ PRODUCTION insio build complete! Available at ${BUILD_DIR}/${BINARY_OBJECTS}$(RESET)"