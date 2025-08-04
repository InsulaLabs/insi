package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/InsulaLabs/insi/ferry"
	"github.com/fatih/color"
	"gopkg.in/yaml.v3"
)

// User represents a user record with authentication and profile data
type User struct {
	Email       string            `json:"email"`
	Password    string            `json:"password"`
	DisplayName string            `json:"display_name"`
	Role        string            `json:"role"`
	Active      bool              `json:"active"`
	LastLogin   time.Time         `json:"last_login"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Metadata    map[string]string `json:"metadata"`
}

// FerryConfig represents the YAML configuration structure
type FerryConfig struct {
	ApiKeyEnv  string   `yaml:"api_key_env"`
	Endpoints  []string `yaml:"endpoints"`
	SkipVerify bool     `yaml:"skip_verify"`
	Timeout    string   `yaml:"timeout"`
	Domain     string   `yaml:"domain,omitempty"`
}

var (
	// Color scheme
	titleColor   = color.New(color.FgHiCyan, color.Bold)
	successColor = color.New(color.FgHiGreen)
	errorColor   = color.New(color.FgHiRed)
	infoColor    = color.New(color.FgHiYellow)
	dataColor    = color.New(color.FgHiWhite)
	fieldColor   = color.New(color.FgHiMagenta)
	promptColor  = color.New(color.FgCyan)
)

func main() {
	var (
		configPath   = flag.String("config", "", "Path to the ferry configuration file (defaults to ferry.yaml, then FERRY_CONFIG env)")
		generateFlag = flag.String("generate", "", "Generate config from comma-separated endpoints")
		prodMode     = flag.Bool("prod", false, "Use production mode")
		useCache     = flag.Bool("cache", false, "Use cache backend instead of value backend")
	)
	flag.Usage = printUsage
	flag.Parse()

	// Handle config generation
	if *generateFlag != "" {
		generateConfig(*generateFlag)
		return
	}

	// Load config
	cfg, err := loadConfig(*configPath)
	if err != nil {
		errorColor.Printf("Failed to load configuration: %v\n", err)
		fmt.Fprintf(os.Stderr, "\nTip: Generate a config file with:\n")
		fmt.Fprintf(os.Stderr, "  %s --generate \"localhost:1001\" > ferry.yaml\n", os.Args[0])
		os.Exit(1)
	}

	// Get API key from environment
	apiKey := os.Getenv(cfg.ApiKeyEnv)
	if apiKey == "" {
		errorColor.Printf("API key environment variable %s is not set\n", cfg.ApiKeyEnv)
		os.Exit(1)
	}

	// Parse timeout
	timeout := 30 * time.Second
	if cfg.Timeout != "" {
		parsedTimeout, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			errorColor.Printf("Invalid timeout format: %v\n", err)
			os.Exit(1)
		}
		timeout = parsedTimeout
	}

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))

	// Create ferry client
	ferryConfig := &ferry.Config{
		ApiKey:     apiKey,
		Endpoints:  cfg.Endpoints,
		SkipVerify: cfg.SkipVerify,
		Timeout:    timeout,
		Domain:     cfg.Domain,
	}

	f, err := ferry.New(logger, ferryConfig)
	if err != nil {
		errorColor.Printf("Failed to create ferry client: %v\n", err)
		os.Exit(1)
	}

	// Test connectivity
	infoColor.Print("Testing connectivity... ")
	if err := f.Ping(3, 1*time.Second); err != nil {
		errorColor.Printf("Failed: %v\n", err)
		os.Exit(1)
	}
	successColor.Println("OK")

	// Create record manager with options
	opts := []ferry.RecordManagerOption{}
	if *prodMode {
		opts = append(opts, ferry.WithProduction())
		infoColor.Println("🚀 Running in PRODUCTION mode")
	} else {
		infoColor.Println("🧪 Running in DEVELOPMENT mode")
	}

	if *useCache {
		opts = append(opts, ferry.WithCache())
		infoColor.Println("💾 Using CACHE backend")
	} else {
		infoColor.Println("💾 Using VALUE backend")
	}

	rm := ferry.GetRecordManager(f, opts...)

	// Register User type
	if err := rm.Register("users", User{}); err != nil {
		errorColor.Printf("Failed to register User type: %v\n", err)
		os.Exit(1)
	}

	// Main menu loop
	ctx := context.Background()
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println()
		titleColor.Println("==== User Record Manager ====")
		dataColor.Println("1. Create User")
		dataColor.Println("2. List Users")
		dataColor.Println("3. View User")
		dataColor.Println("4. Update User")
		dataColor.Println("5. Delete User")
		dataColor.Println("6. List All Record Types")
		dataColor.Println("7. Show All Records")
		dataColor.Println("0. Exit")
		promptColor.Print("Select option: ")

		if !scanner.Scan() {
			break
		}

		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			createUser(ctx, rm, scanner)
		case "2":
			listUsers(ctx, rm)
		case "3":
			viewUser(ctx, rm, scanner)
		case "4":
			updateUser(ctx, rm, scanner)
		case "5":
			deleteUser(ctx, rm, scanner)
		case "6":
			listRecordTypes(ctx, rm)
		case "7":
			showAllRecords(ctx, rm)
		case "0":
			successColor.Println("\n👋 Goodbye!")
			return
		default:
			errorColor.Println("Invalid option!")
		}
	}
}

func generateConfig(endpoints string) {
	// Split endpoints by comma
	endpointList := strings.Split(endpoints, ",")
	for i, ep := range endpointList {
		endpointList[i] = strings.TrimSpace(ep)
	}

	// Create config
	cfg := FerryConfig{
		ApiKeyEnv:  "INSI_API_KEY",
		Endpoints:  endpointList,
		SkipVerify: false,
		Timeout:    "30s",
	}

	// Marshal to YAML
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		errorColor.Printf("Failed to marshal config: %v\n", err)
		os.Exit(1)
	}

	// Print to stdout
	fmt.Print(string(data))
}

func loadConfig(configPath string) (*FerryConfig, error) {
	// Determine config path
	if configPath == "" {
		// Check ferry.yaml
		if _, err := os.Stat("ferry.yaml"); err == nil {
			configPath = "ferry.yaml"
		} else {
			// Check FERRY_CONFIG env
			if envPath := os.Getenv("FERRY_CONFIG"); envPath != "" {
				configPath = envPath
			} else {
				return nil, fmt.Errorf("no config file found: checked ferry.yaml and FERRY_CONFIG env var")
			}
		}
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configPath, err)
	}

	// Parse YAML
	var cfg FerryConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	// Set defaults
	if cfg.ApiKeyEnv == "" {
		cfg.ApiKeyEnv = "INSI_API_KEY"
	}

	return &cfg, nil
}

func createUser(ctx context.Context, rm ferry.RecordManager, scanner *bufio.Scanner) {
	titleColor.Println("\n📝 Create New User")

	user := User{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Active:    true,
		Metadata:  make(map[string]string),
	}

	promptColor.Print("Email: ")
	scanner.Scan()
	user.Email = strings.TrimSpace(scanner.Text())

	promptColor.Print("Password: ")
	scanner.Scan()
	user.Password = strings.TrimSpace(scanner.Text())

	promptColor.Print("Display Name: ")
	scanner.Scan()
	user.DisplayName = strings.TrimSpace(scanner.Text())

	promptColor.Print("Role (admin/user/guest): ")
	scanner.Scan()
	user.Role = strings.TrimSpace(scanner.Text())
	if user.Role == "" {
		user.Role = "user"
	}

	instanceName := strings.ReplaceAll(user.Email, "@", "_at_")
	instanceName = strings.ReplaceAll(instanceName, ".", "_")

	if err := rm.NewInstance(ctx, "users", instanceName, user); err != nil {
		errorColor.Printf("Failed to create user: %v\n", err)
		return
	}

	successColor.Printf("✅ User created successfully! (ID: %s)\n", instanceName)
}

func listUsers(ctx context.Context, rm ferry.RecordManager) {
	titleColor.Println("\n👥 User List")

	instances, err := rm.ListInstances(ctx, "users", 0, 100)
	if err != nil {
		errorColor.Printf("Failed to list users: %v\n", err)
		return
	}

	if len(instances) == 0 {
		infoColor.Println("No users found.")
		return
	}

	for i, instance := range instances {
		// Get user details
		record, err := rm.GetRecord(ctx, "users", instance)
		if err != nil {
			errorColor.Printf("Failed to get user %s: %v\n", instance, err)
			continue
		}

		user, ok := record.(User)
		if !ok {
			errorColor.Printf("Invalid user data for %s\n", instance)
			continue
		}

		status := successColor.Sprint("Active")
		if !user.Active {
			status = errorColor.Sprint("Inactive")
		}

		fmt.Printf("%d. ", i+1)
		fieldColor.Printf("%-20s", instance)
		dataColor.Printf(" | %s | %s | %s\n", user.Email, user.Role, status)
	}
}

func viewUser(ctx context.Context, rm ferry.RecordManager, scanner *bufio.Scanner) {
	titleColor.Println("\n🔍 View User Details")

	promptColor.Print("Enter user ID (email-based): ")
	scanner.Scan()
	instanceName := strings.TrimSpace(scanner.Text())

	record, err := rm.GetRecord(ctx, "users", instanceName)
	if err != nil {
		errorColor.Printf("Failed to get user: %v\n", err)
		return
	}

	user, ok := record.(User)
	if !ok {
		errorColor.Println("Invalid user data")
		return
	}

	titleColor.Printf("\nUser: %s\n", instanceName)
	fmt.Println(strings.Repeat("-", 40))

	printField("Email", user.Email)
	printField("Display Name", user.DisplayName)
	printField("Role", user.Role)
	printField("Active", fmt.Sprintf("%v", user.Active))
	printField("Created", user.CreatedAt.Format(time.RFC3339))
	printField("Updated", user.UpdatedAt.Format(time.RFC3339))

	if !user.LastLogin.IsZero() {
		printField("Last Login", user.LastLogin.Format(time.RFC3339))
	}

	if len(user.Metadata) > 0 {
		fieldColor.Print("Metadata: ")
		metaJSON, _ := json.MarshalIndent(user.Metadata, "  ", "  ")
		dataColor.Printf("\n%s\n", metaJSON)
	}
}

func updateUser(ctx context.Context, rm ferry.RecordManager, scanner *bufio.Scanner) {
	titleColor.Println("\n✏️  Update User")

	promptColor.Print("Enter user ID to update: ")
	scanner.Scan()
	instanceName := strings.TrimSpace(scanner.Text())

	// Get existing user
	record, err := rm.GetRecord(ctx, "users", instanceName)
	if err != nil {
		errorColor.Printf("Failed to get user: %v\n", err)
		return
	}

	user, ok := record.(User)
	if !ok {
		errorColor.Println("Invalid user data")
		return
	}

	infoColor.Println("\nWhat would you like to update?")
	dataColor.Println("1. Display Name")
	dataColor.Println("2. Role")
	dataColor.Println("3. Active Status")
	dataColor.Println("4. Password")
	dataColor.Println("5. Add Metadata")
	promptColor.Print("Select field: ")

	scanner.Scan()
	choice := strings.TrimSpace(scanner.Text())

	switch choice {
	case "1":
		promptColor.Printf("New Display Name (current: %s): ", user.DisplayName)
		scanner.Scan()
		newValue := strings.TrimSpace(scanner.Text())
		if newValue != "" {
			err = rm.SetRecordField(ctx, "users", instanceName, "DisplayName", newValue)
		}

	case "2":
		promptColor.Printf("New Role (current: %s): ", user.Role)
		scanner.Scan()
		newValue := strings.TrimSpace(scanner.Text())
		if newValue != "" {
			err = rm.SetRecordField(ctx, "users", instanceName, "Role", newValue)
		}

	case "3":
		currentStatus := "active"
		if !user.Active {
			currentStatus = "inactive"
		}
		promptColor.Printf("Toggle status? Current: %s (y/n): ", currentStatus)
		scanner.Scan()
		if strings.ToLower(strings.TrimSpace(scanner.Text())) == "y" {
			err = rm.SetRecordField(ctx, "users", instanceName, "Active", !user.Active)
		}

	case "4":
		promptColor.Print("New Password: ")
		scanner.Scan()
		newValue := strings.TrimSpace(scanner.Text())
		if newValue != "" {
			err = rm.SetRecordField(ctx, "users", instanceName, "Password", newValue)
		}

	case "5":
		promptColor.Print("Metadata key: ")
		scanner.Scan()
		key := strings.TrimSpace(scanner.Text())

		promptColor.Print("Metadata value: ")
		scanner.Scan()
		value := strings.TrimSpace(scanner.Text())

		if key != "" {
			if user.Metadata == nil {
				user.Metadata = make(map[string]string)
			}
			user.Metadata[key] = value
			err = rm.SetRecordField(ctx, "users", instanceName, "Metadata", user.Metadata)
		}

	default:
		errorColor.Println("Invalid choice")
		return
	}

	if err != nil {
		errorColor.Printf("Failed to update user: %v\n", err)
		return
	}

	// Update the UpdatedAt field
	err = rm.SetRecordField(ctx, "users", instanceName, "UpdatedAt", time.Now())
	if err != nil {
		errorColor.Printf("Failed to update timestamp: %v\n", err)
	}

	successColor.Println("✅ User updated successfully!")
}

func deleteUser(ctx context.Context, rm ferry.RecordManager, scanner *bufio.Scanner) {
	titleColor.Println("\n🗑️  Delete User")

	promptColor.Print("Enter user ID to delete: ")
	scanner.Scan()
	instanceName := strings.TrimSpace(scanner.Text())

	// Confirm deletion
	promptColor.Printf("Are you sure you want to delete user '%s'? (yes/no): ", instanceName)
	scanner.Scan()
	confirm := strings.ToLower(strings.TrimSpace(scanner.Text()))

	if confirm != "yes" {
		infoColor.Println("Deletion cancelled.")
		return
	}

	if err := rm.DeleteRecord(ctx, "users", instanceName); err != nil {
		errorColor.Printf("Failed to delete user: %v\n", err)
		return
	}

	successColor.Printf("✅ User '%s' deleted successfully!\n", instanceName)
}

func listRecordTypes(ctx context.Context, rm ferry.RecordManager) {
	titleColor.Println("\n📋 Registered Record Types")

	types, err := rm.ListRecordTypes(ctx)
	if err != nil {
		errorColor.Printf("Failed to list record types: %v\n", err)
		return
	}

	for i, recordType := range types {
		fmt.Printf("%d. ", i+1)
		fieldColor.Println(recordType)
	}
}

func showAllRecords(ctx context.Context, rm ferry.RecordManager) {
	titleColor.Println("\n🗂️  All Records")

	allRecords, err := rm.ListAllInstances(ctx, 0, 100)
	if err != nil {
		errorColor.Printf("Failed to list all records: %v\n", err)
		return
	}

	if len(allRecords) == 0 {
		infoColor.Println("No records found.")
		return
	}

	for recordType, instances := range allRecords {
		fieldColor.Printf("\n%s (%d instances):\n", recordType, len(instances))
		for _, instance := range instances {
			dataColor.Printf("  - %s\n", instance)
		}
	}
}

func printField(name, value string) {
	fieldColor.Printf("%-15s: ", name)
	dataColor.Println(value)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\n%s\n", color.CyanString("User Record Manager - CRUD operations for user records"))
	fmt.Fprintf(os.Stderr, "\nFlags:\n")
	flag.PrintDefaults()

	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Configuration:"))
	fmt.Fprintf(os.Stderr, "  The program looks for configuration in this order:\n")
	fmt.Fprintf(os.Stderr, "  1. --config flag (if specified)\n")
	fmt.Fprintf(os.Stderr, "  2. ferry.yaml in current directory\n")
	fmt.Fprintf(os.Stderr, "  3. FERRY_CONFIG environment variable\n")
	fmt.Fprintf(os.Stderr, "  API key is read from the environment variable specified in config (default: INSI_API_KEY)\n")

	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Generate Configuration:"))
	fmt.Fprintf(os.Stderr, "  %s --generate \"endpoint1,endpoint2,...\" > ferry.yaml\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  Example: %s --generate \"localhost:1001\" > ferry.yaml\n", os.Args[0])

	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Example Usage:"))
	fmt.Fprintf(os.Stderr, "  # Generate config for local development\n")
	fmt.Fprintf(os.Stderr, "  %s --generate \"localhost:1001\" > ferry.yaml\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  export INSI_API_KEY=your-api-key\n")
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Run in development mode with value backend\n")
	fmt.Fprintf(os.Stderr, "  %s\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Run in production mode with cache backend\n")
	fmt.Fprintf(os.Stderr, "  %s --prod --cache\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  \n")
	fmt.Fprintf(os.Stderr, "  # Use custom config file\n")
	fmt.Fprintf(os.Stderr, "  %s --config prod-ferry.yaml --prod\n", os.Args[0])

	fmt.Fprintf(os.Stderr, "\n%s\n", color.YellowString("Features:"))
	fmt.Fprintf(os.Stderr, "  - Create, Read, Update, Delete user records\n")
	fmt.Fprintf(os.Stderr, "  - Structured data storage with field-level access\n")
	fmt.Fprintf(os.Stderr, "  - Support for both value store (persistent) and cache (volatile) backends\n")
	fmt.Fprintf(os.Stderr, "  - Development/Production mode separation\n")
	fmt.Fprintf(os.Stderr, "  - Colored CLI interface for better user experience\n")
}
