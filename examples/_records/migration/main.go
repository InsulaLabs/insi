package main

import (
	"context"
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

// UserV1 represents the original user structure
type UserV1 struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Email       string `json:"email"`
	Age         int    `json:"age"`
	IsActive    bool   `json:"is_active"`
	LegacyCode  string `json:"legacy_code"`  // This field will be removed in V2
	PhoneNumber string `json:"phone_number"` // This field will be removed in V2
}

// UserV2 represents the upgraded user structure with additional fields
type UserV2 struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Email       string            `json:"email"`
	Age         int               `json:"age"`
	IsActive    bool              `json:"is_active"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Version     int               `json:"version"`
	Tags        []string          `json:"tags"`
	Preferences map[string]string `json:"preferences"`
}

// FerryConfig represents the YAML configuration
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
	warnColor    = color.New(color.FgYellow, color.Italic)
)

func main() {

	color.HiRed("WARNING - THIS WILL DELETE ALL EXISTING RECORDS IN THE TARGET DATABASE")
	color.HiYellow("DO NOT RUN AGAINST A PRODUCTION DATABASE")
	color.HiYellow("Press Enter to continue")
	fmt.Scanln()

	var (
		configPath = flag.String("config", "", "Path to ferry config (defaults to local-ferry.yaml)")
		skipClean  = flag.Bool("skip-clean", false, "Skip cleaning existing records")
	)
	flag.Parse()

	// Load configuration
	cfg, err := loadConfig(*configPath)
	if err != nil {
		errorColor.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Get API key
	apiKey := os.Getenv(cfg.ApiKeyEnv)
	if apiKey == "" {
		errorColor.Printf("API key not set in %s\n", cfg.ApiKeyEnv)
		os.Exit(1)
	}

	// Parse timeout
	timeout := 30 * time.Second
	if cfg.Timeout != "" {
		timeout, _ = time.ParseDuration(cfg.Timeout)
	}

	// Setup logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
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

	// Create record manager in production mode
	rm := ferry.GetRecordManager(f, ferry.WithProduction())

	ctx := context.Background()

	// Register UserV1 type
	if err := rm.Register("users", UserV1{}); err != nil {
		errorColor.Printf("Failed to register UserV1: %v\n", err)
		os.Exit(1)
	}

	if !*skipClean {
		// Clean up any existing records
		titleColor.Println("\n🧹 Cleaning up existing records...")
		cleanupExistingRecords(ctx, f)
	}

	// Create V1 records
	titleColor.Println("\n📝 Creating V1 user records...")
	createV1Records(ctx, rm)

	// Show V1 records
	titleColor.Println("\n📊 Current V1 Records:")
	showAllRecords(ctx, rm, "V1")

	// Wait a moment
	infoColor.Println("\n⏳ Waiting 2 seconds before migration...")
	time.Sleep(2 * time.Second)

	// Perform migration
	titleColor.Println("\n🔄 Starting migration from V1 to V2...")
	fmt.Println("Note: LegacyCode and PhoneNumber fields will be removed during migration")
	performMigration(ctx, rm)

	// Re-register as V2 to read the upgraded records
	if err := rm.Register("users", UserV2{}); err != nil {
		errorColor.Printf("Failed to register UserV2: %v\n", err)
		os.Exit(1)
	}

	// Show V2 records
	titleColor.Println("\n📊 Migrated V2 Records:")
	showAllRecords(ctx, rm, "V2")

	successColor.Println("\n✅ Migration completed successfully!")
}

func loadConfig(configPath string) (*FerryConfig, error) {
	if configPath == "" {
		if _, err := os.Stat("local-ferry.yaml"); err == nil {
			configPath = "local-ferry.yaml"
		} else if _, err := os.Stat("ferry.yaml"); err == nil {
			configPath = "ferry.yaml"
		} else {
			return nil, fmt.Errorf("no config file found")
		}
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var cfg FerryConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.ApiKeyEnv == "" {
		cfg.ApiKeyEnv = "INSI_API_KEY"
	}

	return &cfg, nil
}

func cleanupExistingRecords(ctx context.Context, f *ferry.Ferry) {
	// Create a value controller to access raw operations
	valueCtrl := ferry.GetValueController(f, "")

	// Set up the same scope as the record manager
	valueCtrl.PushScope("prod")
	valueCtrl.PushScope("records")

	// Get all keys under users prefix
	keys, err := valueCtrl.IterateByPrefix(ctx, "users:", 0, 10000)
	if err != nil {
		infoColor.Printf("No existing records to clean: %v\n", err)
		return
	}

	if len(keys) == 0 {
		infoColor.Println("No existing records found")
		return
	}

	// Delete each key directly
	deleted := 0
	for _, key := range keys {
		if err := valueCtrl.Delete(ctx, key); err != nil {
			errorColor.Printf("Failed to delete key %s: %v\n", key, err)
		} else {
			deleted++
		}
	}

	successColor.Printf("Deleted %d keys from %d total keys\n", deleted, len(keys))
}

func createV1Records(ctx context.Context, rm ferry.RecordManager) {
	v1Users := []UserV1{
		{
			ID:          "user001",
			Name:        "Alice Johnson",
			Email:       "alice@example.com",
			Age:         28,
			IsActive:    true,
			LegacyCode:  "ALICE-2019",
			PhoneNumber: "555-1001",
		},
		{
			ID:          "user002",
			Name:        "Bob Smith",
			Email:       "bob@example.com",
			Age:         35,
			IsActive:    true,
			LegacyCode:  "BOB-2018",
			PhoneNumber: "555-1002",
		},
		{
			ID:          "user003",
			Name:        "Charlie Brown",
			Email:       "charlie@example.com",
			Age:         42,
			IsActive:    false,
			LegacyCode:  "CHARLIE-2017",
			PhoneNumber: "555-1003",
		},
		{
			ID:          "user004",
			Name:        "Diana Prince",
			Email:       "diana@example.com",
			Age:         31,
			IsActive:    true,
			LegacyCode:  "DIANA-2020",
			PhoneNumber: "555-1004",
		},
		{
			ID:          "user005",
			Name:        "Edward Norton",
			Email:       "edward@example.com",
			Age:         39,
			IsActive:    true,
			LegacyCode:  "EDWARD-2019",
			PhoneNumber: "555-1005",
		},
	}

	for _, user := range v1Users {
		if err := rm.NewInstance(ctx, "users", user.ID, user); err != nil {
			errorColor.Printf("Failed to create user %s: %v\n", user.ID, err)
		} else {
			successColor.Printf("Created V1 user: %s (%s)\n", user.ID, user.Name)
		}
	}
}

func performMigration(ctx context.Context, rm ferry.RecordManager) {
	// Define the upgrade function
	upgrader := func(v1 UserV1) (UserV2, error) {
		now := time.Now()

		// Create V2 user with additional fields
		v2 := UserV2{
			ID:          v1.ID,
			Name:        v1.Name,
			Email:       v1.Email,
			Age:         v1.Age,
			IsActive:    v1.IsActive,
			CreatedAt:   now.Add(-30 * 24 * time.Hour), // Simulate created 30 days ago
			UpdatedAt:   now,
			Version:     2,
			Tags:        []string{},
			Preferences: make(map[string]string),
		}

		// Add some business logic during migration
		if v1.Age < 30 {
			v2.Tags = append(v2.Tags, "young-professional")
		} else if v1.Age >= 30 && v1.Age < 40 {
			v2.Tags = append(v2.Tags, "experienced")
		} else {
			v2.Tags = append(v2.Tags, "senior")
		}

		if v1.IsActive {
			v2.Tags = append(v2.Tags, "active-user")
			v2.Preferences["notifications"] = "enabled"
		} else {
			v2.Tags = append(v2.Tags, "inactive-user")
			v2.Preferences["notifications"] = "disabled"
		}

		// Add email preferences based on domain
		if len(v1.Email) > 0 {
			v2.Preferences["email_verified"] = "true"
			v2.Preferences["email_domain"] = "example.com"
		}

		infoColor.Printf("Migrating user %s: added %d tags, %d preferences, removed legacy fields (LegacyCode: %s, Phone: %s)\n",
			v1.ID, len(v2.Tags), len(v2.Preferences), v1.LegacyCode, v1.PhoneNumber)

		return v2, nil
	}

	// Perform the upgrade
	if err := rm.UpgradeRecord(ctx, "users", upgrader); err != nil {
		errorColor.Printf("Migration failed: %v\n", err)
		os.Exit(1)
	}

	successColor.Println("Migration completed!")
}

func showAllRecords(ctx context.Context, rm ferry.RecordManager, version string) {
	instances, err := rm.ListInstances(ctx, "users", 0, 100)
	if err != nil {
		errorColor.Printf("Failed to list instances: %v\n", err)
		return
	}

	fmt.Printf("\n%s\n", color.HiWhiteString(strings.Repeat("=", 80)))

	for i, instance := range instances {
		record, err := rm.GetRecord(ctx, "users", instance)
		if err != nil {
			errorColor.Printf("Failed to get record %s: %v\n", instance, err)
			continue
		}

		fmt.Printf("\n%s %d:\n", fieldColor.Sprint("Record"), i+1)

		switch version {
		case "V1":
			if user, ok := record.(UserV1); ok {
				printV1Record(user)
			}
		case "V2":
			if user, ok := record.(UserV2); ok {
				printV2Record(user)
			}
		}
	}

	fmt.Printf("\n%s\n", color.HiWhiteString(strings.Repeat("=", 80)))
	infoColor.Printf("Total %s records: %d\n", version, len(instances))
}

func printV1Record(user UserV1) {
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("ID"), dataColor.Sprint(user.ID))
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Name"), dataColor.Sprint(user.Name))
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Email"), dataColor.Sprint(user.Email))
	fmt.Printf("  %s: %d\n", fieldColor.Sprint("Age"), user.Age)

	status := errorColor.Sprint("Inactive")
	if user.IsActive {
		status = successColor.Sprint("Active")
	}
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Status"), status)

	// Fields that will be removed in V2
	fmt.Printf("  %s: %s %s\n", fieldColor.Sprint("Legacy Code"), dataColor.Sprint(user.LegacyCode), warnColor.Sprint("(will be removed)"))
	fmt.Printf("  %s: %s %s\n", fieldColor.Sprint("Phone"), dataColor.Sprint(user.PhoneNumber), warnColor.Sprint("(will be removed)"))
}

func printV2Record(user UserV2) {
	// Print basic fields
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("ID"), dataColor.Sprint(user.ID))
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Name"), dataColor.Sprint(user.Name))
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Email"), dataColor.Sprint(user.Email))
	fmt.Printf("  %s: %d\n", fieldColor.Sprint("Age"), user.Age)

	status := errorColor.Sprint("Inactive")
	if user.IsActive {
		status = successColor.Sprint("Active")
	}
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Status"), status)

	// Print new V2 fields
	fmt.Printf("  %s: %d\n", fieldColor.Sprint("Version"), user.Version)
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Created"), user.CreatedAt.Format("2006-01-02 15:04:05"))
	fmt.Printf("  %s: %s\n", fieldColor.Sprint("Updated"), user.UpdatedAt.Format("2006-01-02 15:04:05"))

	if len(user.Tags) > 0 {
		fmt.Printf("  %s: %v\n", fieldColor.Sprint("Tags"), user.Tags)
	}

	if len(user.Preferences) > 0 {
		fmt.Printf("  %s:\n", fieldColor.Sprint("Preferences"))
		for k, v := range user.Preferences {
			fmt.Printf("    - %s: %s\n", k, v)
		}
	}
}
