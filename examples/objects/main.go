package main

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/InsulaLabs/insi/client"
	"golang.org/x/term"
)

// ANSI Color Codes
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorCyan   = "\033[36m"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	slog.SetDefault(logger)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	insiCfg := &client.Config{
		Logger:                 logger.WithGroup("insi-client"),
		ConnectionType:         client.ConnectionTypeRandom,
		SkipVerify:             false,
		EnableLeaderStickiness: true,
		ApiKey:                 os.Getenv("INSI_API_KEY"),
		Endpoints: []client.Endpoint{
			{PublicBinding: "db-0.insula.dev:443", PrivateBinding: "db-0.insula.dev:444", ClientDomain: "db-0.insula.dev", Logger: logger},
			{PublicBinding: "db-1.insula.dev:443", PrivateBinding: "db-1.insula.dev:444", ClientDomain: "db-1.insula.dev", Logger: logger},
			{PublicBinding: "db-2.insula.dev:443", PrivateBinding: "db-2.insula.dev:444", ClientDomain: "db-2.insula.dev", Logger: logger},
		},
	}

	insiClient, err := client.NewClient(insiCfg)
	if err != nil {
		fmt.Printf("%sFailed to create insi client: %v%s\n", ColorRed, err, ColorReset)
		os.Exit(1)
	}

	userController, err := NewUserController(logger, insiClient)
	if err != nil {
		fmt.Printf("%sFailed to create user controller: %v%s\n", ColorRed, err, ColorReset)
		os.Exit(1)
	}

	command := os.Args[1]
	ctx := context.Background()

	switch command {
	case "user":
		if len(os.Args) < 3 {
			printUserUsage()
			os.Exit(1)
		}
		handleUserCommand(ctx, userController, os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func handleUserCommand(ctx context.Context, uc UserController, args []string) {
	if len(args) < 1 {
		printUserUsage()
		return
	}

	subcommand := args[0]
	switch subcommand {
	case "add":
		if len(args) != 2 {
			fmt.Printf("%sUsage: ./rtt user add <email>%s\n", ColorYellow, ColorReset)
			return
		}
		addUser(ctx, uc, args[1])
	case "list":
		listUsers(ctx, uc)
	case "auth":
		if len(args) != 2 {
			fmt.Printf("%sUsage: ./rtt user auth <email>%s\n", ColorYellow, ColorReset)
			return
		}
		authUser(ctx, uc, args[1])
	case "mod":
		if len(args) < 3 {
			printUserModUsage()
			return
		}
		handleUserMod(ctx, uc, args)
	default:
		printUserUsage()
	}
}

func addUser(ctx context.Context, uc UserController, email string) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter Display Name: ")
	displayName, _ := reader.ReadString('\n')
	displayName = strings.TrimSpace(displayName)

	fmt.Print("Enter Password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("\n%sFailed to read password: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	password := string(bytePassword)
	fmt.Println()

	user, err := uc.NewUser(ctx, email, password, displayName)
	if err != nil {
		fmt.Printf("%sError creating user: %v%s\n", ColorRed, err, ColorReset)
		return
	}

	fmt.Printf("%sUser created successfully!%s\n", ColorGreen, ColorReset)
	fmt.Printf("  %sUUID:%s %s\n", ColorCyan, ColorReset, user.UUID)
	fmt.Printf("  %sEmail:%s %s\n", ColorCyan, ColorReset, user.Email)
}

func listUsers(ctx context.Context, uc UserController) {
	users, err := uc.ListUsers(ctx)
	if err != nil {
		fmt.Printf("%sError listing users: %v%s\n", ColorRed, err, ColorReset)
		return
	}

	if len(users) == 0 {
		fmt.Printf("%sNo users found.%s\n", ColorYellow, ColorReset)
		return
	}

	fmt.Printf("%s--- Users ---%s\n", ColorBlue, ColorReset)
	for _, user := range users {
		printUser(*user)
	}
}

func authUser(ctx context.Context, uc UserController, email string) {
	fmt.Print("Enter Password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("\n%sFailed to read password: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	password := string(bytePassword)
	fmt.Println()

	user, err := uc.GetUserByEmailAndPassword(ctx, email, password)
	if err != nil {
		fmt.Printf("%sAuthentication failed: %v%s\n", ColorRed, err, ColorReset)
		return
	}

	fmt.Printf("%sAuthentication successful!%s\n", ColorGreen, ColorReset)
	printUser(*user)
}

func handleUserMod(ctx context.Context, uc UserController, args []string) {
	modField := args[0]
	email := args[1]

	fmt.Printf("Please enter password for user %s to continue: ", email)
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		fmt.Printf("\n%sFailed to read password: %v%s\n", ColorRed, err, ColorReset)
		return
	}
	password := string(bytePassword)
	fmt.Println()

	user, err := uc.GetUserByEmailAndPassword(ctx, email, password)
	if err != nil {
		fmt.Printf("%sAuthentication failed: %v%s\n", ColorRed, err, ColorReset)
		return
	}

	fmt.Printf("%sAuthentication successful. Proceeding with modification.%s\n", ColorGreen, ColorReset)

	switch modField {
	case "pass":
		fmt.Print("Enter new password: ")
		byteNewPassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			fmt.Printf("\n%sFailed to read new password: %v%s\n", ColorRed, err, ColorReset)
			return
		}
		fmt.Println()
		if err := uc.UpdatePassword(ctx, user.UUID, string(byteNewPassword)); err != nil {
			fmt.Printf("%sFailed to update password: %v%s\n", ColorRed, err, ColorReset)
			return
		}
		fmt.Printf("%sPassword updated successfully.%s\n", ColorGreen, ColorReset)

	case "email":
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter new email: ")
		newEmail, _ := reader.ReadString('\n')
		newEmail = strings.TrimSpace(newEmail)
		if err := uc.UpdateEmail(ctx, user.UUID, newEmail); err != nil {
			fmt.Printf("%sFailed to update email: %v%s\n", ColorRed, err, ColorReset)
			return
		}
		fmt.Printf("%sEmail updated successfully to %s.%s\n", ColorGreen, newEmail, ColorReset)

	case "name":
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("Enter new display name: ")
		newName, _ := reader.ReadString('\n')
		newName = strings.TrimSpace(newName)
		if err := uc.UpdateDisplayName(ctx, user.UUID, newName); err != nil {
			fmt.Printf("%sFailed to update display name: %v%s\n", ColorRed, err, ColorReset)
			return
		}
		fmt.Printf("%sDisplay name updated successfully to %s.%s\n", ColorGreen, newName, ColorReset)

	default:
		fmt.Printf("%sUnknown field '%s'.%s\n", ColorRed, modField, ColorReset)
		printUserModUsage()
	}
}

func printUser(user User) {
	fmt.Printf("  %sUUID:%s        %s\n", ColorCyan, ColorReset, user.UUID)
	fmt.Printf("  %sEmail:%s       %s\n", ColorCyan, ColorReset, user.Email)
	fmt.Printf("  %sDisplay Name:%s %s\n", ColorCyan, ColorReset, user.DisplayName)
	fmt.Printf("  %sCreated At:%s   %s\n", ColorCyan, ColorReset, user.CreatedAt.Format(time.RFC1123))
	fmt.Printf("  %sUpdated At:%s   %s\n", ColorCyan, ColorReset, user.UpdatedAt.Format(time.RFC1123))
	fmt.Println("--------------------")
}

func printUsage() {
	fmt.Printf("%sUsage: ./rtt <command>%s\n", ColorYellow, ColorReset)
	fmt.Println("Commands:")
	fmt.Printf("  %suser%s\t- Manage users\n", ColorGreen, ColorReset)
}

func printUserUsage() {
	fmt.Printf("%sUsage: ./rtt user <subcommand>%s\n", ColorYellow, ColorReset)
	fmt.Println("Subcommands:")
	fmt.Printf("  %sadd <email>%s\t- Add a new user\n", ColorGreen, ColorReset)
	fmt.Printf("  %slist%s\t\t- List all users\n", ColorGreen, ColorReset)
	fmt.Printf("  %sauth <email>%s\t- Authenticate a user\n", ColorGreen, ColorReset)
	fmt.Printf("  %smod <field> <email>%s\t- Modify a user's field (pass, email, name)\n", ColorGreen, ColorReset)
}

func printUserModUsage() {
	fmt.Printf("%sUsage: ./rtt user mod <field> <email>%s\n", ColorYellow, ColorReset)
	fmt.Println("Available fields: pass, email, name")
}
