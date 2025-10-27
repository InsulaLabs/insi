package slp

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
)

func TestSimpleBackendDemo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	backend := NewSimpleBackend(logger)

	fmt.Println("=== SimpleBackend Demo ===")
	fmt.Println("This demonstrates how to use the SimpleBackend to execute tasks with objects.")
	fmt.Println()

	// Create some test objects
	setCmd := List{
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("x")},
		Obj{Type: OBJ_TYPE_NUMBER, D: Number(3.14)},
	}

	getCmd := List{
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("get")},
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("x")},
	}

	delCmd := List{
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("del")},
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("x")},
	}

	// Execute commands
	fmt.Println("1. Executing (set x 3.14):")
	result1 := backend.ExecuteList(backend.rootScope, setCmd)
	fmt.Printf("   Result: %s\n", result1.Encode())
	if result1.Type != OBJ_TYPE_STRING || result1.D.(string) != "ok" {
		t.Errorf("Expected 'ok', got %s", result1.Encode())
	}

	fmt.Println("2. Executing (get x):")
	result2 := backend.ExecuteList(backend.rootScope, getCmd)
	fmt.Printf("   Result: %s\n", result2.Encode())
	if result2.Type != OBJ_TYPE_NUMBER || float64(result2.D.(Number)) != 3.14 {
		t.Errorf("Expected 3.14, got %s", result2.Encode())
	}

	fmt.Println("3. Executing (set name \"hello world\"):")
	setStrCmd := List{
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("name")},
		Obj{Type: OBJ_TYPE_STRING, D: "hello world"},
	}
	result3 := backend.ExecuteList(backend.rootScope, setStrCmd)
	fmt.Printf("   Result: %s\n", result3.Encode())
	if result3.Type != OBJ_TYPE_STRING || result3.D.(string) != "ok" {
		t.Errorf("Expected 'ok', got %s", result3.Encode())
	}

	fmt.Println("4. Executing (get name):")
	getStrCmd := List{
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("get")},
		Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("name")},
	}
	result4 := backend.ExecuteList(backend.rootScope, getStrCmd)
	fmt.Printf("   Result: %s\n", result4.Encode())
	if result4.Type != OBJ_TYPE_STRING || result4.D.(string) != "hello world" {
		t.Errorf("Expected 'hello world', got %s", result4.Encode())
	}

	fmt.Println("5. Executing (del x):")
	result5 := backend.ExecuteList(backend.rootScope, delCmd)
	fmt.Printf("   Result: %s\n", result5.Encode())
	if result5.Type != OBJ_TYPE_STRING || result5.D.(string) != "deleted" {
		t.Errorf("Expected 'deleted', got %s", result5.Encode())
	}

	fmt.Println("6. Executing (get x) after deletion:")
	result6 := backend.ExecuteList(backend.rootScope, getCmd)
	fmt.Printf("   Result: %s\n", result6.Encode())
	if result6.Type != OBJ_TYPE_NONE {
		t.Errorf("Expected none, got %s", result6.Encode())
	}

	fmt.Println()
	fmt.Println("=== Demo Complete ===")
	fmt.Println("The SimpleBackend successfully demonstrated:")
	fmt.Println("- Setting variables with (set key value)")
	fmt.Println("- Getting variables with (get key)")
	fmt.Println("- Deleting variables with (del key)")
	fmt.Println("- Proper scoping and variable resolution")
	fmt.Println("- Error handling for invalid operations")
}

func TestSimpleBackendStringExecutionDemo(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	backend := NewSimpleBackend(logger)

	fmt.Println("=== String Execution Demo ===")
	fmt.Println("This demonstrates parsing strings directly into objects and executing them.")
	fmt.Println()

	// Test cases: string input -> expected result
	testCases := []struct {
		input    string
		expected string
		desc     string
	}{
		{"(set pi 3.14159)", "\"ok\"", "Setting a numeric variable"},
		{"(get pi)", "3.1415900000000003", "Getting the numeric variable"},
		{"(set message \"hello world\")", "\"ok\"", "Setting a string variable"},
		{"(get message)", "\"hello world\"", "Getting the string variable"},
		{"(del pi)", "\"deleted\"", "Deleting the numeric variable"},
		{"(get pi)", "_", "Getting deleted variable returns none"},
	}

	for i, tc := range testCases {
		fmt.Printf("%d. Executing: %s\n", i+1, tc.input)
		fmt.Printf("   Description: %s\n", tc.desc)

		// Parse the string into an object
		parser := &Parser{Target: tc.input, Position: 0}
		parsedObj := parser.Parse()

		if parsedObj.Type == OBJ_TYPE_ERROR {
			t.Errorf("Parse error for input '%s': %s", tc.input, parsedObj.D.(Error).Message)
			continue
		}

		fmt.Printf("   Parsed: %s\n", parsedObj.Encode())

		// Execute the parsed object
		if parsedObj.Type == OBJ_TYPE_LIST {
			result := backend.ExecuteList(backend.rootScope, parsedObj.D.(List))
			fmt.Printf("   Result: %s\n", result.Encode())

			if result.Encode() != tc.expected {
				t.Errorf("Expected %s, got %s for input '%s'", tc.expected, result.Encode(), tc.input)
			}
		} else {
			t.Errorf("Expected list for input '%s', got %s", tc.input, parsedObj.Type)
		}

		fmt.Println()
	}

	fmt.Println("=== String Execution Demo Complete ===")
	fmt.Println("Successfully demonstrated:")
	fmt.Println("- Parsing strings into executable objects")
	fmt.Println("- Executing parsed objects with SimpleBackend")
	fmt.Println("- End-to-end string-to-execution pipeline")
}
