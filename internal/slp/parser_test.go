package slp

import (
	"fmt"
	"testing"
)

func TestParser(t *testing.T) {
	testCases := []struct {
		input    string
		expected Obj
	}{
		{
			input: "(set key value)",
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("key")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("value")},
				},
			},
		},
		{
			input: "(get key)",
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("get")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("key")},
				},
			},
		},
		{
			input: "_",
			expected: Obj{
				Type: OBJ_TYPE_NONE,
				D:    None{},
			},
		},
		{
			input: `(set "key with spaces" "value with spaces")`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_STRING, D: "key with spaces"},
					{Type: OBJ_TYPE_STRING, D: "value with spaces"},
				},
			},
		},
		{
			input: `(set "key\nwith\ttabs" "value\r\nwith\nlines")`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_STRING, D: "key\nwith\ttabs"},
					{Type: OBJ_TYPE_STRING, D: "value\r\nwith\nlines"},
				},
			},
		},
		{
			input: `(set "quote \"inside\"" "backslash \\ here")`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_STRING, D: `quote "inside"`},
					{Type: OBJ_TYPE_STRING, D: `backslash \ here`},
				},
			},
		},
		{
			input: `'42`,
			expected: Obj{
				Type: OBJ_TYPE_SOME,
				D:    Some(Obj{Type: OBJ_TYPE_NUMBER, D: Number(42)}),
			},
		},
		{
			input: `'(set key value)`,
			expected: Obj{
				Type: OBJ_TYPE_SOME,
				D: Some(Obj{
					Type: OBJ_TYPE_LIST,
					D: List{
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("key")},
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("value")},
					},
				}),
			},
		},
		{
			input: `'hello`,
			expected: Obj{
				Type: OBJ_TYPE_SOME,
				D:    Some(Obj{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("hello")}),
			},
		},
		{
			input: `(set "unclosed quote`,
			expected: Obj{
				Type: OBJ_TYPE_ERROR,
				D: Error{
					Position: 5,
					Message:  "unclosed quoted string",
				},
			},
		},
		{
			input: `(set key`,
			expected: Obj{
				Type: OBJ_TYPE_ERROR,
				D: Error{
					Position: 0,
					Message:  "unclosed list",
				},
			},
		},
		{
			input: `(set (get key) (nested (deep value)))`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_LIST, D: List{
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("get")},
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("key")},
					}},
					{Type: OBJ_TYPE_LIST, D: List{
						{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("nested")},
						{Type: OBJ_TYPE_LIST, D: List{
							{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("deep")},
							{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("value")},
						}},
					}},
				},
			},
		},
		{
			input: `(unknown_keyword arg1 arg2)`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("unknown_keyword")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("arg1")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("arg2")},
				},
			},
		},
		{
			input: `invalid_keyword`,
			expected: Obj{
				Type: OBJ_TYPE_IDENTIFIER,
				D:    Identifier("invalid_keyword"),
			},
		},
		{
			input: `(set key 42)`,
			expected: Obj{
				Type: OBJ_TYPE_LIST,
				D: List{
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("set")},
					{Type: OBJ_TYPE_IDENTIFIER, D: Identifier("key")},
					{Type: OBJ_TYPE_NUMBER, D: Number(42)},
				},
			},
		},
		{
			input: `3.14`,
			expected: Obj{
				Type: OBJ_TYPE_NUMBER,
				D:    Number(3.14),
			},
		},
		{
			input: `-123`,
			expected: Obj{
				Type: OBJ_TYPE_NUMBER,
				D:    Number(-123)},
		},
		{
			input: `+`,
			expected: Obj{
				Type: OBJ_TYPE_IDENTIFIER,
				D:    Identifier("+")},
		},
		{
			input: `-`,
			expected: Obj{
				Type: OBJ_TYPE_IDENTIFIER,
				D:    Identifier("-")},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			parser := &Parser{
				target:   tc.input,
				position: 0,
			}

			// TODO: Implement Parse method
			result := parser.Parse()

			if result.Type != tc.expected.Type {
				t.Errorf("expected type %s, got %s", tc.expected.Type, result.Type)
			}

			// For error types, check position and message
			if result.Type == OBJ_TYPE_ERROR {
				expectedErr, ok := tc.expected.D.(Error)
				if !ok {
					t.Errorf("expected error but got non-error expected type")
					return
				}
				actualErr, ok := result.D.(Error)
				if !ok {
					t.Errorf("got error type but result.D is not Error")
					return
				}
				if actualErr.Position != expectedErr.Position || actualErr.Message != expectedErr.Message {
					t.Errorf("expected error {Position: %d, Message: %s}, got {Position: %d, Message: %s}",
						expectedErr.Position, expectedErr.Message, actualErr.Position, actualErr.Message)
				}
			}

			// For number types, check value with small epsilon for floating point
			if result.Type == OBJ_TYPE_NUMBER {
				expectedNum, ok := tc.expected.D.(Number)
				if !ok {
					t.Errorf("expected number but got non-number expected type")
					return
				}
				actualNum, ok := result.D.(Number)
				if !ok {
					t.Errorf("got number type but result.D is not Number")
					return
				}
				if !numbersEqual(float64(actualNum), float64(expectedNum)) {
					t.Errorf("expected number %f, got %f", expectedNum, actualNum)
				}
			}

			// For identifier types, check string value
			if result.Type == OBJ_TYPE_IDENTIFIER {
				expectedId, ok := tc.expected.D.(Identifier)
				if !ok {
					t.Errorf("expected identifier but got non-identifier expected type")
					return
				}
				actualId, ok := result.D.(Identifier)
				if !ok {
					t.Errorf("got identifier type but result.D is not Identifier")
					return
				}
				if string(actualId) != string(expectedId) {
					t.Errorf("expected identifier %s, got %s", expectedId, actualId)
				}
			}

			// For some types (quoted expressions), check the contained object
			if result.Type == OBJ_TYPE_SOME {
				expectedSome, ok := tc.expected.D.(Some)
				if !ok {
					t.Errorf("expected some but got non-some expected type")
					return
				}
				actualSome, ok := result.D.(Some)
				if !ok {
					t.Errorf("got some type but result.D is not Some")
					return
				}
				// Compare the contained objects
				if expectedSome.Type != actualSome.Type {
					t.Errorf("expected some containing %s, got some containing %s", expectedSome.Type, actualSome.Type)
				}
				// For now, just check the type matches - could add deeper comparison if needed
			}

			// For now, just print the result to see what we get
			fmt.Printf("Input: %s\nResult: %+v\n\n", tc.input, result)
		})
	}
}

func numbersEqual(a, b float64) bool {
	const epsilon = 1e-10
	return a-b < epsilon && b-a < epsilon
}
