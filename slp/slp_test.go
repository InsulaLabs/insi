package slp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBlock_SuccessCases(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectedLists int
		validate      func(t *testing.T, program *Program)
	}{
		{
			name:          "Simple program",
			input:         `(\' "hello" 123)`,
			expectedLists: 1,
			validate: func(t *testing.T, program *Program) {
				list := program.Lists[0]
				require.NotNil(t, list.Head)
				assert.Equal(t, DataTypeString, list.Head.Data.Type)
				assert.Equal(t, "hello", list.Head.Data.Value)

				require.NotNil(t, list.Head.Next)
				assert.Equal(t, DataTypeNumber, list.Head.Next.Data.Type)
				assert.Equal(t, int64(123), list.Head.Next.Data.Value)
				assert.Nil(t, list.Head.Next.Next)
			},
		},
		{
			name:          "Nested lists",
			input:         `(\' (1 2) ("a" "b"))`,
			expectedLists: 1,
			validate: func(t *testing.T, program *Program) {
				list := program.Lists[0]
				require.NotNil(t, list.Head)
				assert.Equal(t, DataTypeList, list.Head.Data.Type)
				innerList1 := list.Head.Data.Value.(*List)
				assert.Equal(t, int64(1), innerList1.Head.Data.Value)
				assert.Equal(t, int64(2), innerList1.Head.Next.Data.Value)

				require.NotNil(t, list.Head.Next)
				assert.Equal(t, DataTypeList, list.Head.Next.Data.Type)
				innerList2 := list.Head.Next.Data.Value.(*List)
				assert.Equal(t, "a", innerList2.Head.Data.Value)
				assert.Equal(t, "b", innerList2.Head.Next.Data.Value)
			},
		},
		{
			name:          "All data types",
			input:         `(\' an_identifier 1.234 "a string" null (1 2) -50)`,
			expectedLists: 1,
			validate: func(t *testing.T, program *Program) {
				list := program.Lists[0]
				n := list.Head
				assert.Equal(t, DataTypeIdentifier, n.Data.Type)
				assert.Equal(t, "an_identifier", n.Data.Value)

				n = n.Next
				assert.Equal(t, DataTypeNumber, n.Data.Type)
				assert.InEpsilon(t, 1.234, n.Data.Value, 0.001)

				n = n.Next
				assert.Equal(t, DataTypeString, n.Data.Type)
				assert.Equal(t, "a string", n.Data.Value)

				n = n.Next
				assert.Equal(t, DataTypeNull, n.Data.Type)
				assert.Nil(t, n.Data.Value)

				n = n.Next
				assert.Equal(t, DataTypeList, n.Data.Type)

				n = n.Next
				assert.Equal(t, DataTypeNumber, n.Data.Type)
				assert.Equal(t, int64(-50), n.Data.Value)
			},
		},
		{
			name:          "Multiple programs",
			input:         `some text (\' 1) some other text (\' "two")`,
			expectedLists: 2,
			validate: func(t *testing.T, program *Program) {
				list1 := program.Lists[0]
				assert.Equal(t, int64(1), list1.Head.Data.Value)

				list2 := program.Lists[1]
				assert.Equal(t, "two", list2.Head.Data.Value)
			},
		},
		{
			name:          "Empty block",
			input:         "",
			expectedLists: 0,
		},
		{
			name:          "No program in block",
			input:         "this is just some text without a program",
			expectedLists: 0,
		},
		{
			name:          "String with escape characters",
			input:         `(\' "hello\"world\n\t")`,
			expectedLists: 1,
			validate: func(t *testing.T, program *Program) {
				list := program.Lists[0]
				assert.Equal(t, "hello\"world\n\t", list.Head.Data.Value)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			program, err := ParseBlock(tc.input)
			require.NoError(t, err)
			if tc.expectedLists == 0 {
				assert.Nil(t, program)
			} else {
				require.NotNil(t, program)
				assert.Len(t, program.Lists, tc.expectedLists)
				if tc.validate != nil {
					tc.validate(t, program)
				}
			}
		})
	}
}

func TestParseBlock_ErrorCases(t *testing.T) {
	testCases := []struct {
		name        string
		input       string
		expectedPos int
		expectedMsg string
	}{
		{
			name:        "Unterminated list",
			input:       `(\' 1 2`,
			expectedPos: 7,
			expectedMsg: "unexpected end of input, expected ')'",
		},
		{
			name:        "Unterminated string",
			input:       `(\' "abc)`,
			expectedPos: 9,
			expectedMsg: "unterminated string",
		},
		{
			name:        "Invalid character",
			input:       `(\' !)`,
			expectedPos: 4,
			expectedMsg: "unexpected character '!'",
		},
		{
			name:        "Invalid number format - trailing dot",
			input:       `(\' 123.)`,
			expectedPos: 8,
			expectedMsg: "invalid float number format, expected digit after '.'",
		},
		{
			name:        "Invalid number format - dangling minus",
			input:       `(\' -)`,
			expectedPos: 4,
			expectedMsg: "invalid number format, dangling '-'",
		},
		{
			name:        "Unsupported escape sequence",
			input:       `(\' "\q")`,
			expectedPos: 6,
			expectedMsg: "unsupported escape sequence: \\q",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseBlock(tc.input)
			require.Error(t, err)

			parseErr, ok := err.(*ParseError)
			require.True(t, ok, "error should be a *ParseError")

			assert.Equal(t, tc.expectedPos, parseErr.Position)
			assert.Equal(t, tc.expectedMsg, parseErr.Message)
			assert.Equal(t, fmt.Sprintf("parse error at position %d: %s", tc.expectedPos, tc.expectedMsg), parseErr.Error())
		})
	}
}
