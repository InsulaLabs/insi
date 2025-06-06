package slp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// helper to parse a single list expression for testing
func parse(t *testing.T, input string) *Program {
	prog, err := ParseBlock(input)
	require.NoError(t, err)
	require.NotNil(t, prog)
	return prog
}

// helper to create a default processor with some built-in commands
func newTestProc(program *Program, env *Env) *Proc {
	if env == nil {
		env = NewEnv(nil)
	}
	proc := NewProcessor(env, program)

	// Register test commands
	proc.WithCommand("add", func(p *Proc, args []Value) (Value, error) {
		var sum float64
		for _, arg := range args {
			evalArg, err := p.Eval(arg)
			if err != nil {
				return Value{}, err
			}
			switch v := evalArg.Value.(type) {
			case int64:
				sum += float64(v)
			case float64:
				sum += v
			default:
				return Value{}, fmt.Errorf("add requires number arguments, got %s", evalArg.Type)
			}
		}
		return Value{Type: DataTypeNumber, Value: sum}, nil
	})

	proc.WithCommand("def", func(p *Proc, args []Value) (Value, error) {
		if len(args) != 2 {
			return Value{}, fmt.Errorf("def requires 2 arguments, got %d", len(args))
		}
		if args[0].Type != DataTypeIdentifier {
			return Value{}, fmt.Errorf("first argument to def must be an identifier, got %s", args[0].Type)
		}
		varName := args[0].Value.(string)
		val, err := p.Eval(args[1])
		if err != nil {
			return Value{}, err
		}
		p.env.Set(varName, val)
		return val, nil
	})

	return proc
}

func TestProcessor_SimpleExecution(t *testing.T) {
	prog := parse(t, `(\' add 1 2)`)
	proc := newTestProc(prog, nil)

	err := proc.Run()
	require.NoError(t, err)
}

func TestProcessor_DefAndEval(t *testing.T) {
	program := parse(t, `
		(\' def x 10)
		(\' add x 5)
	`)
	env := NewEnv(nil)
	proc := newTestProc(program, env)

	// Manually evaluate the first list to def 'x'
	_, err := proc.evalList(program.Lists[0])
	require.NoError(t, err)

	// Check that 'x' is in the environment
	val, ok := env.Get("x")
	require.True(t, ok)
	assert.Equal(t, int64(10), val.Value)

	// Now evaluate the second list
	result, err := proc.evalList(program.Lists[1])
	require.NoError(t, err)

	// The result of (add x 5) which is (add 10 5) should be 15
	assert.Equal(t, DataTypeNumber, result.Type)
	assert.Equal(t, float64(15), result.Value)
}

func TestProcessor_NestedEvaluation(t *testing.T) {
	program := parse(t, `(\' add 10 (add 3 7))`)
	proc := newTestProc(program, nil)

	result, err := proc.evalList(program.Lists[0])
	require.NoError(t, err)

	assert.Equal(t, DataTypeNumber, result.Type)
	assert.Equal(t, float64(20), result.Value)
}

func TestProcessor_Scoping(t *testing.T) {
	outerEnv := NewEnv(nil)
	outerEnv.Set("x", Value{Type: DataTypeNumber, Value: int64(100)})

	program := parse(t, `(\' def x 1) (\' add x 1)`)

	// Create a new scope for the processor
	innerEnv := NewEnv(outerEnv)
	proc := newTestProc(program, innerEnv)

	// Run (def x 1) in the inner scope
	_, err := proc.evalList(program.Lists[0])
	require.NoError(t, err)

	// Check that inner scope has x=1
	innerVal, _ := innerEnv.Get("x")
	assert.Equal(t, int64(1), innerVal.Value)

	// Check that outer scope is unchanged
	outerVal, _ := outerEnv.Get("x")
	assert.Equal(t, int64(100), outerVal.Value)

	// Run (add x 1) in the inner scope, should use inner x
	result, err := proc.evalList(program.Lists[1])
	require.NoError(t, err)
	assert.Equal(t, float64(2), result.Value)
}

func TestProcessor_Errors(t *testing.T) {
	t.Run("undefined function", func(t *testing.T) {
		program := parse(t, `(\' undefined-func 1 2)`)
		proc := newTestProc(program, nil)
		err := proc.Run()
		require.Error(t, err)
		assert.Equal(t, "undefined function: undefined-func", err.Error())
	})

	t.Run("wrong arg type", func(t *testing.T) {
		program := parse(t, `(\' add 1 "hello")`)
		proc := newTestProc(program, nil)
		err := proc.Run()
		require.Error(t, err)
		assert.Equal(t, "add requires number arguments, got string", err.Error())
	})

	t.Run("def wrong args", func(t *testing.T) {
		program := parse(t, `(\' def x)`)
		proc := newTestProc(program, nil)
		err := proc.Run()
		require.Error(t, err)
		assert.Equal(t, "def requires 2 arguments, got 1", err.Error())
	})
}
