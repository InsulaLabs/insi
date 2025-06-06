package slp

import "fmt"

// Env represents the execution environment, with support for lexical scoping.
type Env struct {
	Parent *Env
	Vars   map[string]Value
}

// NewEnv creates a new environment, optionally with a parent scope.
func NewEnv(parent *Env) *Env {
	return &Env{
		Parent: parent,
		Vars:   make(map[string]Value),
	}
}

// Get retrieves a variable's value, searching up the scope chain.
func (e *Env) Get(name string) (Value, bool) {
	if val, ok := e.Vars[name]; ok {
		return val, true
	}
	if e.Parent != nil {
		return e.Parent.Get(name)
	}
	return Value{}, false
}

// Set defines a variable in the current scope.
func (e *Env) Set(name string, val Value) {
	e.Vars[name] = val
}

// Function is a callable function in SLP. It receives the processor
// for evaluating sub-lists and the raw arguments.
type Function func(p *Proc, args []Value) (Value, error)

// Proc is the SLP processor/interpreter.
type Proc struct {
	env      *Env
	commands map[string]Function
	program  *Program
}

// NewProcessor creates a new SLP processor.
func NewProcessor(env *Env, program *Program) *Proc {
	return &Proc{
		env:      env,
		commands: make(map[string]Function),
		program:  program,
	}
}

// WithCommand registers a single built-in command with the processor.
func (p *Proc) WithCommand(name string, fn Function) *Proc {
	p.commands[name] = fn
	return p
}

// WithCommands registers a map of built-in commands with the processor.
func (p *Proc) WithCommands(commands map[string]Function) *Proc {
	for name, fn := range commands {
		p.commands[name] = fn
	}
	return p
}

// Run executes the program loaded into the processor.
func (p *Proc) Run() error {
	if p.program == nil {
		return nil
	}
	for _, list := range p.program.Lists {
		_, err := p.evalList(list)
		if err != nil {
			return err
		}
	}
	return nil
}

// Eval evaluates a single value.
func (p *Proc) Eval(val Value) (Value, error) {
	switch val.Type {
	case DataTypeList:
		return p.evalList(val.Value.(*List))
	case DataTypeIdentifier:
		varName := val.Value.(string)
		if value, ok := p.env.Get(varName); ok {
			return value, nil
		}
		// If it's not in the env, it might be a built-in command identifier
		if _, ok := p.commands[varName]; ok {
			return Value{Type: DataTypeFunction, Value: p.commands[varName]}, nil
		}
		return Value{}, fmt.Errorf("undefined variable or command: %s", varName)
	default:
		// Literals evaluate to themselves
		return val, nil
	}
}

// evalList evaluates a list, treating the first element as a function call.
func (p *Proc) evalList(list *List) (Value, error) {
	if list.Head == nil {
		return Value{Type: DataTypeNull}, nil
	}

	headVal := list.Head.Data
	if headVal.Type != DataTypeIdentifier {
		return Value{}, fmt.Errorf("list must start with an identifier to be executed")
	}
	funcName := headVal.Value.(string)

	var fn Function
	var ok bool

	// 1. Look in built-in commands
	fn, ok = p.commands[funcName]
	if !ok {
		// 2. Look in environment variables
		envVal, exists := p.env.Get(funcName)
		if !exists {
			return Value{}, fmt.Errorf("undefined function: %s", funcName)
		}
		if envVal.Type != DataTypeFunction {
			return Value{}, fmt.Errorf("variable '%s' is not a function", funcName)
		}
		fn, ok = envVal.Value.(Function)
		if !ok {
			return Value{}, fmt.Errorf("internal error: variable '%s' has function type but is not a function", funcName)
		}
	}

	var args []Value
	curr := list.Head.Next
	for curr != nil {
		args = append(args, curr.Data)
		curr = curr.Next
	}

	return fn(p, args)
}
