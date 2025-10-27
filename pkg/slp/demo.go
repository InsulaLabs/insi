package slp

import (
	"context"
	"fmt"
	"log/slog"
)

// This simple backend is meant for demonstration
// the actual backend should be implemented elsewhere to interact with
// whatever system the user wants.
// This is just to demonstrate how to work with objects and the Backend interface etc

type Env interface {
	Context() context.Context
	ResolveIdentifier(ctx context.Context, name string) Obj
}

type Callable func(env Env) Obj

type Backend interface {

	/*
		Attempt to load a function to call (first item in a list being executed)
	*/
	ResolveCallable(env Env, identifier string) Obj

	/*
		Attempt to treat a given list as one that "does" somethign
	*/
	ExecuteList(env Env, list List) Obj
}

type simpleKws string

const (
	kwSet simpleKws = "set"
	kwGet simpleKws = "get"
	kwDel simpleKws = "del"
)

type simpleScope struct {
	parent    *simpleScope
	variables map[string]Obj
}

type SimpleBackend struct {
	rootScope *simpleScope
	logger    *slog.Logger
}

func NewSimpleBackend(logger *slog.Logger) *SimpleBackend {
	logger = logger.WithGroup("simple-backend")
	return &SimpleBackend{
		rootScope: &simpleScope{
			variables: make(map[string]Obj),
		},
		logger: logger,
	}
}

func (s *simpleScope) Context() context.Context {
	return context.Background()
}

func (s *simpleScope) ResolveIdentifier(ctx context.Context, name string) Obj {
	if obj, exists := s.variables[name]; exists {
		return obj
	}

	if s.parent != nil {
		return s.parent.ResolveIdentifier(ctx, name)
	}

	return Obj{Type: OBJ_TYPE_NONE, D: None{}}
}

func (sb *SimpleBackend) ResolveCallable(env Env, identifier string) Obj {
	switch identifier {
	case string(kwSet):
		return Obj{
			Type: OBJ_TYPE_FN,
			D: Fn{
				Identifier: identifier,
				Function:   sb.setFn,
			},
		}
	case string(kwGet):
		return Obj{
			Type: OBJ_TYPE_FN,
			D: Fn{
				Identifier: identifier,
				Function:   sb.getFn,
			},
		}
	case string(kwDel):
		return Obj{
			Type: OBJ_TYPE_FN,
			D: Fn{
				Identifier: identifier,
				Function:   sb.delFn,
			},
		}
	default:
		if obj := env.ResolveIdentifier(env.Context(), identifier); obj.Type != OBJ_TYPE_NONE {
			return obj
		}
		return Obj{Type: OBJ_TYPE_NONE, D: None{}}
	}
}

func (sb *SimpleBackend) ExecuteList(env Env, list List) Obj {
	if len(list) == 0 {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "empty list"}}
	}

	first := list[0]
	var callable Callable

	switch first.Type {
	case OBJ_TYPE_IDENTIFIER:
		fnObj := sb.ResolveCallable(env, string(first.D.(Identifier)))
		if fnObj.Type == OBJ_TYPE_FN {
			callable = fnObj.D.(Fn).Function
		} else {
			return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "unknown function: " + string(first.D.(Identifier))}}
		}
	case OBJ_TYPE_FN:
		callable = first.D.(Fn).Function
	default:
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "first element must be callable"}}
	}

	argsEnv := &simpleScope{
		parent:    env.(*simpleScope),
		variables: make(map[string]Obj),
	}

	for i, arg := range list[1:] {
		argsEnv.variables[fmt.Sprintf("arg%d", i)] = arg
	}

	return callable(argsEnv)
}

func getArg(env Env, index int) (Obj, bool) {
	scope := env.(*simpleScope)
	argName := fmt.Sprintf("arg%d", index)
	arg, exists := scope.variables[argName]
	return arg, exists
}

func (sb *SimpleBackend) setFn(env Env) Obj {
	keyArg, ok := getArg(env, 0)
	if !ok {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "set requires key argument"}}
	}

	if keyArg.Type != OBJ_TYPE_IDENTIFIER {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "set key must be identifier"}}
	}

	valueArg, ok := getArg(env, 1)
	if !ok {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "set requires value argument"}}
	}

	key := string(keyArg.D.(Identifier))

	currentScope := env.(*simpleScope)
	for currentScope.parent != nil {
		currentScope = currentScope.parent
	}
	currentScope.variables[key] = valueArg

	sb.logger.Info("set variable", "key", key, "value", valueArg.Encode())
	return Obj{Type: OBJ_TYPE_STRING, D: "ok"}
}

func (sb *SimpleBackend) getFn(env Env) Obj {
	keyArg, ok := getArg(env, 0)
	if !ok {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "get requires key argument"}}
	}

	if keyArg.Type != OBJ_TYPE_IDENTIFIER {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "get key must be identifier"}}
	}

	key := string(keyArg.D.(Identifier))
	value := env.ResolveIdentifier(env.Context(), key)

	sb.logger.Info("get variable", "key", key, "value", value.Encode())
	return value
}

func (sb *SimpleBackend) delFn(env Env) Obj {
	keyArg, ok := getArg(env, 0)
	if !ok {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "del requires key argument"}}
	}

	if keyArg.Type != OBJ_TYPE_IDENTIFIER {
		return Obj{Type: OBJ_TYPE_ERROR, D: Error{Message: "del key must be identifier"}}
	}

	key := string(keyArg.D.(Identifier))

	currentScope := env.(*simpleScope)
	for currentScope.parent != nil {
		currentScope = currentScope.parent
	}
	delete(currentScope.variables, key)

	sb.logger.Info("deleted variable", "key", key)
	return Obj{Type: OBJ_TYPE_STRING, D: "deleted"}
}

var _ Backend = &SimpleBackend{}
var _ Env = &simpleScope{}
