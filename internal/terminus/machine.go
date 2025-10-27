package terminus

import (
	"github.com/InsulaLabs/insi/pkg/fwi"
	"github.com/InsulaLabs/insi/pkg/slp"
)

type VirtualMachine struct {
	svfs    sessionVFS
	session *Session

	kvBackend fwi.KV

	/*

		We use the KV backend for the environment and data storage

		So: (set x 3) would set "x" under the current scope.

		(in some_scope
			(set x 3))

		(get some_scope:x)

	*/
	fns map[string]slp.Callable
}

func (x *VirtualMachine) ExecuteCommand(command string) (string, error) {

	/*
		If command does not start and does not end with '()' add them manually for convienance

		Then, we send to slp


	*/

	return "", nil
}
