package vm

type CAG struct {
	vm *VM
}

func NewCAG(vm *VM) *CAG {
	return &CAG{vm: vm}
}

func (c *CAG) Step(block string) error {
	return ErrVMNotImplemented
}

func (c *CAG) Finalize() error {
	return ErrVMNotImplemented
}
