package vm

type SPC struct {
	vm *VM
}

func NewSPC(vm *VM) *SPC {
	return &SPC{vm: vm}
}

func (s *SPC) Step(block string) error {
	return s.vm.Step(block)
}

func (s *SPC) Finalize() error {
	return s.vm.Finalize()
}
