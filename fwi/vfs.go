package fwi

const (
	vfsIdentifier = "vfs-v1"
)

type FS interface {
	// io compatible FS abstraction over a KV interface
	// using a "scope"
}

type fsImpl struct {
	kv KV
}

func NewFS(kv KV) FS {
	return &fsImpl{
		kv: kv,
	}
}

func (f *fsImpl) Start() {
	f.kv.PushScope(vfsIdentifier)
}

func (f *fsImpl) Stop() {
	f.kv.PopScope()
}

/*

	TODO: Make an I/O compatible FS abstraction over the
	FWI interface set. We need to install/setup a "large file"
	sync across insi cluster though. we have a 1mb limit on the
	kv interface.


*/
