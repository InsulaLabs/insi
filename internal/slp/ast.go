package slp

type KW string

const (
	KW_CTX    KW = "ctx" // set the operational context (mem/disk)
	KW_SET    KW = "set"
	KW_GET    KW = "get"
	KW_CAS    KW = "cas" // compare and swap
	KW_SNX    KW = "snx" // set if not exists
	KW_BMP    KW = "bmp" // bump integer
	KW_DELETE KW = "del"
	KW_ITER   KW = "iter" // iterate keys (list * 0 100)
)
