package slp

/*
	Very simple parser. Everything is a lisp-string.

	(COMMAND ARG1 ARG2 ARG3)


	(COMMAND (COMMAND ARGB1 ARGB2 ARGB3) ARG2 ARG3)

	We will use "{}" to denote a list of data rather than a list of things
	to be "processed"


	(tl {sqlkit imgkit wekit})

*/

type Command int

const (
	CMD_NO_OP Command = iota
	CMD_TOOL_LOAD
	CMD_LINK_RESOURCE
	CMD_BLOOM_CONCEPT
)

var commandMap = map[string]Command{
	"\\tool-load":     CMD_TOOL_LOAD,
	"\\tl":            CMD_TOOL_LOAD,
	"\\link-resource": CMD_LINK_RESOURCE,
	"\\lr":            CMD_LINK_RESOURCE,
	"\\bloom-concept": CMD_BLOOM_CONCEPT,
	"\\bc":            CMD_BLOOM_CONCEPT,
}

type SLP struct {
}
