package slp

type ObjType string

const (
	OBJ_TYPE_NONE       ObjType = "none"
	OBJ_TYPE_SOME       ObjType = "some" // something that has a value
	OBJ_TYPE_LIST       ObjType = "list" // something that has a list of datums
	OBJ_TYPE_ERROR      ObjType = "error"
	OBJ_TYPE_STRING     ObjType = "string"     // quoted string
	OBJ_TYPE_NUMBER     ObjType = "number"     // numeric value
	OBJ_TYPE_IDENTIFIER ObjType = "identifier" // unquoted identifier
)

type List []Obj
type Some Obj // quoted expression that needs to be resolved later
type None struct{}
type Error struct {
	Position int
	Message  string
}
type Number float64
type Identifier string

type Obj struct {
	Type ObjType
	D    any
}
