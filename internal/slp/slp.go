package slp

import "fmt"

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
type Some = Obj // quoted expression that needs to be resolved later
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

func (o Obj) Encode() string {
	switch o.Type {
	case OBJ_TYPE_NONE:
		return "_"
	case OBJ_TYPE_SOME:
		quoted := o.D.(Some)
		return "'" + quoted.Encode()
	case OBJ_TYPE_LIST:
		list := o.D.(List)
		if len(list) == 0 {
			return "()"
		}
		result := "("
		for i, item := range list {
			if i > 0 {
				result += " "
			}
			result += item.Encode()
		}
		result += ")"
		return result
	case OBJ_TYPE_STRING:
		str := o.D.(string)
		return escapeString(str)
	case OBJ_TYPE_NUMBER:
		num := o.D.(Number)
		// Check if it's a whole number
		if float64(num) == float64(int64(num)) {
			return fmt.Sprintf("%d", int64(num))
		}
		return fmt.Sprintf("%g", float64(num))
	case OBJ_TYPE_IDENTIFIER:
		return string(o.D.(Identifier))
	case OBJ_TYPE_ERROR:
		err := o.D.(Error)
		return fmt.Sprintf("ERROR:%d:%s", err.Position, err.Message)
	default:
		return fmt.Sprintf("UNKNOWN_TYPE:%s", o.Type)
	}
}

func escapeString(s string) string {
	result := "\""
	for _, r := range s {
		switch r {
		case '"':
			result += "\\\""
		case '\\':
			result += "\\\\"
		case '\n':
			result += "\\n"
		case '\t':
			result += "\\t"
		case '\r':
			result += "\\r"
		default:
			result += string(r)
		}
	}
	result += "\""
	return result
}
