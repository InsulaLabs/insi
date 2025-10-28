package svm

import (
	"fmt"
	"strconv"
	"strings"
)

type BuildErrorCode int

const (
	BuildError_Unknown BuildErrorCode = iota

	BuildError_InvalidSyntax
	BuildError_DuplicateProcessName

	BuildError_InvalidLevelACall     // Level A call attempt in Level B
	BuildError_ArgumentCountMismatch // Argument count mismatch between function and call

	BuildError_InvalidDataScopeName
	BuildError_InvalidDataScopeRequiresStaticString // Argument to data-scope must be "static string"

	BuildError_UnknownBindingReference          // Bind/unbind call to name of unknown static-fn (we can check if its a string so we might as well at startup time)
	BuildError_BindOrUnbindRequiresStaticString // The argument to bind and unbind must be a "string" not an identifier, to ensure it matches a static function

	BuildError_InvalidEveryRequiresStaticString // The argument to every must be a "static string"
	BuildError_InvalidEveryDuration             // An 'Every' duration is not a valid duration string
)

func buildErrorCodeToString(id BuildErrorCode) string {
	switch id {
	case BuildError_Unknown:
		return "Unknown"
	case BuildError_InvalidSyntax:
		return "Invalid Syntax"
	case BuildError_DuplicateProcessName:
		return "Duplicate Process Name"
	case BuildError_InvalidLevelACall:
		return "Level A call attempt in Level B"
	case BuildError_ArgumentCountMismatch:
		return "Argument count mismatch between function and call"
	case BuildError_InvalidDataScopeName:
		return "Invalid Data Scope Name"
	case BuildError_InvalidDataScopeRequiresStaticString:
		return "Argument to data-scope must be a \"static string\""
	case BuildError_UnknownBindingReference:
		return "Bind/unbind call to name of unknown static-fn (we can check if its a string so we might as well at startup time)"
	case BuildError_BindOrUnbindRequiresStaticString:
		return "The argument to bind and unbind must be a \"string\" not an identifier, to ensure it matches a static function"
	case BuildError_InvalidEveryRequiresStaticString:
		return "The argument to every must be a \"static string\""
	case BuildError_InvalidEveryDuration:
		return "An 'Every' duration is not a valid duration string"
	default:
		// Ensure all BuildErrorCode cases are explicitly covered; fallback for unknown codes.
		return fmt.Sprintf("Unknown BuildErrorCode (%d)", id)
	}
}

/*
notes:
	(bind static-string identifier-matching-statitic-fn-defintiion)
	(unbind static-string identifier-matching-statitic-fn-defintiion)
	(emit some some) ; may be any value, will be evaluated at time of call
*/

type BuildError struct {
	ID           BuildErrorCode
	StartIndex   uint32
	ErrorMessage string
}

func (e *BuildError) Error() string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("BuildError: (%d) %s", e.ID, buildErrorCodeToString(e.ID)))
	sb.WriteString(" at index ")
	sb.WriteString(strconv.FormatUint(uint64(e.StartIndex), 10))
	sb.WriteString(": ")
	sb.WriteString(e.ErrorMessage)
	sb.WriteString("\n")
	return sb.String()
}

func NewBuildError(id BuildErrorCode, startIndex uint32, errorMessage string) *BuildError {
	return &BuildError{
		ID:           id,
		StartIndex:   startIndex,
		ErrorMessage: errorMessage,
	}
}
