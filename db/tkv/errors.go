package tkv

import (
	"errors"
	"fmt"
)

type ErrKeyNotFound struct {
	Key string
}

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
}

type ErrKeyExists struct {
	Key string
}

func (e *ErrKeyExists) Error() string {
	return fmt.Sprintf("key '%s' already exists", e.Key)
}

// ErrCASFailed is returned when a Compare-And-Swap operation fails because the
// expected value did not match the actual value.
type ErrCASFailed struct {
	Key string
}

func (e *ErrCASFailed) Error() string {
	return fmt.Sprintf("cas failed for key '%s': optimistic lock failure", e.Key)
}

type ErrInternal struct {
	Err error
}

func (e *ErrInternal) Error() string {
	return fmt.Sprintf("internal error: %v", e.Err)
}

func (e *ErrInternal) Unwrap() error {
	return e.Err
}

type ErrDataCorruption struct {
	Key    string
	Reason string
}

func (e *ErrDataCorruption) Error() string {
	return fmt.Sprintf("data corruption for key %s: %s", e.Key, e.Reason)
}

func IsErrKeyNotFound(err error) bool {
	var e *ErrKeyNotFound
	return errors.As(err, &e)
}
