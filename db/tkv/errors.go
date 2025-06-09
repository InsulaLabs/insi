package tkv

import (
	"errors"
	"fmt"
)

// ErrKeyNotFound is returned when a key is not found in the store.
type ErrKeyNotFound struct {
	Key string
}

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
}

// ErrKeyExists is returned when trying to create a key that already exists.
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

// ErrInternal is returned when an internal error occurs.
type ErrInternal struct {
	Err error
}

func (e *ErrInternal) Error() string {
	return fmt.Sprintf("internal error: %v", e.Err)
}

func (e *ErrInternal) Unwrap() error {
	return e.Err
}

// ErrDataCorruption is returned when an object's data is found to be corrupted.
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
