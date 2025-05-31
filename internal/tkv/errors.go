package tkv

import "fmt"

// ErrKeyNotFound is returned when a key is not found in the store.
type ErrKeyNotFound struct {
	Key string
}

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
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
