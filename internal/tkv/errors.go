package tkv

import "fmt"

type ErrKeyNotFound struct {
	Key string
}

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key not found: %s", e.Key)
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
