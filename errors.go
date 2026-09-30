package workflow

import "errors"

// CodedError carries a caller-defined code the engine persists but never interprets.
type CodedError struct {
	Code string
	Err  error
}

func (e *CodedError) Error() string { return e.Err.Error() }

func (e *CodedError) Unwrap() error { return e.Err }

// WithCode tags err with code for Failure.Code, returning nil for a nil err. It
// composes with Permanent in either order.
func WithCode(code string, err error) error {
	if err == nil {
		return nil
	}
	return &CodedError{Code: code, Err: err}
}

// codeOf uses the outermost tag in err's chain.
func codeOf(err error) string {
	coded, ok := errors.AsType[*CodedError](err)
	if !ok {
		return ""
	}
	return coded.Code
}

// PermanentError marks a task failure as non-retryable.
type PermanentError struct {
	Err error
}

func (e *PermanentError) Error() string { return e.Err.Error() }

func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the executor parks without retrying, returning nil for a nil err.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}
