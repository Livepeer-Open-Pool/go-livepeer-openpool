package core

import "fmt"

// ErrorKind says what went wrong in terms a caller can act on. The API maps
// each kind to an HTTP status.
type ErrorKind int

const (
	// Invalid input: the request cannot be accepted as it is.
	Invalid ErrorKind = iota + 1
	// NotFound: the thing named does not exist, or not for this caller.
	NotFound
	// Conflict: the request is well-formed but the current state forbids it.
	Conflict
	// Forbidden: the caller may not do this.
	Forbidden
	// Unavailable: something this service depends on did not answer.
	Unavailable
)

// Error is a failure with a message meant for the person who caused it.
type Error struct {
	Kind    ErrorKind
	Message string
}

func (e *Error) Error() string { return e.Message }

func invalidf(format string, args ...any) error {
	return &Error{Kind: Invalid, Message: fmt.Sprintf(format, args...)}
}

func notFoundf(format string, args ...any) error {
	return &Error{Kind: NotFound, Message: fmt.Sprintf(format, args...)}
}

func conflictf(format string, args ...any) error {
	return &Error{Kind: Conflict, Message: fmt.Sprintf(format, args...)}
}

func forbiddenf(format string, args ...any) error {
	return &Error{Kind: Forbidden, Message: fmt.Sprintf(format, args...)}
}

func unavailablef(format string, args ...any) error {
	return &Error{Kind: Unavailable, Message: fmt.Sprintf(format, args...)}
}
