package platform

import "net/http"

// APIError is the only way a handler or service should surface an error that
// must become a specific HTTP status + code. ErrorHandler is the sole place
// that renders it.
type APIError struct {
	Status  int
	Code    string
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

func NotFound(code, message string) *APIError {
	return &APIError{Status: http.StatusNotFound, Code: code, Message: message}
}

func Unprocessable(code, message string) *APIError {
	return &APIError{Status: http.StatusUnprocessableEntity, Code: code, Message: message}
}

func BadRequest(code, message string) *APIError {
	return &APIError{Status: http.StatusBadRequest, Code: code, Message: message}
}

func Unavailable(code, message string) *APIError {
	return &APIError{Status: http.StatusServiceUnavailable, Code: code, Message: message}
}
