package auth

import (
	"net/http"
	"testing"
)

func TestRequestBufferLimitDoesNotRotateCredentials(t *testing.T) {
	known := &Error{HTTPStatus: http.StatusInsufficientStorage, Message: "exceeded request buffer limit while retrying upstream"}
	if !isRequestInvalidError(known) {
		t.Fatal("buffer limit must be treated as request-specific")
	}
	other := &Error{HTTPStatus: http.StatusInsufficientStorage, Message: "temporary upstream storage unavailable"}
	if isRequestInvalidError(other) {
		t.Fatal("unrelated 507 must keep ordinary upstream recovery")
	}
}
