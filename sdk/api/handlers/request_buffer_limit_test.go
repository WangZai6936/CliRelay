package handlers

import (
	"errors"
	"testing"
)

func TestStreamingBootstrapCanUseBoundedAlternativeForRequestBufferLimit(t *testing.T) {
	if !bootstrapEligibleStreamErr(errors.New("exceeded request buffer limit while retrying upstream")) {
		t.Fatal("pre-output buffer rejection must reach bounded alternate-account recovery")
	}
}
