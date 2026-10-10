package handlers

import (
	"errors"
	"testing"
)

func TestStreamingBootstrapDoesNotRetryRequestBufferLimit(t *testing.T) {
	if bootstrapEligibleStreamErr(errors.New("exceeded request buffer limit while retrying upstream")) {
		t.Fatal("identical large request must not be replayed")
	}
}
