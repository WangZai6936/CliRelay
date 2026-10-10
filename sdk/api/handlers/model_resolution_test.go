package handlers

import (
	"context"
	"fmt"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"testing"
)

func TestTextModelFallbackFailureClassificationExcludesOtherCauses(t *testing.T) {
	for _, tc := range []struct {
		err    error
		reason string
	}{
		{&coreauth.Error{Code: "auth_not_found"}, "no_route"},
		{&coreauth.Error{Code: "auth_unavailable", HTTPStatus: 503}, "no_route"},
		{&coreauth.Error{HTTPStatus: 404, Message: `{"error":{"code":"model_not_found"}}`}, "model_unavailable"},
		{&coreauth.Error{HTTPStatus: 400, Message: `{"error":{"code":"model_disabled"}}`}, "model_unavailable"},
		{&coreauth.Error{HTTPStatus: 403, Code: "auth_not_found"}, ""},
		{&coreauth.Error{HTTPStatus: 404, Message: "endpoint not found"}, ""},
		{&coreauth.Error{HTTPStatus: 429, Code: "model_cooldown"}, ""},
		{&coreauth.Error{HTTPStatus: 500, Message: "temporary provider failure"}, ""},
		{&coreauth.Error{HTTPStatus: 400, Message: `{"error":{"code":"invalid_encrypted_content"}}`}, ""},
		{&coreauth.Error{HTTPStatus: 400, Message: `{"error":{"code":"context_length_exceeded"}}`}, ""},
		{fmt.Errorf("wrapped: %w", context.Canceled), ""},
	} {
		if got := textModelFailureReason(tc.err); got != tc.reason {
			t.Fatalf("error=%v classified=%q want=%q", tc.err, got, tc.reason)
		}
	}
}
