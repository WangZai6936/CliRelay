package executor

import (
	"bytes"
	"net/http"
	"strings"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

// Stream errors are semantic failures even when the HTTP response is 200.
// Inspect them before translation, so an error followed by [DONE] cannot be
// turned into a successful response by an OpenAI-compatible translator.
func openAICompatResponseError(payload []byte) error {
	if !gjson.ValidBytes(payload) {
		return nil
	}
	kind := gjson.GetBytes(payload, "type").String()
	errorNode := gjson.GetBytes(payload, "error")
	if !errorNode.Exists() || errorNode.Type == gjson.Null {
		errorNode = gjson.GetBytes(payload, "response.error")
	}
	hasError := errorNode.Exists() && errorNode.Type != gjson.Null
	if !hasError && kind != "error" && kind != "response.failed" && kind != "response.incomplete" {
		return nil
	}
	code := strings.ToLower(errorNode.Get("code").String())
	typ := strings.ToLower(errorNode.Get("type").String())
	status := http.StatusBadGateway
	switch {
	case code == "invalid_api_key" || code == "authentication_error" || typ == "authentication_error":
		status = http.StatusUnauthorized
	case code == "permission_denied" || typ == "permission_error":
		status = http.StatusForbidden
	case code == "rate_limit_exceeded" || typ == "rate_limit_error" || typ == "rate_limit_exceeded":
		status = http.StatusTooManyRequests
	case typ == "invalid_request_error" || code == "invalid_request_error":
		status = http.StatusBadRequest
	}
	return statusErr{code: status, msg: string(payload), upstreamBody: bytes.Clone(payload)}
}

func openAICompatStreamCompleted(payload []byte) bool {
	if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) || gjson.GetBytes(payload, "type").String() == "response.completed" {
		return true
	}
	for _, choice := range gjson.GetBytes(payload, "choices").Array() {
		if strings.TrimSpace(choice.Get("finish_reason").String()) != "" {
			return true
		}
	}
	return false
}

func openAICompatIncompleteStreamError() error {
	return &cliproxyauth.Error{
		Code:       "response_stream_incomplete",
		Message:    "upstream chat completion stream closed before a finish reason or completion marker",
		Retryable:  true,
		HTTPStatus: http.StatusBadGateway,
	}
}
