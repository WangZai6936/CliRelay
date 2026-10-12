package executor

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexChatPromptCacheLogBodyMatchesWire(t *testing.T) {
	auth := codexSessionIsolationAuth("chat-cache-log", "chat-acct-log", "log@example.com")
	original := []byte(`{"model":"gpt-6.1-sol","prompt_cache_key":"logging-client-session","messages":[{"role":"user","content":"Opening logging check"}],"reasoning_effort":"high"}`)
	req := cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: original}
	httpReq, loggedBody, err := (&CodexExecutor{}).cacheHelper(context.Background(), auth, sdktranslator.FormatOpenAI, "https://example.test", req, original, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	actual := readRequestBody(t, httpReq)
	if string(actual) != string(loggedBody) {
		t.Fatal("recorded body differs from transmitted body")
	}
	if gjson.GetBytes(loggedBody, "prompt_cache_key").String() == "" {
		t.Fatal("recorded body omits cache key")
	}
	if gjson.GetBytes(loggedBody, "reasoning_effort").String() != "high" {
		t.Fatal("logging changed effort")
	}
}

func TestCodexChatPromptCacheStableAcrossTurnsAndTransports(t *testing.T) {
	auth := codexSessionIsolationAuth("chat-cache-a", "chat-acct-a", "chat-a@example.com")
	turns := [][]byte{
		[]byte(`{"model":"gpt-6.1-sol","messages":[{"role":"system","content":"Stable instructions"},{"role":"user","content":"Opening request"}],"reasoning_effort":"high","tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`),
		[]byte(`{"model":"gpt-6.1-sol","messages":[{"role":"system","content":"Stable instructions"},{"role":"user","content":"Opening request"},{"role":"assistant","content":"First reply"},{"role":"user","content":"Follow-up request"}],"reasoning_effort":"high","tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`),
	}
	for i, payload := range turns {
		turns[i] = append(payload[:len(payload)-1], []byte(`,"context_padding":"`+strings.Repeat("x", codexChatSyntheticCacheMinPayloadBytes)+`"}`)...)
	}
	var session string
	for _, payload := range turns {
		req := cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: payload}
		httpReq := codexCacheHelperRequest(t, auth, sdktranslator.FormatOpenAI, req)
		body := readRequestBody(t, httpReq)
		key := gjson.GetBytes(body, "prompt_cache_key").String()
		if key == "" {
			t.Fatalf("chat cache key missing: %s", body)
		}
		if session != "" && session != key {
			t.Fatal("growing history changed session")
		}
		session = key
		if httpReq.Header.Get("Session_id") != key || httpReq.Header.Get("Conversation_id") != key {
			t.Fatal("HTTP cache identity differs")
		}
		wsBody, wsHeaders := applyCodexPromptCacheHeaders(auth, sdktranslator.FormatOpenAI, req, payload, cliproxyexecutor.Options{})
		if gjson.GetBytes(wsBody, "prompt_cache_key").String() != key || wsHeaders.Get("Session_id") != key || wsHeaders.Get("Conversation_id") != key {
			t.Fatal("websocket cache identity differs")
		}
		var original, actual map[string]any
		if err := json.Unmarshal(payload, &original); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &actual); err != nil {
			t.Fatal(err)
		}
		delete(actual, "prompt_cache_key")
		if !reflect.DeepEqual(original, actual) {
			t.Fatal("cache helper changed request semantics")
		}
	}
	for _, different := range []cliproxyexecutor.Request{
		{Model: "gpt-6.1-sol", Payload: []byte(`{"messages":[{"role":"user","content":"Different opening"}]}`)},
		{Model: "gpt-6-astra", Payload: turns[0]},
	} {
		got := codexCacheHelperRequest(t, auth, sdktranslator.FormatOpenAI, different)
		if gjson.GetBytes(readRequestBody(t, got), "prompt_cache_key").String() == session {
			t.Fatal("distinct conversation/model shared a key")
		}
	}
	otherAuth := codexSessionIsolationAuth("chat-cache-b", "chat-acct-b", "chat-b@example.com")
	got := codexCacheHelperRequest(t, otherAuth, sdktranslator.FormatOpenAI, cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: turns[0]})
	if gjson.GetBytes(readRequestBody(t, got), "prompt_cache_key").String() == session {
		t.Fatal("distinct accounts shared a key")
	}
}

func TestCodexChatPromptCacheExplicitKeyAndEmptySeed(t *testing.T) {
	auth := codexSessionIsolationAuth("chat-cache-explicit", "chat-acct-explicit", "chat@example.com")
	req := cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: []byte(`{"prompt_cache_key":"client-session","messages":[{"role":"user","content":"hello"}]}`)}
	want := codexAccountScopedExplicitSessionID(auth, "client-session")
	got := codexCacheHelperRequest(t, auth, sdktranslator.FormatOpenAI, req)
	if gjson.GetBytes(readRequestBody(t, got), "prompt_cache_key").String() != want {
		t.Fatal("explicit key priority lost")
	}
	req.Payload = []byte(`{"messages":[]}`)
	got, _, err := (&CodexExecutor{}).cacheHelper(context.Background(), auth, sdktranslator.FormatOpenAI, "https://example.test", req, req.Payload, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(readRequestBody(t, got), "prompt_cache_key").Exists() || got.Header.Get("Session_id") != "" {
		t.Fatal("empty seed generated a shared cache identity")
	}
}

func TestCodexChatPromptCacheShortFallbackUnchanged(t *testing.T) {
	auth := codexSessionIsolationAuth("short-cache", "short-acct", "short@example.com")
	req := cliproxyexecutor.Request{Model: "gpt-6.1-sol", Payload: []byte(`{"messages":[{"role":"user","content":"Small request"}]}`)}
	got := codexCacheHelperRequest(t, auth, sdktranslator.FormatOpenAI, req)
	if gjson.GetBytes(readRequestBody(t, got), "prompt_cache_key").Exists() || got.Header.Get("Session_id") != "" {
		t.Fatal("small chat fallback changed")
	}
	body, headers := applyCodexPromptCacheHeaders(auth, sdktranslator.FormatOpenAI, req, req.Payload, cliproxyexecutor.Options{})
	if gjson.GetBytes(body, "prompt_cache_key").Exists() || headers.Get("Session_id") != "" {
		t.Fatal("small websocket chat fallback changed")
	}
	if !shouldSynthesizeCodexPromptCache(sdktranslator.FormatOpenAIResponse, req.Payload) {
		t.Fatal("native Responses fallback changed")
	}
	if shouldSynthesizeCodexPromptCache(sdktranslator.FormatOpenAI, make([]byte, codexChatSyntheticCacheMinPayloadBytes-1)) || !shouldSynthesizeCodexPromptCache(sdktranslator.FormatOpenAI, make([]byte, codexChatSyntheticCacheMinPayloadBytes)) {
		t.Fatal("size boundary incorrect")
	}
}
