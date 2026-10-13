package handlers

import (
	"bytes"
	"context"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

// markMeaningfulStreamOutput excludes role/startup/usage/terminal frames from
// the request's visible-output failure stage. Replay still locks on any emitted
// bytes: changing that stricter boundary would require buffering every protocol.
func markMeaningfulStreamOutput(ctx context.Context, payload []byte) {
	if meaningfulStreamOutput(payload) {
		usage.MarkRequestOutput(ctx)
	}
}

func meaningfulStreamOutput(payload []byte) bool {
	for _, raw := range bytes.Split(payload, []byte("\n")) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 || bytes.HasPrefix(line, []byte(":")) || bytes.HasPrefix(line, []byte("event:")) || bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[5:])
		}
		if bytes.Equal(line, []byte("[DONE]")) {
			continue
		}
		if !gjson.ValidBytes(line) {
			return true
		}
		root := gjson.ParseBytes(line)
		kind := root.Get("type").String()
		if strings.HasSuffix(kind, ".delta") && root.Get("delta").Type == gjson.String && root.Get("delta").String() != "" {
			return true
		}
		if kind == "response.output_item.added" && root.Get("item.type").String() == "function_call" {
			return true
		}
		for _, path := range []string{"item.content", "item.summary"} {
			for _, part := range root.Get(path).Array() {
				if part.Get("text").String() != "" {
					return true
				}
			}
		}
		if strings.HasPrefix(kind, "response.content_part.") && root.Get("part.text").String() != "" {
			return true
		}
		for _, choice := range root.Get("choices").Array() {
			for _, field := range []string{"text", "delta.content", "delta.reasoning_content", "delta.reasoning", "delta.function_call.name", "delta.function_call.arguments"} {
				if value := choice.Get(field); value.Type == gjson.String && value.String() != "" {
					return true
				}
			}
			for _, call := range choice.Get("delta.tool_calls").Array() {
				if call.Get("function.name").String() != "" || call.Get("function.arguments").String() != "" {
					return true
				}
			}
		}
		for _, field := range []string{"delta.text", "delta.thinking", "delta.partial_json", "content_block.text"} {
			if value := root.Get(field); value.Type == gjson.String && value.String() != "" {
				return true
			}
		}
		if kind == "content_block_start" && root.Get("content_block.type").String() == "tool_use" {
			return true
		}
		for _, candidate := range root.Get("candidates").Array() {
			for _, part := range candidate.Get("content.parts").Array() {
				if part.Get("text").String() != "" || part.Get("functionCall").Exists() {
					return true
				}
			}
		}
	}
	return false
}
