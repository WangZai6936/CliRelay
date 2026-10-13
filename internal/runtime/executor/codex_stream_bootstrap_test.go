package executor

import (
	"bytes"
	"strings"
	"testing"
)

func TestCodexStreamBootstrapPreservesSuccessfulEventOrder(t *testing.T) {
	var bootstrap codexStreamBootstrap
	for _, line := range []string{"event: response.created", `data: {"type":"response.created"}`, "", `data: {"type":"response.in_progress"}`, `data: {"type":"keepalive"}`} {
		if got := bootstrap.prepare([]byte(line), []string{line}); len(got) != 0 {
			t.Fatalf("startup event escaped: %q", got)
		}
	}
	got := bootstrap.prepare([]byte(`data: {"type":"response.output_text.delta","delta":"ok"}`), []string{"output"})
	want := []string{"event: response.created", `data: {"type":"response.created"}`, `data: {"type":"response.in_progress"}`, `data: {"type":"keepalive"}`, "output"}
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("event %d = %q, want %q", i, got[i], want[i])
		}
	}
	if bootstrap.pending != nil || bootstrap.bytes != 0 {
		t.Fatal("startup buffer retained data after forwarding")
	}
	if got := bootstrap.prepare([]byte(`data: {"type":"response.completed"}`), []string{"complete"}); len(got) != 1 || string(got[0]) != "complete" {
		t.Fatalf("established stream changed: %q", got)
	}
}

func TestCodexStreamBootstrapBoundedAndUnknownEventsForward(t *testing.T) {
	for _, test := range []struct {
		name string
		line string
		data string
	}{
		{"buffer limit", `data: {"type":"response.created"}`, strings.Repeat("x", codexStreamStartupBufferLimit)},
		{"new event", `data: {"type":"response.new_output_event"}`, "new event"},
		{"empty completion", `data: {"type":"response.completed"}`, "complete"},
		{"tool announcement", `data: {"type":"response.output_item.added","item":{"type":"function_call","name":"read"}}`, "tool"},
		{"reasoning summary", `data: {"type":"response.output_item.added","item":{"type":"reasoning","summary":[{"type":"summary_text","text":"thinking"}]}}`, "summary"},
		{"reasoning content", `data: {"type":"response.output_item.added","item":{"type":"reasoning","content":[{"type":"reasoning_text","text":"thinking"}]}}`, "content"},
		{"unknown reasoning shape", `data: {"type":"response.output_item.added","item":{"type":"reasoning","summary":{"text":"thinking"}}}`, "unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var bootstrap codexStreamBootstrap
			bootstrap.prepare([]byte(`data: {"type":"response.created"}`), []string{"start"})
			got := bootstrap.prepare([]byte(test.line), []string{test.data})
			if len(got) != 2 || !bytes.Equal(got[0], []byte("start")) || !bytes.Equal(got[1], []byte(test.data)) {
				t.Fatalf("events were lost or reordered: chunks=%d", len(got))
			}
			if !bootstrap.started || bootstrap.pending != nil || bootstrap.bytes != 0 {
				t.Fatal("buffer was not released")
			}
		})
	}
}

func TestCodexStreamBootstrapBuffersReasoningHeaderWithoutVisibleContent(t *testing.T) {
	var bootstrap codexStreamBootstrap
	bootstrap.prepare([]byte(`data: {"type":"response.created"}`), []string{"created"})
	header := `data: {"type":"response.output_item.added","item":{"id":"rs_mock","type":"reasoning","encrypted_content":"opaque","summary":[],"content":[]}}`
	if got := bootstrap.prepare([]byte(header), []string{header}); len(got) != 0 || bootstrap.started {
		t.Fatalf("reasoning header committed an otherwise empty stream: %q", got)
	}
	got := bootstrap.prepare([]byte(`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}`), []string{"thinking"})
	if len(got) != 3 || string(got[0]) != "created" || string(got[1]) != header || string(got[2]) != "thinking" {
		t.Fatalf("reasoning output was lost or reordered: %q", got)
	}
}

func TestCodexStreamBootstrapIgnoredEventsDoNotReleaseStartup(t *testing.T) {
	var bootstrap codexStreamBootstrap
	bootstrap.prepare([]byte(`data: {"type":"response.created"}`), []string{"assistant role"})
	for _, line := range []string{
		`data: {"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"opaque","summary":[]}}`,
		`data: {"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`data: {"type":"response.unknown_ignored_event"}`,
	} {
		if got := bootstrap.prepare([]byte(line), []string{""}); len(got) != 0 {
			t.Fatalf("ignored event released startup: %q", got)
		}
		if bootstrap.started || bootstrap.bytes != len("assistant role") || len(bootstrap.pending) != 1 {
			t.Fatal("ignored event changed the uncommitted startup buffer")
		}
	}
	got := bootstrap.prepare([]byte(`data: {"type":"response.output_text.delta","delta":"ok"}`), []string{"output"})
	if len(got) != 2 || string(got[0]) != "assistant role" || string(got[1]) != "output" {
		t.Fatalf("first actual output was lost or reordered: %q", got)
	}
}
