package diagnostics

import (
	"context"
	"errors"
	"testing"
)

func TestUpstreamConnectionStagesAndAttemptReset(t *testing.T) {
	d := &RequestDiagnostic{}
	ctx := WithContext(context.Background(), d)
	SetUpstreamRequest(ctx, 1, "codex", "mock", "", "https://example.invalid", "POST")
	RecordUpstreamTransport(ctx, "http1_canary", "tls_fingerprint")
	RecordUpstreamConnection(ctx, "opaque-id", false)
	SetUpstreamError(ctx, errors.New("connect failed"))
	if d.Snapshot().Upstream.ErrorStage != "before_headers" {
		t.Fatal("wrong pre-header stage")
	}
	RecordUpstreamHTTPResponse(ctx, "HTTP/1.1")
	RecordUpstreamBodyRead(ctx, 100)
	SetUpstreamError(ctx, errors.New("bootstrap failed"))
	if d.Snapshot().Upstream.ErrorStage != "before_output" {
		t.Fatal("raw SSE was mistaken for output")
	}
	RecordUpstreamOutput(ctx, 20)
	SetUpstreamError(ctx, errors.New("stream interrupted"))
	snapshot := d.Snapshot().Upstream
	if snapshot.ErrorStage != "after_output" || snapshot.FirstOutputAt == "" || snapshot.ErrorAt == "" {
		t.Fatal("missing visible output/error timing")
	}
	*snapshot.ConnectionReused = true
	if *d.Snapshot().Upstream.ConnectionReused {
		t.Fatal("snapshot shares mutable pointer")
	}
	SetUpstreamRequest(ctx, 2, "codex", "other", "", "https://example.invalid", "POST")
	next := d.Snapshot().Upstream
	if next.Protocol != "" || next.ConnectionID != "" || next.ConnectionReused != nil || next.OutputBytes != 0 || next.ErrorStage != "" || next.TransportGroup != "" {
		t.Fatalf("attempt state leaked: %+v", next)
	}
}
