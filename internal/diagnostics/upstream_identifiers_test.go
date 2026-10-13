package diagnostics

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestUpstreamIdentifiersResetBetweenAttempts(t *testing.T) {
	d := &RequestDiagnostic{}
	ctx := WithContext(context.Background(), d)
	SetUpstreamRequest(ctx, 1, "test", "auth", "", "https://upstream.invalid", "POST")
	SetUpstreamResponse(ctx, 200)
	SetUpstreamResponseIdentifiers(ctx, http.Header{"X-Request-Id": {"upstream-request"}, "Cf-Ray": {"ray-1"}, "Authorization": {"private"}})
	SetUpstreamError(ctx, errors.New("first attempt failed"))
	first := d.Snapshot().Upstream
	if first.RequestID != "upstream-request" || first.CFRay != "ray-1" {
		t.Fatalf("missing identifiers: %+v", first)
	}
	SetUpstreamRequest(ctx, 2, "test", "other", "", "https://other.invalid", "POST")
	next := d.Snapshot().Upstream
	if next.Status != 0 || next.ErrorMessage != "" || next.RequestID != "" || next.CFRay != "" {
		t.Fatalf("old attempt leaked into new diagnostics: %+v", next)
	}
}
