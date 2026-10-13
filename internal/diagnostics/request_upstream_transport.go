package diagnostics

import (
	"context"
	"time"
)

// RecordUpstreamTransport records only bounded diagnostic labels, never
// request headers, proxy credentials or TLS key material.
func RecordUpstreamTransport(ctx context.Context, group, kind string) {
	mutateUpstream(ctx, func(u *UpstreamSnapshot) {
		u.TransportGroup = boundedUpstreamIdentifier(group)
		u.TransportKind = boundedUpstreamIdentifier(kind)
		if u.StartedAt == "" {
			u.StartedAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
	})
}

func RecordUpstreamConnection(ctx context.Context, id string, reused bool) {
	mutateUpstream(ctx, func(u *UpstreamSnapshot) {
		u.ConnectionID = boundedUpstreamIdentifier(id)
		u.ConnectionReused = &reused
	})
}

func RecordUpstreamHTTPResponse(ctx context.Context, protocol string) {
	mutateUpstream(ctx, func(u *UpstreamSnapshot) {
		u.Protocol = boundedUpstreamIdentifier(protocol)
		u.ResponseHeadersAt = time.Now().UTC().Format(time.RFC3339Nano)
	})
}

func RecordUpstreamBodyRead(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	mutateUpstream(ctx, func(u *UpstreamSnapshot) {
		if u.FirstBodyAt == "" {
			u.FirstBodyAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		u.BodyBytes += int64(n)
	})
}

// RecordUpstreamOutput is called only after startup buffering releases a
// translated payload. Raw SSE headers and keepalives are not visible output.
func RecordUpstreamOutput(ctx context.Context, n int) {
	if n <= 0 {
		return
	}
	mutateUpstream(ctx, func(u *UpstreamSnapshot) {
		if u.FirstOutputAt == "" {
			u.FirstOutputAt = time.Now().UTC().Format(time.RFC3339Nano)
		}
		u.OutputBytes += int64(n)
	})
}

func mutateUpstream(ctx context.Context, update func(*UpstreamSnapshot)) {
	d := FromContext(ctx)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Upstream == nil {
		d.Upstream = &UpstreamSnapshot{}
	}
	update(d.Upstream)
}

func recordUpstreamErrorTimeLocked(u *UpstreamSnapshot) {
	now := time.Now().UTC()
	if u.ErrorAt == "" {
		u.ErrorAt = now.Format(time.RFC3339Nano)
	}
	if recorded, err := time.Parse(time.RFC3339Nano, u.ErrorAt); err == nil {
		now = recorded
	}
	if started, err := time.Parse(time.RFC3339Nano, u.StartedAt); err == nil {
		u.ErrorAfterMS = now.Sub(started).Milliseconds()
	}
	switch {
	case u.OutputBytes > 0:
		u.ErrorStage = "after_output"
	case u.ResponseHeadersAt != "":
		u.ErrorStage = "before_output"
	default:
		u.ErrorStage = "before_headers"
	}
}
