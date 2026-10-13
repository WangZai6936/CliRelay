package usage

import (
	"context"
	"testing"
	"time"
)

func TestRequestOutcomeTracksRetryChainWithoutProviderSuccessInference(t *testing.T) {
	previous := defaultManager
	defaultManager = NewManager(16)
	observer := &observingPlugin{seen: make(chan Record, 16)}
	defaultManager.Register(observer)
	t.Cleanup(func() { defaultManager.Stop(); defaultManager.Wait(context.Background()); defaultManager = previous })
	ctx := BeginRequest(context.Background())
	PublishRecord(ctx, Record{APIKey: "key", Model: "model", Failed: true})
	PublishRecord(ctx, Record{APIKey: "key", Model: "model", Failed: true})
	PublishRecord(ctx, Record{APIKey: "key", Model: "model"})
	MarkRequestSucceeded(ctx)
	FinishRequest(ctx)
	FinishRequest(ctx)
	var identity string
	for i := 1; i <= 4; i++ {
		select {
		case record := <-observer.seen:
			if identity == "" {
				identity = record.RequestCorrelationID
			}
			if record.RequestCorrelationID != identity || identity == "" {
				t.Fatal("retry chain lost correlation identity")
			}
			if i < 4 && record.Attempt != int64(i) {
				t.Fatalf("attempt = %d", record.Attempt)
			}
			if i == 4 {
				o := record.RequestOutcome
				if o == nil || o.Result != OutcomeRecovered || o.Attempts != 3 || o.FailedAttempts != 2 {
					t.Fatalf("outcome = %+v", o)
				}
				if record.APIKey != "key" || record.InputContent != "" {
					t.Fatal("terminal metadata identity invalid")
				}
			}
		case <-time.After(time.Second):
			t.Fatal("missing usage event")
		}
	}
	select {
	case <-observer.seen:
		t.Fatal("terminal emitted twice")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestRequestOutcomeTerminalEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, want, stage string
		mark              func(context.Context)
		cancel            bool
	}{
		{"provider success without client terminal", OutcomeUnknown, "", nil, false},
		{"retry exhausted", OutcomeFailed, FailureBeforeOutput, MarkRequestFailed, false},
		{"midstream failure", OutcomeFailed, FailureAfterOutput, func(ctx context.Context) { MarkRequestOutput(ctx); MarkRequestFailed(ctx) }, false},
		{"client cancellation", OutcomeClientCanceled, FailureBeforeOutput, nil, true},
		{"explicit cancel", OutcomeClientCanceled, FailureBeforeOutput, MarkRequestCanceled, false},
		{"delivery failure then cancel", OutcomeClientDeliveryFailed, FailureBeforeOutput, func(ctx context.Context) { MarkRequestDeliveryFailed(ctx); MarkRequestCanceled(ctx) }, true},
		{"cancel then delivery failure", OutcomeClientDeliveryFailed, FailureBeforeOutput, func(ctx context.Context) { MarkRequestCanceled(ctx); MarkRequestDeliveryFailed(ctx) }, true},
		{"client write failure", OutcomeClientDeliveryFailed, FailureAfterOutput, func(ctx context.Context) {
			MarkRequestOutput(ctx)
			MarkRequestDeliveryFailed(ctx)
			MarkRequestFailed(ctx)
			MarkRequestSucceeded(ctx)
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := defaultManager
			defaultManager = NewManager(4)
			observer := &observingPlugin{seen: make(chan Record, 4)}
			defaultManager.Register(observer)
			t.Cleanup(func() { defaultManager.Stop(); defaultManager.Wait(context.Background()); defaultManager = previous })
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := BeginRequest(base)
			PublishRecord(ctx, Record{Model: "model"})
			if tc.mark != nil {
				tc.mark(ctx)
			}
			if tc.cancel {
				cancel()
			}
			FinishRequest(ctx)
			<-observer.seen
			select {
			case record := <-observer.seen:
				if record.RequestOutcome == nil || record.RequestOutcome.Result != tc.want || record.RequestOutcome.FailureStage != tc.stage {
					t.Fatalf("outcome = %+v", record.RequestOutcome)
				}
			case <-time.After(time.Second):
				t.Fatal("missing terminal event")
			}
		})
	}
}

func TestRequestOutcomeIdleTrackerDoesNotFabricateRequest(t *testing.T) {
	ctx := BeginRequest(context.Background())
	FinishRequest(ctx)
	if trackerFromContext(ctx).finished {
		t.Fatal("an unused tracker must not be published")
	}
}

func TestRequestOutcomeSurvivesAlternateExecutionParentAndIsolatesWebsocketTurns(t *testing.T) {
	outer := BeginRequest(context.Background())
	alternate := WithRequestTracker(context.Background(), outer)
	if RequestIdentity(alternate) != RequestIdentity(outer) {
		t.Fatal("alternate parent lost tracker")
	}
	turn := BeginRequest(outer)
	if RequestIdentity(turn) == RequestIdentity(outer) {
		t.Fatal("WebSocket turns share handshake identity")
	}
	if RequestIdentity(WithRequestTracker(turn, outer)) != RequestIdentity(turn) {
		t.Fatal("turn tracker was overwritten")
	}
	ObserveRequestIdentity(turn, Record{APIKey: "key", Model: "model", InputContent: "secret-prompt"})
	tracker := trackerFromContext(turn)
	if tracker.identity.APIKey != "key" || tracker.identity.InputContent != "" || tracker.attempts != 0 {
		t.Fatal("identity observation manufactured a billing attempt or retained body")
	}
}
