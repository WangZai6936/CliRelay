package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/identity"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRequestBufferLimitCanRotateWithoutInvalidatingRequest(t *testing.T) {
	known := &Error{HTTPStatus: http.StatusInsufficientStorage, Message: "exceeded request buffer limit while retrying upstream"}
	if isRequestInvalidError(known) {
		t.Fatal("buffer limit must permit a bounded alternative")
	}
	other := &Error{HTTPStatus: http.StatusInsufficientStorage, Message: "temporary upstream storage unavailable"}
	if isRequestInvalidError(other) {
		t.Fatal("unrelated 507 must keep ordinary upstream recovery")
	}
}

type bufferTestExecutor struct {
	*blockingExecutor
	calls       []string
	allFail     bool
	secondError error
}

func (e *bufferTestExecutor) next(a *Auth) error {
	e.calls = append(e.calls, a.ID)
	if len(e.calls) > 1 && e.secondError != nil {
		return e.secondError
	}
	if len(e.calls) == 1 || e.allFail {
		return &Error{HTTPStatus: 507, Message: "exceeded request buffer limit while retrying upstream"}
	}
	return nil
}
func (e *bufferTestExecutor) Execute(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if err := e.next(a); err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}
func (e *bufferTestExecutor) ExecuteStream(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if err := e.next(a); err != nil {
		return nil, err
	}
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("ok")}
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}
func bufferTestManager(t *testing.T, e *bufferTestExecutor, auths ...*Auth) *Manager {
	t.Helper()
	m := NewManager(nil, &FillFirstSelector{}, nil)
	m.SetRetryConfig(10, time.Minute)
	m.RegisterExecutor(e)
	if len(auths) == 0 {
		for _, id := range []string{"a", "b", "c"} {
			auths = append(auths, &Auth{ID: id, Provider: "blocking", Status: StatusActive})
		}
	}
	for _, a := range auths {
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	return m
}
func TestBufferRecoverySharesOneAlternativeAcrossManagerInvocations(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, fail := range []bool{false, true} {
			for _, cap := range []int{0, 1, 4} {
				t.Run(fmt.Sprintf("stream=%v/fail=%v/cap=%d", stream, fail, cap), func(t *testing.T) {
					e := &bufferTestExecutor{blockingExecutor: &blockingExecutor{}, allFail: fail}
					m := bufferTestManager(t, e)
					ctx := WithRequestBufferRecovery(context.Background())
					if cap > 0 {
						ctx = WithRequestRecoveryBudget(ctx, cap, time.Second)
					}
					req := cliproxyexecutor.Request{Model: "test-model"}
					var err error
					if stream {
						var r *cliproxyexecutor.StreamResult
						r, err = m.ExecuteStream(ctx, []string{"blocking"}, req, cliproxyexecutor.Options{})
						if r != nil {
							for range r.Chunks {
							}
						}
					} else {
						_, err = m.Execute(ctx, []string{"blocking"}, req, cliproxyexecutor.Options{})
					}
					want := []string{"a", "b"}
					if cap == 1 {
						want = []string{"a"}
					}
					if len(e.calls) != len(want) || e.calls[0] != "a" || (len(e.calls) == 2 && e.calls[1] == "a") {
						t.Fatalf("calls=%v want=%v", e.calls, want)
					}
					want = append([]string(nil), e.calls...)
					if (fail || cap == 1) != (err != nil) {
						t.Fatalf("error=%v", err)
					}
					if err != nil {
						_, _ = m.ExecuteStream(ctx, []string{"blocking"}, req, cliproxyexecutor.Options{})
						if !reflect.DeepEqual(e.calls, want) {
							t.Fatalf("bootstrap escaped shared cap: %v", e.calls)
						}
					}
					a, _ := m.GetByID("a")
					if a.Unavailable || !a.NextRetryAfter.IsZero() || a.LastError != nil {
						t.Fatalf("buffer error damaged account health: %+v", a)
					}
				})
			}
		}
	}
}
func TestBufferRecoveryRetainsAlternativeFailure(t *testing.T) {
	e := &bufferTestExecutor{blockingExecutor: &blockingExecutor{}, secondError: &Error{HTTPStatus: 503, Code: "temporarily_unavailable", Message: "maintenance"}}
	m := bufferTestManager(t, e)
	_, err := m.Execute(context.Background(), []string{"blocking"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if statusCodeFromError(err) != 503 || len(e.calls) != 2 {
		t.Fatalf("calls=%v err=%v", e.calls, err)
	}
}
func TestBufferRecoveryPreservesPinnedAndSinglePickRouting(t *testing.T) {
	for _, meta := range []map[string]any{{cliproxyexecutor.PinnedAuthMetadataKey: "a"}, {cliproxyexecutor.SinglePickMetadataKey: true}} {
		e := &bufferTestExecutor{blockingExecutor: &blockingExecutor{}}
		m := bufferTestManager(t, e)
		_, err := m.Execute(context.Background(), []string{"blocking"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{Metadata: meta})
		if err == nil || !reflect.DeepEqual(e.calls, []string{"a"}) {
			t.Fatalf("routing escaped: calls=%v err=%v", e.calls, err)
		}
	}
}
func TestBufferRecoveryCannotCrossTenantScope(t *testing.T) {
	e := &bufferTestExecutor{blockingExecutor: &blockingExecutor{}}
	m := bufferTestManager(t, e,
		&Auth{ID: "a", Provider: "blocking", Status: StatusActive, TenantID: identity.SystemTenantID},
		&Auth{ID: "b", Provider: "blocking", Status: StatusActive, TenantID: "22222222-2222-2222-2222-222222222222"})
	_, err := m.Execute(context.Background(), []string{"blocking"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if err == nil || !reflect.DeepEqual(e.calls, []string{"a"}) {
		t.Fatalf("tenant escaped: calls=%v err=%v", e.calls, err)
	}
}
func TestBufferRecoveryDeadlineAndCancellation(t *testing.T) {
	ctx := WithRequestBufferRecovery(context.Background())
	s := bufferRecoveryFrom(ctx)
	err := &Error{HTTPStatus: 507, Message: "exceeded request buffer limit while retrying upstream"}
	s.recordFailure("a", err)
	s.mu.Lock()
	s.deadline = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if s.available(ctx) || s.consume(ctx, "b") != err {
		t.Fatal("expired recovery made another call")
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if !errors.Is(s.failure(ctx), context.Canceled) {
		t.Fatal("client cancellation lost")
	}
}

func TestBufferRecoveryCannotCrossChannelScope(t *testing.T) {
	e := &bufferTestExecutor{blockingExecutor: &blockingExecutor{}}
	m := bufferTestManager(t, e,
		&Auth{ID: "a", Provider: "blocking", Status: StatusActive, Label: "allowed"},
		&Auth{ID: "b", Provider: "blocking", Status: StatusActive, Label: "blocked"})
	_, err := m.Execute(context.Background(), []string{"blocking"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{Metadata: map[string]any{"allowed-channels": "allowed"}})
	if err == nil || !reflect.DeepEqual(e.calls, []string{"a"}) {
		t.Fatalf("channel escaped: calls=%v err=%v", e.calls, err)
	}
}
