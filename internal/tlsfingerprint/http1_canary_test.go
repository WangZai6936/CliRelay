package tlsfingerprint

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

func TestForcedHTTP1PoolsConnectionsAndClosesIdle(t *testing.T) {
	var handshakes atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" {
			t.Errorf("received %s", r.Proto)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		handshakes.Add(1)
		for _, protocol := range hello.SupportedProtos {
			if protocol == "h2" {
				t.Error("HTTP1 canary still advertises h2")
			}
		}
		return nil, nil
	}}
	server.StartTLS()
	defer server.Close()
	rt, err := New(Options{Profile: ProfileChrome, ForceHTTP1: true, InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.CloseIdleConnections()
	request := func() {
		t.Helper()
		req, _ := http.NewRequest("GET", server.URL, nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	request()
	request()
	if handshakes.Load() != 1 {
		t.Fatal("HTTP1 opens one connection per request")
	}
	rt.CloseIdleConnections()
	request()
	if handshakes.Load() != 2 {
		t.Fatal("CloseIdleConnections did not release the HTTP1 pool")
	}
}

func TestForcedHTTP1ConcurrentPool(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" {
			t.Errorf("unexpected protocol %s", r.Proto)
		}
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	rt, err := New(Options{Profile: ProfileChrome, ForceHTTP1: true, InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.CloseIdleConnections()
	var workers sync.WaitGroup
	errors := make(chan error, 32)
	for i := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			path := fmt.Sprintf("/request-%d", i)
			req, _ := http.NewRequest("GET", server.URL+path, nil)
			resp, err := rt.RoundTrip(req)
			if err != nil {
				errors <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				errors <- err
				return
			}
			if string(body) != path {
				errors <- fmt.Errorf("response crossed requests: got %q want %q", body, path)
			}
		}()
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
