package config

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestCodexHTTPTransportConfigRoundTrip(t *testing.T) {
	var cfg Config
	if err := yaml.Unmarshal([]byte("codex-http-transport:\n  http1-percent: 5\n"), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.CodexHTTPTransport.CanaryPercent() != 5 {
		t.Fatal("YAML setting lost")
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Config
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip.CodexHTTPTransport.CanaryPercent() != 5 {
		t.Fatal("runtime JSON setting lost")
	}
	for input, want := range map[int]int{-1: 0, 0: 0, 5: 5, 200: 100} {
		if got := (CodexHTTPTransportConfig{HTTP1Percent: input}).CanaryPercent(); got != want {
			t.Fatalf("%d normalized to %d, want %d", input, got, want)
		}
	}
}
