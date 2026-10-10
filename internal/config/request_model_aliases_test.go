package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRequestModelAliasesLoadFromServerConfigWithoutClientChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("port: 8317\nrequest-model-aliases:\n  jiezi: gpt-6-luna\n  gpt-6-sol: gpt-6-luna\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ResolveRequestModelAlias("jiezi") != "gpt-6-luna" || cfg.SDKConfig.ResolveRequestModelAlias("gpt-6-sol") != "gpt-6-luna" || cfg.ResolveRequestModelAlias("grok-4.7") != "grok-4.7" {
		t.Fatal("model aliases were not retained by server/SDK config")
	}
}

func TestRequestModelAliasResolutionIsIdempotentForChainsAndCycles(t *testing.T) {
	cfg := &SDKConfig{RequestModelAliases: map[string]string{"legacy": "middle", "middle": "gpt-6-luna"}}
	resolved := cfg.ResolveRequestModelAlias("legacy")
	if resolved != "gpt-6-luna" || cfg.ResolveRequestModelAlias(resolved) != resolved {
		t.Fatal("alias chain made admission and execution disagree")
	}
	cfg.RequestModelAliases = map[string]string{"legacy": "middle", "middle": "legacy"}
	if cfg.ResolveRequestModelAlias("legacy") != "legacy" {
		t.Fatal("cycle should retain the original admission rules")
	}
}
