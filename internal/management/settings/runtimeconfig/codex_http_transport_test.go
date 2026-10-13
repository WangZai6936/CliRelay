package runtimeconfig

import (
	"encoding/json"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"testing"
)

func TestCodexHTTPTransportRuntimeSettingAndRollback(t *testing.T) {
	spec, ok := SpecByKey(RuntimeSettingCodexHTTPTransport)
	if !ok {
		t.Fatal("Codex protocol policy is absent from persisted runtime settings")
	}
	cfg := &config.Config{}
	if spec.Meaningful(cfg) {
		t.Fatal("default zero should not be imported as an explicit canary")
	}
	if !spec.Apply(cfg, json.RawMessage(`{"http1-percent":5}`)) || cfg.CodexHTTPTransport.CanaryPercent() != 5 {
		t.Fatal("cannot enable persisted canary")
	}
	if ReloadScopeForKey(RuntimeSettingCodexHTTPTransport) != ReloadRuntime {
		t.Fatal("executor config will not reload")
	}
	if !spec.Apply(cfg, json.RawMessage(`{"http1-percent":0}`)) || cfg.CodexHTTPTransport.CanaryPercent() != 0 {
		t.Fatal("cannot roll back persisted canary to default protocol")
	}
}
