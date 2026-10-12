package executor

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestConfiguredReasoningDefaultPreservesClientIntent(t *testing.T) {
	models := []string{"gpt-6-luna", "gpt-6.1-sol", "gpt-6-astra", "grok-4.7-build-fast", "grok-4.7", "grok-composer-2.5-fast", "grok-4.6"}
	rules := make([]config.PayloadModelRule, 0, len(models))
	for _, model := range models {
		rules = append(rules, config.PayloadModelRule{Name: model, Protocol: "codex"})
	}
	cfg := &config.Config{Payload: config.PayloadConfig{Default: []config.PayloadRule{{Models: rules, Params: map[string]any{"reasoning.effort": "low"}}}}}
	for _, model := range models {
		provider := "codex"
		if model[:4] == "grok" {
			provider = "xai"
		}
		t.Run(model, func(t *testing.T) {
			if provider == "xai" {
				info := xaiModelInfoFromPayload(xaiModelPayload{ID: model}, model, 1)
				id := "speed-default-intent-" + model
				registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: info.ID, UserDefined: info.UserDefined}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			for _, test := range []struct{ name, source, payload, suffix, want string }{
				{"omitted chat", "openai", `{"messages":[{"role":"user","content":"hello"}]}`, "", "low"},
				{"explicit high chat", "openai", `{"messages":[{"role":"user","content":"hello"}],"reasoning_effort":"high"}`, "", "high"},
				{"explicit medium chat", "openai", `{"messages":[],"reasoning_effort":"medium"}`, "", "medium"},
				{"explicit none chat", "openai", `{"messages":[],"reasoning_effort":"none"}`, "", "none"},
				{"omitted responses", "openai-response", `{"input":"hello"}`, "", "low"},
				{"explicit high responses", "openai-response", `{"input":"hello","reasoning":{"effort":"high"}}`, "", "high"},
				{"suffix high", "openai", `{"messages":[]}`, "(high)", "medium"},
				{"suffix high responses", "openai-response", `{"input":"hello"}`, "(high)", "high"},
			} {
				t.Run(test.name, func(t *testing.T) {
					payload := []byte(test.payload)
					payload, _ = sjson.SetBytes(payload, "model", model+test.suffix)
					ec := newExecutionContext(context.Background(), provider, cfg, nil, cliproxyexecutor.Request{Model: model + test.suffix, Payload: payload}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString(test.source)}, ExecutionOptions{TargetFormat: sdktranslator.FromString("codex")})
					translated, original := ec.TranslateRequestPair(payload)
					if test.suffix != "" {
						// A suffix is applied by ApplyThinking later; the default must not replace it.
						if gjson.GetBytes(translated, "reasoning.effort").String() == "low" {
							t.Fatal("default replaced suffix intent")
						}
						withSuffix, err := thinking.ApplyThinking(translated, model+test.suffix, test.source, provider, provider)
						if err != nil {
							t.Fatalf("suffix thinking: %v", err)
						}
						configured := ec.ApplyPayloadConfig(withSuffix, original)
						configured = ensureTranslatedCodexModel(configured, ec.BaseModel)
						if got := gjson.GetBytes(configured, "model").String(); got != model {
							t.Fatalf("upstream received local suffix: %q", got)
						}
						if got := gjson.GetBytes(configured, "reasoning.effort").String(); got != "high" {
							t.Fatalf("default replaced applied suffix with %q", got)
						}
					} else if got := gjson.GetBytes(translated, "reasoning.effort").String(); got != test.want {
						t.Fatalf("effort=%q, want %q", got, test.want)
					}
					if test.suffix == "" {
						configured := ec.ApplyPayloadConfig(translated, original)
						if got := gjson.GetBytes(configured, "reasoning.effort").String(); got != test.want {
							t.Fatalf("payload rules replaced client intent: %q", got)
						}
					}
				})
			}
		})
	}
	// An unrelated model retains the translator's medium fallback.
	payload := []byte(`{"messages":[]}`)
	ec := newExecutionContext(context.Background(), "codex", cfg, nil, cliproxyexecutor.Request{Model: "unrelated-model", Payload: payload}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai")}, ExecutionOptions{TargetFormat: sdktranslator.FromString("codex")})
	translated, _ := ec.TranslateRequestPair(payload)
	if got := gjson.GetBytes(translated, "reasoning.effort").String(); got != "medium" {
		t.Fatalf("unrelated model changed: %q", got)
	}
}

func TestLiveGrokReasoningSurvivesUnknownCapabilities(t *testing.T) {
	models, ok := parseXAIModels([]byte(`{"data":[{"id":"grok-4.6"},{"id":"grok-4.7"},{"id":"grok-4.7-build-fast"},{"id":"grok-composer-2.5-fast"}]}`), 1)
	if !ok {
		t.Fatal("catalog parse failed")
	}
	reg := registry.GetGlobalRegistry()
	for _, model := range models {
		id := "speed-regression-" + model.ID
		info := &registry.ModelInfo{ID: model.ID, UserDefined: model.UserDefined}
		if model.Thinking != nil {
			info.Thinking = &registry.ThinkingSupport{Levels: model.Thinking.Levels, ZeroAllowed: model.Thinking.ZeroAllowed, DynamicAllowed: model.Thinking.DynamicAllowed, Min: model.Thinking.Min, Max: model.Thinking.Max}
		}
		reg.RegisterClient(id, "xai", []*registry.ModelInfo{info})
		t.Cleanup(func() { reg.UnregisterClient(id) })
		for _, effort := range []string{"low", "medium", "high"} {
			body := []byte(`{"reasoning":{"effort":"` + effort + `"}}`)
			result, err := thinking.ApplyThinking(body, model.ID, "openai", "xai", "xai")
			if err != nil {
				t.Fatalf("%s %s: %v", model.ID, effort, err)
			}
			if got := gjson.GetBytes(result, "reasoning.effort").String(); got != effort {
				t.Fatalf("%s: effort stripped/changed to %q", model.ID, got)
			}
		}
		result, err := thinking.ApplyThinking([]byte(`{"reasoning":{"effort":"low"}}`), model.ID+"(high)", "openai", "xai", "xai")
		if err != nil || gjson.GetBytes(result, "reasoning.effort").String() != "high" {
			t.Fatalf("suffix did not win for %s: %v", model.ID, err)
		}
	}
	unrelated := xaiModelInfoFromPayload(xaiModelPayload{ID: "unrelated-xai-model"}, "unrelated-xai-model", 1)
	if unrelated.UserDefined {
		t.Fatal("unrelated model capability behavior changed")
	}
}
