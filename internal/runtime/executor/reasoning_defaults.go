package executor

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// applyConfiguredReasoningDefault applies administrator defaults before thinking
// validation. Translators may synthesize medium even when a client omitted effort;
// that synthetic value must not shadow a configured default. Explicit request
// settings and model suffixes retain priority. Without a matching rule, behavior
// is unchanged, including the translator's existing fallback.
func (ec *ExecutionContext) applyConfiguredReasoningDefault(translated, source []byte) []byte {
	if ec == nil || ec.Config == nil || len(translated) == 0 ||
		ec.Execution.TargetFormat.String() != "codex" ||
		(ec.Provider != "codex" && ec.Provider != "xai") {
		return translated
	}
	if thinking.ParseSuffix(ec.Request.Model).HasSuffix ||
		thinking.ParseSuffix(ec.RequestedModel).HasSuffix {
		return translated
	}
	for _, body := range [][]byte{source, ec.OriginalPayload} {
		if gjson.GetBytes(body, "reasoning_effort").Exists() ||
			gjson.GetBytes(body, "reasoning.effort").Exists() ||
			thinking.ExtractThinkingLevel(body, ec.SourceFormat.String()) != "" {
			return translated
		}
	}
	protocol := strings.TrimSpace(ec.Execution.PayloadConfigFormat)
	if protocol == "" {
		protocol = ec.Execution.TargetFormat.String()
	}
	candidates := payloadModelCandidates(ec.BaseModel, ec.RequestedModel)
	for _, rule := range ec.Config.Payload.Default {
		if !payloadModelRulesMatch(rule.Models, protocol, candidates) {
			continue
		}
		value, ok := rule.Params["reasoning.effort"]
		if !ok {
			continue
		}
		result, err := sjson.SetBytes(translated, "reasoning.effort", value)
		if err != nil {
			return translated
		}
		return result
	}
	return translated
}

// preserveSuffixReasoningForDefaults makes an already applied model suffix
// visible to the missing-field check. Native Responses requests may not contain
// reasoning.effort at all, but their explicit suffix must still beat defaults.
func (ec *ExecutionContext) preserveSuffixReasoningForDefaults(payload, original []byte) []byte {
	if ec == nil || ec.Execution.TargetFormat.String() != "codex" ||
		(ec.Provider != "codex" && ec.Provider != "xai") ||
		(!thinking.ParseSuffix(ec.Request.Model).HasSuffix && !thinking.ParseSuffix(ec.RequestedModel).HasSuffix) {
		return original
	}
	effort := gjson.GetBytes(payload, "reasoning.effort")
	if !effort.Exists() {
		return original
	}
	if len(original) == 0 {
		original = []byte(`{}`)
	}
	result, err := sjson.SetRawBytes(original, "reasoning.effort", []byte(effort.Raw))
	if err != nil {
		return original
	}
	return result
}
