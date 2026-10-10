package api

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"
	modelconfigsettings "github.com/router-for-me/CLIProxyAPI/v6/internal/management/settings/modelconfig"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	internalrouting "github.com/router-for-me/CLIProxyAPI/v6/internal/routing"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/tidwall/gjson"
)

func isTextGenerationPath(path string) bool {
	for _, suffix := range []string{"/responses", "/chat/completions", "/completions", "/messages"} {
		if strings.HasSuffix(strings.TrimRight(path, "/"), suffix) {
			return true
		}
	}
	return false
}

func hasTextModality(modalities []string) bool {
	if len(modalities) == 0 {
		return true
	}
	for _, kind := range modalities {
		if strings.EqualFold(strings.TrimSpace(kind), "text") {
			return true
		}
	}
	return false
}

// Other modalities need their own capability policy. Tool history is text.
func isTextOnlyModelPayload(payload []byte) bool {
	if !gjson.ValidBytes(payload) || gjson.GetBytes(payload, "model").Type != gjson.String {
		return false
	}
	for _, modality := range gjson.GetBytes(payload, "modalities").Array() {
		if modality.String() != "text" {
			return false
		}
	}
	for _, tool := range gjson.GetBytes(payload, "tools").Array() {
		switch tool.Get("type").String() {
		case "image_generation", "audio_generation", "video_generation":
			return false
		}
	}
	var textOnly func(gjson.Result) bool
	textOnly = func(value gjson.Result) bool {
		if value.IsArray() {
			for _, item := range value.Array() {
				if !textOnly(item) {
					return false
				}
			}
			return true
		}
		if !value.IsObject() {
			return true
		}
		ok := true
		value.ForEach(func(key, item gjson.Result) bool {
			switch key.String() {
			case "type":
				switch item.String() {
				case "image", "image_url", "input_image", "input_audio", "audio", "video", "video_url", "file", "input_file":
					ok = false
				}
			case "image_url", "input_audio", "video_url", "file":
				ok = false
			case "input", "messages", "content":
				ok = textOnly(item)
			}
			return ok
		})
		return ok
	}
	return textOnly(gjson.ParseBytes(payload))
}

func (s *Server) requestModelResolver(c *gin.Context, gate handlers.ModelGate) handlers.RequestModelResolver {
	tenantID := requestTenantID(c)
	path := ""
	ctx := context.Background()
	if c.Request != nil {
		path = c.Request.URL.Path
		ctx = c.Request.Context()
	}
	ctx = context.WithValue(ctx, util.ContextKeyGin, c)
	route := pathRouteContextFromGin(c)
	routeGroup := ""
	if route != nil {
		routeGroup = route.Group
	}
	groups := allowedChannelGroupsFromAccessMetadata(c)
	var channels map[string]struct{}
	if raw, ok := c.Get("accessMetadata"); ok {
		if metadata, ok := raw.(map[string]string); ok {
			channels = internalrouting.ParseNormalizedSet(metadata["allowed-channels"], func(v string) string { return strings.ToLower(strings.TrimSpace(v)) })
		}
	}
	canExecute := func(model string) bool {
		if s.handlers == nil || s.handlers.AuthManager == nil {
			return false
		}
		normalized, ok := s.handlers.ResolveRequestRoutingModel(ctx, model)
		return ok && s.handlers.AuthManager.CanServeModelWithScopesForTenant(tenantID, thinking.ParseSuffix(normalized).ModelName, channels, groups, routeGroup)
	}
	return func(original string, payload []byte, failureReason string) handlers.RequestModelResolution {
		result := handlers.RequestModelResolution{RequestedModel: original, Model: strings.TrimSpace(original)}
		if s.cfg == nil || result.Model == "" || len(result.Model) > 256 || strings.ContainsAny(result.Model, "\r\n\x00") {
			return result
		}
		if alias := s.cfg.ResolveRequestModelAlias(result.Model); alias != result.Model {
			result.Model, result.Reason = alias, "alias"
		}
		target := s.cfg.ResolveRequestModelAlias(s.cfg.TextModelFallback)
		if target == "" || target == result.Model || s.handlers == nil || s.handlers.AuthManager == nil || !isTextGenerationPath(path) {
			return result
		}
		var rejectionCode string
		if gate != nil {
			if rejection := gate(result.Model); rejection != nil {
				if detail, ok := rejection.Error.(*modelGateError); ok {
					rejectionCode = detail.code
				}
			}
		}
		resolvable := s.handlers.CanResolveRequestModel(ctx, result.Model)
		reason := failureReason
		if reason == "" {
			if rejectionCode == "model_not_found" {
				reason = "model_disabled"
			} else if !resolvable {
				reason = "model_unresolved"
			} else if !canExecute(result.Model) {
				reason = "no_route"
			}
		}
		if reason == "" || (rejectionCode == "model_not_allowed" && resolvable) {
			return result
		}
		if !isTextOnlyModelPayload(payload) {
			return result
		}
		base := thinking.ParseSuffix(result.Model).ModelName
		if registry.IsImageGenerationModel(base) {
			return result
		}
		row, knownByCatalog := modelconfigsettings.GetConfigForTenant(tenantID, base)
		if knownByCatalog && !hasTextModality(row.OutputModalities) {
			return result
		}
		// Preserve a configured route group, while allowing provider namespaces
		// to change when switching providers. Never override a conflicting group.
		if slash := strings.IndexByte(base, '/'); slash >= 0 {
			prefix := base[:slash]
			if routeGroup != "" && prefix != routeGroup && !internalrouting.IsCcSwitchMappedTargetModel(route, base) {
				return result
			}
			if _, configured := resolvePathRouteContext(s.cfg, s.handlers.AuthManager, tenantID, prefix); configured {
				if strings.Contains(target, "/") {
					if !strings.HasPrefix(target, prefix+"/") {
						return result
					}
				} else {
					target = prefix + "/" + target
				}
			}
		}
		// A known model denied by authorization stays denied. An unresolved name
		// can resolve to the explicitly authorized target, as legacy aliases do.
		known := knownByCatalog || registry.GetGlobalRegistry().GetModelInfo(base, "") != nil || s.handlers.CanResolveRequestModel(ctx, result.Model)
		if rejectionCode == "model_not_allowed" && known {
			return result
		}
		if gate != nil && gate(target) != nil {
			return result
		}
		if targetInfo, exists := modelconfigsettings.GetConfigForTenant(tenantID, thinking.ParseSuffix(target).ModelName); exists && (!targetInfo.Enabled || !hasTextModality(targetInfo.InputModalities) || !hasTextModality(targetInfo.OutputModalities)) {
			return result
		}
		if registry.IsImageGenerationModel(target) || !canExecute(target) {
			return result
		}
		result.Model, result.Reason = target, reason
		return result
	}
}
