package handlers

import (
	"context"
	"errors"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const RequestModelResolverContextKey = "cliproxy.request_model_resolver"
const RequestModelResolutionContextKey = "cliproxy.model_resolution"

type RequestModelResolution struct {
	RequestedModel string `json:"requested_model"`
	Model          string `json:"resolved_model"`
	Reason         string `json:"reason"`
}

// RequestModelResolver shares the server's HTTP/WebSocket admission policy.
// failureReason is set only for a classified model/route failure before output.
type RequestModelResolver func(model string, payload []byte, failureReason string) RequestModelResolution

func RequestModelResolverFromGin(c *gin.Context) RequestModelResolver {
	if c == nil {
		return nil
	}
	value, _ := c.Get(RequestModelResolverContextKey)
	resolver, _ := value.(RequestModelResolver)
	return resolver
}

// CanResolveRequestModel mirrors execution's provider/group/prefix resolution.
func (h *BaseAPIHandler) CanResolveRequestModel(ctx context.Context, model string) bool {
	_, ok := h.ResolveRequestRoutingModel(ctx, model)
	return ok
}

// ResolveRequestRoutingModel includes auto resolution and preserves the same
// group/model normalization used by execution.
func (h *BaseAPIHandler) ResolveRequestRoutingModel(ctx context.Context, model string) (string, bool) {
	if h == nil {
		return "", false
	}
	_, normalized, err := h.getRequestDetails(ctx, model)
	return normalized, err == nil
}

func RememberRequestModelResolution(c *gin.Context, resolution RequestModelResolution) {
	if c == nil || resolution.Reason == "" || resolution.Model == resolution.RequestedModel {
		return
	}
	if previous, ok := c.Get(RequestModelResolutionContextKey); ok {
		if first, ok := previous.(RequestModelResolution); ok && first.RequestedModel != "" {
			resolution.RequestedModel = first.RequestedModel
		}
	}
	c.Set(RequestModelResolutionContextKey, resolution)
	if c.Writer != nil && !c.Writer.Written() {
		c.Header("X-Clirelay-Resolved-Model", resolution.Model)
		c.Header("X-Clirelay-Model-Fallback", resolution.Reason)
	}
}

func textModelFailureReason(err error) string {
	if err == nil {
		return ""
	}
	var authErr *coreauth.Error
	if errors.As(err, &authErr) && authErr != nil {
		if (authErr.HTTPStatus == 0 || authErr.HTTPStatus == 503) && (authErr.Code == "auth_not_found" || authErr.Code == "auth_unavailable") {
			return "no_route"
		}
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || (status.StatusCode() != 400 && status.StatusCode() != 404) {
		return ""
	}
	var withBody interface{ UpstreamErrorBody() []byte }
	var payload []byte
	if errors.As(err, &withBody) {
		payload = withBody.UpstreamErrorBody()
	} else {
		payload = []byte(err.Error())
	}
	for _, path := range []string{"error.code", "code"} {
		switch gjson.GetBytes(payload, path).String() {
		case "model_not_found", "model_disabled":
			return "model_unavailable"
		}
	}
	return ""
}

// fallbackExecution runs once on a synchronous failure before any output.
// Ordinary 403/429/5xx, invalid requests and established streams are excluded.
func (h *BaseAPIHandler) fallbackExecution(ctx context.Context, req coreexecutor.Request, opts coreexecutor.Options, err error) ([]string, coreexecutor.Request, coreexecutor.Options, bool) {
	if ctx == nil || ctx.Err() != nil || opts.Alt != "" {
		return nil, req, opts, false
	}
	reason := textModelFailureReason(err)
	if reason == "" {
		return nil, req, opts, false
	}
	c, _ := ctx.Value(requestctx.ContextKeyGin).(*gin.Context)
	resolver := RequestModelResolverFromGin(c)
	if resolver == nil {
		return nil, req, opts, false
	}
	resolution := resolver(req.Model, req.Payload, reason)
	if resolution.Model == "" || resolution.Model == req.Model {
		return nil, req, opts, false
	}
	providers, model, rejection := h.getRequestDetails(ctx, resolution.Model)
	if rejection != nil {
		return nil, req, opts, false
	}
	body, mutationErr := sjson.SetBytes(req.Payload, "model", model)
	if mutationErr != nil {
		return nil, req, opts, false
	}
	req.Model, req.Payload = model, body
	opts.OriginalRequest = body
	if opts.Metadata != nil {
		metadata := make(map[string]any, len(opts.Metadata))
		for key, value := range opts.Metadata {
			metadata[key] = value
		}
		metadata[coreexecutor.RequestedModelMetadataKey] = model
		delete(metadata, coreexecutor.PinnedAuthMetadataKey)
		opts.Metadata = metadata
	}
	RememberRequestModelResolution(c, resolution)
	return providers, req, opts, true
}
