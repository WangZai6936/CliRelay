package handlers

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/requestctx"
)

type streamStartupResult struct {
	data    <-chan []byte
	headers http.Header
	errs    <-chan *StreamErrorMessage
	err     *StreamErrorMessage
}

type streamStartupDeliveryError struct{ error }

// IsStreamStartupDeliveryError distinguishes a failed heartbeat write from a
// provider's own HTTP 499, which still requires a normal API error response.
func IsStreamStartupDeliveryError(err error) bool {
	var deliveryErr *streamStartupDeliveryError
	return errors.As(err, &deliveryErr)
}

// WriteStreamKeepAlive marks transport-only writes while preserving the
// previous flag, including when a response writer panics.
func WriteStreamKeepAlive(c *gin.Context, write func()) {
	previous, exists := c.Get(requestctx.GinKeyTransportHeartbeat)
	c.Set(requestctx.GinKeyTransportHeartbeat, true)
	defer func() {
		if !exists {
			previous = false
		}
		c.Set(requestctx.GinKeyTransportHeartbeat, previous)
	}()
	write()
}

// StartStreamWithKeepAlive keeps synchronous provider initialization alive
// while the HTTP owner alone writes the response. Heartbeats are transport
// activity, so they neither consume recovery attempts nor lock output replay.
func (h *BaseAPIHandler) StartStreamWithKeepAlive(c *gin.Context, ctx context.Context, handlerType, modelName string, rawJSON []byte, alt string) (<-chan []byte, http.Header, <-chan *StreamErrorMessage, *StreamErrorMessage) {
	interval := StreamingKeepAliveInterval(h.Cfg)
	c.Set(requestctx.GinKeyAsyncResponseStatus, c.Writer.Status())
	if interval <= 0 {
		data, headers, errs, err := h.StartStreamWithAuthManager(ctx, handlerType, modelName, rawJSON, alt)
		ApplyRequestModelResolutionHeaders(c)
		if err == nil {
			c.Set(requestctx.GinKeyAsyncResponseStatus, http.StatusOK)
		}
		return data, headers, errs, err
	}
	workerCtx, cancelWorker := context.WithCancelCause(ctx)
	started := make(chan streamStartupResult, 1)
	pending, handedOff := true, false
	defer func() {
		if !handedOff {
			cancelWorker(context.Canceled)
			if pending {
				// This also covers panics in the HTTP owner's writer/flush.
				<-started
			}
		}
	}()
	go func() {
		var result streamStartupResult
		defer func() {
			// Gin's recovery middleware cannot recover a worker panic.
			if recover() != nil {
				result = streamStartupResult{err: &StreamErrorMessage{StatusCode: http.StatusInternalServerError, Error: errors.New("stream initialization failed")}}
			}
			started <- result
		}()
		result.data, result.headers, result.errs, result.err = h.StartStreamWithAuthManager(workerCtx, handlerType, modelName, rawJSON, alt)
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			cancelWorker(ctx.Err())
			// Do not return the original Gin context to its pool while startup
			// still uses it. Provider initialization must respect this context.
			return nil, nil, nil, &StreamErrorMessage{StatusCode: 499, Error: ctx.Err()}
		case result := <-started:
			pending = false
			ApplyRequestModelResolutionHeaders(c)
			if result.err != nil {
				cancelWorker(result.err.Error)
			} else {
				// The caller prepares SSE immediately after this return. The
				// worker context remains alive until the caller cancels its parent.
				c.Set(requestctx.GinKeyAsyncResponseStatus, http.StatusOK)
			}
			if result.err != nil && c.Writer.Written() {
				// A heartbeat already committed SSE/200. Let the normal protocol
				// forwarder serialize the terminal error instead of appending JSON.
				data := make(chan []byte)
				close(data)
				errs := make(chan *StreamErrorMessage, 1)
				errs <- result.err
				close(errs)
				return data, nil, errs, nil
			}
			handedOff = result.err == nil
			return result.data, result.headers, result.errs, result.err
		case <-ticker.C:
			if ctx.Err() != nil {
				continue
			}
			if !c.Writer.Written() {
				ApplyRequestModelResolutionHeaders(c)
				PrepareStreamingResponse(c)
				c.Set(requestctx.GinKeyAsyncResponseStatus, http.StatusOK)
			}
			var writeErr error
			WriteStreamKeepAlive(c, func() {
				_, writeErr = c.Writer.Write([]byte(": keep-alive\n\n"))
				if writeErr == nil {
					c.Writer.Flush()
				}
			})
			if writeErr != nil {
				cancelWorker(writeErr)
				// Return only after the worker has stopped touching Gin.
				return nil, nil, nil, &StreamErrorMessage{StatusCode: 499, Error: &streamStartupDeliveryError{writeErr}}
			}
		}
	}
}
