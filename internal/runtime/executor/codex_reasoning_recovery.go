package executor

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/http"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

const codexErrorPrefetchLimit = 256 << 10

type replayResponseBody struct {
	io.Reader
	closer io.Closer
}

func (b *replayResponseBody) Close() error { return b.closer.Close() }

type terminalReadError struct{ err error }

func (r *terminalReadError) Read([]byte) (int, error) {
	if r.err == nil {
		return 0, io.EOF
	}
	err := r.err
	r.err = nil
	return 0, err
}

// peekCodexEncryptedError examines only the first SSE data event. Normal streams
// retain every byte and are not buffered until generation finishes. A read error
// consumed while peeking is replayed as well, so HTTP/2 failures are not hidden.
func peekCodexEncryptedError(response *http.Response) []byte {
	if response == nil || response.Body == nil {
		return nil
	}
	if response.StatusCode == http.StatusBadRequest {
		original := response.Body
		body, err := io.ReadAll(io.LimitReader(original, codexErrorPrefetchLimit+1))
		response.Body = &replayResponseBody{Reader: io.MultiReader(bytes.NewReader(body), original, &terminalReadError{err: err}), closer: original}
		if len(body) <= codexErrorPrefetchLimit && isInvalidEncryptedContent(body) {
			return body
		}
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return nil
	}
	original := response.Body
	reader := bufio.NewReaderSize(original, 16<<10)
	var prefix bytes.Buffer
	var consumedError error
	var payload []byte
	for prefix.Len() < codexErrorPrefetchLimit {
		line, err := reader.ReadSlice('\n')
		prefix.Write(line)
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			consumedError = err
		}
		// A line can span several ReadSlice calls. Inspect the complete last line.
		captured := prefix.Bytes()
		end := len(captured)
		if end > 0 && captured[end-1] == '\n' {
			end--
		}
		start := bytes.LastIndexByte(captured[:end], '\n') + 1
		completeLine := bytes.TrimSpace(captured[start:end])
		if bytes.HasPrefix(completeLine, dataTag) {
			candidate := bytes.TrimSpace(completeLine[len(dataTag):])
			if isInvalidEncryptedContent(candidate) {
				payload = bytes.Clone(candidate)
			}
			break
		}
		if err != nil || bytes.HasPrefix(completeLine, []byte(":")) {
			break
		}
	}
	response.Body = &replayResponseBody{
		Reader: io.MultiReader(bytes.NewReader(prefix.Bytes()), reader, &terminalReadError{err: consumedError}),
		closer: original,
	}
	return payload
}

func isInvalidEncryptedContent(payload []byte) bool {
	for _, path := range []string{"error.code", "response.error.code", "code"} {
		if gjson.GetBytes(payload, path).String() == "invalid_encrypted_content" {
			return true
		}
	}
	return false
}

// doCodexRequestWithReasoningRecovery handles legacy/unrecognised reasoning
// state once, only when the upstream explicitly rejects it before generation.
// Valid native state is untouched. Tool calls, results and visible messages are
// preserved, and a partially generated stream is never replayed.
func doCodexRequestWithReasoningRecovery(ctx context.Context, client *http.Client, request *http.Request, body []byte, recorder UpstreamRecorder, reporters ...*usageReporter) (*http.Response, []byte, error) {
	response, err := client.Do(request)
	if err != nil {
		return nil, body, err
	}
	invalid := peekCodexEncryptedError(response)
	if len(invalid) == 0 {
		return response, body, nil
	}
	repaired := filterEncryptedReasoning(body, func(string) bool { return true })
	if samePayloadStorage(repaired, body) || ctx.Err() != nil {
		return response, body, nil
	}
	if errBudget := cliproxyauth.ReserveAdditionalRecoveryAttempt(ctx, statusErr{code: http.StatusBadRequest, msg: "invalid_encrypted_content"}); errBudget != nil {
		// The original response still owns the rejected error. Do not discard it
		// or report another provider attempt when the shared budget is exhausted.
		return response, body, nil
	}
	// Preserve the rejected attempt in the diagnostic log, without reporting it
	// as the final outcome of this executor call.
	recorder.RecordResponseMetadata(response.StatusCode, response.Header.Clone())
	recorder.AppendResponseChunk(invalid)
	_ = response.Body.Close()
	count := 0
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("type").String() == "reasoning" && item.Get("encrypted_content").String() != "" {
			count++
		}
	}
	logWithRequestID(ctx).Infof("codex: retrying rejected encrypted reasoning once with %d reasoning items removed", count)
	retry := request.Clone(ctx)
	retry.Body = io.NopCloser(bytes.NewReader(repaired))
	retry.ContentLength = int64(len(repaired))
	retry.Header.Del("Content-Length")
	retry.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(repaired)), nil }
	recorder.RecordRequest(retry.URL.String(), retry.Method, retry.Header.Clone(), repaired)
	if len(reporters) > 0 && reporters[0] != nil {
		reporters[0].providerAttempts.Store(2)
	}
	response, err = client.Do(retry)
	if err != nil {
		return nil, repaired, err
	}
	if response.Header == nil {
		response.Header = make(http.Header)
	}
	response.Header.Set("X-Clirelay-Reasoning-Repaired", "1")
	return response, repaired, nil
}

func sendExecutorStreamChunk(ctx context.Context, out chan<- cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- chunk:
		return true
	}
}
