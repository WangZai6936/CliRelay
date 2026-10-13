package executor

import (
	"bytes"

	"github.com/tidwall/gjson"
)

const codexStreamStartupBufferLimit = 1024 * 1024

// Startup notifications and reasoning headers have no visible model output.
// Holding them until the next usable event keeps a load-shed or disconnect
// eligible for the handler's bounded bootstrap retry. Successful streams retain
// their original ordering, including opaque encrypted reasoning content.
type codexStreamBootstrap struct {
	started bool
	bytes   int
	pending [][]byte
}

func (b *codexStreamBootstrap) prepare(line []byte, translated []string) [][]byte {
	chunks := make([][]byte, 0, len(translated))
	added := 0
	for _, payload := range translated {
		if payload != "" {
			chunks = append(chunks, []byte(payload))
			added += len(payload)
		}
	}
	if b.started {
		return chunks
	}
	// Translators can discard upstream events, including reasoning items in
	// Chat Completions. Those events must not flush a buffered assistant role:
	// there is still no downstream payload that would prevent a safe retry.
	if len(chunks) == 0 {
		return nil
	}
	if isCodexStreamStartupLine(line) && b.bytes+added <= codexStreamStartupBufferLimit {
		b.pending = append(b.pending, chunks...)
		b.bytes += added
		return nil
	}
	// Unknown events and the buffer limit retain the previous streaming behavior
	// instead of buffering arbitrary upstream data indefinitely.
	b.started = true
	ready := append(b.pending, chunks...)
	b.pending = nil
	b.bytes = 0
	return ready
}

func isCodexStreamStartupLine(line []byte) bool {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || bytes.HasPrefix(line, []byte(":")) || bytes.HasPrefix(line, []byte("event:")) || bytes.HasPrefix(line, []byte("id:")) || bytes.HasPrefix(line, []byte("retry:")) {
		return true
	}
	if !bytes.HasPrefix(line, dataTag) {
		return false
	}
	data := bytes.TrimSpace(line[len(dataTag):])
	switch gjson.GetBytes(data, "type").String() {
	case "response.created", "response.in_progress", "keepalive":
		return true
	case "response.output_item.added":
		item := gjson.GetBytes(data, "item")
		return item.Get("type").String() == "reasoning" &&
			isEmptyCodexReasoningField(item.Get("summary")) &&
			isEmptyCodexReasoningField(item.Get("content"))
	default:
		return false
	}
}

func isEmptyCodexReasoningField(value gjson.Result) bool {
	return !value.Exists() || value.Type == gjson.Null || (value.IsArray() && len(value.Array()) == 0)
}
