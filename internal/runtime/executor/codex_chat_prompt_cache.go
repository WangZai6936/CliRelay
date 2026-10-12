package executor

import sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"

// Small third-party chat requests showed no consistent latency benefit from
// changing cache routing. Limit the automatic fallback to larger bodies; an
// explicit client cache key is handled before this guard. Native Responses
// retains its existing behavior. Bytes are a cheap size proxy, not token count.
const codexChatSyntheticCacheMinPayloadBytes = 256 * 1024

func shouldSynthesizeCodexPromptCache(from sdktranslator.Format, payload []byte) bool {
 return from != sdktranslator.FormatOpenAI || len(payload) >= codexChatSyntheticCacheMinPayloadBytes
}
