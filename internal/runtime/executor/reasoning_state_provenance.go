package executor

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	reasoningOriginLimit     = 32768
	reasoningOriginTTL       = 7 * 24 * time.Hour
	reasoningOriginFileLimit = 8 << 20
)

// Only hashes and the issuing provider are persisted. Neither prompts, native
// ciphertext nor client credentials belong in this registry.
type reasoningOrigin struct {
	Key      string    `json:"key"`
	Provider string    `json:"provider"`
	At       time.Time `json:"at"`
}

type reasoningOriginRegistry struct {
	mu        sync.Mutex
	path      string
	entries   map[string]reasoningOrigin
	order     *list.List
	positions map[string]*list.Element
}

var reasoningOriginRegistries sync.Map

func reasoningProviderFamily(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "codex", "openai":
		return "openai"
	case "xai", "grok":
		return "xai"
	default:
		return strings.ToLower(strings.TrimSpace(provider))
	}
}

func reasoningOriginScope(ctx context.Context) string {
	key := strings.TrimSpace(apiKeyFromContext(ctx))
	if key == "" {
		return ""
	}
	tenant := strings.TrimSpace(contextStringValue(ctx, util.ContextKeyTrustedTenantID))
	sum := sha256.Sum256([]byte(tenant + "\x00" + key))
	return hex.EncodeToString(sum[:])
}

func reasoningOriginKey(scope, encrypted string) string {
	sum := sha256.Sum256([]byte(encrypted))
	return scope + ":" + hex.EncodeToString(sum[:])
}

func reasoningOrigins(cfg *config.Config) *reasoningOriginRegistry {
	path := ""
	if cfg != nil && strings.TrimSpace(cfg.AuthDir) != "" {
		path = filepath.Join(util.StateDir(cfg.AuthDir), "reasoning-state-provenance.jsonl")
	}
	if current, ok := reasoningOriginRegistries.Load(path); ok {
		return current.(*reasoningOriginRegistry)
	}
	registry := newReasoningOriginRegistry(path)
	actual, _ := reasoningOriginRegistries.LoadOrStore(path, registry)
	return actual.(*reasoningOriginRegistry)
}

func newReasoningOriginRegistry(path string) *reasoningOriginRegistry {
	r := &reasoningOriginRegistry{path: path, entries: make(map[string]reasoningOrigin), order: list.New(), positions: make(map[string]*list.Element)}
	if path == "" {
		return r
	}
	file, err := os.Open(path)
	if err != nil {
		return r
	}
	defer file.Close()
	scanner := bufio.NewScanner(io.LimitReader(file, reasoningOriginFileLimit+1))
	now := time.Now()
	for scanner.Scan() {
		var entry reasoningOrigin
		if json.Unmarshal(scanner.Bytes(), &entry) != nil || len(entry.Key) != 129 || entry.Key[64] != ':' || entry.Provider == "" || now.Sub(entry.At) > reasoningOriginTTL || entry.At.After(now.Add(time.Minute)) {
			continue
		}
		if len(r.entries) >= reasoningOriginLimit {
			r.prune(now)
		}
		r.entries[entry.Key] = entry
		if position := r.positions[entry.Key]; position != nil {
			r.order.MoveToBack(position)
		} else {
			r.positions[entry.Key] = r.order.PushBack(entry.Key)
		}
	}
	return r
}

func (r *reasoningOriginRegistry) prune(now time.Time) {
	for front := r.order.Front(); front != nil; front = r.order.Front() {
		key := front.Value.(string)
		if len(r.entries) < reasoningOriginLimit && now.Sub(r.entries[key].At) <= reasoningOriginTTL {
			return
		}
		delete(r.entries, key)
		delete(r.positions, key)
		r.order.Remove(front)
	}
}

func (r *reasoningOriginRegistry) remember(scope, encrypted, provider string) {
	provider = reasoningProviderFamily(provider)
	if scope == "" || encrypted == "" || provider == "" {
		return
	}
	key := reasoningOriginKey(scope, encrypted)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.entries[key]; ok && time.Since(existing.At) < reasoningOriginTTL {
		return
	}
	now := time.Now().UTC()
	if len(r.entries) >= reasoningOriginLimit {
		r.prune(now)
	}
	entry := reasoningOrigin{Key: key, Provider: provider, At: now}
	r.entries[key] = entry
	if position := r.positions[key]; position != nil {
		r.order.MoveToBack(position)
	} else {
		r.positions[key] = r.order.PushBack(key)
	}
	if r.path == "" {
		return
	}
	if info, err := os.Stat(r.path); err == nil && info.Size() >= reasoningOriginFileLimit {
		r.compact()
		return
	}
	file, err := os.OpenFile(r.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return // Upstream availability must not depend on this optional cache.
	}
	_ = json.NewEncoder(file).Encode(entry)
	_ = file.Close()
}

func (r *reasoningOriginRegistry) compact() {
	file, err := os.CreateTemp(filepath.Dir(r.path), ".reasoning-origin-*")
	if err != nil {
		return
	}
	name := file.Name()
	defer os.Remove(name)
	encoder := json.NewEncoder(file)
	for _, entry := range r.entries {
		if encoder.Encode(entry) != nil {
			_ = file.Close()
			return
		}
	}
	if file.Close() == nil {
		_ = os.Rename(name, r.path)
	}
}

func (r *reasoningOriginRegistry) issuer(scope, encrypted string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := reasoningOriginKey(scope, encrypted)
	entry, ok := r.entries[key]
	if !ok || time.Since(entry.At) > reasoningOriginTTL {
		delete(r.entries, key)
		if position := r.positions[key]; position != nil {
			r.order.Remove(position)
			delete(r.positions, key)
		}
		return ""
	}
	return entry.Provider
}

func observeReasoningOrigins(ctx context.Context, cfg *config.Config, provider string, chunk []byte) {
	if !bytes.Contains(chunk, []byte(`"encrypted_content"`)) {
		return
	}
	scope := reasoningOriginScope(ctx)
	if scope == "" {
		return
	}
	registry := reasoningOrigins(cfg)
	observeItem := func(item gjson.Result) {
		if item.Get("type").String() == "reasoning" {
			registry.remember(scope, item.Get("encrypted_content").String(), provider)
		}
	}
	for _, line := range bytes.Split(chunk, []byte{'\n'}) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, dataTag) {
			line = bytes.TrimSpace(line[len(dataTag):])
		}
		event := gjson.ParseBytes(line)
		if !event.IsObject() {
			continue
		}
		if event.Get("type").String() == "response.output_item.done" {
			observeItem(event.Get("item"))
		}
		output := event.Get("response.output")
		if !output.IsArray() {
			output = event.Get("output")
		}
		for _, item := range output.Array() {
			observeItem(item)
		}
	}
}

// stripForeignReasoningState removes only reasoning issued by a known foreign
// provider. Unknown and same-provider state is preserved byte-for-byte; visible
// messages and function-call/result pairs are never changed.
func stripForeignReasoningState(ctx context.Context, cfg *config.Config, provider string, body []byte) []byte {
	scope := reasoningOriginScope(ctx)
	if scope == "" || !bytes.Contains(body, []byte(`"encrypted_content"`)) {
		return body
	}
	family := reasoningProviderFamily(provider)
	registry := reasoningOrigins(cfg)
	return filterEncryptedReasoning(body, func(encrypted string) bool {
		issuer := registry.issuer(scope, encrypted)
		return issuer != "" && issuer != family
	})
}

func filterEncryptedReasoning(body []byte, remove func(string) bool) []byte {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body
	}
	kept := make([]json.RawMessage, 0, len(input.Array()))
	changed := false
	for _, item := range input.Array() {
		encrypted := item.Get("encrypted_content").String()
		if item.Get("type").String() == "reasoning" && encrypted != "" && remove(encrypted) {
			changed = true
			continue
		}
		kept = append(kept, json.RawMessage(item.Raw))
	}
	if !changed || len(kept) == 0 {
		return body
	}
	raw, err := json.Marshal(kept)
	if err != nil {
		return body
	}
	updated, err := sjson.SetRawBytes(body, "input", raw)
	if err != nil {
		return body
	}
	return updated
}
