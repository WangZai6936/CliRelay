package config

// CodexHTTPTransportConfig compares a small, stable request cohort using
// HTTP/1.1 with the existing transport. Zero leaves protocol selection unchanged.
// It only applies to Codex HTTP requests; websocket and other providers are
// unaffected. The setting is read when an upstream client is built, so config
// reloads apply to new attempts without replacing live connection pools.
type CodexHTTPTransportConfig struct {
	HTTP1Percent int `yaml:"http1-percent,omitempty" json:"http1-percent,omitempty"`
}

func (c CodexHTTPTransportConfig) CanaryPercent() int {
	if c.HTTP1Percent < 0 {
		return 0
	}
	if c.HTTP1Percent > 100 {
		return 100
	}
	return c.HTTP1Percent
}
