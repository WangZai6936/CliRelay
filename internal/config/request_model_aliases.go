package config

import "strings"

// ResolveRequestModelAlias resolves to a terminal model so admission and
// execution remain identical even if the operator chains aliases. Cycles keep
// the original model and its ordinary admission rules.
func (cfg *SDKConfig) ResolveRequestModelAlias(model string) string {
	model = strings.TrimSpace(model)
	if cfg == nil || len(cfg.RequestModelAliases) == 0 {
		return model
	}
	original := model
	for hops := 0; hops <= len(cfg.RequestModelAliases); hops++ {
		target := strings.TrimSpace(cfg.RequestModelAliases[model])
		if target == "" {
			for source, candidate := range cfg.RequestModelAliases {
				if strings.EqualFold(strings.TrimSpace(source), model) {
					target = strings.TrimSpace(candidate)
					break
				}
			}
		}
		if target == "" || target == model {
			return model
		}
		model = target
	}
	return original
}
