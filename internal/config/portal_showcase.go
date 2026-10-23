package config

// PortalShowcaseModel is an operator-curated public display item. It grants no
// model access and contains no upstream endpoints or credentials.
type PortalShowcaseModel struct {
	Name string `yaml:"name" json:"name"`
	Icon string `yaml:"icon" json:"icon"`
}
