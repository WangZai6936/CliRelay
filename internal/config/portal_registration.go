package config

// PortalRegistrationConfig assigns public signups to an operator-selected tenant
// and permission profile. These values are never accepted from the signup form.
type PortalRegistrationConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
	// TenantID defaults to the system tenant when empty.
	TenantID string `yaml:"tenant-id,omitempty" json:"tenant-id,omitempty"`
	// PermissionProfileID optionally applies the tenant's existing quota/model policy.
	PermissionProfileID string `yaml:"permission-profile-id,omitempty" json:"permission-profile-id,omitempty"`
}
