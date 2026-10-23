package config

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPortalRegistrationConfigIsOptInAndSurvivesYAMLRoundTrip(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(configPath, []byte("port: 8317\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PortalRegistration.Enabled {
		t.Fatal("existing deployments must not silently enable registration")
	}
	want := PortalRegistrationConfig{Enabled: true, TenantID: "00000000-0000-0000-0000-000000000001", PermissionProfileID: "public-signup"}
	cfg.PortalRegistration = want
	data, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err = LoadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PortalRegistration != want {
		t.Fatalf("registration policy after reload: %+v / %v", cfg.PortalRegistration, err)
	}
}
