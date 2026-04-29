package app

import (
	"path/filepath"
	"testing"
)

func TestConfigDirForUninstallAllowsDefaultConfigDir(t *testing.T) {
	defaultDir, err := defaultAppConfigDir()
	if err != nil {
		t.Fatalf("default app config dir: %v", err)
	}

	configDir, err := configDirForUninstall(filepath.Join(defaultDir, "config.yaml"))
	if err != nil {
		t.Fatalf("config dir for uninstall: %v", err)
	}
	if !samePath(configDir, defaultDir) {
		t.Fatalf("expected %q, got %q", defaultDir, configDir)
	}
}

func TestConfigDirForUninstallRejectsCustomConfigDir(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")

	_, err := configDirForUninstall(configPath)
	if err == nil {
		t.Fatal("expected custom config directory to be rejected")
	}
}

func TestConfigDirForUninstallRejectsTraversalOutsideDefaultConfigDir(t *testing.T) {
	defaultDir, err := defaultAppConfigDir()
	if err != nil {
		t.Fatalf("default app config dir: %v", err)
	}

	configPath := filepath.Join(defaultDir, "..", "other-app", "config.yaml")
	_, err = configDirForUninstall(configPath)
	if err == nil {
		t.Fatal("expected traversal outside default config directory to be rejected")
	}
}
