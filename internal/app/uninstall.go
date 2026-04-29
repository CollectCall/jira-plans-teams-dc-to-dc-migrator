package app

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func runUninstall(cfg Config) error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating current executable: %w", err)
	}
	exePath, err = filepath.EvalSymlinks(exePath)
	if err != nil {
		exePath = filepath.Clean(exePath)
	}

	configDir, err := configDirForUninstall(cfg.ConfigPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Warning: uninstall removes the binary and the local config directory at %s\n", configDir)
	if err := os.RemoveAll(configDir); err != nil {
		return fmt.Errorf("removing config directory: %w", err)
	}

	if runtime.GOOS == "windows" {
		if err := removeWindowsExecutable(exePath); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "teams-migrator uninstall staged for %s\n", exePath)
		fmt.Fprintf(os.Stdout, "Removed config directory for %s\n", cfg.ConfigPath)
		return nil
	}

	if err := os.Remove(exePath); err != nil {
		return fmt.Errorf("removing executable: %w", err)
	}

	fmt.Fprintf(os.Stdout, "Removed %s\n", exePath)
	fmt.Fprintf(os.Stdout, "Removed config directory for %s\n", cfg.ConfigPath)
	return nil
}

func configDirForUninstall(configPath string) (string, error) {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return "", errors.New("config path is empty")
	}

	cleanConfigPath, err := filepath.Abs(filepath.Clean(configPath))
	if err != nil {
		return "", fmt.Errorf("resolving config path: %w", err)
	}
	configDir := filepath.Dir(cleanConfigPath)
	defaultDir, err := defaultAppConfigDir()
	if err != nil {
		return "", err
	}
	if !samePath(configDir, defaultDir) {
		return "", fmt.Errorf("refusing to remove non-standard config directory %s; uninstall only removes %s", configDir, defaultDir)
	}
	return defaultDir, nil
}

func defaultAppConfigDir() (string, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locating user config directory: %w", err)
	}
	userConfigDir = strings.TrimSpace(userConfigDir)
	if userConfigDir == "" {
		return "", errors.New("user config directory is empty")
	}
	dir, err := filepath.Abs(filepath.Join(userConfigDir, "teams-migrator"))
	if err != nil {
		return "", fmt.Errorf("resolving user config directory: %w", err)
	}
	return filepath.Clean(dir), nil
}

func samePath(a, b string) bool {
	a = filepath.Clean(a)
	b = filepath.Clean(b)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func removeWindowsExecutable(exePath string) error {
	script, err := os.CreateTemp("", "teams-migrator-uninstall-*.cmd")
	if err != nil {
		return fmt.Errorf("creating uninstall script: %w", err)
	}
	scriptPath := script.Name()
	scriptBody := fmt.Sprintf(`@echo off
ping 127.0.0.1 -n 3 > nul
del "%s"
del "%%~f0"
`, exePath)
	if _, err := script.WriteString(scriptBody); err != nil {
		script.Close()
		return fmt.Errorf("writing uninstall script: %w", err)
	}
	if err := script.Close(); err != nil {
		return fmt.Errorf("closing uninstall script: %w", err)
	}
	if err := exec.Command("cmd", "/C", "start", "", scriptPath).Start(); err != nil {
		return fmt.Errorf("starting uninstall script: %w", err)
	}
	return nil
}
