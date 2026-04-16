package app

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

	if cfg.PurgeConfig {
		configDir := filepath.Dir(cfg.ConfigPath)
		if err := os.RemoveAll(configDir); err != nil {
			return fmt.Errorf("removing config directory: %w", err)
		}
	}

	if runtime.GOOS == "windows" {
		if err := removeWindowsExecutable(exePath); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "teams-migrator uninstall staged for %s\n", exePath)
		if cfg.PurgeConfig {
			fmt.Fprintf(os.Stdout, "Removed config directory for %s\n", cfg.ConfigPath)
		}
		return nil
	}

	if err := os.Remove(exePath); err != nil {
		return fmt.Errorf("removing executable: %w", err)
	}

	fmt.Fprintf(os.Stdout, "Removed %s\n", exePath)
	if cfg.PurgeConfig {
		fmt.Fprintf(os.Stdout, "Removed config directory for %s\n", cfg.ConfigPath)
	}
	return nil
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
