package app

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const outputRetentionLimit = 10

func outputPathForName(cfg Config, name string) string {
	return filepath.Join(cfg.OutputDir, timestampedFilename(name, cfg.OutputTimestamp))
}

func timestampedFilename(name, stamp string) string {
	if strings.TrimSpace(stamp) == "" {
		return name
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	return fmt.Sprintf("%s.%s%s", base, stamp, ext)
}

func pruneOutputFamily(dir, name string, keep int) error {
	if dir == "" || keep < 1 {
		return nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	prefix := base + "."

	matches := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryName := entry.Name()
		if !strings.HasPrefix(entryName, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(entryName, prefix)
		if !strings.HasSuffix(remainder, ext) {
			continue
		}
		stamp := strings.TrimSuffix(remainder, ext)
		if strings.TrimSpace(stamp) == "" {
			continue
		}
		matches = append(matches, entryName)
	}

	if len(matches) <= keep {
		return nil
	}

	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	for _, stale := range matches[keep:] {
		if err := os.Remove(filepath.Join(dir, stale)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func latestOutputFamilyPath(dir, name string) (string, bool) {
	if strings.TrimSpace(dir) == "" {
		return "", false
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", false
	}

	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	exactName := ""

	matches := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		entryName := entry.Name()
		if entryName == name {
			exactName = filepath.Join(dir, entryName)
			continue
		}
		if !strings.HasPrefix(entryName, base+".") || !strings.HasSuffix(entryName, ext) {
			continue
		}
		matches = append(matches, entryName)
	}

	if len(matches) == 0 {
		return exactName, exactName != ""
	}

	sort.Sort(sort.Reverse(sort.StringSlice(matches)))
	return filepath.Join(dir, matches[0]), true
}
