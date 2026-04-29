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
	dir, err := cleanOutputDirPath(dir)
	if err != nil {
		return err
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
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, stale := range matches[keep:] {
		stale, err := cleanOutputEntryName(stale)
		if err != nil {
			return err
		}
		if err := root.Remove(stale); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func latestOutputFamilyPath(dir, name string) (string, bool) {
	if strings.TrimSpace(dir) == "" {
		return "", false
	}
	dir, err := cleanOutputDirPath(dir)
	if err != nil {
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
			path, err := outputFilePathFromEntry(dir, entryName)
			if err != nil {
				continue
			}
			exactName = path
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
	path, err := outputFilePathFromEntry(dir, matches[0])
	if err != nil {
		return "", false
	}
	return path, true
}

func cleanOutputDirPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	if pathHasParentElement(path) {
		return "", fmt.Errorf("output directory %q must not contain parent path traversal", path)
	}
	return filepath.Clean(path), nil
}

func cleanInputFilePath(label, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", nil
	}
	if pathHasParentElement(path) {
		return "", fmt.Errorf("%s path %q must not contain parent path traversal", label, path)
	}
	return filepath.Clean(path), nil
}

func outputFilePathFromEntry(dir, entryName string) (string, error) {
	dir, err := cleanOutputDirPath(dir)
	if err != nil {
		return "", err
	}
	entryName, err = cleanOutputEntryName(entryName)
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, entryName), nil
}

func cleanOutputEntryName(entryName string) (string, error) {
	entryName = strings.TrimSpace(entryName)
	if entryName == "" || entryName != filepath.Base(entryName) || pathHasParentElement(entryName) {
		return "", fmt.Errorf("output file name %q must be a local file name", entryName)
	}
	return entryName, nil
}

func pathHasParentElement(path string) bool {
	if volume := filepath.VolumeName(path); volume != "" {
		path = strings.TrimPrefix(path, volume)
	}
	for _, element := range strings.FieldsFunc(path, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if element == ".." {
			return true
		}
	}
	return false
}
