package app

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTimestampedFilename(t *testing.T) {
	got := timestampedFilename("issues-with-teams.pre-migration.csv", "20260417-194500")
	want := "issues-with-teams.pre-migration.20260417-194500.csv"
	if got != want {
		t.Fatalf("unexpected timestamped filename: want %q, got %q", want, got)
	}
}

func TestPruneOutputFamilyKeepsNewestTimestampedFiles(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"issues-with-teams.pre-migration.20260417-194500.csv",
		"issues-with-teams.pre-migration.20260417-194501.csv",
		"issues-with-teams.pre-migration.20260417-194502.csv",
		"issues-with-teams.pre-migration.20260417-194503.csv",
		"issues-with-teams.pre-migration.csv",
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	if err := pruneOutputFamily(dir, "issues-with-teams.pre-migration.csv", 2); err != nil {
		t.Fatalf("pruneOutputFamily returned error: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}

	want := []string{
		"issues-with-teams.pre-migration.20260417-194502.csv",
		"issues-with-teams.pre-migration.20260417-194503.csv",
		"issues-with-teams.pre-migration.csv",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected remaining files:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestLatestOutputFamilyPathPrefersNewestTimestampedFile(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"issues-with-teams.pre-migration.csv",
		"issues-with-teams.pre-migration.20260417-194500.csv",
		"issues-with-teams.pre-migration.20260417-194503.csv",
	}
	for _, name := range names {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, ok := latestOutputFamilyPath(dir, "issues-with-teams.pre-migration.csv")
	if !ok {
		t.Fatal("expected latestOutputFamilyPath to find a matching file")
	}
	want := filepath.Join(dir, "issues-with-teams.pre-migration.20260417-194503.csv")
	if got != want {
		t.Fatalf("unexpected latest output path: want %q, got %q", want, got)
	}
}

func TestCleanOutputDirPathRejectsParentTraversal(t *testing.T) {
	separator := string(os.PathSeparator)
	for _, path := range []string{
		"../out",
		"out/../other",
		t.TempDir() + separator + ".." + separator + "other",
	} {
		if _, err := cleanOutputDirPath(path); err == nil {
			t.Fatalf("expected %q to be rejected", path)
		}
	}
}

func TestCleanOutputDirPathAllowsRelativeAndAbsoluteOutputDirs(t *testing.T) {
	for _, path := range []string{
		"out",
		filepath.Join("reports", "migration"),
		t.TempDir(),
	} {
		if got, err := cleanOutputDirPath(path); err != nil {
			t.Fatalf("cleanOutputDirPath(%q) returned error: %v", path, err)
		} else if got == "" {
			t.Fatalf("cleanOutputDirPath(%q) returned empty path", path)
		}
	}
}

func TestOutputFilePathFromEntryRejectsNonLocalNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"../report.csv",
		filepath.Join("nested", "report.csv"),
		"",
	} {
		if _, err := outputFilePathFromEntry(dir, name); err == nil {
			t.Fatalf("expected %q to be rejected", name)
		}
	}
}
