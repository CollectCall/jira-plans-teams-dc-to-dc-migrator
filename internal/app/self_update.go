package app

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	releaseRepo         = "CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator"
	releaseBaseURL      = "https://github.com/" + releaseRepo + "/releases/download"
	envSkipUpdatePrompt = "TEAMS_MIGRATOR_SKIP_UPDATE_CHECK"
)

type githubRelease struct {
	TagName string `json:"tag_name"`
}

func runSelfUpdate() error {
	release, err := fetchLatestRelease()
	if err != nil {
		return err
	}
	return applyReleaseUpdate(release)
}

func maybeOfferSelfUpdate(cfg Config) (bool, error) {
	if cfg.NoInput || !isInteractiveTerminal() || currentVersion() == "" || currentVersion() == "dev" {
		return false, nil
	}
	if cfg.Command == "version" || cfg.Command == "self-update" || cfg.Command == "uninstall" || boolEnv(envSkipUpdatePrompt, false) {
		return false, nil
	}

	release, err := fetchLatestRelease()
	if err != nil || release.TagName == "" || release.TagName == currentVersion() {
		return false, nil
	}

	ok, err := promptForSelfUpdate(release.TagName)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if err := applyReleaseUpdate(release); err != nil {
		return false, err
	}
	fmt.Fprintln(os.Stdout, "Rerun your command to continue on the updated version.")
	return true, nil
}

func applyReleaseUpdate(release githubRelease) error {
	if release.TagName == "" {
		return errors.New("latest release did not include a tag name")
	}
	if currentVersion() == release.TagName {
		fmt.Fprintf(os.Stdout, "teams-migrator is already up to date (%s)\n", currentVersion())
		return nil
	}

	archiveName, checksumName, binaryName, err := releaseAssetNames(release.TagName)
	if err != nil {
		return err
	}
	archiveURL := fmt.Sprintf("%s/%s/%s", releaseBaseURL, release.TagName, archiveName)
	checksumURL := fmt.Sprintf("%s/%s/%s", releaseBaseURL, release.TagName, checksumName)

	archiveData, err := downloadURL(archiveURL)
	if err != nil {
		return err
	}
	checksumData, err := downloadURL(checksumURL)
	if err != nil {
		return err
	}
	if err := verifyChecksum(archiveName, archiveData, checksumData); err != nil {
		return err
	}

	binaryData, err := extractBinary(archiveName, binaryName, archiveData)
	if err != nil {
		return err
	}

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locating current executable: %w", err)
	}
	exePath, err = filepath.EvalSymlinks(exePath)
	if err != nil {
		exePath = filepath.Clean(exePath)
	}

	if runtime.GOOS == "windows" {
		if err := replaceWindowsExecutable(exePath, binaryData); err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "Update staged for %s. Restart teams-migrator to use %s.\n", exePath, release.TagName)
		return nil
	}

	if err := replaceExecutable(exePath, binaryData); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Updated teams-migrator from %s to %s\n", currentVersion(), release.TagName)
	return nil
}

func fetchLatestRelease() (githubRelease, error) {
	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/repos/"+releaseRepo+"/releases/latest", nil)
	if err != nil {
		return githubRelease{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "teams-migrator/"+currentVersion())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return githubRelease{}, fmt.Errorf("fetching latest release metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return githubRelease{}, fmt.Errorf("fetching latest release metadata: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return githubRelease{}, fmt.Errorf("decoding latest release metadata: %w", err)
	}
	return release, nil
}

func releaseAssetNames(tag string) (archiveName, checksumName, binaryName string, err error) {
	switch runtime.GOARCH {
	case "amd64", "arm64":
	default:
		return "", "", "", fmt.Errorf("self-update is unsupported on %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	checksumName = "checksums.txt"
	binaryName = "teams-migrator"
	switch runtime.GOOS {
	case "linux", "darwin":
		archiveName = fmt.Sprintf("teams-migrator_%s_%s_%s.tar.gz", tag, runtime.GOOS, runtime.GOARCH)
	case "windows":
		archiveName = fmt.Sprintf("teams-migrator_%s_windows_%s.zip", tag, runtime.GOARCH)
		binaryName = "teams-migrator.exe"
	default:
		return "", "", "", fmt.Errorf("self-update is unsupported on %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	return archiveName, checksumName, binaryName, nil
}

func downloadURL(url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "teams-migrator/"+currentVersion())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("downloading %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("downloading %s: %s: %s", url, resp.Status, strings.TrimSpace(string(body)))
	}
	return io.ReadAll(resp.Body)
}

func verifyChecksum(archiveName string, archiveData, checksumData []byte) error {
	want := ""
	for _, line := range strings.Split(string(checksumData), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if fields[1] == archiveName {
			want = fields[0]
			break
		}
	}
	if want == "" {
		return fmt.Errorf("no checksum found for %s", archiveName)
	}

	sum := sha256.Sum256(archiveData)
	got := hex.EncodeToString(sum[:])
	if !strings.EqualFold(want, got) {
		return fmt.Errorf("checksum mismatch for %s", archiveName)
	}
	return nil
}

func extractBinary(archiveName, binaryName string, archiveData []byte) ([]byte, error) {
	switch {
	case strings.HasSuffix(archiveName, ".tar.gz"):
		gz, err := gzip.NewReader(bytes.NewReader(archiveData))
		if err != nil {
			return nil, fmt.Errorf("opening archive: %w", err)
		}
		defer gz.Close()

		tr := tar.NewReader(gz)
		for {
			header, err := tr.Next()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("reading archive: %w", err)
			}
			if filepath.Base(header.Name) != binaryName {
				continue
			}
			return io.ReadAll(tr)
		}
	case strings.HasSuffix(archiveName, ".zip"):
		zr, err := zip.NewReader(bytes.NewReader(archiveData), int64(len(archiveData)))
		if err != nil {
			return nil, fmt.Errorf("opening archive: %w", err)
		}
		for _, file := range zr.File {
			if filepath.Base(file.Name) != binaryName {
				continue
			}
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("opening archive member: %w", err)
			}
			defer rc.Close()
			return io.ReadAll(rc)
		}
	}
	return nil, fmt.Errorf("binary %s not found in %s", binaryName, archiveName)
}

func replaceExecutable(exePath string, binaryData []byte) error {
	dir := filepath.Dir(exePath)
	tmp, err := os.CreateTemp(dir, "teams-migrator-update-*")
	if err != nil {
		return fmt.Errorf("creating temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(binaryData); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temporary file: %w", err)
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return fmt.Errorf("setting permissions: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary file: %w", err)
	}
	if err := os.Rename(tmpPath, exePath); err != nil {
		return fmt.Errorf("replacing executable: %w", err)
	}
	return nil
}

func replaceWindowsExecutable(exePath string, binaryData []byte) error {
	dir := filepath.Dir(exePath)
	tmp, err := os.CreateTemp(dir, "teams-migrator-update-*.exe")
	if err != nil {
		return fmt.Errorf("creating temporary file: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(binaryData); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary file: %w", err)
	}

	script, err := os.CreateTemp("", "teams-migrator-update-*.cmd")
	if err != nil {
		return fmt.Errorf("creating update script: %w", err)
	}
	scriptPath := script.Name()
	scriptBody := fmt.Sprintf(`@echo off
ping 127.0.0.1 -n 3 > nul
copy /Y "%s" "%s" > nul
del "%s"
del "%%~f0"
`, tmpPath, exePath, tmpPath)
	if _, err := script.WriteString(scriptBody); err != nil {
		script.Close()
		return fmt.Errorf("writing update script: %w", err)
	}
	if err := script.Close(); err != nil {
		return fmt.Errorf("closing update script: %w", err)
	}
	if err := exec.Command("cmd", "/C", "start", "", scriptPath).Start(); err != nil {
		return fmt.Errorf("starting update script: %w", err)
	}
	return nil
}
