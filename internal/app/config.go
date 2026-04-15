package app

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	envSourceBaseURL   = "TEAMS_MIGRATOR_SOURCE_BASE_URL"
	envSourceUsername  = "TEAMS_MIGRATOR_SOURCE_USERNAME"
	envSourcePassword  = "TEAMS_MIGRATOR_SOURCE_PASSWORD"
	envTargetBaseURL   = "TEAMS_MIGRATOR_TARGET_BASE_URL"
	envTargetUsername  = "TEAMS_MIGRATOR_TARGET_USERNAME"
	envTargetPassword  = "TEAMS_MIGRATOR_TARGET_PASSWORD"
	envIdentityMapping = "TEAMS_MIGRATOR_IDENTITY_MAPPING_FILE"
	envTeamsFile       = "TEAMS_MIGRATOR_TEAMS_FILE"
	envPersonsFile     = "TEAMS_MIGRATOR_PERSONS_FILE"
	envResourcesFile   = "TEAMS_MIGRATOR_RESOURCES_FILE"
	envIssuesCSV       = "TEAMS_MIGRATOR_ISSUES_CSV"
	envOutputDir       = "TEAMS_MIGRATOR_OUTPUT_DIR"
	envReportFormat    = "TEAMS_MIGRATOR_REPORT_FORMAT"
	envStrict          = "TEAMS_MIGRATOR_STRICT"
	envDryRun          = "TEAMS_MIGRATOR_DRY_RUN"
	envReportInput     = "TEAMS_MIGRATOR_REPORT_INPUT"
)

type Config struct {
	Command             string
	SourceBaseURL       string
	SourceUsername      string
	SourcePassword      string
	TargetBaseURL       string
	TargetUsername      string
	TargetPassword      string
	IdentityMappingFile string
	TeamsFile           string
	PersonsFile         string
	ResourcesFile       string
	IssuesCSV           string
	OutputDir           string
	ReportInput         string
	ReportFormat        ReportFormat
	Strict              bool
	DryRun              bool
	Apply               bool
	NoInput             bool
	ConfigPath          string
	Profile             string
	Redacted            bool
}

func parseConfig(args []string) (Config, error) {
	command, remaining, err := parseCommand(args)
	if err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := Config{Command: command}
	fs.StringVar(&cfg.SourceBaseURL, "source-base-url", envValue(envSourceBaseURL), "Source Jira base URL")
	fs.StringVar(&cfg.SourceUsername, "source-username", envValue(envSourceUsername), "Source Jira username for basic auth")
	fs.StringVar(&cfg.SourcePassword, "source-password", envValue(envSourcePassword), "Source Jira password for basic auth")
	fs.StringVar(&cfg.TargetBaseURL, "target-base-url", envValue(envTargetBaseURL), "Target Jira base URL")
	fs.StringVar(&cfg.TargetUsername, "target-username", envValue(envTargetUsername), "Target Jira username for basic auth")
	fs.StringVar(&cfg.TargetPassword, "target-password", envValue(envTargetPassword), "Target Jira password for basic auth")
	fs.StringVar(&cfg.IdentityMappingFile, "identity-mapping", envValue(envIdentityMapping), "Path to identity mapping CSV")
	fs.StringVar(&cfg.TeamsFile, "teams-file", envValue(envTeamsFile), "Path to source teams JSON export")
	fs.StringVar(&cfg.PersonsFile, "persons-file", envValue(envPersonsFile), "Path to source persons JSON export")
	fs.StringVar(&cfg.ResourcesFile, "resources-file", envValue(envResourcesFile), "Path to source resources JSON export")
	fs.StringVar(&cfg.IssuesCSV, "issues-csv", envValue(envIssuesCSV), "Path to issues CSV")
	fs.StringVar(&cfg.OutputDir, "output-dir", envValue(envOutputDir), "Directory for generated reports")
	fs.StringVar(&cfg.ReportInput, "input", envValue(envReportInput), "Input JSON report for the report subcommand")
	fs.StringVar(&cfg.ConfigPath, "config", defaultConfigPath(), "Path to config.yaml profile store")
	fs.StringVar(&cfg.Profile, "profile", envValue(envProfile), "Saved profile name from config.yaml")
	fs.BoolVar(&cfg.Redacted, "redacted", true, "Redact secrets in config show output")

	reportFormat := envValue(envReportFormat)
	fs.StringVar(&reportFormat, "format", reportFormat, "Report format: json or csv")

	cfg.Strict = boolEnv(envStrict, false)
	cfg.DryRun = boolEnv(envDryRun, true)
	fs.BoolVar(&cfg.Strict, "strict", cfg.Strict, "Exit non-zero when warnings or errors are present")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "Preview mutating operations without sending writes")
	fs.BoolVar(&cfg.Apply, "apply", false, "Execute mutating operations; overrides dry-run for migrate")
	fs.BoolVar(&cfg.NoInput, "no-input", false, "Disable interactive prompts and require flags or environment variables")

	if err := fs.Parse(remaining); err != nil {
		return Config{}, err
	}

	if strings.TrimSpace(reportFormat) != "" {
		cfg.ReportFormat = ReportFormat(strings.ToLower(reportFormat))
	}
	if cfg.ReportFormat == "" {
		cfg.ReportFormat = ReportFormatJSON
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = "out"
	}
	if cfg.Apply {
		cfg.DryRun = false
	}

	if cfg.Command != "version" && cfg.Command != "self-update" {
		store, err := loadProfileStore(cfg.ConfigPath)
		if err != nil {
			return Config{}, fmt.Errorf("loading config store: %w", err)
		}
		applySavedProfile(&cfg, resolveProfile(cfg, store))

		selectedProfile := cfg.Profile
		if selectedProfile == "" {
			if store.CurrentProfile != "" {
				selectedProfile = store.CurrentProfile
			} else {
				selectedProfile = "default"
			}
		}
		cfg.Profile = selectedProfile
	}

	if cfg.Command != "config init" && cfg.Command != "config show" && cfg.Command != "config path" && cfg.Command != "version" && cfg.Command != "self-update" && !cfg.NoInput {
		if err := completeConfigInteractively(&cfg); err != nil {
			return Config{}, err
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func parseCommand(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errUsage
	}
	if args[0] == "config" {
		if len(args) < 2 {
			return "", nil, errUsage
		}
		if args[1] != "init" && args[1] != "show" && args[1] != "path" {
			return "", nil, fmt.Errorf("unknown config subcommand %q", args[1])
		}
		return "config " + args[1], args[2:], nil
	}
	return args[0], args[1:], nil
}

func (c Config) validate() error {
	switch c.ReportFormat {
	case ReportFormatJSON, ReportFormatCSV:
	default:
		return fmt.Errorf("unsupported report format %q", c.ReportFormat)
	}

	switch c.Command {
	case "validate", "plan", "migrate", "report", "config init", "config show", "config path", "version", "self-update":
	default:
		return fmt.Errorf("unknown command %q", c.Command)
	}

	if c.Command == "report" && c.ReportInput == "" {
		return errors.New("report command requires --input or TEAMS_MIGRATOR_REPORT_INPUT")
	}

	return nil
}

func (c Config) requireCoreInputs() []Finding {
	var findings []Finding

	if c.IdentityMappingFile == "" {
		findings = append(findings, newFinding(SeverityInfo, "identity_mapping_optional", "No identity mapping CSV supplied; the tool will try to auto-resolve users by matching source and target emails"))
	}

	if c.SourceBaseURL == "" && c.TeamsFile == "" {
		findings = append(findings, newFinding(SeverityWarning, "missing_source_teams_input", "Neither source API base URL nor teams export file was provided"))
	}
	if c.SourceBaseURL == "" && c.PersonsFile == "" {
		findings = append(findings, newFinding(SeverityWarning, "missing_source_persons_input", "Neither source API base URL nor persons export file was provided"))
	}
	if c.SourceBaseURL == "" && c.ResourcesFile == "" {
		findings = append(findings, newFinding(SeverityWarning, "missing_source_resources_input", "Neither source API base URL nor resources export file was provided"))
	}
	if c.TargetBaseURL == "" {
		findings = append(findings, newFinding(SeverityWarning, "missing_target_base_url", "Target Jira base URL was not provided"))
	}

	for _, path := range []struct {
		label string
		value string
	}{
		{label: "identity mapping", value: c.IdentityMappingFile},
		{label: "teams export", value: c.TeamsFile},
		{label: "persons export", value: c.PersonsFile},
		{label: "resources export", value: c.ResourcesFile},
		{label: "issues CSV", value: c.IssuesCSV},
		{label: "report input", value: c.ReportInput},
	} {
		if path.value == "" {
			continue
		}
		if _, err := os.Stat(path.value); err != nil {
			findings = append(findings, newFinding(SeverityError, "missing_file", fmt.Sprintf("%s file not found: %s", path.label, path.value)))
		}
	}

	if c.IdentityMappingFile != "" {
		findings = append(findings, validateIdentityMappingFile(c.IdentityMappingFile)...)
	}

	return findings
}

func validateIdentityMappingFile(path string) []Finding {
	file, err := os.Open(path)
	if err != nil {
		return []Finding{newFinding(SeverityError, "identity_mapping_unreadable", fmt.Sprintf("Could not open identity mapping file: %v", err))}
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		return []Finding{newFinding(SeverityError, "identity_mapping_invalid_csv", fmt.Sprintf("Could not parse identity mapping CSV: %v", err))}
	}
	if len(rows) == 0 {
		return []Finding{newFinding(SeverityError, "identity_mapping_empty", "Identity mapping CSV is empty")}
	}

	header := rows[0]
	if len(header) < 2 || !strings.EqualFold(strings.TrimSpace(header[0]), "sourceEmail") || !strings.EqualFold(strings.TrimSpace(header[1]), "targetEmail") {
		return []Finding{newFinding(SeverityError, "identity_mapping_header", "Identity mapping CSV must start with sourceEmail,targetEmail")}
	}

	return nil
}

func envValue(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return ""
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func ensureOutputDir(path string) error {
	if path == "" {
		return nil
	}
	return os.MkdirAll(path, 0o755)
}

func defaultOutputPath(cfg Config) string {
	ext := string(cfg.ReportFormat)
	return filepath.Join(cfg.OutputDir, fmt.Sprintf("%s-report.%s", strings.ReplaceAll(cfg.Command, " ", "-"), ext))
}
