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
	envSourceAuthToken = "TEAMS_MIGRATOR_SOURCE_AUTH_TOKEN"
	envSourceUsername  = "TEAMS_MIGRATOR_SOURCE_USERNAME"
	envSourcePassword  = "TEAMS_MIGRATOR_SOURCE_PASSWORD"
	envTargetBaseURL   = "TEAMS_MIGRATOR_TARGET_BASE_URL"
	envTargetAuthToken = "TEAMS_MIGRATOR_TARGET_AUTH_TOKEN"
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
	SourceAuthToken     string
	SourceUsername      string
	SourcePassword      string
	TargetBaseURL       string
	TargetAuthToken     string
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
}

func parseConfig(args []string) (Config, error) {
	if len(args) == 0 {
		return Config{}, errUsage
	}

	command := args[0]
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := Config{Command: command}
	fs.StringVar(&cfg.SourceBaseURL, "source-base-url", envOrDefault(envSourceBaseURL, ""), "Source Jira base URL")
	fs.StringVar(&cfg.SourceAuthToken, "source-auth-token", envOrDefault(envSourceAuthToken, ""), "Source Jira auth token")
	fs.StringVar(&cfg.SourceUsername, "source-username", envOrDefault(envSourceUsername, ""), "Source Jira username for basic auth")
	fs.StringVar(&cfg.SourcePassword, "source-password", envOrDefault(envSourcePassword, ""), "Source Jira password for basic auth")
	fs.StringVar(&cfg.TargetBaseURL, "target-base-url", envOrDefault(envTargetBaseURL, ""), "Target Jira base URL")
	fs.StringVar(&cfg.TargetAuthToken, "target-auth-token", envOrDefault(envTargetAuthToken, ""), "Target Jira auth token")
	fs.StringVar(&cfg.TargetUsername, "target-username", envOrDefault(envTargetUsername, ""), "Target Jira username for basic auth")
	fs.StringVar(&cfg.TargetPassword, "target-password", envOrDefault(envTargetPassword, ""), "Target Jira password for basic auth")
	fs.StringVar(&cfg.IdentityMappingFile, "identity-mapping", envOrDefault(envIdentityMapping, ""), "Path to identity mapping CSV")
	fs.StringVar(&cfg.TeamsFile, "teams-file", envOrDefault(envTeamsFile, ""), "Path to source teams JSON export")
	fs.StringVar(&cfg.PersonsFile, "persons-file", envOrDefault(envPersonsFile, ""), "Path to source persons JSON export")
	fs.StringVar(&cfg.ResourcesFile, "resources-file", envOrDefault(envResourcesFile, ""), "Path to source resources JSON export")
	fs.StringVar(&cfg.IssuesCSV, "issues-csv", envOrDefault(envIssuesCSV, ""), "Path to issues CSV")
	fs.StringVar(&cfg.OutputDir, "output-dir", envOrDefault(envOutputDir, "out"), "Directory for generated reports")
	fs.StringVar(&cfg.ReportInput, "input", envOrDefault(envReportInput, ""), "Input JSON report for the report subcommand")

	reportFormat := envOrDefault(envReportFormat, string(ReportFormatJSON))
	fs.StringVar(&reportFormat, "format", reportFormat, "Report format: json or csv")

	cfg.Strict = boolEnv(envStrict, false)
	cfg.DryRun = boolEnv(envDryRun, true)
	fs.BoolVar(&cfg.Strict, "strict", cfg.Strict, "Exit non-zero when warnings or errors are present")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "Preview mutating operations without sending writes")
	fs.BoolVar(&cfg.Apply, "apply", false, "Execute mutating operations; overrides dry-run for migrate")

	if err := fs.Parse(args[1:]); err != nil {
		return Config{}, err
	}

	cfg.ReportFormat = ReportFormat(strings.ToLower(reportFormat))
	if cfg.Apply {
		cfg.DryRun = false
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) validate() error {
	switch c.ReportFormat {
	case ReportFormatJSON, ReportFormatCSV:
	default:
		return fmt.Errorf("unsupported report format %q", c.ReportFormat)
	}

	switch c.Command {
	case "validate", "plan", "migrate", "report":
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
		findings = append(findings, newFinding(SeverityError, "missing_identity_mapping", "Identity mapping CSV is required"))
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

func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
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
	return filepath.Join(cfg.OutputDir, fmt.Sprintf("%s-report.%s", cfg.Command, ext))
}
