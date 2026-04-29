package app

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	envSourceBaseURL     = "TEAMS_MIGRATOR_SOURCE_BASE_URL"
	envSourceUsername    = "TEAMS_MIGRATOR_SOURCE_USERNAME"
	envSourcePassword    = "TEAMS_MIGRATOR_SOURCE_PASSWORD"
	envTargetBaseURL     = "TEAMS_MIGRATOR_TARGET_BASE_URL"
	envTargetUsername    = "TEAMS_MIGRATOR_TARGET_USERNAME"
	envTargetPassword    = "TEAMS_MIGRATOR_TARGET_PASSWORD"
	envIdentityMapping   = "TEAMS_MIGRATOR_IDENTITY_MAPPING_FILE"
	envTeamsFile         = "TEAMS_MIGRATOR_TEAMS_FILE"
	envPersonsFile       = "TEAMS_MIGRATOR_PERSONS_FILE"
	envResourcesFile     = "TEAMS_MIGRATOR_RESOURCES_FILE"
	envIssuesCSV         = "TEAMS_MIGRATOR_ISSUES_CSV"
	envFilterSourceCSV   = "TEAMS_MIGRATOR_FILTER_SOURCE_CSV"
	envOutputDir         = "TEAMS_MIGRATOR_OUTPUT_DIR"
	envReportFormat      = "TEAMS_MIGRATOR_REPORT_FORMAT"
	envTeamScope         = "TEAMS_MIGRATOR_TEAM_SCOPE"
	envIssueProjectScope = "TEAMS_MIGRATOR_ISSUE_PROJECT_SCOPE"
	envStrict            = "TEAMS_MIGRATOR_STRICT"
	envDryRun            = "TEAMS_MIGRATOR_DRY_RUN"
	envReportInput       = "TEAMS_MIGRATOR_REPORT_INPUT"
	envPhase             = "TEAMS_MIGRATOR_PHASE"
)

const (
	filterDataSourceScriptRunner = "scriptrunner"
	filterDataSourceDatabaseCSV  = "db-csv"
)

type Config struct {
	Command                     string
	Help                        bool
	SourceBaseURL               string
	SourceUsername              string
	SourcePassword              string
	TargetBaseURL               string
	TargetUsername              string
	TargetPassword              string
	IdentityMappingFile         string
	IdentityMappingSet          bool
	TeamsFile                   string
	PersonsFile                 string
	ResourcesFile               string
	IssuesCSV                   string
	FilterSourceCSV             string
	OutputDir                   string
	ReportInput                 string
	ReportFormat                ReportFormat
	TeamScope                   string
	IssueProjectScope           string
	IssueTeamIDsInScope         bool
	IssueTeamIDsInScopeSet      bool
	FilterTeamIDsInScope        bool
	FilterTeamIDsInScopeSet     bool
	ParentLinkInScope           bool
	ParentLinkInScopeSet        bool
	FilterDataSource            string
	FilterScriptRunnerInstalled bool
	FilterScriptRunnerEndpoint  string
	Strict                      bool
	DryRun                      bool
	Apply                       bool
	NoInput                     bool
	ConfigPath                  string
	Profile                     string
	ProfileExplicit             bool
	ProfileLoaded               bool
	OutputTimestamp             string
	Phase                       string
	PhaseExplicit               bool
}

func parseConfig(args []string) (Config, error) {
	command, remaining, err := parseCommand(args)
	if err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	cfg := Config{Command: command}
	if command == "help" || hasHelpArg(remaining) {
		cfg.Help = true
		return cfg, nil
	}
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
	fs.StringVar(&cfg.FilterSourceCSV, "filter-source-csv", envValue(envFilterSourceCSV), "Path to CSV with source filters that contain team IDs")
	fs.StringVar(&cfg.OutputDir, "output-dir", envValue(envOutputDir), "Directory for generated reports")
	fs.StringVar(&cfg.ReportInput, "input", envValue(envReportInput), "Input JSON report for the report subcommand")
	fs.StringVar(&cfg.ConfigPath, "config", defaultConfigPath(), "Path to config.yaml profile store")
	fs.StringVar(&cfg.Profile, "profile", envValue(envProfile), "Saved profile name from config.yaml")
	fs.StringVar(&cfg.Phase, "phase", envValue(envPhase), "Migration phase for migrate: pre-migrate, migrate, or post-migrate")
	fs.StringVar(&cfg.TeamScope, "team-scope", envValue(envTeamScope), "Team migration scope: all, shared-only, or non-shared-only")
	fs.StringVar(&cfg.IssueProjectScope, "issue-project-scope", envValue(envIssueProjectScope), "Issue correction project scope: all or a comma-separated list of Jira project keys")

	reportFormat := ""
	if command == "report" {
		reportFormat = envValue(envReportFormat)
	}
	reportFormatFlagProvided := stringFlagProvided(remaining, "--format")
	profileExplicit := envIsSet(envProfile) || stringFlagProvided(remaining, "--profile")
	fs.StringVar(&reportFormat, "format", reportFormat, "Report format: json or csv")

	cfg.Strict = boolEnv(envStrict, false)
	cfg.DryRun = boolEnv(envDryRun, true)
	fs.BoolVar(&cfg.Strict, "strict", cfg.Strict, "Exit non-zero when warnings or errors are present")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "Preview planned changes without sending writes")
	fs.BoolVar(&cfg.Apply, "apply", false, "Execute writes for migrate")
	fs.BoolVar(&cfg.NoInput, "no-input", false, "Disable interactive prompts and require flags or environment variables")
	cfg.PhaseExplicit = envIsSet(envPhase) || stringFlagProvided(remaining, "--phase")

	if err := fs.Parse(remaining); err != nil {
		return Config{}, err
	}
	if cfg.Command != "report" && reportFormatFlagProvided {
		return Config{}, errors.New("--format is only supported with the report command")
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
	if cfg.OutputTimestamp == "" {
		cfg.OutputTimestamp = time.Now().Format("20060102-150405")
	}
	if cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir); err != nil {
		return Config{}, err
	}
	if cfg.Apply {
		cfg.DryRun = false
	}
	if normalized := normalizeMigrationPhase(cfg.Phase); normalized != "" {
		cfg.Phase = normalized
	} else if cfg.Command == "migrate" {
		cfg.Phase = defaultMigrationPhase(cfg.Command)
	}
	if strings.TrimSpace(cfg.IdentityMappingFile) != "" {
		cfg.IdentityMappingSet = true
	}
	cfg.ProfileExplicit = profileExplicit

	if cfg.Command != "init" && cfg.Command != "config show" && cfg.Command != "version" && cfg.Command != "self-update" && cfg.Command != "uninstall" {
		store, err := loadProfileStore(cfg.ConfigPath)
		if err != nil {
			return Config{}, fmt.Errorf("loading config store: %w", err)
		}
		if profileExplicit || cfg.Command != "migrate" || cfg.NoInput || !isInteractiveTerminal() {
			selectedProfile, profile, loaded := resolveProfile(cfg, store)
			if profileExplicit && !loaded {
				return Config{}, fmt.Errorf("saved profile %q was not found in %s; run teams-migrator init to create it or choose an existing profile", cfg.Profile, cfg.ConfigPath)
			}
			if selectedProfile != "" {
				cfg.Profile = selectedProfile
			}
			if loaded {
				applySavedProfile(&cfg, profile)
				cfg.ProfileLoaded = true
			}
		}
	}

	if strings.TrimSpace(cfg.TeamScope) == "" {
		cfg.TeamScope = "all"
	} else {
		cfg.TeamScope = strings.ToLower(strings.TrimSpace(cfg.TeamScope))
	}
	if strings.TrimSpace(cfg.IssueProjectScope) == "" {
		cfg.IssueProjectScope = "all"
	}
	if normalized := normalizeFilterDataSource(cfg.FilterDataSource); normalized != "" {
		cfg.FilterDataSource = normalized
	} else {
		cfg.FilterDataSource = strings.ToLower(strings.TrimSpace(cfg.FilterDataSource))
	}
	applyDefaultReferenceExportScopes(&cfg)

	if cfg.Command != "init" && cfg.Command != "config show" && cfg.Command != "version" && cfg.Command != "self-update" && cfg.Command != "uninstall" && !cfg.NoInput {
		if cfg.Command == "migrate" && isInteractiveTerminal() {
			// The migrate command runs a dedicated multi-phase session in Run so
			// credentials and prior answers can stay in memory across phases.
		} else {
			if err := completeConfigInteractively(&cfg); err != nil {
				return Config{}, err
			}
		}
	}
	if cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir); err != nil {
		return Config{}, err
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func applyDefaultReferenceExportScopes(cfg *Config) {
	if cfg.Command != "migrate" {
		return
	}

	cfg.IssueTeamIDsInScope = true
	cfg.IssueTeamIDsInScopeSet = true

	cfg.ParentLinkInScope = strings.TrimSpace(cfg.SourceBaseURL) != "" || outputFamilyExists(cfg.OutputDir, "issues-with-parent-link.pre-migration.csv")
	cfg.ParentLinkInScopeSet = true

	if normalizeFilterDataSource(cfg.FilterDataSource) == "" && strings.TrimSpace(cfg.FilterSourceCSV) != "" {
		cfg.FilterDataSource = filterDataSourceDatabaseCSV
	}
	cfg.FilterTeamIDsInScope = filterReferenceExportsAvailable(*cfg)
	cfg.FilterTeamIDsInScopeSet = true
}

func filterReferenceExportsAvailable(cfg Config) bool {
	if outputFamilyExists(cfg.OutputDir, "filters-with-team-clauses.pre-migration.csv") {
		return true
	}
	switch normalizeFilterDataSource(cfg.FilterDataSource) {
	case filterDataSourceDatabaseCSV:
		return strings.TrimSpace(cfg.FilterSourceCSV) != ""
	case filterDataSourceScriptRunner:
		return cfg.FilterScriptRunnerInstalled && strings.TrimSpace(cfg.SourceBaseURL) != ""
	default:
		return false
	}
}

func outputFamilyExists(outputDir, suffix string) bool {
	_, ok := latestOutputFamilyPath(outputDir, suffix)
	return ok
}

func parseCommand(args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, errUsage
	}
	if args[0] == "help" || isHelpArg(args[0]) {
		return "help", nil, nil
	}
	if args[0] == "init" {
		return "init", args[1:], nil
	}
	if args[0] == "config" {
		if len(args) < 2 {
			return "", nil, errUsage
		}
		if isHelpArg(args[1]) {
			return "help", nil, nil
		}
		if args[1] != "show" {
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
	case "init", "migrate", "report", "config show", "version", "self-update", "uninstall":
	default:
		return fmt.Errorf("unknown command %q", c.Command)
	}

	switch c.TeamScope {
	case "all", "shared-only", "non-shared-only":
	default:
		return fmt.Errorf("unsupported team scope %q; use all, shared-only, or non-shared-only", c.TeamScope)
	}
	if _, err := normalizeIssueProjectScope(c.IssueProjectScope); err != nil {
		return err
	}

	switch c.FilterDataSource {
	case "", filterDataSourceScriptRunner, filterDataSourceDatabaseCSV:
	default:
		return fmt.Errorf("unsupported filter data source %q; use %s or %s", c.FilterDataSource, filterDataSourceScriptRunner, filterDataSourceDatabaseCSV)
	}

	if c.Command == "report" && c.ReportInput == "" {
		return errors.New("report command requires --input or TEAMS_MIGRATOR_REPORT_INPUT")
	}
	if c.Command == "migrate" {
		if normalizeMigrationPhase(c.Phase) == "" {
			return fmt.Errorf("unsupported migration phase %q; use pre-migrate, migrate, or post-migrate", c.Phase)
		}
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
	} else if strings.TrimSpace(c.TargetUsername) == "" || strings.TrimSpace(c.TargetPassword) == "" {
		findings = append(findings, newFinding(SeverityError, "missing_target_credentials", "Target Jira credentials were not provided; set --target-username/--target-password or TEAMS_MIGRATOR_TARGET_USERNAME/TEAMS_MIGRATOR_TARGET_PASSWORD"))
	}
	findings = append(findings, validateMigrationPhaseInputs(c)...)

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
	if normalizeFilterDataSource(c.FilterDataSource) == filterDataSourceDatabaseCSV && strings.TrimSpace(c.FilterSourceCSV) != "" {
		if _, err := os.Stat(c.FilterSourceCSV); err != nil {
			findings = append(findings, newFinding(SeverityError, "missing_file", fmt.Sprintf("filter source CSV file not found: %s", c.FilterSourceCSV)))
		}
	}

	return findings
}

func validateMigrationPhaseInputs(c Config) []Finding {
	if c.Command != "migrate" {
		return nil
	}

	switch normalizeMigrationPhase(c.Phase) {
	case phasePreMigrate:
		if !c.DryRun {
			return []Finding{newFinding(SeverityError, "pre_migrate_apply_unsupported", "Pre-migrate is a read-only phase; rerun without --apply")}
		}
		if c.ParentLinkInScope && strings.TrimSpace(c.SourceBaseURL) == "" {
			return []Finding{newFinding(SeverityError, "pre_migrate_parent_link_missing_source", "Parent Link corrections are in scope, but no source Jira base URL is available for the pre-migrate export. Configure source Jira API access first.")}
		}
		if c.FilterTeamIDsInScope {
			switch normalizeFilterDataSource(c.FilterDataSource) {
			case "":
				return []Finding{newFinding(SeverityError, "pre_migrate_filter_method_missing", "Filter team-ID updates are in scope, but no filter inventory method is configured. Re-run init and choose ScriptRunner or DB CSV.")}
			case filterDataSourceDatabaseCSV:
				if strings.TrimSpace(c.FilterSourceCSV) == "" {
					return []Finding{newFinding(SeverityError, "pre_migrate_filter_csv_missing", "Filter team-ID updates are in scope and the configured method is DB CSV, but no --filter-source-csv path was provided or saved in the profile.")}
				}
			case filterDataSourceScriptRunner:
				if !c.FilterScriptRunnerInstalled {
					return []Finding{newFinding(SeverityError, "pre_migrate_filter_endpoint_not_installed", "Filter team-ID updates are in scope, but the ScriptRunner endpoint is not marked installed. Install it first or switch the method to DB CSV.")}
				}
				if strings.TrimSpace(c.SourceBaseURL) == "" {
					return []Finding{newFinding(SeverityError, "pre_migrate_filter_endpoint_missing_source", "Filter team-ID updates are in scope and the configured method is ScriptRunner, but no source Jira base URL is available.")}
				}
			}
		}
	}

	return nil
}

func normalizeIssueProjectScope(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || strings.EqualFold(trimmed, "all") {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		projectKey := strings.ToUpper(strings.TrimSpace(part))
		if projectKey == "" {
			continue
		}
		for _, r := range projectKey {
			if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' && r != '-' {
				return nil, fmt.Errorf("unsupported issue project scope %q; use all or a comma-separated list of Jira project keys", raw)
			}
		}
		if _, ok := seen[projectKey]; ok {
			continue
		}
		seen[projectKey] = struct{}{}
		out = append(out, projectKey)
	}
	if len(out) == 0 {
		return nil, nil
	}
	sort.Strings(out)
	return out, nil
}

func validateIdentityMappingFile(path string) []Finding {
	cleanPath, err := cleanInputFilePath("identity mapping", path)
	if err != nil {
		return []Finding{newFinding(SeverityError, "identity_mapping_unreadable", err.Error())}
	}
	file, err := os.OpenInRoot(filepath.Dir(cleanPath), filepath.Base(cleanPath))
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

func envIsSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

func normalizeFilterDataSource(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case filterDataSourceScriptRunner:
		return filterDataSourceScriptRunner
	case filterDataSourceDatabaseCSV, "db", "csv", "database", "database-csv":
		return filterDataSourceDatabaseCSV
	default:
		return ""
	}
}

func stringFlagProvided(args []string, name string) bool {
	for i, arg := range args {
		if arg == name {
			return i < len(args)-1
		}
		if strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

func hasHelpArg(args []string) bool {
	for _, arg := range args {
		if isHelpArg(arg) {
			return true
		}
	}
	return false
}

func isHelpArg(arg string) bool {
	switch strings.TrimSpace(arg) {
	case "-h", "--help":
		return true
	default:
		return false
	}
}

func ensureOutputDir(path string) error {
	if path == "" {
		return nil
	}
	cleanPath, err := cleanOutputDirPath(path)
	if err != nil {
		return err
	}
	return os.MkdirAll(cleanPath, 0o755)
}

func defaultOutputPath(cfg Config) string {
	ext := string(cfg.ReportFormat)
	return outputPathForName(cfg, fmt.Sprintf("%s-report.%s", strings.ReplaceAll(cfg.Command, " ", "-"), ext))
}

func defaultOutputPathForFormat(cfg Config, format ReportFormat) string {
	ext := string(format)
	return outputPathForName(cfg, fmt.Sprintf("%s-report.%s", strings.ReplaceAll(cfg.Command, " ", "-"), ext))
}
