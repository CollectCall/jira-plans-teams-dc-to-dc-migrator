package app

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	envConfigPath = "TEAMS_MIGRATOR_CONFIG_PATH"
	envProfile    = "TEAMS_MIGRATOR_PROFILE"
)

type ProfileStore struct {
	CurrentProfile string
	Profiles       map[string]SavedProfile
}

type SavedProfile struct {
	SourceBaseURL               string
	TargetBaseURL               string
	IdentityMappingFile         string
	IdentityMappingSet          bool
	TeamsFile                   string
	PersonsFile                 string
	ResourcesFile               string
	IssuesCSV                   string
	FilterSourceCSV             string
	OutputDir                   string
	ReportFormat                string
	TeamScope                   string
	IssueProjectScope           string
	FilterTeamIDsInScope        bool
	FilterTeamIDsInScopeSet     bool
	ParentLinkInScope           bool
	ParentLinkInScopeSet        bool
	FilterDataSource            string
	FilterScriptRunnerInstalled bool
	FilterScriptRunnerEndpoint  string
}

func defaultConfigPath() string {
	if configured := strings.TrimSpace(os.Getenv(envConfigPath)); configured != "" {
		return configured
	}
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		return "config.yaml"
	}
	return filepath.Join(dir, "teams-migrator", "config.yaml")
}

func loadProfileStore(path string) (ProfileStore, error) {
	store := ProfileStore{Profiles: map[string]SavedProfile{}}

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return store, nil
		}
		return store, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	currentProfile := ""
	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		switch {
		case !strings.HasPrefix(line, " "):
			if strings.HasPrefix(trimmed, "current_profile:") {
				store.CurrentProfile = parseYAMLScalar(strings.TrimSpace(strings.TrimPrefix(trimmed, "current_profile:")))
			}
		case strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    "):
			if strings.HasSuffix(trimmed, ":") {
				currentProfile = strings.TrimSuffix(trimmed, ":")
				store.Profiles[currentProfile] = SavedProfile{}
			}
		case strings.HasPrefix(line, "    "):
			if currentProfile == "" {
				continue
			}
			parts := strings.SplitN(trimmed, ":", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := parseYAMLScalar(strings.TrimSpace(parts[1]))
			profile := store.Profiles[currentProfile]
			assignProfileField(&profile, key, value)
			store.Profiles[currentProfile] = profile
		}
	}
	if err := scanner.Err(); err != nil {
		return store, err
	}

	return store, nil
}

func saveProfileStore(path string, store ProfileStore) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	var lines []string
	lines = append(lines, fmt.Sprintf("current_profile: %s", yamlQuote(store.CurrentProfile)))
	lines = append(lines, "profiles:")

	names := make([]string, 0, len(store.Profiles))
	for name := range store.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		lines = append(lines, fmt.Sprintf("  %s:", name))
		profile := store.Profiles[name]
		for _, entry := range profileEntries(profile) {
			lines = append(lines, fmt.Sprintf("    %s: %s", entry.key, yamlQuote(entry.value)))
		}
	}

	content := strings.Join(lines, "\n") + "\n"
	return os.WriteFile(path, []byte(content), 0o600)
}

func applySavedProfile(cfg *Config, profile SavedProfile) {
	if cfg.SourceBaseURL == "" {
		cfg.SourceBaseURL = profile.SourceBaseURL
	}
	if cfg.TargetBaseURL == "" {
		cfg.TargetBaseURL = profile.TargetBaseURL
	}
	if cfg.IdentityMappingFile == "" {
		cfg.IdentityMappingFile = profile.IdentityMappingFile
	}
	if !cfg.IdentityMappingSet {
		cfg.IdentityMappingSet = profile.IdentityMappingSet || strings.TrimSpace(profile.IdentityMappingFile) != ""
	}
	if cfg.TeamsFile == "" {
		cfg.TeamsFile = profile.TeamsFile
	}
	if cfg.PersonsFile == "" {
		cfg.PersonsFile = profile.PersonsFile
	}
	if cfg.ResourcesFile == "" {
		cfg.ResourcesFile = profile.ResourcesFile
	}
	if cfg.IssuesCSV == "" {
		cfg.IssuesCSV = profile.IssuesCSV
	}
	if cfg.FilterSourceCSV == "" {
		cfg.FilterSourceCSV = profile.FilterSourceCSV
	}
	if cfg.OutputDir == "" {
		cfg.OutputDir = profile.OutputDir
	}
	if cfg.ReportFormat == "" {
		cfg.ReportFormat = ReportFormat(profile.ReportFormat)
	}
	if cfg.TeamScope == "" {
		cfg.TeamScope = profile.TeamScope
	}
	if cfg.IssueProjectScope == "" {
		cfg.IssueProjectScope = profile.IssueProjectScope
	}
	if !cfg.FilterTeamIDsInScopeSet {
		cfg.FilterTeamIDsInScope = profile.FilterTeamIDsInScope
		cfg.FilterTeamIDsInScopeSet = profile.FilterTeamIDsInScopeSet
	}
	if !cfg.ParentLinkInScopeSet {
		cfg.ParentLinkInScope = profile.ParentLinkInScope
		cfg.ParentLinkInScopeSet = profile.ParentLinkInScopeSet
	}
	if cfg.FilterDataSource == "" {
		cfg.FilterDataSource = profile.FilterDataSource
	}
	cfg.FilterScriptRunnerInstalled = cfg.FilterScriptRunnerInstalled || profile.FilterScriptRunnerInstalled
	if cfg.FilterScriptRunnerEndpoint == "" {
		cfg.FilterScriptRunnerEndpoint = profile.FilterScriptRunnerEndpoint
	}
}

func resolveProfile(cfg Config, store ProfileStore) SavedProfile {
	name := cfg.Profile
	if name == "" {
		if store.CurrentProfile != "" {
			name = store.CurrentProfile
		} else {
			name = "default"
		}
	}
	profile, ok := store.Profiles[name]
	if !ok {
		return SavedProfile{}
	}
	return profile
}

func savedProfileFromConfig(cfg Config, includeSecrets bool) SavedProfile {
	profile := SavedProfile{
		SourceBaseURL:               cfg.SourceBaseURL,
		TargetBaseURL:               cfg.TargetBaseURL,
		IdentityMappingFile:         cfg.IdentityMappingFile,
		IdentityMappingSet:          cfg.IdentityMappingSet || strings.TrimSpace(cfg.IdentityMappingFile) != "",
		TeamsFile:                   cfg.TeamsFile,
		PersonsFile:                 cfg.PersonsFile,
		ResourcesFile:               cfg.ResourcesFile,
		IssuesCSV:                   cfg.IssuesCSV,
		FilterSourceCSV:             cfg.FilterSourceCSV,
		OutputDir:                   cfg.OutputDir,
		ReportFormat:                string(cfg.ReportFormat),
		TeamScope:                   cfg.TeamScope,
		IssueProjectScope:           cfg.IssueProjectScope,
		FilterTeamIDsInScope:        cfg.FilterTeamIDsInScope,
		FilterTeamIDsInScopeSet:     cfg.FilterTeamIDsInScopeSet,
		ParentLinkInScope:           cfg.ParentLinkInScope,
		ParentLinkInScopeSet:        cfg.ParentLinkInScopeSet,
		FilterDataSource:            cfg.FilterDataSource,
		FilterScriptRunnerInstalled: cfg.FilterScriptRunnerInstalled,
		FilterScriptRunnerEndpoint:  cfg.FilterScriptRunnerEndpoint,
	}
	_ = includeSecrets
	return profile
}

type profileEntry struct {
	key   string
	value string
}

func profileEntries(profile SavedProfile) []profileEntry {
	return []profileEntry{
		{key: "source_base_url", value: profile.SourceBaseURL},
		{key: "target_base_url", value: profile.TargetBaseURL},
		{key: "identity_mapping_file", value: profile.IdentityMappingFile},
		{key: "identity_mapping_set", value: formatBool(profile.IdentityMappingSet)},
		{key: "teams_file", value: profile.TeamsFile},
		{key: "persons_file", value: profile.PersonsFile},
		{key: "resources_file", value: profile.ResourcesFile},
		{key: "issues_csv", value: profile.IssuesCSV},
		{key: "filter_source_csv", value: profile.FilterSourceCSV},
		{key: "output_dir", value: profile.OutputDir},
		{key: "report_format", value: profile.ReportFormat},
		{key: "team_scope", value: profile.TeamScope},
		{key: "issue_project_scope", value: profile.IssueProjectScope},
		{key: "filter_team_ids_in_scope", value: formatBool(profile.FilterTeamIDsInScope)},
		{key: "filter_team_ids_in_scope_set", value: formatBool(profile.FilterTeamIDsInScopeSet)},
		{key: "parent_link_in_scope", value: formatBool(profile.ParentLinkInScope)},
		{key: "parent_link_in_scope_set", value: formatBool(profile.ParentLinkInScopeSet)},
		{key: "filter_data_source", value: profile.FilterDataSource},
		{key: "filter_scriptrunner_installed", value: formatBool(profile.FilterScriptRunnerInstalled)},
		{key: "filter_scriptrunner_endpoint", value: profile.FilterScriptRunnerEndpoint},
	}
}

func assignProfileField(profile *SavedProfile, key, value string) {
	switch key {
	case "source_base_url":
		profile.SourceBaseURL = value
	case "target_base_url":
		profile.TargetBaseURL = value
	case "identity_mapping_file":
		profile.IdentityMappingFile = value
		profile.IdentityMappingSet = true
	case "identity_mapping_set":
		profile.IdentityMappingSet = parseBoolScalar(value)
	case "teams_file":
		profile.TeamsFile = value
	case "persons_file":
		profile.PersonsFile = value
	case "resources_file":
		profile.ResourcesFile = value
	case "issues_csv":
		profile.IssuesCSV = value
	case "filter_source_csv":
		profile.FilterSourceCSV = value
	case "output_dir":
		profile.OutputDir = value
	case "report_format":
		profile.ReportFormat = value
	case "team_scope":
		profile.TeamScope = value
	case "issue_project_scope":
		profile.IssueProjectScope = value
	case "filter_team_ids_in_scope":
		profile.FilterTeamIDsInScope = parseBoolScalar(value)
	case "filter_team_ids_in_scope_set":
		profile.FilterTeamIDsInScopeSet = parseBoolScalar(value)
	case "parent_link_in_scope":
		profile.ParentLinkInScope = parseBoolScalar(value)
	case "parent_link_in_scope_set":
		profile.ParentLinkInScopeSet = parseBoolScalar(value)
	case "filter_data_source":
		profile.FilterDataSource = normalizeFilterDataSource(value)
	case "filter_scriptrunner_installed":
		profile.FilterScriptRunnerInstalled = parseBoolScalar(value)
	case "filter_scriptrunner_endpoint":
		profile.FilterScriptRunnerEndpoint = value
	}
}

func yamlQuote(value string) string {
	return strconv.Quote(value)
}

func parseYAMLScalar(value string) string {
	if value == "" {
		return ""
	}
	unquoted, err := strconv.Unquote(value)
	if err == nil {
		return unquoted
	}
	return value
}

func formatBool(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func parseBoolScalar(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}
