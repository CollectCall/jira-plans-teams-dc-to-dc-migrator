package app

import (
	"encoding/json"
	"fmt"
	"os"
)

type configShowOutput struct {
	ConfigPath      string                    `json:"configPath"`
	CurrentProfile  string                    `json:"currentProfile"`
	SelectedProfile string                    `json:"selectedProfile"`
	Profile         map[string]any            `json:"profile,omitempty"`
	Profiles        map[string]map[string]any `json:"profiles,omitempty"`
}

func runConfigShow(cfg Config) error {
	configPath, err := cleanInputFilePath("config", cfg.ConfigPath)
	if err != nil {
		return err
	}
	cfg.ConfigPath = configPath
	store, err := loadProfileStore(cfg.ConfigPath)
	if err != nil {
		return err
	}

	selectedProfile := cfg.Profile
	if selectedProfile == "" {
		if store.CurrentProfile != "" {
			selectedProfile = store.CurrentProfile
		} else {
			selectedProfile = "default"
		}
	}

	out := configShowOutput{
		ConfigPath:      cfg.ConfigPath,
		CurrentProfile:  store.CurrentProfile,
		SelectedProfile: selectedProfile,
		Profiles:        map[string]map[string]any{},
	}

	for name, profile := range store.Profiles {
		out.Profiles[name] = profileToMap(profile)
	}
	if profile, ok := store.Profiles[selectedProfile]; ok {
		out.Profile = profileToMap(profile)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(out); err != nil {
		return fmt.Errorf("encoding config output: %w", err)
	}
	return nil
}

func profileToMap(profile SavedProfile) map[string]any {
	out := map[string]any{
		"source_base_url":                     profile.SourceBaseURL,
		"target_base_url":                     profile.TargetBaseURL,
		"identity_mapping_file":               profile.IdentityMappingFile,
		"identity_mapping_set":                profile.IdentityMappingSet,
		"teams_file":                          profile.TeamsFile,
		"persons_file":                        profile.PersonsFile,
		"resources_file":                      profile.ResourcesFile,
		"issues_csv":                          profile.IssuesCSV,
		"filter_source_csv":                   profile.FilterSourceCSV,
		"output_dir":                          profile.OutputDir,
		"team_scope":                          profile.TeamScope,
		"issue_project_scope":                 profile.IssueProjectScope,
		"issue_team_ids_in_scope":             profile.IssueTeamIDsInScope,
		"issue_team_ids_in_scope_set":         profile.IssueTeamIDsInScopeSet,
		"filter_team_ids_in_scope":            profile.FilterTeamIDsInScope,
		"filter_team_ids_in_scope_set":        profile.FilterTeamIDsInScopeSet,
		"parent_link_in_scope":                profile.ParentLinkInScope,
		"parent_link_in_scope_set":            profile.ParentLinkInScopeSet,
		"filter_data_source":                  profile.FilterDataSource,
		"filter_scriptrunner_installed":       profile.FilterScriptRunnerInstalled,
		"filter_scriptrunner_endpoint":        profile.FilterScriptRunnerEndpoint,
		"post_migrate_issue_workers":          profile.PostMigrateIssueWorkers,
		"post_migrate_issue_fallback_workers": profile.PostMigrateIssueFallbackWorkers,
	}
	return out
}
