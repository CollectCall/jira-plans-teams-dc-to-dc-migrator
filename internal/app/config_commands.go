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
		"source_base_url":       profile.SourceBaseURL,
		"target_base_url":       profile.TargetBaseURL,
		"identity_mapping_file": profile.IdentityMappingFile,
		"teams_file":            profile.TeamsFile,
		"persons_file":          profile.PersonsFile,
		"resources_file":        profile.ResourcesFile,
		"issues_csv":            profile.IssuesCSV,
		"output_dir":            profile.OutputDir,
		"report_format":         profile.ReportFormat,
		"team_scope":            profile.TeamScope,
	}
	return out
}
