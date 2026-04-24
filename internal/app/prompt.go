package app

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	term "github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/internal/thirdparty/golang.org/x/term"
)

type wizardContext struct {
	Title  string
	Reader *bufio.Reader
	Step   int
}

type wizardField struct {
	Label        string
	Description  string
	InputHelp    string
	ArtifactInfo string
	Example      string
	Default      string
	Optional     bool
}

func completeConfigInteractively(cfg *Config) error {
	if !isInteractiveTerminal() {
		return nil
	}

	switch cfg.Command {
	case "report":
		if cfg.ReportInput == "" {
			wizard := newWizard("Teams Migrator", "Guided report export")
			value, err := wizard.value(wizardField{
				Label:        "Report input JSON path",
				Description:  "Provide the path to a previously generated JSON report that you want to re-render as JSON or CSV.",
				InputHelp:    "Type a file path to an existing JSON report on disk.",
				ArtifactInfo: "This is typically something like out/migrate-report.json.",
				Example:      "out/migrate-report.json",
			})
			if err != nil {
				return err
			}
			cfg.ReportInput = value
		}
		return nil
	case "migrate":
	default:
		return nil
	}

	if !needsInteractiveSetup(*cfg) {
		return nil
	}

	wizard := newWizard("Teams Migrator", "Guided run setup")
	wizard.intro([]string{
		"This tool helps migrate teams used by Jira Advanced Roadmaps (formerly Portfolio, now called Plans in Cloud) between Server/Data Center instances, prepares mapping artifacts, and can fix related Jira references after migration.",
	})
	if cfg.ProfileLoaded {
		wizard.note(fmt.Sprintf("Loaded defaults from saved profile %q.", cfg.Profile))
	}

	if !cfg.IdentityMappingSet {
		value, err := wizard.value(wizardField{
			Label:        "Identity mapping CSV",
			Description:  "This optional file connects source users to target users so team membership can be rebuilt correctly.",
			InputHelp:    "Type the path to an existing CSV file on disk, or press Enter to skip it.",
			ArtifactInfo: "If you skip this, the tool will try to auto-resolve mappings by matching identical source and target emails and will generate a reviewable mapping artifact.",
			Example:      "/Users/you/migration/identity-mapping.csv",
			Optional:     true,
		})
		if err != nil {
			return err
		}
		cfg.IdentityMappingFile = value
		cfg.IdentityMappingSet = true
	}

	sourceMode := inferSourceMode(*cfg)
	usePreparedSourceArtifacts := canUsePreparedSourceArtifacts(*cfg)
	if sourceMode == "" && !usePreparedSourceArtifacts {
		value, err := wizard.choice(wizardField{
			Label:        "Source data mode",
			Description:  "Choose whether source teams/persons/resources should come from exported JSON files or directly from the source Jira API.",
			ArtifactInfo: "API mode is the default and reads these datasets directly from Jira for a guided dry run. File mode is available when you already have teams, persons, and resources JSON exports.",
			Default:      "api",
		}, []string{"file", "api"})
		if err != nil {
			return err
		}
		sourceMode = value
	}

	if sourceMode == "file" {
		var err error
		if cfg.TeamsFile == "" {
			cfg.TeamsFile, err = wizard.value(wizardField{
				Label:        "Source teams JSON",
				Description:  "This artifact contains the exported source teams dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /team or an array of TeamDTO records.",
				Example:      "/path/to/teams.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.PersonsFile == "" {
			cfg.PersonsFile, err = wizard.value(wizardField{
				Label:        "Source persons JSON",
				Description:  "This artifact contains the exported source persons dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /person or an array of PersonDTO records.",
				Example:      "/path/to/persons.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.ResourcesFile == "" {
			cfg.ResourcesFile, err = wizard.value(wizardField{
				Label:        "Source resources JSON",
				Description:  "This artifact contains the exported source team membership dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /resource or an array of ResourceDTO records.",
				Example:      "/path/to/resources.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.SourceBaseURL == "" {
			cfg.SourceBaseURL, err = wizard.value(wizardField{
				Label:        "Optional source Jira base URL",
				Description:  "This is optional in file mode, but recommended if you want the tool to discover the Teams issue field and export issues that use it.",
				InputHelp:    "Type the Jira base URL, or press Enter to skip.",
				ArtifactInfo: "If provided, the tool will query Jira issue fields and generate the pre-migration issues-with-teams CSV.",
				Example:      "https://source.example.com/jira",
				Optional:     true,
			})
			if err != nil {
				return err
			}
		}
	} else if !usePreparedSourceArtifacts {
		var err error
		if cfg.SourceBaseURL == "" {
			cfg.SourceBaseURL, err = wizard.value(wizardField{
				Label:        "Source Jira base URL",
				Description:  "This is the Jira Server/Data Center instance the tool will read from.",
				InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
				ArtifactInfo: "The Teams API path is added automatically.",
				Example:      "https://source.example.com/jira",
			})
			if err != nil {
				return err
			}
		}
	}

	if sourceNeedsAuth(*cfg) {
		if err := promptForAuth(wizard, "source", &cfg.SourceUsername, &cfg.SourcePassword); err != nil {
			return err
		}
	}

	if cfg.TargetBaseURL == "" {
		value, err := wizard.value(wizardField{
			Label:        "Target Jira base URL",
			Description:  "This is the Jira Server/Data Center instance the tool will write to in apply mode.",
			InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
			ArtifactInfo: "The tool deduplicates teams here and, in apply mode, creates teams and resources here.",
			Example:      "https://target.example.com/jira",
		})
		if err != nil {
			return err
		}
		cfg.TargetBaseURL = value
	}
	if targetNeedsAuth(*cfg) {
		if err := promptForAuth(wizard, "target", &cfg.TargetUsername, &cfg.TargetPassword); err != nil {
			return err
		}
	}

	if cfg.Command == "migrate" && !cfg.PhaseExplicit {
		value, err := wizard.choice(wizardField{
			Label:       "Migration phase",
			Description: "Choose the next phase to run.",
			InputHelp:   "Type the number of your choice and press Enter.",
			ArtifactInfo: strings.Join([]string{
				"pre-migrate: fetch source/target data and generate comparison artifacts only.",
				"migrate: create destination teams and memberships from the current mappings.",
			}, " "),
			Default: defaultMigrationPhase(cfg.Command),
		}, availableMigrationPhases(*cfg))
		if err != nil {
			return err
		}
		cfg.Phase = value
		cfg.PhaseExplicit = true
	}

	if cfg.TeamScope == "" || cfg.TeamScope == "all" {
		value, err := wizard.choice(wizardField{
			Label:        "Team migration scope",
			Description:  "Choose whether this run should include all teams, only shared teams, or only non-shared teams.",
			InputHelp:    "Type the number of your choice and press Enter.",
			ArtifactInfo: "Non-shared teams cannot be created by this tool. They must already exist in the destination plan before migration, so splitting shared and non-shared runs is supported.",
			Default:      "all",
		}, []string{"all", "shared-only", "non-shared-only"})
		if err != nil {
			return err
		}
		cfg.TeamScope = value
	}

	if strings.TrimSpace(cfg.IssueProjectScope) == "" || strings.EqualFold(strings.TrimSpace(cfg.IssueProjectScope), "all") {
		value, err := wizard.value(wizardField{
			Label:        "Issue correction project scope",
			Description:  "Choose which Jira projects should be in scope for issue-based correction exports and post-migrate rewrites.",
			InputHelp:    "Type all, or a comma-separated list like ABC,DEF.",
			ArtifactInfo: "This scope is applied to issue/team and Parent Link correction flows. Filters are not project-scoped.",
			Default:      nonEmptyDefault(cfg.IssueProjectScope, "all"),
		})
		if err != nil {
			return err
		}
		cfg.IssueProjectScope = value
	}

	return nil
}

func completeMigrateSessionInteractively(cfg *Config) error {
	if !isInteractiveTerminal() {
		return nil
	}

	wizard := newWizard("Teams Migrator", "Guided migrate run")
	introLines := []string{
		"This tool walks through the migration in phases without re-asking saved answers.",
		"Pre-migrate is read-only and writes review artifacts. Migrate previews and then creates safe destination teams and memberships after confirmation. Post-migrate is a later correction phase for Jira issue, Parent Link, and filter references after destination team IDs exist.",
	}
	if cfg.ProfileLoaded {
		introLines = append(introLines, fmt.Sprintf("Loaded defaults from saved profile %q.", cfg.Profile))
	}
	wizard.intro(introLines)

	if !cfg.PhaseExplicit {
		value, err := wizard.choice(wizardField{
			Label:       "Start or continue at",
			Description: "Choose where this run should start. The same in-memory credentials and answers will be reused if you continue to the next phase.",
			InputHelp:   "Type the number of your choice and press Enter.",
			ArtifactInfo: strings.Join([]string{
				"pre-migrate: fetch source/target data and generate comparison artifacts only.",
				"migrate: preview and then create destination teams.",
			}, " "),
			Default: defaultMigrationPhase(cfg.Command),
		}, availableMigrationPhases(*cfg))
		if err != nil {
			return err
		}
		cfg.Phase = value
		cfg.PhaseExplicit = true
	}

	if !cfg.IdentityMappingSet {
		value, err := wizard.value(wizardField{
			Label:        "Identity mapping CSV",
			Description:  "This optional file connects source users to target users so team membership can be rebuilt correctly.",
			InputHelp:    "Type the path to an existing CSV file on disk, or press Enter to skip it.",
			ArtifactInfo: "If you skip this, the tool will try to auto-resolve mappings by matching identical source and target emails and will generate a reviewable mapping artifact.",
			Example:      "/Users/you/migration/identity-mapping.csv",
			Optional:     true,
		})
		if err != nil {
			return err
		}
		cfg.IdentityMappingFile = value
		cfg.IdentityMappingSet = true
	}

	sourceMode := inferSourceMode(*cfg)
	usePreparedSourceArtifacts := canUsePreparedSourceArtifacts(*cfg)
	if sourceMode == "" && !usePreparedSourceArtifacts {
		value, err := wizard.choice(wizardField{
			Label:        "Source data mode",
			Description:  "Choose whether source teams/persons/resources should come from exported JSON files or directly from the source Jira API.",
			ArtifactInfo: "API mode is the default and reads these datasets directly from Jira for a guided dry run. File mode is available when you already have teams, persons, and resources JSON exports.",
			Default:      "api",
		}, []string{"file", "api"})
		if err != nil {
			return err
		}
		sourceMode = value
	}

	if sourceMode == "file" {
		var err error
		if cfg.TeamsFile == "" {
			cfg.TeamsFile, err = wizard.value(wizardField{
				Label:        "Source teams JSON",
				Description:  "This artifact contains the exported source teams dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /team or an array of TeamDTO records.",
				Example:      "/path/to/teams.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.PersonsFile == "" {
			cfg.PersonsFile, err = wizard.value(wizardField{
				Label:        "Source persons JSON",
				Description:  "This artifact contains the exported source persons dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /person or an array of PersonDTO records.",
				Example:      "/path/to/persons.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.ResourcesFile == "" {
			cfg.ResourcesFile, err = wizard.value(wizardField{
				Label:        "Source resources JSON",
				Description:  "This artifact contains the exported source team membership dataset.",
				InputHelp:    "Type the path to an existing JSON file on disk.",
				ArtifactInfo: "The file should contain the payload returned by GET /resource or an array of ResourceDTO records.",
				Example:      "/path/to/resources.json",
			})
			if err != nil {
				return err
			}
		}
		if cfg.SourceBaseURL == "" {
			cfg.SourceBaseURL, err = wizard.value(wizardField{
				Label:        "Optional source Jira base URL",
				Description:  "This is optional in file mode, but recommended if the run needs Jira issue-field discovery or correction exports.",
				InputHelp:    "Type the Jira base URL, or press Enter to skip.",
				ArtifactInfo: "If provided, the tool can discover Jira issue fields and generate pre- and post-migration correction exports.",
				Example:      "https://source.example.com/jira",
				Optional:     true,
			})
			if err != nil {
				return err
			}
		}
	} else if !usePreparedSourceArtifacts {
		var err error
		if cfg.SourceBaseURL == "" {
			cfg.SourceBaseURL, err = wizard.value(wizardField{
				Label:        "Source Jira base URL",
				Description:  "This is the Jira Server/Data Center instance the tool will read from.",
				InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
				ArtifactInfo: "The Teams API path is added automatically.",
				Example:      "https://source.example.com/jira",
			})
			if err != nil {
				return err
			}
		}
	}

	if sourceNeedsAuth(*cfg) {
		if err := promptForAuth(wizard, "source", &cfg.SourceUsername, &cfg.SourcePassword); err != nil {
			return err
		}
	}

	if cfg.TargetBaseURL == "" {
		value, err := wizard.value(wizardField{
			Label:        "Target Jira base URL",
			Description:  "This is the Jira Server/Data Center instance the tool will write to in apply mode.",
			InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
			ArtifactInfo: "The tool deduplicates teams here and, in apply mode, creates teams and resources here.",
			Example:      "https://target.example.com/jira",
		})
		if err != nil {
			return err
		}
		cfg.TargetBaseURL = value
	}
	if targetNeedsAuth(*cfg) {
		if err := promptForAuth(wizard, "target", &cfg.TargetUsername, &cfg.TargetPassword); err != nil {
			return err
		}
	}

	return nil
}

func needsInteractiveSetup(cfg Config) bool {
	if cfg.Command == "migrate" && !cfg.DryRun {
		return true
	}

	sourceMode := inferSourceMode(cfg)
	switch sourceMode {
	case "":
		return true
	case "file":
		if cfg.TeamsFile == "" || cfg.PersonsFile == "" || cfg.ResourcesFile == "" {
			return true
		}
	case "api":
		if cfg.SourceBaseURL == "" {
			return true
		}
	}

	if cfg.TargetBaseURL == "" {
		return true
	}

	if sourceNeedsAuth(cfg) || targetNeedsAuth(cfg) {
		return true
	}

	return false
}

func inferSourceMode(cfg Config) string {
	if cfg.SourceBaseURL != "" && cfg.TeamsFile == "" && cfg.PersonsFile == "" && cfg.ResourcesFile == "" {
		return "api"
	}
	if cfg.TeamsFile != "" || cfg.PersonsFile != "" || cfg.ResourcesFile != "" {
		return "file"
	}
	return ""
}

func inferSourceModeOrDefault(cfg Config) string {
	mode := inferSourceMode(cfg)
	if mode == "" {
		return "file"
	}
	return mode
}

func sourceNeedsAuth(cfg Config) bool {
	if canUsePreparedSourceArtifacts(cfg) {
		return false
	}
	return strings.TrimSpace(cfg.SourceBaseURL) != "" && (strings.TrimSpace(cfg.SourceUsername) == "" || strings.TrimSpace(cfg.SourcePassword) == "")
}

func canUsePreparedSourceArtifacts(cfg Config) bool {
	return postMigrateCanUsePreparedArtifacts(cfg) || migrateCanUsePreparedArtifacts(cfg)
}

func targetNeedsAuth(cfg Config) bool {
	return strings.TrimSpace(cfg.TargetBaseURL) != "" && (strings.TrimSpace(cfg.TargetUsername) == "" || strings.TrimSpace(cfg.TargetPassword) == "")
}

func runConfigInitWizard(cfg Config) error {
	if cfg.NoInput {
		return errors.New("init requires interactive input; remove --no-input")
	}
	if !isInteractiveTerminal() {
		return errors.New("init requires a terminal")
	}

	store, err := loadProfileStore(cfg.ConfigPath)
	if err != nil {
		return err
	}

	wizard := newWizard("Teams Migrator", "Init")
	wizard.intro([]string{
		"This wizard creates or updates a reusable profile for future runs.",
		"Usernames and passwords are not stored in the profile. The CLI prompts for them at runtime when needed.",
	})
	wizard.note(fmt.Sprintf("Config file: %s", cfg.ConfigPath))

	profileName, editingExisting, err := chooseInitProfile(wizard, store, &cfg)
	if err != nil {
		return err
	}
	cfg.Profile = profileName

	cfg.IdentityMappingFile, err = wizard.value(wizardField{
		Label:        "Default identity mapping CSV",
		Description:  "This optional file connects source users to target users so team membership can be rebuilt correctly.",
		InputHelp:    "Type the path to an existing CSV file on disk, or press Enter to skip it.",
		ArtifactInfo: "If skipped, the tool will auto-resolve identical emails where possible and generate a reviewable mapping artifact.",
		Example:      "/Users/you/migration/identity-mapping.csv",
		Default:      cfg.IdentityMappingFile,
		Optional:     true,
	})
	if err != nil {
		return err
	}
	cfg.IdentityMappingSet = true

	sourceMode, err := wizard.choice(wizardField{
		Label:        "Default source mode",
		Description:  "Choose whether this profile should default to source JSON exports or direct source Jira API access.",
		ArtifactInfo: "File mode requires teams, persons, and resources JSON exports. API mode reads those datasets at runtime and supports issue-field discovery.",
		Default:      inferSourceModeOrDefault(cfg),
	}, []string{"file", "api"})
	if err != nil {
		return err
	}

	if sourceMode == "api" {
		cfg.SourceBaseURL, err = wizard.value(wizardField{
			Label:        "Default source Jira base URL",
			Description:  "This is the source Jira Server/Data Center instance for this profile.",
			InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
			ArtifactInfo: "The Teams API path is added automatically.",
			Example:      "https://source.example.com/jira",
			Default:      cfg.SourceBaseURL,
		})
		if err != nil {
			return err
		}
		cfg.TeamsFile = ""
		cfg.PersonsFile = ""
		cfg.ResourcesFile = ""
	} else {
		cfg.TeamsFile, err = wizard.value(wizardField{
			Label:        "Default source teams JSON",
			Description:  "This artifact contains the exported source teams dataset.",
			InputHelp:    "Type the path to an existing JSON file on disk.",
			ArtifactInfo: "Used when no explicit --teams-file flag is provided.",
			Example:      "/path/to/teams.json",
			Default:      cfg.TeamsFile,
		})
		if err != nil {
			return err
		}
		cfg.PersonsFile, err = wizard.value(wizardField{
			Label:        "Default source persons JSON",
			Description:  "This artifact contains the exported source persons dataset.",
			InputHelp:    "Type the path to an existing JSON file on disk.",
			ArtifactInfo: "Used when no explicit --persons-file flag is provided.",
			Example:      "/path/to/persons.json",
			Default:      cfg.PersonsFile,
		})
		if err != nil {
			return err
		}
		cfg.ResourcesFile, err = wizard.value(wizardField{
			Label:        "Default source resources JSON",
			Description:  "This artifact contains the exported source team membership dataset.",
			InputHelp:    "Type the path to an existing JSON file on disk.",
			ArtifactInfo: "Used when no explicit --resources-file flag is provided.",
			Example:      "/path/to/resources.json",
			Default:      cfg.ResourcesFile,
		})
		if err != nil {
			return err
		}
		cfg.SourceBaseURL, err = wizard.value(wizardField{
			Label:        "Optional source Jira base URL",
			Description:  "Optional in file mode, but recommended for issue-field discovery and pre-migration issue export.",
			InputHelp:    "Type the Jira base URL, or press Enter to skip.",
			ArtifactInfo: "If provided, the tool can search the source Jira instance for issues that use the Teams field.",
			Example:      "https://source.example.com/jira",
			Default:      cfg.SourceBaseURL,
			Optional:     true,
		})
		if err != nil {
			return err
		}
	}

	cfg.TargetBaseURL, err = wizard.value(wizardField{
		Label:        "Default target Jira base URL",
		Description:  "This is the destination Jira Server/Data Center instance for this profile.",
		InputHelp:    "Type the Jira base URL. Do not include /rest/teams-api/1.0.",
		ArtifactInfo: "The Teams API path is added automatically.",
		Example:      "https://target.example.com/jira",
		Default:      cfg.TargetBaseURL,
	})
	if err != nil {
		return err
	}

	cfg.OutputDir, err = wizard.value(wizardField{
		Label:        "Default output directory",
		Description:  "Reports and derived artifacts are written here by default.",
		InputHelp:    "Type a directory path. It will be created if needed.",
		ArtifactInfo: "Relative paths are resolved from the current working directory at runtime.",
		Default:      nonEmptyDefault(cfg.OutputDir, "out"),
	})
	if err != nil {
		return err
	}
	teamScope, err := wizard.choice(wizardField{
		Label:        "Default team migration scope",
		Description:  "Choose whether this profile should migrate all teams, only shared teams, or only non-shared teams by default.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: "Non-shared teams must already exist in the destination plan before migration. Use shared-only first if you want a two-batch flow.",
		Default:      nonEmptyDefault(cfg.TeamScope, "all"),
	}, []string{"all", "shared-only", "non-shared-only"})
	if err != nil {
		return err
	}
	cfg.TeamScope = teamScope

	cfg.IssueTeamIDsInScope = true
	cfg.IssueTeamIDsInScopeSet = true
	cfg.ParentLinkInScope = strings.TrimSpace(cfg.SourceBaseURL) != ""
	cfg.ParentLinkInScopeSet = false
	cfg.FilterTeamIDsInScope = filterReferenceExportsAvailable(cfg)
	cfg.FilterTeamIDsInScopeSet = false

	if issueTeamCorrectionsInScope(cfg) || cfg.ParentLinkInScope {
		issueProjectScope, err := wizard.value(wizardField{
			Label:        "Default issue correction project scope",
			Description:  "Choose which Jira projects should be in scope for issue-based correction exports and post-migrate rewrites by default.",
			InputHelp:    "Type all, or a comma-separated list like ABC,DEF.",
			ArtifactInfo: "This scope applies to issue/team and Parent Link correction flows. Filters are not project-scoped.",
			Default:      nonEmptyDefault(cfg.IssueProjectScope, "all"),
		})
		if err != nil {
			return err
		}
		cfg.IssueProjectScope = issueProjectScope
	}

	if err := configureInitCorrectionScopes(wizard, &cfg); err != nil {
		return err
	}

	setCurrent := "yes"
	if !editingExisting || store.CurrentProfile != profileName {
		setCurrent, err = wizard.choice(wizardField{
			Label:       "Set as current profile",
			Description: "If set to yes, this profile becomes the default when you run teams-migrator without --profile.",
			InputHelp:   "Type the number of your choice and press Enter.",
			Default:     defaultYesNoForCurrentProfile(store, profileName),
		}, []string{"yes", "no"})
		if err != nil {
			return err
		}
	}

	store.Profiles[profileName] = savedProfileFromConfig(cfg, false)
	if setCurrent == "yes" {
		store.CurrentProfile = profileName
	} else if store.CurrentProfile == "" {
		store.CurrentProfile = profileName
	}

	if ok, err := wizard.confirm(wizardField{
		Label:        "Save profile",
		Description:  "Review the summary below and confirm that you want to write this profile to disk.",
		ArtifactInfo: profileSummary(cfg),
		InputHelp:    "Type yes to save, or anything else to cancel.",
		Default:      "yes",
	}, "yes"); err != nil {
		return err
	} else if !ok {
		return errors.New("init cancelled")
	}

	if err := saveProfileStore(cfg.ConfigPath, store); err != nil {
		return err
	}

	wizard.success(
		"Profile saved",
		fmt.Sprintf("Saved profile %q to %s", profileName, cfg.ConfigPath),
		[]string{
			fmt.Sprintf("Inspect profile: ./bin/teams-migrator config show --profile %s", profileName),
			fmt.Sprintf("Next: ./bin/teams-migrator migrate --profile %s", profileName),
		},
	)
	return nil
}

func chooseInitProfile(wizard *wizardContext, store ProfileStore, cfg *Config) (string, bool, error) {
	if strings.TrimSpace(cfg.Profile) != "" {
		profileName := strings.TrimSpace(cfg.Profile)
		if profile, ok := store.Profiles[profileName]; ok {
			applySavedProfile(cfg, profile)
			return profileName, true, nil
		}
		return profileName, false, nil
	}

	names := profileNames(store)
	if len(names) == 0 {
		profileName, err := wizard.value(wizardField{
			Label:        "Profile name",
			Description:  "Choose a short profile name for this environment or migration setup.",
			ArtifactInfo: "Examples: default, prod, dc-migration-test",
			Default:      defaultProfileName(store, cfg.Profile),
		})
		return profileName, false, err
	}

	action, err := wizard.choice(wizardField{
		Label:       "Profile action",
		Description: "Choose whether to edit an existing profile or create a new profile.",
		InputHelp:   "Type the number of your choice and press Enter.",
		Default:     "edit existing",
	}, []string{"edit existing", "create new"})
	if err != nil {
		return "", false, err
	}

	if action == "edit existing" {
		profileName, err := promptProfileSelection(store, names)
		if err != nil {
			return "", false, err
		}
		applySavedProfile(cfg, store.Profiles[profileName])
		return profileName, true, nil
	}

	profileName, err := wizard.value(wizardField{
		Label:        "New profile name",
		Description:  "Choose a short profile name for this environment or migration setup.",
		ArtifactInfo: "Examples: default, prod, dc-migration-test",
		Default:      nextNewProfileName(store),
	})
	if err != nil {
		return "", false, err
	}
	return profileName, false, nil
}

func promptForAuth(wizard *wizardContext, label string, username, password *string) error {
	if strings.TrimSpace(*username) != "" && strings.TrimSpace(*password) != "" {
		return nil
	}
	if strings.TrimSpace(*username) == "" {
		user, err := wizard.value(wizardField{
			Label:       fmt.Sprintf("%s username", titleCase(label)),
			Description: fmt.Sprintf("Enter the username for basic authentication against the %s Jira instance.", label),
		})
		if err != nil {
			return err
		}
		*username = user
	}
	if strings.TrimSpace(*password) == "" {
		pass, err := wizard.secret(wizardField{
			Label:       fmt.Sprintf("%s password", titleCase(label)),
			Description: fmt.Sprintf("Enter the password for basic authentication against the %s Jira instance.", label),
			InputHelp:   "Type the password value. Input is hidden.",
		})
		if err != nil {
			return err
		}
		*password = pass
	}
	return nil
}

func newWizard(title, subtitle string) *wizardContext {
	return &wizardContext{
		Title:  fmt.Sprintf("%s | %s", title, subtitle),
		Reader: bufio.NewReader(os.Stdin),
	}
}

func (w *wizardContext) intro(lines []string) {
	renderWizardSection(w.Title, "Welcome", lines, "", "", "", "Press Enter to continue.")
	_, _ = w.Reader.ReadString('\n')
}

func (w *wizardContext) note(message string) {
	renderWizardSection(w.Title, "Note", []string{message}, "", "", "", "Press Enter to continue.")
	_, _ = w.Reader.ReadString('\n')
}

func (w *wizardContext) noteLines(lines []string) {
	renderWizardSection(w.Title, "Note", lines, "", "", "", "Press Enter to continue.")
	_, _ = w.Reader.ReadString('\n')
}

func (w *wizardContext) success(title, message string, next []string) {
	lines := []string{message}
	if len(next) > 0 {
		lines = append(lines, "", "Suggested next steps:")
		lines = append(lines, next...)
	}
	renderWizardSection(w.Title, title, lines, "", "", "", "Press Enter to finish.")
	_, _ = w.Reader.ReadString('\n')
}

func (w *wizardContext) value(field wizardField) (string, error) {
	w.Step++
	renderWizardSection(w.Title, fmt.Sprintf("Step %d | %s", w.Step, field.Label), []string{field.Description}, field.InputHelp, field.ArtifactInfo, field.Example, promptFooter(field.Default, field.Optional))
	return readLine(w.Reader, field.Default)
}

func (w *wizardContext) choice(field wizardField, choices []string) (string, error) {
	w.Step++
	theme := currentUITheme()
	for {
		lines := []string{field.Description, "", "Choices:"}
		for i, choice := range choices {
			lines = append(lines, fmt.Sprintf("%d. %s", i+1, choice))
		}
		renderWizardSection(w.Title, fmt.Sprintf("Step %d | %s", w.Step, field.Label), lines, field.InputHelp, field.ArtifactInfo, field.Example, choiceFooter(field.Default, choices))

		value, err := readLine(w.Reader, field.Default)
		if err != nil {
			return "", err
		}
		value = strings.ToLower(strings.TrimSpace(value))
		for i, choice := range choices {
			if value == choice || value == fmt.Sprintf("%d", i+1) {
				return choice, nil
			}
		}

		fmt.Fprintln(os.Stdout)
		fmt.Fprintf(os.Stdout, "%s\n", theme.style(fmt.Sprintf("Invalid choice. Enter a number from 1 to %d.", len(choices)), theme.errorColor))
	}
}

func promptProfileSelection(store ProfileStore, names []string) (string, error) {
	wizard := newWizard("Teams Migrator", "Select profile")
	defaultProfile := store.CurrentProfile
	if defaultProfile == "" || !profileNameInList(defaultProfile, names) {
		defaultProfile = names[0]
	}
	return wizard.choice(wizardField{
		Label:        "Profile",
		Description:  "Multiple saved profiles are available. Choose which migration profile to use for this run.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: fmt.Sprintf("Current profile: %s", defaultProfile),
		Default:      defaultProfile,
	}, names)
}

func profileNameInList(name string, names []string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func (w *wizardContext) secret(field wizardField) (string, error) {
	w.Step++
	renderWizardSection(w.Title, fmt.Sprintf("Step %d | %s", w.Step, field.Label), []string{field.Description}, field.InputHelp, field.ArtifactInfo, field.Example, "Input is hidden. Press Enter when done.")
	return readSecretLine()
}

func (w *wizardContext) confirm(field wizardField, confirmation string) (bool, error) {
	w.Step++
	lines := []string{field.Description}
	renderWizardSection(w.Title, fmt.Sprintf("Step %d | %s", w.Step, field.Label), lines, field.InputHelp, field.ArtifactInfo, field.Example, promptFooter(field.Default, field.Optional))
	value, err := readLine(w.Reader, field.Default)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(value) == confirmation, nil
}

func renderWizardSection(title, heading string, body []string, inputHelp, artifactInfo, example, footer string) {
	theme := currentUITheme()
	fmt.Fprintln(os.Stdout)
	for _, line := range theme.borderLine(title, heading) {
		fmt.Fprintln(os.Stdout, line)
	}
	for _, line := range body {
		if line == "" {
			fmt.Fprintln(os.Stdout)
			continue
		}
		fmt.Fprintln(os.Stdout, line)
	}
	if inputHelp != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintf(os.Stdout, "%s %s\n", theme.style("What to enter now:", theme.hintColor), inputHelp)
	}
	if artifactInfo != "" {
		fmt.Fprintf(os.Stdout, "%s %s\n", theme.style("About this artifact:", theme.hintColor), artifactInfo)
	}
	if example != "" {
		fmt.Fprintf(os.Stdout, "%s %s\n", theme.style("Example:", theme.hintColor), example)
	}
	if footer != "" {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, theme.style(footer, theme.hintColor))
	}
	fmt.Fprint(os.Stdout, theme.style("> ", theme.titleColor))
}

func readLine(reader *bufio.Reader, defaultValue string) (string, error) {
	value, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultValue, nil
	}
	return value, nil
}

func promptFooter(defaultValue string, optional bool) string {
	if defaultValue == "" {
		if optional {
			return "Enter a value or press Enter to skip. Ctrl+C cancels."
		}
		return "Enter a value and press Enter. Ctrl+C cancels."
	}
	return fmt.Sprintf("Press Enter to accept the default [%s]. Ctrl+C cancels.", defaultValue)
}

func choiceFooter(defaultValue string, choices []string) string {
	defaultChoice := defaultValue
	for i, choice := range choices {
		if choice == defaultValue {
			defaultChoice = fmt.Sprintf("%d (%s)", i+1, choice)
			break
		}
	}
	return fmt.Sprintf("Enter a choice number. Press Enter to accept the default [%s]. Ctrl+C cancels.", defaultChoice)
}

func isInteractiveTerminal() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func titleCase(value string) string {
	if value == "" {
		return value
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

func defaultProfileName(store ProfileStore, current string) string {
	if current != "" {
		return current
	}
	if store.CurrentProfile != "" {
		return store.CurrentProfile
	}
	return "default"
}

func defaultYesNoForCurrentProfile(store ProfileStore, profileName string) string {
	if store.CurrentProfile == "" || store.CurrentProfile == profileName {
		return "yes"
	}
	return "no"
}

func nextNewProfileName(store ProfileStore) string {
	if _, ok := store.Profiles["default"]; !ok {
		return "default"
	}
	for i := 2; ; i++ {
		name := fmt.Sprintf("profile-%d", i)
		if _, ok := store.Profiles[name]; !ok {
			return name
		}
	}
}

func nonEmptyDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func defaultYesNoChoice(valueSet, value bool, fallback string) string {
	if !valueSet {
		return fallback
	}
	if value {
		return "yes"
	}
	return "no"
}

func defaultFilterTeamDataSource(cfg Config) string {
	if normalized := normalizeFilterDataSource(cfg.FilterDataSource); normalized != "" {
		return normalized
	}
	if cfg.FilterScriptRunnerInstalled {
		return filterDataSourceScriptRunner
	}
	return filterDataSourceDatabaseCSV
}

func configurePostMigrationCorrectionScopes(wizard *wizardContext, cfg *Config) error {
	choice, err := wizard.choice(wizardField{
		Label:       "Jira reference exports",
		Description: "Choose which optional Jira reference artifacts pre-migrate should prepare for later correction runs.",
		InputHelp:   "Type the number of your choice and press Enter.",
		ArtifactInfo: strings.Join([]string{
			"These exports do not update Jira during pre-migrate.",
			"all three: issue Team-field IDs, Parent Link references, and filter JQL team IDs.",
			"Individual choices are useful for focused testing.",
			"skip: migrate teams and memberships only.",
		}, " "),
		Default: "skip",
	}, []string{"all three", "issue/team only", "parent link only", "filter only", "skip"})
	if err != nil {
		return err
	}

	cfg.IssueTeamIDsInScope = choice == "all three" || choice == "issue/team only"
	cfg.IssueTeamIDsInScopeSet = true
	cfg.ParentLinkInScope = choice == "all three" || choice == "parent link only"
	cfg.ParentLinkInScopeSet = true
	cfg.FilterTeamIDsInScope = choice == "all three" || choice == "filter only"
	cfg.FilterTeamIDsInScopeSet = true

	if !cfg.FilterTeamIDsInScope {
		cfg.FilterDataSource = ""
		cfg.FilterScriptRunnerInstalled = false
		cfg.FilterScriptRunnerEndpoint = ""
		return nil
	}

	return configureFilterTeamIDSource(wizard, cfg)
}

func configurePostMigrationCorrectionScopesForWizard(wizard *wizardContext, cfg *Config) error {
	if err := configurePostMigrationCorrectionScopes(wizard, cfg); err != nil {
		return err
	}
	if (issueTeamCorrectionsInScope(*cfg) || cfg.ParentLinkInScope) && strings.TrimSpace(cfg.IssueProjectScope) == "" {
		value, err := wizard.value(wizardField{
			Label:        "Issue correction project scope",
			Description:  "Choose which Jira projects should be in scope for issue-based correction exports and post-migrate rewrites.",
			InputHelp:    "Type all, or a comma-separated list like ABC,DEF.",
			ArtifactInfo: "This scope is applied to issue/team and Parent Link correction flows. Filters are not project-scoped.",
			Default:      nonEmptyDefault(cfg.IssueProjectScope, "all"),
		})
		if err != nil {
			return err
		}
		cfg.IssueProjectScope = value
	}
	return nil
}

func promptPostMigrationCorrectionScopes(cfg *Config) error {
	if !isInteractiveTerminal() {
		return nil
	}
	wizard := newWizard("Teams Migrator", "Post-migrate corrections")
	return configurePostMigrationCorrectionScopesForWizard(wizard, cfg)
}

func configureInitCorrectionScopes(wizard *wizardContext, cfg *Config) error {
	if err := configureParentLinkScope(wizard, cfg); err != nil {
		return err
	}
	if err := configureFilterTeamIDScope(wizard, cfg); err != nil {
		return err
	}
	return nil
}

func configureFilterTeamIDScope(wizard *wizardContext, cfg *Config) error {
	inScope, err := wizard.choice(wizardField{
		Label:        "Filters using team IDs in scope",
		Description:  "Choose whether post-migrate filter updates for Team-field IDs are part of this migration.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: "If this is in scope, the pre-migrate phase needs a reliable source list of filters whose JQL references team IDs so those IDs can be rewritten after destination teams are created.",
		Default:      defaultYesNoChoice(cfg.FilterTeamIDsInScopeSet, cfg.FilterTeamIDsInScope, "no"),
	}, []string{"no", "yes"})
	if err != nil {
		return err
	}

	cfg.FilterTeamIDsInScope = inScope == "yes"
	cfg.FilterTeamIDsInScopeSet = true
	if !cfg.FilterTeamIDsInScope {
		cfg.FilterDataSource = ""
		cfg.FilterScriptRunnerInstalled = false
		cfg.FilterScriptRunnerEndpoint = ""
		return nil
	}

	return configureFilterTeamIDSource(wizard, cfg)
}

func configureFilterTeamIDSource(wizard *wizardContext, cfg *Config) error {
	wizard.noteLines([]string{
		"Jira does not provide a generic filter-search REST endpoint that can reliably find all filters whose JQL uses team IDs.",
		"For full coverage, use either a ScriptRunner custom REST endpoint on the source Jira instance or a CSV produced from a database query.",
		"The tool can resolve the Jira Teams custom field ID automatically when it has source Jira API access.",
	})

	hasScriptRunner, err := wizard.choice(wizardField{
		Label:        "Expose ScriptRunner custom endpoint",
		Description:  "Choose whether to expose a ScriptRunner custom REST endpoint for filter discovery.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: "This requires ScriptRunner for Jira to be installed on the source Jira instance. If you choose no, supply a CSV generated from a database query instead.",
		Default:      defaultYesNoChoice(cfg.FilterDataSource != "", defaultFilterTeamDataSource(*cfg) == filterDataSourceScriptRunner, "yes"),
	}, []string{"yes", "no"})
	if err != nil {
		return err
	}

	if hasScriptRunner == "no" {
		cfg.FilterDataSource = filterDataSourceDatabaseCSV
		cfg.FilterScriptRunnerInstalled = false
		cfg.FilterScriptRunnerEndpoint = ""
		wizard.noteLines([]string{
			"Use a CSV generated from a database query for the source list of filters in this environment.",
			"Keep that CSV with the other pre-migrate artifacts so post-migrate can rewrite team IDs later.",
			fmt.Sprintf("Example query: %s", filterInventoryCSVExampleQuery()),
			fmt.Sprintf("If ScriptRunner becomes available later, the custom endpoint script in this repo is %s.", teamFilterScriptRunnerScriptPath),
			"A published script URL still needs to be added.",
		})
		cfg.FilterSourceCSV, err = wizard.value(wizardField{
			Label:        "Default source filter CSV",
			Description:  "This optional path points to a CSV exported from the Jira database query you will use as the source list of filters that contain team IDs.",
			InputHelp:    "Type the CSV path, or press Enter to leave it unset and provide --filter-source-csv per run.",
			ArtifactInfo: "Expected columns: Filter ID, Filter Name, Owner, JQL. The tool will parse the JQL and generate the normalized pre-migration filter export from it.",
			Example:      "/Users/you/migration/source-filters.csv",
			Default:      cfg.FilterSourceCSV,
			Optional:     true,
		})
		if err != nil {
			return err
		}
		return nil
	}

	cfg.FilterDataSource = filterDataSourceScriptRunner
	installed, err := wizard.choice(wizardField{
		Label:        "Custom filter endpoint installed",
		Description:  "Choose whether the custom ScriptRunner endpoint for team-ID filter discovery is already installed on the source Jira instance.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: "The expected endpoint path is /rest/scriptrunner/latest/custom/findSourceTeamFiltersDB and it requires the resolved Jira Teams custom field ID.",
		Default:      defaultYesNoChoice(cfg.FilterDataSource == filterDataSourceScriptRunner, cfg.FilterScriptRunnerInstalled, "yes"),
	}, []string{"yes", "no"})
	if err != nil {
		return err
	}

	cfg.FilterScriptRunnerInstalled = installed == "yes"
	if !cfg.FilterScriptRunnerInstalled {
		cfg.FilterScriptRunnerEndpoint = ""
		wizard.noteLines([]string{
			"Install the ScriptRunner custom endpoint before running the pre-migrate source filter export.",
			fmt.Sprintf("Current script source in this repo: %s", teamFilterScriptRunnerScriptPath),
			"A published script URL still needs to be added.",
		})
		return nil
	}

	return verifyConfiguredScriptRunnerFilterEndpoint(wizard, cfg)
}

func configureParentLinkScope(wizard *wizardContext, cfg *Config) error {
	inScope, err := wizard.choice(wizardField{
		Label:        "Parent Link corrections in scope",
		Description:  "Choose whether Parent Link issue references should be exported pre-migrate and corrected post-migrate.",
		InputHelp:    "Type the number of your choice and press Enter.",
		ArtifactInfo: "Parent Link corrections use source parent issue keys to resolve the new target parent issue IDs before updating child issues in the target Jira.",
		Default:      defaultYesNoChoice(cfg.ParentLinkInScopeSet, cfg.ParentLinkInScope, "no"),
	}, []string{"no", "yes"})
	if err != nil {
		return err
	}
	cfg.ParentLinkInScope = inScope == "yes"
	cfg.ParentLinkInScopeSet = true
	return nil
}

func verifyConfiguredScriptRunnerFilterEndpoint(wizard *wizardContext, cfg *Config) error {
	if strings.TrimSpace(cfg.SourceBaseURL) == "" {
		cfg.FilterScriptRunnerEndpoint = ""
		wizard.noteLines([]string{
			"Skipping ScriptRunner endpoint verification because this profile does not have a source Jira base URL yet.",
			fmt.Sprintf("Expected path: {{jira-url}}%s?enabled=true&lastId=0&limit=500&teamFieldId=<resolved Teams field ID>", teamFilterScriptRunnerEndpointPath),
			"The tool can resolve the field ID automatically later when source Jira API access is configured.",
		})
		return nil
	}

	wizard.noteLines([]string{
		"Endpoint verification uses temporary source Jira credentials and does not store them in the saved profile.",
		"The ScriptRunner endpoint itself requires Jira admin permission.",
	})

	var username string
	var password string
	if err := promptForAuth(wizard, "source verification", &username, &password); err != nil {
		return err
	}

	endpointURL, fieldLabel, err := verifyTeamFilterScriptRunnerEndpoint(cfg.SourceBaseURL, username, password)
	if err != nil {
		cfg.FilterScriptRunnerEndpoint = ""
		wizard.noteLines([]string{
			"Could not verify the ScriptRunner filter endpoint yet.",
			err.Error(),
			fmt.Sprintf("Expected path: {{jira-url}}%s?enabled=true&lastId=0&limit=500&teamFieldId=<resolved Teams field ID>", teamFilterScriptRunnerEndpointPath),
			fmt.Sprintf("Current script source in this repo: %s", teamFilterScriptRunnerScriptPath),
			"A published script URL still needs to be added.",
		})
		return nil
	}

	cfg.FilterScriptRunnerEndpoint = endpointURL
	wizard.noteLines([]string{
		"Verified the ScriptRunner custom endpoint for team-ID filter discovery.",
		fmt.Sprintf("Resolved Teams field: %s", fieldLabel),
		fmt.Sprintf("Verified endpoint: %s", endpointURL),
	})
	return nil
}

func profileSummary(cfg Config) string {
	lines := []string{
		fmt.Sprintf("Profile: %s", cfg.Profile),
		fmt.Sprintf("Identity mapping: %s", cfg.IdentityMappingFile),
	}
	if inferSourceMode(cfg) == "api" {
		lines = append(lines, fmt.Sprintf("Source mode: api (%s)", cfg.SourceBaseURL))
	} else {
		lines = append(lines, "Source mode: file")
		lines = append(lines, fmt.Sprintf("Teams file: %s", cfg.TeamsFile))
		lines = append(lines, fmt.Sprintf("Persons file: %s", cfg.PersonsFile))
		lines = append(lines, fmt.Sprintf("Resources file: %s", cfg.ResourcesFile))
		if cfg.SourceBaseURL != "" {
			lines = append(lines, fmt.Sprintf("Optional source Jira URL: %s", cfg.SourceBaseURL))
		}
	}
	lines = append(lines, fmt.Sprintf("Target base URL: %s", cfg.TargetBaseURL))
	lines = append(lines, fmt.Sprintf("Output dir: %s", cfg.OutputDir))
	lines = append(lines, fmt.Sprintf("Team scope: %s", cfg.TeamScope))
	lines = append(lines, fmt.Sprintf("Issue correction project scope: %s", nonEmptyDefault(cfg.IssueProjectScope, "all")))
	lines = append(lines, fmt.Sprintf("Issue/team corrections in scope: %t", cfg.IssueTeamIDsInScope))
	lines = append(lines, fmt.Sprintf("Team-ID filter updates in scope: %t", cfg.FilterTeamIDsInScope))
	lines = append(lines, fmt.Sprintf("Parent Link corrections in scope: %t", cfg.ParentLinkInScope))
	if cfg.FilterTeamIDsInScope {
		lines = append(lines, fmt.Sprintf("Filter data source: %s", nonEmptyDefault(cfg.FilterDataSource, "not configured")))
		if cfg.FilterDataSource == filterDataSourceDatabaseCSV && cfg.FilterSourceCSV != "" {
			lines = append(lines, fmt.Sprintf("Filter source CSV: %s", cfg.FilterSourceCSV))
		}
		if cfg.FilterDataSource == filterDataSourceScriptRunner {
			lines = append(lines, fmt.Sprintf("ScriptRunner endpoint installed: %t", cfg.FilterScriptRunnerInstalled))
			if cfg.FilterScriptRunnerEndpoint != "" {
				lines = append(lines, fmt.Sprintf("Verified ScriptRunner endpoint: %s", cfg.FilterScriptRunnerEndpoint))
			}
		}
	}
	lines = append(lines, "Credentials: prompted at runtime")
	return strings.Join(lines, "\n")
}

func promptSecretValue(label, description string) (string, error) {
	if !isInteractiveTerminal() {
		return "", errors.New("secret input requires a terminal")
	}
	renderWizardSection("Teams Migrator | Secrets", label, []string{description}, "Type the value and press Enter.", "Input is hidden and works across supported terminals.", "", "Ctrl+C cancels.")
	return readSecretLine()
}

type applyPreviewChoice string

const (
	applyPreviewStop  applyPreviewChoice = "stop"
	applyPreviewAgain applyPreviewChoice = "preview again"
	applyPreviewApply applyPreviewChoice = "apply now"
)

func promptApplyAfterPreview() (applyPreviewChoice, error) {
	if !isInteractiveTerminal() {
		return applyPreviewApply, nil
	}
	reader := bufio.NewReader(os.Stdin)
	renderWizardSection("Teams Migrator | Apply", "Choose next step", []string{
		"You have just seen the preview for the planned mappings and writes.",
		"Stopping here keeps this run in dry-run mode. Applying creates records on the target Jira instance where the plan marked them as add or created.",
		"Non-shared teams cannot be created by this tool and must already exist in the destination plan before migration.",
		"",
		"Choices:",
		"1. stop",
		"2. preview again",
		"3. apply now",
	}, "Type the number of your choice and press Enter.", "", "", "Press Enter to stop. Ctrl+C cancels.")
	value, err := readLine(reader, string(applyPreviewStop))
	if err != nil {
		return applyPreviewStop, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "3", string(applyPreviewApply), "apply":
		return applyPreviewApply, nil
	case "2", string(applyPreviewAgain), "preview":
		return applyPreviewAgain, nil
	default:
		return applyPreviewStop, nil
	}
}

func promptProceedToPostMigrationCorrections() (bool, error) {
	if !isInteractiveTerminal() {
		return false, nil
	}
	reader := bufio.NewReader(os.Stdin)
	renderWizardSection("Teams Migrator | Next phase", "Start post-migrate now?", []string{
		"The migrate phase is complete. Destination team IDs and correction inputs are ready.",
		"Post-migrate is a separate phase that updates Jira issue, Parent Link, and filter references where prepared mappings are still valid.",
		"You can stop here and run it later with: teams-migrator migrate --phase post-migrate",
		"",
		"Choices:",
		"1. yes",
		"2. stop",
	}, "Type the number of your choice and press Enter.", "", "", "Press Enter to stop after migrate. Ctrl+C cancels.")
	value, err := readLine(reader, "2")
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func promptContinueToMigrationPhase(nextPhase string) (bool, error) {
	if !isInteractiveTerminal() {
		return false, nil
	}
	reader := bufio.NewReader(os.Stdin)
	nextLabel := nextPhase
	switch nextPhase {
	case phaseMigrate:
		nextLabel = "migrate"
	case phasePostMigrate:
		nextLabel = "post-migrate"
	}
	renderWizardSection("Teams Migrator | Continue", fmt.Sprintf("Continue to %s now?", titleCase(nextLabel)), []string{
		fmt.Sprintf("The current phase completed successfully."),
		fmt.Sprintf("Continue now to the %s phase using the same in-memory credentials and saved scope, or stop here and resume later.", nextLabel),
		"",
		"Choices:",
		"1. yes",
		"2. stop",
	}, "Type the number of your choice and press Enter.", "", "", "Press Enter to stop here. Ctrl+C cancels.")
	value, err := readLine(reader, "2")
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func showPreparedPostMigrationFilesPreview(state migrationState) error {
	if !isInteractiveTerminal() {
		return nil
	}

	lines := []string{
		"The migrate phase prepared the following files for post-migration corrections:",
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "migration_team_id_mapping"); ok {
		lines = append(lines, fmt.Sprintf("Migration team ID mapping: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_issue_team_mapping"); ok {
		lines = append(lines, fmt.Sprintf("Issue/team correction mapping: %s", artifact.Path))
	} else {
		lines = append(lines, "Issue/team correction mapping: not prepared")
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_target_issue_snapshot"); ok {
		lines = append(lines, fmt.Sprintf("Target issue snapshot: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_issue_comparison"); ok {
		lines = append(lines, fmt.Sprintf("Issue comparison export: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_parent_link_mapping"); ok {
		lines = append(lines, fmt.Sprintf("Parent-link correction mapping: %s", artifact.Path))
	} else if len(state.ParentLinkRows) > 0 {
		lines = append(lines, "Parent-link correction mapping: not prepared")
	} else {
		lines = append(lines, "Parent-link correction mapping: not in scope or no pre-migrate parent-link export was available")
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_target_parent_link_snapshot"); ok {
		lines = append(lines, fmt.Sprintf("Target Parent Link snapshot: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_parent_link_comparison"); ok {
		lines = append(lines, fmt.Sprintf("Parent-link comparison export: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_filter_team_mapping"); ok {
		lines = append(lines, fmt.Sprintf("Filter/team correction mapping: %s", artifact.Path))
	} else if len(state.FilterTeamClauseRows) > 0 {
		lines = append(lines, "Filter/team correction mapping: not prepared")
	} else {
		lines = append(lines, "Filter/team correction mapping: not in scope or no pre-migrate filter export was available")
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_target_filter_snapshot"); ok {
		lines = append(lines, fmt.Sprintf("Target filter snapshot: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_filter_target_match"); ok {
		lines = append(lines, fmt.Sprintf("Filter target match export: %s", artifact.Path))
	}
	if artifact, ok := findArtifactByKey(state.Artifacts, "post_migrate_filter_comparison"); ok {
		lines = append(lines, fmt.Sprintf("Filter JQL comparison export: %s", artifact.Path))
	}

	reader := bufio.NewReader(os.Stdin)
	renderWizardSection("Teams Migrator | Post-migrate", "Prepared correction files", lines, "", "", "", "Press Enter to continue to the post-migration preview. Ctrl+C cancels.")
	_, err := readLine(reader, "")
	return err
}

func promptForSelfUpdate(latest string) (bool, error) {
	if !isInteractiveTerminal() {
		return false, nil
	}
	reader := bufio.NewReader(os.Stdin)
	renderWizardSection("Teams Migrator | Update", "New version available", []string{
		fmt.Sprintf("Current version: %s", currentVersion()),
		fmt.Sprintf("Latest release: %s", latest),
		"Updating now will replace the installed binary and stop this run.",
	}, "Type yes to update now, or press Enter to continue without updating.", "", "", "Default: no. Ctrl+C cancels.")
	value, err := readLine(reader, "no")
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func readSecretLine() (string, error) {
	fd := int(os.Stdin.Fd())
	value, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stdout)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}
