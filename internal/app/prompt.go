package app

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	term "github.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/internal/thirdparty/golang.org/x/term"
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
				ArtifactInfo: "This is typically something like out/plan-report.json or out/migrate-report.json.",
				Example:      "out/migrate-report.json",
			})
			if err != nil {
				return err
			}
			cfg.ReportInput = value
		}
		return nil
	case "validate", "plan", "migrate":
	default:
		return nil
	}

	if !needsInteractiveSetup(*cfg) {
		return nil
	}

	wizard := newWizard("Teams Migrator", "Guided run setup")
	wizard.intro([]string{
		"This tool migrates Jira Teams and team membership between Server/Data Center instances.",
		"The interface adapts to your terminal: richer styling when supported, plain mode when not.",
	})
	if cfg.Profile != "" {
		wizard.note(fmt.Sprintf("Loaded defaults from saved profile %q.", cfg.Profile))
	}

	if cfg.IdentityMappingFile == "" {
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
	}

	sourceMode := inferSourceMode(*cfg)
	if sourceMode == "" {
		value, err := wizard.choice(wizardField{
			Label:        "Source data mode",
			Description:  "Choose whether source teams/persons/resources should come from exported JSON files or directly from the source Jira API.",
			ArtifactInfo: "File mode is easier for a first dry run. API mode is needed for live reads and issue-field discovery.",
			Default:      "file",
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
	} else {
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
	return cfg.SourceBaseURL != "" && cfg.SourceUsername == ""
}

func targetNeedsAuth(cfg Config) bool {
	return cfg.TargetBaseURL != "" && cfg.TargetUsername == ""
}

func runConfigInitWizard(cfg Config) error {
	if cfg.NoInput {
		return errors.New("config init requires interactive input; remove --no-input")
	}
	if !isInteractiveTerminal() {
		return errors.New("config init requires a terminal")
	}

	store, err := loadProfileStore(cfg.ConfigPath)
	if err != nil {
		return err
	}

	wizard := newWizard("Teams Migrator", "Config init")
	wizard.intro([]string{
		"This wizard creates or updates a reusable profile for future runs.",
		"Usernames and passwords are not stored in the profile. The CLI prompts for them at runtime when needed.",
	})
	wizard.note(fmt.Sprintf("Config file: %s", cfg.ConfigPath))

	profileName, err := wizard.value(wizardField{
		Label:        "Profile name",
		Description:  "Choose a short profile name for this environment or migration setup.",
		ArtifactInfo: "Examples: default, prod, dc-migration-test",
		Default:      defaultProfileName(store, cfg.Profile),
	})
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

	sourceMode, err := wizard.choice(wizardField{
		Label:        "Default source mode",
		Description:  "Choose whether this profile should default to source JSON exports or direct source Jira API access.",
		ArtifactInfo: "File mode is often easiest for repeatable planning. API mode is useful when you want live reads.",
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
	format, err := wizard.choice(wizardField{
		Label:       "Default report format",
		Description: "Choose how reports should be written unless you override --format on a run.",
		InputHelp:   "Type the number of your choice and press Enter.",
		Default:     nonEmptyDefault(string(cfg.ReportFormat), "json"),
	}, []string{"json", "csv"})
	if err != nil {
		return err
	}
	cfg.ReportFormat = ReportFormat(format)

	setCurrent, err := wizard.choice(wizardField{
		Label:       "Set as current profile",
		Description: "If set to yes, this profile becomes the default when you run teams-migrator without --profile.",
		InputHelp:   "Type the number of your choice and press Enter.",
		Default:     "yes",
	}, []string{"yes", "no"})
	if err != nil {
		return err
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
		return errors.New("config init cancelled")
	}

	if err := saveProfileStore(cfg.ConfigPath, store); err != nil {
		return err
	}

	wizard.success(
		"Profile saved",
		fmt.Sprintf("Saved profile %q to %s", profileName, cfg.ConfigPath),
		[]string{
			fmt.Sprintf("Config path: ./bin/teams-migrator config path"),
			fmt.Sprintf("Inspect profile: ./bin/teams-migrator config show --profile %s", profileName),
			fmt.Sprintf("Next: ./bin/teams-migrator plan --profile %s", profileName),
		},
	)
	return nil
}

func promptForAuth(wizard *wizardContext, label string, username, password *string) error {
	user, err := wizard.value(wizardField{
		Label:       fmt.Sprintf("%s username", titleCase(label)),
		Description: fmt.Sprintf("Enter the username for basic authentication against the %s Jira instance.", label),
	})
	if err != nil {
		return err
	}
	pass, err := wizard.secret(wizardField{
		Label:       fmt.Sprintf("%s password", titleCase(label)),
		Description: fmt.Sprintf("Enter the password for basic authentication against the %s Jira instance.", label),
		InputHelp:   "Type the password value. Input is hidden.",
	})
	if err != nil {
		return err
	}
	*username = user
	*password = pass
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

func nonEmptyDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
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
	lines = append(lines, fmt.Sprintf("Report format: %s", cfg.ReportFormat))
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

func confirmApplyAfterPreview() (bool, error) {
	if !isInteractiveTerminal() {
		return true, nil
	}
	reader := bufio.NewReader(os.Stdin)
	renderWizardSection("Teams Migrator | Apply", "Apply mode confirmation", []string{
		"You have just seen the dry-run preview for the planned mappings and writes.",
		"Apply mode will now create records on the target Jira instance where the plan marked them as create or created.",
	}, "Type APPLY exactly to continue, or press Ctrl+C to cancel.", "", "", "Enter APPLY to continue. Ctrl+C cancels.")
	value, err := readLine(reader, "")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(value) == "APPLY", nil
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
