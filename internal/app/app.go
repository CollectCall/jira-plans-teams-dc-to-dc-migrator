package app

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

var errUsage = errors.New("usage")

func Run(args []string) int {
	var postMigrateFollowUpState *migrationState

	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, errUsage) {
			printUsage(os.Stderr)
			return ExitFailure
		}
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		printUsage(os.Stderr)
		return ExitFailure
	}
	if cfg.Help {
		printUsage(os.Stdout)
		return ExitSuccess
	}

	if updated, err := maybeOfferSelfUpdate(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	} else if updated {
		return ExitSuccess
	}

	if shouldRunInteractiveMigrateSession(cfg) {
		return runInteractiveMigrateSession(cfg)
	}

	var report Report
	switch cfg.Command {
	case "version":
		fmt.Fprintln(os.Stdout, currentVersion())
		return ExitSuccess
	case "self-update":
		if err := runSelfUpdate(); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
		return ExitSuccess
	case "uninstall":
		if err := runUninstall(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
		return ExitSuccess
	case "migrate":
		if (runsMigratePhase(cfg.Command, cfg.Phase) || runsPostMigratePhase(cfg.Command, cfg.Phase)) && !cfg.DryRun && !cfg.NoInput && isInteractiveTerminal() {
			state, findings := loadMigrationState(cfg)
			_, previewFindings, previewActions := executeMigrationWithState(cfg, false, state, findings)
			preview := populateExecutionReport(newReport(cfg), state, previewFindings, previewActions, "apply_preview", "Preview generated before apply mode confirmation")
			preview.DryRun = true
			printSummary(os.Stdout, preview, nil)
			choice, err := promptApplyAfterPreview()
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if choice != applyPreviewApply {
				fmt.Fprintln(os.Stderr, "error: apply mode cancelled")
				return ExitFailure
			}
			state, findings, actions := executeMigrationWithState(cfg, true, state, findings)
			report = populateExecutionReport(newReport(cfg), state, findings, actions, "", "")
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "apply_mode_active", "Apply mode executed remote write operations where mappings were resolvable"))
			finalizeReport(&report)
			stateCopy := state
			postMigrateFollowUpState = &stateCopy
			break
		}
		report = runMigrate(cfg)
	case "report":
		report, err = runReport(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
	case "init":
		if err := runConfigInitWizard(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
		return ExitSuccess
	case "config show":
		if err := runConfigShow(cfg); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
		return ExitSuccess
	default:
		fmt.Fprintf(os.Stderr, "error: unsupported command %q\n", cfg.Command)
		return ExitFailure
	}

	reportPaths, err := writeReportOutputs(cfg, report)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	}

	printSummary(os.Stdout, report, reportPaths)
	if postMigrateFollowUpState != nil {
		proceed, err := promptProceedToPostMigrationCorrections()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
		if proceed {
			followUpFindings := preparePostMigrationTargetArtifacts(cfg, postMigrateFollowUpState, nil)
			if err := showPreparedPostMigrationFilesPreview(*postMigrateFollowUpState); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			postPreview := buildPostMigrationFollowUpPreviewReport(cfg, *postMigrateFollowUpState)
			postPreview.Findings = append(postPreview.Findings, followUpFindings...)
			finalizeReport(&postPreview)
			printSummary(os.Stdout, postPreview, nil)
		}
	}
	return exitCodeFor(report)
}

func shouldRunInteractiveMigrateSession(cfg Config) bool {
	return cfg.Command == "migrate" && !cfg.NoInput && isInteractiveTerminal()
}

func runInteractiveMigrateSession(cfg Config) int {
	if err := ensureInteractiveMigrateProfileSelected(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	}

	if err := completeMigrateSessionInteractively(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	}
	var err error
	cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	}
	refreshInteractiveMigrateReferenceExportScopes(&cfg)

	for {
		switch normalizeMigrationPhase(cfg.Phase) {
		case phasePreMigrate:
			report, err := runInteractiveReadOnlyPhase(cfg, phasePreMigrate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if report.Stats.Errors > 0 {
				return exitCodeFor(report)
			}
			printPreMigrateReviewChecklist(os.Stdout, report)
			proceed, err := promptContinueToMigrationPhase(phaseMigrate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if !proceed {
				return exitCodeFor(report)
			}
			cfg.Phase = phaseMigrate
			cfg.PhaseExplicit = true
		case phaseMigrate:
			report, applied, err := runInteractiveApplyPhase(cfg, phaseMigrate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if report.Stats.Errors > 0 || !applied {
				return exitCodeFor(report)
			}
			proceed, err := promptProceedToPostMigrationCorrections()
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if !proceed {
				return exitCodeFor(report)
			}
			cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if err := showPreparedPostMigrationFilesFromCurrentOutputs(cfg); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			cfg.Phase = phasePostMigrate
			cfg.PhaseExplicit = true
		case phasePostMigrate:
			cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			report, _, err := runInteractiveApplyPhase(cfg, phasePostMigrate)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			return exitCodeFor(report)
		default:
			fmt.Fprintf(os.Stderr, "error: unsupported migration phase %q\n", cfg.Phase)
			return ExitFailure
		}
	}
}

func refreshInteractiveMigrateReferenceExportScopes(cfg *Config) {
	if cfg == nil || cfg.Command != "migrate" {
		return
	}
	if outputDir, err := cleanOutputDirPath(cfg.OutputDir); err == nil {
		cfg.OutputDir = outputDir
	} else {
		return
	}
	applyDefaultReferenceExportScopes(cfg)
}

func ensureInteractiveMigrateProfileSelected(cfg *Config) error {
	if cfg.ProfileLoaded {
		return nil
	}

	configPath, err := cleanInputFilePath("config", cfg.ConfigPath)
	if err != nil {
		return err
	}
	cfg.ConfigPath = configPath
	store, err := loadProfileStore(cfg.ConfigPath)
	if err != nil {
		return fmt.Errorf("loading config store: %w", err)
	}
	names := profileNames(store)
	if len(names) == 0 {
		return fmt.Errorf("no saved profiles found in %s; run teams-migrator init to create a profile, or run it again to add another profile", cfg.ConfigPath)
	}

	if cfg.ProfileExplicit {
		name, profile, loaded := resolveProfile(*cfg, store)
		if !loaded {
			return fmt.Errorf("saved profile %q was not found in %s; run teams-migrator init to create it or choose an existing profile", name, cfg.ConfigPath)
		}
		cfg.Profile = name
		applySavedProfile(cfg, profile)
		cfg.ProfileLoaded = true
		return nil
	}

	selected := ""
	if len(names) == 1 {
		selected = names[0]
	} else {
		var err error
		selected, err = promptProfileSelection(store, names)
		if err != nil {
			return err
		}
	}

	profile := store.Profiles[selected]
	cfg.Profile = selected
	applySavedProfile(cfg, profile)
	cfg.ProfileLoaded = true
	return nil
}

func runInteractiveReadOnlyPhase(cfg Config, phase string) (Report, error) {
	phaseCfg := cfg
	phaseCfg.Phase = phase
	phaseCfg.DryRun = true
	phaseCfg.OutputTimestamp = interactivePhaseOutputTimestamp(phase)
	var err error
	phaseCfg.OutputDir, err = cleanOutputDirPath(phaseCfg.OutputDir)
	if err != nil {
		return Report{}, err
	}

	printPhaseBoundary(os.Stdout, phase, "Preparing read-only artifacts", []string{
		"Fetching source and target data and writing review files. No Jira writes will be sent.",
	})
	report := runMigrate(phaseCfg)
	reportPaths, err := writeReportOutputs(phaseCfg, report)
	if err != nil {
		return Report{}, err
	}
	printSummary(os.Stdout, report, reportPaths)
	return report, nil
}

func runInteractiveApplyPhase(cfg Config, phase string) (Report, bool, error) {
	var err error
	cfg.OutputDir, err = cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return Report{}, false, err
	}
	stamp := interactivePhaseOutputTimestamp(phase)

	for {
		previewCfg := cfg
		previewCfg.Phase = phase
		previewCfg.DryRun = true
		previewCfg.OutputTimestamp = stamp
		previewCfg.OutputDir, err = cleanOutputDirPath(previewCfg.OutputDir)
		if err != nil {
			return Report{}, false, err
		}

		printPhaseBoundary(os.Stdout, phase, "Previewing phase", []string{
			"Building the plan for this phase. No Jira writes will be sent.",
		})
		state, findings := loadMigrationState(previewCfg)
		_, previewFindings, previewActions := executeMigrationWithState(previewCfg, false, state, findings)
		preview := populateExecutionReport(newReport(previewCfg), state, previewFindings, previewActions, "apply_preview", "Preview generated before apply mode confirmation")
		previewCfg.OutputDir, err = cleanOutputDirPath(previewCfg.OutputDir)
		if err != nil {
			return Report{}, false, err
		}
		previewPaths, err := writeReportOutputs(previewCfg, preview)
		if err != nil {
			return Report{}, false, err
		}
		printSummary(os.Stdout, preview, previewPaths)
		if preview.Stats.Errors > 0 {
			return preview, false, nil
		}

		choice, err := promptApplyAfterPreview()
		if err != nil {
			return Report{}, false, err
		}
		if choice == applyPreviewAgain {
			continue
		}
		if choice != applyPreviewApply {
			return preview, false, nil
		}

		applyCfg := cfg
		applyCfg.Phase = phase
		applyCfg.DryRun = false
		applyCfg.OutputTimestamp = stamp

		printPhaseBoundary(os.Stdout, phase, "Applying phase", []string{
			"Executing the approved writes for this phase.",
		})
		state, findings, actions := executeMigrationWithState(applyCfg, true, state, findings)
		report := finalizeMigrationExecutionReport(newReport(applyCfg), applyCfg, state, findings, actions)
		applyCfg.OutputDir, err = cleanOutputDirPath(applyCfg.OutputDir)
		if err != nil {
			return Report{}, false, err
		}
		reportPaths, err := writeReportOutputs(applyCfg, report)
		if err != nil {
			return Report{}, false, err
		}
		printPhaseBoundary(os.Stdout, phase, "Apply completed", []string{
			"The write phase finished. The summary below shows the applied results.",
		})
		printSummary(os.Stdout, report, reportPaths)
		return report, true, nil
	}
}

func printPhaseBoundary(w io.Writer, phase, heading string, lines []string) {
	theme := currentUITheme()
	title := "Teams Migrator"
	if phaseLabel := phaseDisplayName(phase); phaseLabel != "" {
		title = fmt.Sprintf("Teams Migrator | %s", phaseLabel)
	}
	fmt.Fprintln(w)
	for _, line := range theme.borderLine(title, heading) {
		fmt.Fprintln(w, line)
	}
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
}

func phaseDisplayName(phase string) string {
	switch normalizeMigrationPhase(phase) {
	case phasePreMigrate:
		return "Pre-migrate"
	case phaseMigrate:
		return "Migrate"
	case phasePostMigrate:
		return "Post-migrate"
	default:
		return titleCase(strings.ReplaceAll(phase, "-", " "))
	}
}

func printPreMigrateReviewChecklist(w io.Writer, report Report) {
	theme := currentUITheme()
	artifacts := summaryArtifacts(report)
	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Review Checklist", theme.titleColor))
	fmt.Fprintln(w, "- Review team-mapping comparison first for create/reuse/skip decisions.")
	fmt.Fprintln(w, "- Review team-membership mapping next for user resolution and skipped memberships.")
	if path := firstArtifactPathContaining(artifacts, "team mapping"); path != "" {
		fmt.Fprintf(w, "- Team mapping file: %s\n", path)
	}
	if path := firstArtifactPathContaining(artifacts, "membership"); path != "" {
		fmt.Fprintf(w, "- Membership mapping file: %s\n", path)
	}
	fmt.Fprintln(w, "- Resume later with: teams-migrator migrate --phase migrate")
}

func firstArtifactPathContaining(artifacts []string, needle string) string {
	needle = strings.ToLower(strings.TrimSpace(needle))
	for _, artifact := range artifacts {
		if strings.Contains(strings.ToLower(artifact), needle) {
			return artifact
		}
	}
	return ""
}

func showPreparedPostMigrationFilesFromCurrentOutputs(cfg Config) error {
	postCfg := cfg
	postCfg.Phase = phasePostMigrate
	postCfg.DryRun = true
	var err error
	postCfg.OutputDir, err = cleanOutputDirPath(postCfg.OutputDir)
	if err != nil {
		return err
	}

	state, findings := loadMigrationState(postCfg)
	if hasErrors(findings) {
		return nil
	}
	_ = preparePostMigrationTargetArtifacts(postCfg, &state, nil)
	return showPreparedPostMigrationFilesPreview(state)
}

func interactivePhaseOutputTimestamp(phase string) string {
	slug := strings.ReplaceAll(normalizeMigrationPhase(phase), "-", "_")
	if slug == "" {
		slug = "migrate"
	}
	return fmt.Sprintf("%s-%s", time.Now().Format("20060102-150405"), slug)
}

func writeReportOutputs(cfg Config, report Report) ([]string, error) {
	var reportPaths []string
	if cfg.Command != "report" {
		outputDir, err := cleanOutputDirPath(cfg.OutputDir)
		if err != nil {
			return nil, err
		}
		cfg.OutputDir = outputDir
		if err := ensureOutputDir(outputDir); err != nil {
			return nil, fmt.Errorf("creating output directory: %w", err)
		}
		reportBase := strings.ReplaceAll(cfg.Command, " ", "-")
		jsonPath := defaultOutputPathForFormat(cfg, ReportFormatJSON)
		if err := writeReport(report, ReportFormatJSON, jsonPath); err != nil {
			return nil, fmt.Errorf("writing json report: %w", err)
		}
		if err := pruneOutputFamily(outputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatJSON), outputRetentionLimit); err != nil {
			return nil, fmt.Errorf("pruning json reports: %w", err)
		}
		csvPath := defaultOutputPathForFormat(cfg, ReportFormatCSV)
		if err := writeReport(report, ReportFormatCSV, csvPath); err != nil {
			return nil, fmt.Errorf("writing csv report: %w", err)
		}
		if err := pruneOutputFamily(outputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatCSV), outputRetentionLimit); err != nil {
			return nil, fmt.Errorf("pruning csv reports: %w", err)
		}
		reportPaths = []string{jsonPath, csvPath}
	}
	return reportPaths, nil
}
