package app

import (
	"errors"
	"fmt"
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
	case "plan":
		report = runPlan(cfg)
	case "migrate":
		if (runsMigratePhase(cfg.Command, cfg.Phase) || runsPostMigratePhase(cfg.Command, cfg.Phase)) && !cfg.DryRun && !cfg.NoInput && isInteractiveTerminal() {
			state, findings := loadMigrationState(cfg)
			_, previewFindings, previewActions := executeMigrationWithState(cfg, false, state, findings)
			preview := populateExecutionReport(newReport(cfg), state, previewFindings, previewActions, "apply_preview", "Preview generated before apply mode confirmation")
			preview.DryRun = true
			printSummary(os.Stdout, preview, nil)
			ok, err := confirmApplyAfterPreview()
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			if !ok {
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
	case "scan-filters":
		report = runScanFilters(cfg)
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
	case "config path":
		fmt.Fprintln(os.Stdout, cfg.ConfigPath)
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
			followUpFindings := preparePostMigrationTargetArtifacts(cfg, postMigrateFollowUpState)
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
	if err := completeMigrateSessionInteractively(&cfg); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return ExitFailure
	}

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
			if err := showPreparedPostMigrationFilesFromCurrentOutputs(cfg); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				return ExitFailure
			}
			cfg.Phase = phasePostMigrate
			cfg.PhaseExplicit = true
		case phasePostMigrate:
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

func runInteractiveReadOnlyPhase(cfg Config, phase string) (Report, error) {
	phaseCfg := cfg
	phaseCfg.Phase = phase
	phaseCfg.DryRun = true
	phaseCfg.OutputTimestamp = interactivePhaseOutputTimestamp(phase)

	report := runMigrate(phaseCfg)
	reportPaths, err := writeReportOutputs(phaseCfg, report)
	if err != nil {
		return Report{}, err
	}
	printSummary(os.Stdout, report, reportPaths)
	return report, nil
}

func runInteractiveApplyPhase(cfg Config, phase string) (Report, bool, error) {
	stamp := interactivePhaseOutputTimestamp(phase)

	previewCfg := cfg
	previewCfg.Phase = phase
	previewCfg.DryRun = true
	previewCfg.OutputTimestamp = stamp

	preview := runPreview(previewCfg)
	previewPaths, err := writeReportOutputs(previewCfg, preview)
	if err != nil {
		return Report{}, false, err
	}
	printSummary(os.Stdout, preview, previewPaths)
	if preview.Stats.Errors > 0 {
		return preview, false, nil
	}

	ok, err := confirmApplyAfterPreview()
	if err != nil {
		return Report{}, false, err
	}
	if !ok {
		return preview, false, nil
	}

	applyCfg := cfg
	applyCfg.Phase = phase
	applyCfg.DryRun = false
	applyCfg.OutputTimestamp = stamp

	report := runMigrate(applyCfg)
	reportPaths, err := writeReportOutputs(applyCfg, report)
	if err != nil {
		return Report{}, false, err
	}
	printSummary(os.Stdout, report, reportPaths)
	return report, true, nil
}

func showPreparedPostMigrationFilesFromCurrentOutputs(cfg Config) error {
	postCfg := cfg
	postCfg.Phase = phasePostMigrate
	postCfg.DryRun = true

	state, findings := loadMigrationState(postCfg)
	if hasErrors(findings) {
		return nil
	}
	_ = preparePostMigrationTargetArtifacts(postCfg, &state)
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
		if err := ensureOutputDir(cfg.OutputDir); err != nil {
			return nil, fmt.Errorf("creating output directory: %w", err)
		}
		reportBase := strings.ReplaceAll(cfg.Command, " ", "-")
		jsonPath := defaultOutputPathForFormat(cfg, ReportFormatJSON)
		if err := writeReport(report, ReportFormatJSON, jsonPath); err != nil {
			return nil, fmt.Errorf("writing json report: %w", err)
		}
		if err := pruneOutputFamily(cfg.OutputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatJSON), outputRetentionLimit); err != nil {
			return nil, fmt.Errorf("pruning json reports: %w", err)
		}
		csvPath := defaultOutputPathForFormat(cfg, ReportFormatCSV)
		if err := writeReport(report, ReportFormatCSV, csvPath); err != nil {
			return nil, fmt.Errorf("writing csv report: %w", err)
		}
		if err := pruneOutputFamily(cfg.OutputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatCSV), outputRetentionLimit); err != nil {
			return nil, fmt.Errorf("pruning csv reports: %w", err)
		}
		reportPaths = []string{jsonPath, csvPath}
	}
	return reportPaths, nil
}
