package app

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

var errUsage = errors.New("usage")

func Run(args []string) int {
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
	case "validate":
		report = runValidate(cfg)
	case "plan":
		report = runPlan(cfg)
	case "migrate":
		if !cfg.DryRun && !cfg.NoInput && isInteractiveTerminal() {
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
	case "config init":
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

	var reportPaths []string
	if cfg.Command != "report" {
		if err := ensureOutputDir(cfg.OutputDir); err != nil {
			fmt.Fprintf(os.Stderr, "error: creating output directory: %v\n", err)
			return ExitFailure
		}
		reportBase := strings.ReplaceAll(cfg.Command, " ", "-")
		jsonPath := defaultOutputPathForFormat(cfg, ReportFormatJSON)
		if err := writeReport(report, ReportFormatJSON, jsonPath); err != nil {
			fmt.Fprintf(os.Stderr, "error: writing json report: %v\n", err)
			return ExitFailure
		}
		if err := pruneOutputFamily(cfg.OutputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatJSON), outputRetentionLimit); err != nil {
			fmt.Fprintf(os.Stderr, "error: pruning json reports: %v\n", err)
			return ExitFailure
		}
		csvPath := defaultOutputPathForFormat(cfg, ReportFormatCSV)
		if err := writeReport(report, ReportFormatCSV, csvPath); err != nil {
			fmt.Fprintf(os.Stderr, "error: writing csv report: %v\n", err)
			return ExitFailure
		}
		if err := pruneOutputFamily(cfg.OutputDir, fmt.Sprintf("%s-report.%s", reportBase, ReportFormatCSV), outputRetentionLimit); err != nil {
			fmt.Fprintf(os.Stderr, "error: pruning csv reports: %v\n", err)
			return ExitFailure
		}
		reportPaths = []string{jsonPath, csvPath}
	}

	printSummary(os.Stdout, report, reportPaths)
	return exitCodeFor(report)
}
