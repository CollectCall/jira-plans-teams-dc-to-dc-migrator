package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func runMigrate(cfg Config) Report {
	report := newReport(cfg)
	report.Findings = append(report.Findings, cfg.requireCoreInputs()...)
	if hasErrors(report.Findings) {
		finalizeReport(&report)
		return report
	}

	applyPhase := (runsMigratePhase(cfg.Command, cfg.Phase) || runsPostMigratePhase(cfg.Command, cfg.Phase)) && !cfg.DryRun
	state, findings, actions := executeMigration(cfg, applyPhase)
	return finalizeMigrationExecutionReport(report, cfg, state, findings, actions)
}

func runPreview(cfg Config) Report {
	report := newReport(cfg)
	report.DryRun = true
	report.Findings = append(report.Findings, cfg.requireCoreInputs()...)
	if hasErrors(report.Findings) {
		finalizeReport(&report)
		return report
	}

	state, findings, actions := executeMigration(cfg, false)
	return populateExecutionReport(report, state, findings, actions, "apply_preview", "Preview generated before apply mode confirmation")
}

func finalizeMigrationExecutionReport(report Report, cfg Config, state migrationState, findings []Finding, actions []Action) Report {
	report = populateExecutionReport(report, state, findings, actions, "", "")
	switch normalizeMigrationPhase(cfg.Phase) {
	case phasePreMigrate:
		report.Findings = append(report.Findings, newFinding(SeverityInfo, "pre_migrate_phase_complete", "Pre-migrate phase completed; no remote writes were sent"))
	case phasePostMigrate:
		if !cfg.DryRun {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "post_migrate_phase_complete", "Post-migrate corrections were applied where the current target state still matched the prepared rewrites"))
		} else {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "post_migrate_phase_preview", "Post-migrate phase preview generated; rerun with --apply to execute the prepared corrections"))
		}
	case phaseMigrate:
		if !cfg.DryRun {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "apply_mode_active", "Apply mode executed remote write operations where mappings were resolvable"))
		} else {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "dry_run_default", "Dry-run is active; no remote writes were sent"))
		}
	default:
		if !cfg.DryRun {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "apply_mode_active", "Apply mode executed remote write operations where mappings were resolvable"))
		} else {
			report.Findings = append(report.Findings, newFinding(SeverityInfo, "dry_run_default", "Dry-run is active; no remote writes were sent"))
		}
	}
	if !runsMigratePhase(cfg.Command, cfg.Phase) && !runsPostMigratePhase(cfg.Command, cfg.Phase) && !cfg.DryRun {
		report.Findings = append(report.Findings, newFinding(SeverityError, "phase_apply_unsupported", fmt.Sprintf("%s does not support apply mode", normalizeMigrationPhase(cfg.Phase))))
	}
	finalizeReport(&report)
	return report
}

func runReport(cfg Config) (Report, error) {
	reportInput, err := cleanInputFilePath("report input", cfg.ReportInput)
	if err != nil {
		return Report{}, err
	}
	file, err := os.OpenInRoot(filepath.Dir(reportInput), filepath.Base(reportInput))
	if err != nil {
		return Report{}, fmt.Errorf("opening report input: %w", err)
	}
	defer file.Close()

	var report Report
	if err := json.NewDecoder(file).Decode(&report); err != nil {
		return Report{}, fmt.Errorf("decoding report input: %w", err)
	}

	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return Report{}, err
	}
	cfg.OutputDir = outputDir
	if err := ensureOutputDir(outputDir); err != nil {
		return Report{}, err
	}
	if err := writeReport(report, cfg.ReportFormat, defaultOutputPath(cfg)); err != nil {
		return Report{}, err
	}
	return report, nil
}

func newReport(cfg Config) Report {
	return Report{
		Command:     cfg.Command,
		Phase:       cfg.Phase,
		DryRun:      cfg.DryRun,
		Strict:      cfg.Strict,
		GeneratedAt: time.Now().UTC(),
		Source: Endpoint{
			BaseURL: cfg.SourceBaseURL,
			Mode:    sourceMode(cfg),
		},
		Target: Endpoint{
			BaseURL: cfg.TargetBaseURL,
			Mode:    targetMode(cfg),
		},
		Inputs: InputFiles{
			IdentityMapping:      cfg.IdentityMappingFile,
			Teams:                cfg.TeamsFile,
			Persons:              cfg.PersonsFile,
			Resources:            cfg.ResourcesFile,
			IssuesCSV:            cfg.IssuesCSV,
			FilterSourceCSV:      cfg.FilterSourceCSV,
			TeamScope:            cfg.TeamScope,
			IssueProjectScope:    cfg.IssueProjectScope,
			IssueTeamIDsInScope:  cfg.IssueTeamIDsInScope,
			FilterTeamIDsInScope: cfg.FilterTeamIDsInScope,
			ParentLinkInScope:    cfg.ParentLinkInScope,
			FilterDataSource:     cfg.FilterDataSource,
		},
		ExitBehavior: ExitBehavior{
			SuccessCode:     ExitSuccess,
			FatalErrorCode:  ExitFailure,
			StrictIssueCode: ExitStrictIssue,
		},
	}
}

func finalizeReport(report *Report) {
	report.Stats = ReportStats{}
	report.ExitBehavior.StrictIssuesDetected = false
	for _, finding := range report.Findings {
		switch finding.Severity {
		case SeverityInfo:
			report.Stats.Infos++
		case SeverityWarning:
			report.Stats.Warnings++
		case SeverityError:
			report.Stats.Errors++
		}
	}
	report.Stats.Actions = len(report.Actions)
	report.ExitBehavior.StrictIssuesDetected = report.Strict && (report.Stats.Warnings > 0 || report.Stats.Errors > 0)
}

func populateExecutionReport(report Report, state migrationState, findings []Finding, actions []Action, infoCode, infoMessage string) Report {
	report.Findings = append(report.Findings, findings...)
	report.Actions = append(report.Actions, actions...)
	report.Metadata = migrationMetadata(state)
	if infoCode != "" && infoMessage != "" {
		report.Findings = append(report.Findings, newFinding(SeverityInfo, infoCode, infoMessage))
	}
	finalizeReport(&report)
	return report
}

func buildPostMigrationFollowUpPreviewReport(cfg Config, state migrationState) Report {
	postCfg := cfg
	postCfg.Phase = phasePostMigrate
	postCfg.DryRun = true

	report := newReport(postCfg)
	report.Metadata = migrationMetadata(state)
	report.Findings = append(report.Findings,
		newFinding(SeverityInfo, "post_migrate_followup_preview", "Post-migrate preview generated from the prepared correction mapping files"),
		newFinding(SeverityInfo, "post_migrate_phase_preview", "Post-migrate phase preview generated; rerun with --apply to execute the prepared corrections"),
	)
	finalizeReport(&report)
	return report
}

func newFinding(severity Severity, code, message string) Finding {
	return Finding{Severity: severity, Code: code, Message: message}
}

func hasErrors(findings []Finding) bool {
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			return true
		}
	}
	return false
}

func sourceMode(cfg Config) string {
	if cfg.TeamsFile != "" || cfg.PersonsFile != "" || cfg.ResourcesFile != "" {
		return "file"
	}
	if cfg.SourceBaseURL != "" {
		return "api"
	}
	return "unset"
}

func targetMode(cfg Config) string {
	if cfg.TargetBaseURL != "" {
		return "api"
	}
	return "unset"
}
