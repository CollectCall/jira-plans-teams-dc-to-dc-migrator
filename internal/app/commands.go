package app

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func runValidate(cfg Config) Report {
	report := newReport(cfg)
	report.Findings = append(report.Findings, cfg.requireCoreInputs()...)
	report.Findings = append(report.Findings, newFinding(SeverityInfo, "validate_complete", "Validation completed"))
	finalizeReport(&report)
	return report
}

func runPlan(cfg Config) Report {
	report := newReport(cfg)
	report.Findings = append(report.Findings, cfg.requireCoreInputs()...)
	if hasErrors(report.Findings) {
		finalizeReport(&report)
		return report
	}

	state, findings, actions := executeMigration(cfg, false)
	report.Findings = append(report.Findings, findings...)
	report.Actions = append(report.Actions, actions...)
	report.Metadata = migrationMetadata(state)
	report.Findings = append(report.Findings, newFinding(SeverityInfo, "plan_generated", "Execution plan generated from source and target data"))
	finalizeReport(&report)
	return report
}

func runMigrate(cfg Config) Report {
	report := newReport(cfg)
	report.Findings = append(report.Findings, cfg.requireCoreInputs()...)
	if hasErrors(report.Findings) {
		finalizeReport(&report)
		return report
	}

	state, findings, actions := executeMigration(cfg, !cfg.DryRun)
	report.Findings = append(report.Findings, findings...)
	report.Actions = append(report.Actions, actions...)
	report.Metadata = migrationMetadata(state)
	if !cfg.DryRun {
		report.Findings = append(report.Findings, newFinding(SeverityInfo, "apply_mode_active", "Apply mode executed remote write operations where mappings were resolvable"))
	} else {
		report.Findings = append(report.Findings, newFinding(SeverityInfo, "dry_run_default", "Dry-run is active; no remote writes were sent"))
	}
	finalizeReport(&report)
	return report
}

func runReport(cfg Config) (Report, error) {
	file, err := os.Open(cfg.ReportInput)
	if err != nil {
		return Report{}, fmt.Errorf("opening report input: %w", err)
	}
	defer file.Close()

	var report Report
	if err := json.NewDecoder(file).Decode(&report); err != nil {
		return Report{}, fmt.Errorf("decoding report input: %w", err)
	}

	if err := ensureOutputDir(cfg.OutputDir); err != nil {
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
			IdentityMapping: cfg.IdentityMappingFile,
			Teams:           cfg.TeamsFile,
			Persons:         cfg.PersonsFile,
			Resources:       cfg.ResourcesFile,
			IssuesCSV:       cfg.IssuesCSV,
		},
		ExitBehavior: ExitBehavior{
			SuccessCode:     ExitSuccess,
			FatalErrorCode:  ExitFailure,
			StrictIssueCode: ExitStrictIssue,
		},
	}
}

func finalizeReport(report *Report) {
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
