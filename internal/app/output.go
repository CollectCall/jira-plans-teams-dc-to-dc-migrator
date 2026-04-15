package app

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

func writeReport(report Report, format ReportFormat, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	switch format {
	case ReportFormatJSON:
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	case ReportFormatCSV:
		return writeCSVReport(file, report)
	default:
		return fmt.Errorf("unsupported report format %q", format)
	}
}

func writeCSVReport(w io.Writer, report Report) error {
	writer := csv.NewWriter(w)
	defer writer.Flush()

	if err := writer.Write([]string{"section", "severity", "code", "message", "kind", "status", "details"}); err != nil {
		return err
	}

	for _, finding := range report.Findings {
		if err := writer.Write([]string{"finding", string(finding.Severity), finding.Code, finding.Message, "", "", ""}); err != nil {
			return err
		}
	}

	for _, action := range report.Actions {
		if err := writer.Write([]string{"action", "", "", "", action.Kind, action.Status, action.Details}); err != nil {
			return err
		}
	}

	if err := writer.Write([]string{"stats", "", "", "", "actions", strconv.Itoa(report.Stats.Actions), ""}); err != nil {
		return err
	}
	if err := writer.Write([]string{"stats", "", "", "", "warnings", strconv.Itoa(report.Stats.Warnings), ""}); err != nil {
		return err
	}
	if err := writer.Write([]string{"stats", "", "", "", "errors", strconv.Itoa(report.Stats.Errors), ""}); err != nil {
		return err
	}

	return writer.Error()
}

func printSummary(w io.Writer, report Report) {
	fmt.Fprintf(w, "command=%s dry_run=%t strict=%t actions=%d warnings=%d errors=%d\n",
		report.Command,
		report.DryRun,
		report.Strict,
		report.Stats.Actions,
		report.Stats.Warnings,
		report.Stats.Errors,
	)
	for _, finding := range report.Findings {
		fmt.Fprintf(w, "[%s] %s: %s\n", finding.Severity, finding.Code, finding.Message)
	}
}

func exitCodeFor(report Report) int {
	if report.Stats.Errors > 0 {
		return ExitFailure
	}
	if report.ExitBehavior.StrictIssuesDetected {
		return ExitStrictIssue
	}
	return ExitSuccess
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: teams-migrator <validate|plan|migrate|report> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Flags:")
	fmt.Fprintln(w, "  --source-base-url       Source Jira base URL")
	fmt.Fprintln(w, "  --source-auth-token     Source Jira auth token")
	fmt.Fprintln(w, "  --source-username       Source Jira username for basic auth")
	fmt.Fprintln(w, "  --source-password       Source Jira password for basic auth")
	fmt.Fprintln(w, "  --target-base-url       Target Jira base URL")
	fmt.Fprintln(w, "  --target-auth-token     Target Jira auth token")
	fmt.Fprintln(w, "  --target-username       Target Jira username for basic auth")
	fmt.Fprintln(w, "  --target-password       Target Jira password for basic auth")
	fmt.Fprintln(w, "  --identity-mapping      Path to identity mapping CSV")
	fmt.Fprintln(w, "  --teams-file            Path to source teams JSON export")
	fmt.Fprintln(w, "  --persons-file          Path to source persons JSON export")
	fmt.Fprintln(w, "  --resources-file        Path to source resources JSON export")
	fmt.Fprintln(w, "  --issues-csv            Optional issues CSV")
	fmt.Fprintln(w, "  --output-dir            Directory for generated reports")
	fmt.Fprintln(w, "  --format                Report output: json or csv")
	fmt.Fprintln(w, "  --strict                Exit non-zero on warnings or errors")
	fmt.Fprintln(w, "  --dry-run               Preview mutating operations")
	fmt.Fprintln(w, "  --apply                 Disable dry-run for migrate")
	fmt.Fprintln(w, "  --input                 Input report JSON for the report command")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Environment:")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_BASE_URL")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_AUTH_TOKEN")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_USERNAME")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_PASSWORD")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_BASE_URL")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_AUTH_TOKEN")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_USERNAME")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_PASSWORD")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_IDENTITY_MAPPING_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TEAMS_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_PERSONS_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_RESOURCES_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_ISSUES_CSV")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_OUTPUT_DIR")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_REPORT_FORMAT")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_STRICT")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_DRY_RUN")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_REPORT_INPUT")
}
