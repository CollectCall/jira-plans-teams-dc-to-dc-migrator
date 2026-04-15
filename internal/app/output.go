package app

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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

func printSummary(w io.Writer, report Report, reportPath string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style(summaryTitle(report), theme.titleColor))
	fmt.Fprintf(w, "%s %s\n", theme.style("Mode:", theme.labelColor), summaryMode(report))
	fmt.Fprintf(w, "%s %s\n", theme.style("Source:", theme.labelColor), summaryEndpoint(report.Source))
	fmt.Fprintf(w, "%s %s\n", theme.style("Target:", theme.labelColor), summaryEndpoint(report.Target))
	if reportPath != "" {
		fmt.Fprintf(w, "%s %s\n", theme.style("Report:", theme.labelColor), reportPath)
	}

	artifactLines := summaryArtifacts(report)
	if len(artifactLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Artifacts", theme.titleColor))
		for _, line := range artifactLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	migrationLines := summarizeMigrationActions(report.Actions)

	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Results", theme.titleColor))
	fmt.Fprintf(w, "- Migration actions: %d\n", countMigrationActions(report.Actions))
	fmt.Fprintf(w, "- Warnings: %d\n", report.Stats.Warnings)
	fmt.Fprintf(w, "- Errors: %d\n", report.Stats.Errors)

	if len(migrationLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Migration Summary", theme.titleColor))
		for _, line := range migrationLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	previewSections := summaryPreviews(report)
	for _, section := range previewSections {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style(section.Title, theme.titleColor))
		for _, line := range section.Lines {
			fmt.Fprintln(w, line)
		}
	}

	infoLines, warningLines, errorLines := categorizeFindings(report.Findings)
	if len(errorLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Errors", theme.errorColor))
		for _, line := range errorLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}
	if len(warningLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Warnings", theme.hintColor))
		for _, line := range warningLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}
	if len(infoLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Notes", theme.labelColor))
		for _, line := range infoLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}
}

func summaryTitle(report Report) string {
	switch report.Command {
	case "validate":
		return "Validation completed"
	case "plan":
		return "Migration plan generated"
	case "migrate":
		if report.DryRun {
			return "Migration dry run completed"
		}
		return "Migration run completed"
	case "report":
		return "Report exported"
	default:
		return "Operation completed"
	}
}

func summaryMode(report Report) string {
	parts := []string{}
	if report.DryRun {
		parts = append(parts, "dry-run")
	} else if report.Command == "migrate" {
		parts = append(parts, "apply")
	}
	if report.Strict {
		parts = append(parts, "strict")
	}
	if len(parts) == 0 {
		return "standard"
	}
	return strings.Join(parts, ", ")
}

func summaryEndpoint(endpoint Endpoint) string {
	if endpoint.BaseURL == "" {
		if endpoint.Mode == "" || endpoint.Mode == "unset" {
			return "not configured"
		}
		return endpoint.Mode
	}
	return fmt.Sprintf("%s (%s)", endpoint.BaseURL, endpoint.Mode)
}

func summaryArtifacts(report Report) []string {
	lines := []string{}
	if report.Metadata == nil {
		return lines
	}
	if artifacts, ok := report.Metadata["artifacts"].([]Artifact); ok {
		for _, artifact := range artifacts {
			if artifact.Label != "" && artifact.Path != "" {
				lines = append(lines, fmt.Sprintf("%s: %s", artifact.Label, artifact.Path))
			}
		}
	}
	if artifacts, ok := report.Metadata["artifacts"].([]any); ok {
		for _, item := range artifacts {
			artifact, ok := item.(map[string]any)
			if !ok {
				continue
			}
			label, _ := artifact["label"].(string)
			path, _ := artifact["path"].(string)
			if label != "" && path != "" {
				lines = append(lines, fmt.Sprintf("%s: %s", label, path))
			}
		}
	}
	return uniqueStrings(lines)
}

func summarizeMigrationActions(actions []Action) []string {
	if len(actions) == 0 {
		return nil
	}
	counts := map[string]int{}
	for _, action := range actions {
		if !isMigrationAction(action.Kind) {
			continue
		}
		key := action.Kind + ":" + action.Status
		counts[key]++
	}
	if len(counts) == 0 {
		return nil
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		parts := strings.SplitN(key, ":", 2)
		kind := parts[0]
		status := parts[1]
		lines = append(lines, fmt.Sprintf("%s %s: %d", titleize(kind), status, counts[key]))
	}
	return lines
}

func countMigrationActions(actions []Action) int {
	count := 0
	for _, action := range actions {
		if isMigrationAction(action.Kind) {
			count++
		}
	}
	return count
}

func isMigrationAction(kind string) bool {
	switch kind {
	case "team", "resource", "program", "plan":
		return true
	default:
		return false
	}
}

func categorizeFindings(findings []Finding) (infoLines, warningLines, errorLines []string) {
	for _, finding := range findings {
		if isArtifactFinding(finding.Code) {
			continue
		}
		line := finding.Message
		switch finding.Severity {
		case SeverityInfo:
			infoLines = append(infoLines, line)
		case SeverityWarning:
			warningLines = append(warningLines, line)
		case SeverityError:
			errorLines = append(errorLines, line)
		}
	}
	return uniqueStrings(infoLines), uniqueStrings(warningLines), uniqueStrings(errorLines)
}

func isArtifactFinding(code string) bool {
	if strings.HasSuffix(code, "_generated") {
		return true
	}
	switch code {
	case "teams_field_issue_exported":
		return true
	default:
		return false
	}
}

type previewSection struct {
	Title string
	Lines []string
}

func summaryPreviews(report Report) []previewSection {
	if report.Metadata == nil {
		return nil
	}
	imd, ok := report.Metadata["imd"].(map[string]any)
	if !ok {
		return nil
	}

	sections := []previewSection{}
	if rows := programPreviewRows(imd["programs"]); len(rows) > 0 {
		sections = append(sections, previewSection{Title: "Program Preview", Lines: rows})
	}
	if rows := planPreviewRows(imd["plans"]); len(rows) > 0 {
		sections = append(sections, previewSection{Title: "Plan Preview", Lines: rows})
	}
	if rows := teamPreviewRows(imd["teams"]); len(rows) > 0 {
		sections = append(sections, previewSection{Title: "Team Preview", Lines: rows})
	}
	if rows := membershipPreviewRows(imd["resources"]); len(rows) > 0 {
		sections = append(sections, previewSection{Title: "Team Membership Preview", Lines: rows})
	}
	if rows := issuePreviewRows(imd["issues"]); len(rows) > 0 {
		sections = append(sections, previewSection{Title: "Issues With Team Values Preview", Lines: rows})
	}
	return sections
}

func programPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []ProgramMapping:
		return limitLines(formatProgramMappings(rows))
	case []any:
		return limitLines(formatProgramMappingMaps(rows))
	default:
		return nil
	}
}

func planPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []PlanMapping:
		return limitLines(formatPlanMappings(rows))
	case []any:
		return limitLines(formatPlanMappingMaps(rows))
	default:
		return nil
	}
}

func teamPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []TeamMapping:
		return limitLines(formatTeamMappings(rows))
	case []any:
		return limitLines(formatTeamMappingMaps(rows))
	default:
		return nil
	}
}

func membershipPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []ResourcePlan:
		return limitLines(formatResourcePlans(rows))
	case []any:
		return limitLines(formatResourcePlanMaps(rows))
	default:
		return nil
	}
}

func issuePreviewRows(value any) []string {
	switch rows := value.(type) {
	case []IssueTeamRow:
		return limitLines(formatIssueRows(rows))
	case []any:
		return limitLines(formatIssueRowMaps(rows))
	default:
		return nil
	}
}

func formatProgramMappings(rows []ProgramMapping) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s -> %s (%s)", row.SourceTitle, labelWithID(row.TargetTitle, row.TargetProgramID), row.Decision))
	}
	return lines
}

func formatProgramMappingMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s -> %s (%s)",
			asString(row["sourceTitle"]),
			labelWithID(asString(row["targetTitle"]), asString(row["targetProgramId"])),
			asString(row["decision"]),
		))
	}
	return lines
}

func formatPlanMappings(rows []PlanMapping) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s [%s] -> %s (%s)", row.SourceTitle, row.SourceProgramTitle, labelWithID(row.TargetTitle, row.TargetPlanID), row.Decision))
	}
	return lines
}

func formatPlanMappingMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s [%s] -> %s (%s)",
			asString(row["sourceTitle"]),
			asString(row["sourceProgramTitle"]),
			labelWithID(asString(row["targetTitle"]), asString(row["targetPlanId"])),
			asString(row["decision"]),
		))
	}
	return lines
}

func formatTeamMappings(rows []TeamMapping) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s -> %s (%s)", row.SourceTitle, labelWithID(row.TargetTitle, row.TargetTeamID), row.Decision))
	}
	return lines
}

func formatTeamMappingMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s -> %s (%s)",
			asString(row["sourceTitle"]),
			labelWithID(asString(row["targetTitle"]), asString(row["targetTeamId"])),
			asString(row["decision"]),
		))
	}
	return lines
}

func formatResourcePlans(rows []ResourcePlan) []string {
	lines := []string{}
	for _, row := range rows {
		person := row.SourceEmail
		if person == "" {
			person = fmt.Sprintf("person:%d", row.SourcePersonID)
		}
		lines = append(lines, fmt.Sprintf("- %s / %s -> %s / %s (%s)", row.SourceTeamName, person, row.TargetTeamName, row.TargetUserID, row.Status))
	}
	return lines
}

func formatResourcePlanMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		person := asString(row["sourceEmail"])
		if person == "" {
			person = fmt.Sprintf("person:%s", asString(row["sourcePersonId"]))
		}
		lines = append(lines, fmt.Sprintf("- %s / %s -> %s / %s (%s)",
			asString(row["sourceTeamName"]),
			person,
			asString(row["targetTeamName"]),
			asString(row["targetUserId"]),
			asString(row["status"]),
		))
	}
	return lines
}

func formatIssueRows(rows []IssueTeamRow) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s: %s [%s]", row.IssueKey, row.Summary, row.SourceTeamNames))
	}
	return lines
}

func formatIssueRowMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s: %s [%s]",
			asString(row["issueKey"]),
			asString(row["summary"]),
			asString(row["sourceTeamNames"]),
		))
	}
	return lines
}

func limitLines(lines []string) []string {
	const maxPreviewRows = 5
	if len(lines) <= maxPreviewRows {
		return lines
	}
	remaining := len(lines) - maxPreviewRows
	out := append([]string{}, lines[:maxPreviewRows]...)
	out = append(out, fmt.Sprintf("- ... and %d more", remaining))
	return out
}

func labelWithID(label, id string) string {
	if label == "" {
		return id
	}
	if id == "" {
		return label
	}
	return fmt.Sprintf("%s [%s]", label, id)
}

func asString(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func titleize(value string) string {
	if value == "" {
		return value
	}
	value = strings.ReplaceAll(value, "_", " ")
	return strings.ToUpper(value[:1]) + value[1:]
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
	fmt.Fprintln(w, "Usage: teams-migrator <validate|plan|migrate|report|version|self-update> [flags]")
	fmt.Fprintln(w, "       teams-migrator config <init|show|path> [flags]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Flags:")
	fmt.Fprintln(w, "  --source-base-url       Source Jira base URL")
	fmt.Fprintln(w, "  --source-username       Source Jira username for basic auth")
	fmt.Fprintln(w, "  --source-password       Source Jira password for basic auth")
	fmt.Fprintln(w, "  --target-base-url       Target Jira base URL")
	fmt.Fprintln(w, "  --target-username       Target Jira username for basic auth")
	fmt.Fprintln(w, "  --target-password       Target Jira password for basic auth")
	fmt.Fprintln(w, "  --identity-mapping      Path to identity mapping CSV")
	fmt.Fprintln(w, "  --teams-file            Path to source teams JSON export")
	fmt.Fprintln(w, "  --persons-file          Path to source persons JSON export")
	fmt.Fprintln(w, "  --resources-file        Path to source resources JSON export")
	fmt.Fprintln(w, "  --issues-csv            Optional issues CSV")
	fmt.Fprintln(w, "  --output-dir            Directory for generated reports")
	fmt.Fprintln(w, "  --format                Report output: json or csv")
	fmt.Fprintln(w, "  --config                Path to config.yaml profile store")
	fmt.Fprintln(w, "  --profile               Saved profile name")
	fmt.Fprintln(w, "  --redacted              Kept for compatibility; config show no longer reads secrets from YAML")
	fmt.Fprintln(w, "  --strict                Exit non-zero on warnings or errors")
	fmt.Fprintln(w, "  --dry-run               Preview mutating operations")
	fmt.Fprintln(w, "  --apply                 Disable dry-run for migrate")
	fmt.Fprintln(w, "  --no-input              Disable interactive prompts")
	fmt.Fprintln(w, "  --input                 Input report JSON for the report command")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Environment:")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_BASE_URL")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_USERNAME")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_PASSWORD")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_BASE_URL")
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
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_CONFIG_PATH")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_PROFILE")
}
