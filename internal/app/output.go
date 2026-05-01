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

func printSummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style(summaryTitle(report), theme.titleColor))
	fmt.Fprintf(w, "%s %s\n", theme.style("Mode:", theme.labelColor), summaryMode(report))
	fmt.Fprintf(w, "%s %s\n", theme.style("Source:", theme.labelColor), summaryEndpoint(report.Source))
	fmt.Fprintf(w, "%s %s\n", theme.style("Target:", theme.labelColor), summaryEndpoint(report.Target))
	if len(reportPaths) == 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Report:", theme.labelColor), reportPaths[0])
	} else if len(reportPaths) > 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Reports:", theme.labelColor), strings.Join(reportPaths, ", "))
	}

	phaseLines := summaryPhaseLines(report)
	if len(phaseLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Execution Phases", theme.titleColor))
		for _, line := range phaseLines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	artifactLines := summaryArtifacts(report)
	if len(artifactLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Artifacts", theme.titleColor))
		for _, line := range summaryArtifactLines(artifactLines) {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	migrationLines := summarizeMigrationActions(report.Actions)

	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Results", theme.titleColor))
	fmt.Fprintf(w, "- %s: %d\n", migrationActionCountLabel(report), countMigrationActions(report.Actions))
	fmt.Fprintf(w, "- Warnings: %d\n", report.Stats.Warnings)
	fmt.Fprintf(w, "- Errors: %d\n", report.Stats.Errors)

	if len(migrationLines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Action Summary", theme.titleColor))
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
		if hasJiraConnectivityFinding(report.Findings) {
			fmt.Fprintln(w)
			fmt.Fprintln(w, theme.style("Connectivity", theme.hintColor))
			fmt.Fprintln(w, "- Could not reach source/target Jira. Check base URL, VPN/DNS, and credentials.")
		}
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

func printInteractivePreMigrateSummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style("Pre-migrate phase completed", theme.titleColor))
	fmt.Fprintf(w, "%s %s\n", theme.style("Source:", theme.labelColor), summaryEndpoint(report.Source))
	fmt.Fprintf(w, "%s %s\n", theme.style("Target:", theme.labelColor), summaryEndpoint(report.Target))
	if len(reportPaths) == 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Report:", theme.labelColor), reportPaths[0])
	} else if len(reportPaths) > 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Reports:", theme.labelColor), strings.Join(reportPaths, ", "))
	}

	artifacts := summaryArtifacts(report)
	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Prepared", theme.titleColor))
	if len(artifacts) > 0 {
		fmt.Fprintf(w, "- Review artifacts: %d\n", len(artifacts))
	}
	if imd := metadataIMD(report); imd != nil {
		for _, line := range preMigratePreviewLines(imd) {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	if imd := metadataIMD(report); imd != nil {
		if lines := migratePreviewLines(imd); len(lines) > 0 {
			fmt.Fprintln(w)
			fmt.Fprintln(w, theme.style("Migrate Readiness", theme.titleColor))
			for _, line := range lines {
				fmt.Fprintf(w, "- %s\n", line)
			}
		}
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Status", theme.titleColor))
	fmt.Fprintf(w, "- Warnings: %d\n", report.Stats.Warnings)
	fmt.Fprintf(w, "- Errors: %d\n", report.Stats.Errors)
	if report.Stats.Warnings > 0 {
		fmt.Fprintln(w, "- Warning details are in the report files.")
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Next Steps", theme.titleColor))
	fmt.Fprintln(w, "- Review team mapping decisions before applying the migrate phase.")
	if path := firstArtifactPathContaining(artifacts, "team mapping comparison"); path != "" {
		fmt.Fprintf(w, "- Team mapping: %s\n", artifactPathFromSummaryLine(path))
	}
	if path := firstArtifactPathContaining(artifacts, "team membership mapping comparison"); path != "" {
		fmt.Fprintf(w, "- Membership mapping: %s\n", artifactPathFromSummaryLine(path))
	}
	fmt.Fprintln(w, "- Resume later with: teams-migrator migrate --phase migrate")
}

func printInteractivePhasePreviewSummary(w io.Writer, report Report, reportPaths []string) {
	switch reportPhase(report) {
	case phaseMigrate:
		printInteractiveMigratePreviewSummary(w, report, reportPaths)
	case phasePostMigrate:
		printInteractivePostMigratePreviewSummary(w, report, reportPaths)
	default:
		printSummary(w, report, reportPaths)
	}
}

func printInteractivePhaseApplySummary(w io.Writer, report Report, reportPaths []string) {
	switch reportPhase(report) {
	case phaseMigrate:
		printInteractiveMigrateApplySummary(w, report, reportPaths)
	case phasePostMigrate:
		printInteractivePostMigrateApplySummary(w, report, reportPaths)
	default:
		printSummary(w, report, reportPaths)
	}
}

func printInteractiveMigratePreviewSummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style("Migrate preview ready", theme.titleColor))
	printSummaryReportPaths(w, reportPaths)

	if imd := metadataIMD(report); imd != nil {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Plan", theme.titleColor))
		for _, line := range migratePreviewLines(imd) {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	if lines := summarizeResourceActions(report.Actions); len(lines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Memberships", theme.titleColor))
		for _, line := range lines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	printCompactStatus(w, report, "Planned actions")
	printMappingFileNextSteps(w, report)
}

func printInteractiveMigrateApplySummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style("Migrate phase completed", theme.titleColor))
	printSummaryReportPaths(w, reportPaths)

	if lines := summarizeMigrationActions(report.Actions); len(lines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Applied", theme.titleColor))
		for _, line := range lines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	} else {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Applied", theme.titleColor))
		fmt.Fprintln(w, "- No destination teams or memberships were created.")
	}

	if imd := metadataIMD(report); imd != nil {
		if lines := postMigratePreviewLines(imd); len(lines) > 0 {
			fmt.Fprintln(w)
			fmt.Fprintln(w, theme.style("Prepared For Post-migrate", theme.titleColor))
			for _, line := range lines {
				fmt.Fprintf(w, "- %s\n", line)
			}
		}
	}

	printCompactStatus(w, report, "Applied actions")
	if artifacts := summaryArtifacts(report); len(artifacts) > 0 {
		if path := firstArtifactPathContaining(artifacts, "migration team id mapping"); path != "" {
			fmt.Fprintln(w)
			fmt.Fprintln(w, theme.style("Next Steps", theme.titleColor))
			fmt.Fprintf(w, "- Team ID mapping: %s\n", artifactPathFromSummaryLine(path))
			fmt.Fprintln(w, "- Continue to post-migrate when you are ready to update Jira references.")
		}
	}
}

func printInteractivePostMigratePreviewSummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style("Post-migrate preview ready", theme.titleColor))
	printSummaryReportPaths(w, reportPaths)

	if imd := metadataIMD(report); imd != nil {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Correction Plan", theme.titleColor))
		for _, line := range postMigratePreviewLines(imd) {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	printCompactStatus(w, report, "")
	printPostMigrateReviewFiles(w, report)
}

func printInteractivePostMigrateApplySummary(w io.Writer, report Report, reportPaths []string) {
	theme := currentUITheme()
	fmt.Fprintf(w, "%s\n", theme.style("Post-migrate phase completed", theme.titleColor))
	printSummaryReportPaths(w, reportPaths)

	if lines := summarizePostMigrateActions(report.Actions); len(lines) > 0 {
		fmt.Fprintln(w)
		fmt.Fprintln(w, theme.style("Results", theme.titleColor))
		for _, line := range lines {
			fmt.Fprintf(w, "- %s\n", line)
		}
	}

	printCompactStatus(w, report, "")
	printPostMigrateReviewFiles(w, report)
}

func printSummaryReportPaths(w io.Writer, reportPaths []string) {
	theme := currentUITheme()
	if len(reportPaths) == 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Report:", theme.labelColor), reportPaths[0])
	} else if len(reportPaths) > 1 {
		fmt.Fprintf(w, "%s %s\n", theme.style("Reports:", theme.labelColor), strings.Join(reportPaths, ", "))
	}
}

func printCompactStatus(w io.Writer, report Report, actionLabel string) {
	theme := currentUITheme()
	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Status", theme.titleColor))
	if actionLabel != "" {
		fmt.Fprintf(w, "- %s: %d\n", actionLabel, countMigrationActions(report.Actions))
	}
	fmt.Fprintf(w, "- Warnings: %d\n", report.Stats.Warnings)
	fmt.Fprintf(w, "- Errors: %d\n", report.Stats.Errors)
}

func printMappingFileNextSteps(w io.Writer, report Report) {
	theme := currentUITheme()
	artifacts := summaryArtifacts(report)
	if len(artifacts) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Review Files", theme.titleColor))
	if path := firstArtifactPathContaining(artifacts, "team mapping"); path != "" {
		fmt.Fprintf(w, "- Team mapping: %s\n", artifactPathFromSummaryLine(path))
	}
	if path := firstArtifactPathContaining(artifacts, "membership"); path != "" {
		fmt.Fprintf(w, "- Membership mapping: %s\n", artifactPathFromSummaryLine(path))
	}
}

func printPostMigrateReviewFiles(w io.Writer, report Report) {
	theme := currentUITheme()
	artifacts := summaryArtifacts(report)
	lines := []string{}
	for _, needle := range []string{"issue comparison", "parent-link comparison", "filter jql comparison"} {
		if path := firstArtifactPathContaining(artifacts, needle); path != "" {
			lines = append(lines, artifactPathFromSummaryLine(path))
		}
	}
	if len(lines) == 0 {
		return
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, theme.style("Review Files", theme.titleColor))
	for _, line := range lines {
		fmt.Fprintf(w, "- %s\n", line)
	}
}

func summarizePostMigrateActions(actions []Action) []string {
	counts := map[string]int{}
	for _, action := range actions {
		switch action.Kind {
		case "post_migrate_issue_update":
			counts["Issue updates "+action.Status]++
		case "post_migrate_parent_link_update":
			counts["Parent Link updates "+action.Status]++
		case "post_migrate_filter_update":
			counts["Filter updates "+action.Status]++
		}
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("%s: %d", key, counts[key]))
	}
	return lines
}

func summarizeResourceActions(actions []Action) []string {
	counts := map[string]int{}
	for _, action := range actions {
		if action.Kind != "resource" {
			continue
		}
		counts[action.Status]++
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("Memberships %s: %d", key, counts[key]))
	}
	return lines
}

func migrationActionCountLabel(report Report) string {
	if report.Command == "migrate" && report.DryRun {
		return "Planned actions"
	}
	return "Applied actions"
}

func summaryTitle(report Report) string {
	switch report.Command {
	case "migrate":
		switch reportPhase(report) {
		case phasePreMigrate:
			return "Pre-migrate phase completed"
		case phasePostMigrate:
			if report.DryRun {
				return "Post-migrate preview completed"
			}
			return "Post-migrate apply completed"
		}
		if report.DryRun {
			return "Migrate phase dry run completed"
		}
		return "Migrate phase completed"
	case "report":
		return "Report exported"
	default:
		return "Operation completed"
	}
}

func summaryMode(report Report) string {
	parts := []string{}
	if phase := reportPhase(report); phase != "" && report.Command == "migrate" {
		parts = append(parts, phase)
	}
	if report.DryRun {
		parts = append(parts, "preview")
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

func summaryArtifactLines(artifacts []string) []string {
	if len(artifacts) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("Generated artifacts: %d", len(artifacts))}
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

func hasJiraConnectivityFinding(findings []Finding) bool {
	for _, finding := range findings {
		if isArtifactFinding(finding.Code) {
			continue
		}
		if finding.Severity != SeverityError && finding.Severity != SeverityWarning {
			continue
		}
		if isJiraConnectivityMessage(finding.Message) {
			return true
		}
	}
	return false
}

func isJiraConnectivityMessage(message string) bool {
	lower := strings.ToLower(message)
	needles := []string{
		"dial tcp",
		"no such host",
		"connection refused",
		"connection reset",
		"i/o timeout",
		"context deadline exceeded",
		"tls handshake timeout",
		"jira authentication failed",
		"returned 401",
		"returned 403",
	}
	for _, needle := range needles {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
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

func summaryPhaseLines(report Report) []string {
	imd := metadataIMD(report)
	if imd == nil {
		return nil
	}
	selectedPhase := reportPhase(report)

	issueRows := issueTeamRowsFromValue(imd["issues"])
	parentLinkRows := parentLinkRowsFromValue(imd["parentLinks"])
	parentLinkComparisons := parentLinkComparisonRowsFromValue(imd["parentLinkComparisons"])
	filterComparisons := filterComparisonRowsFromValue(imd["filterComparisons"])
	filterRows := filterTeamClauseRowsFromValue(imd["filters"])
	teamMappings := teamMappingsFromValue(imd["teams"])
	targetTeamIDs := targetTeamIDsBySource(teamMappings)

	preDetails := []string{}
	if artifactCount := len(summaryArtifacts(report)); artifactCount > 0 {
		preDetails = append(preDetails, fmt.Sprintf("%d review artifact(s)", artifactCount))
	}
	if len(issueRows) > 0 {
		preDetails = append(preDetails, fmt.Sprintf("%d issue/team export row(s)", len(issueRows)))
	}
	if len(parentLinkRows) > 0 {
		preDetails = append(preDetails, fmt.Sprintf("%d parent-link export row(s)", len(parentLinkRows)))
	}
	if len(filterRows) > 0 {
		preDetails = append(preDetails, fmt.Sprintf("%d filter team match(es)", len(filterRows)))
	}

	createCount := 0
	reuseCount := 0
	skipCount := 0
	for _, mapping := range teamMappings {
		switch mapping.Decision {
		case "add", "created":
			createCount++
		case "merge":
			reuseCount++
		case "skipped", "conflict":
			skipCount++
		}
	}

	preStatus := "ready"
	migrateStatus := "up next"
	postStatus := "up next"
	switch selectedPhase {
	case phasePreMigrate:
		preStatus = "completed"
	case phaseMigrate:
		preStatus = "skipped"
		migrateStatus = "planned"
		if report.Command == "migrate" && !report.DryRun {
			migrateStatus = "completed"
		}
	case phasePostMigrate:
		preStatus = "skipped"
		if createCount == 0 {
			migrateStatus = "skipped"
		} else {
			migrateStatus = "blocked"
		}
		postStatus = "preview"
		if report.Command == "migrate" && !report.DryRun {
			postStatus = "completed"
		}
	}

	issueUpdateCount := countMappedIssueUpdates(issueRows, targetTeamIDs)
	parentLinkUpdateCount := countReadyParentLinkUpdates(parentLinkComparisons)
	filterUpdateCount := countReadyFilterUpdates(filterComparisons)

	lines := []string{
		fmt.Sprintf("Pre-migrate [%s]: fetch source and destination data and generate comparison docs%s", preStatus, formatPhaseDetails(preDetails)),
		fmt.Sprintf("Migrate [%s]: create destination teams%s", migrateStatus, formatPhaseDetails(nonEmptyDetails(
			countLabel(createCount, "team to create", "teams to create"),
			countLabel(reuseCount, "existing match reused", "existing matches reused"),
			countLabel(skipCount, "team skipped/conflicted", "teams skipped/conflicted"),
		))),
		fmt.Sprintf("Post-migrate [%s]: update Jira team references%s", postStatus, formatPhaseDetails(nonEmptyDetails(
			countLabel(issueUpdateCount, "issue ready for team ID rewrite", "issues ready for team ID rewrites"),
			countLabel(parentLinkUpdateCount, "issue ready for Parent Link rewrite", "issues ready for Parent Link rewrites"),
			countLabel(filterUpdateCount, "filter ready for team ID rewrite", "filters ready for team ID rewrites"),
		))),
	}
	return lines
}

func summaryPreviews(report Report) []previewSection {
	imd := metadataIMD(report)
	if imd == nil {
		return nil
	}

	sections := []previewSection{}
	if rows := preMigratePreviewLines(imd); len(rows) > 0 {
		sections = append(sections, previewSection{Title: phasePreviewTitle(report, phasePreMigrate), Lines: rows})
	}
	if rows := migratePreviewLines(imd); len(rows) > 0 {
		sections = append(sections, previewSection{Title: phasePreviewTitle(report, phaseMigrate), Lines: rows})
	}
	if rows := postMigratePreviewLines(imd); len(rows) > 0 {
		sections = append(sections, previewSection{Title: phasePreviewTitle(report, phasePostMigrate), Lines: rows})
	}
	return sections
}

func phasePreviewTitle(report Report, phase string) string {
	if report.Command != "migrate" || report.DryRun {
		switch phase {
		case phasePreMigrate:
			return "Pre-migrate Preview"
		case phaseMigrate:
			return "Migrate Preview"
		case phasePostMigrate:
			return "Post-migrate Preview"
		}
	}

	current := reportPhase(report)
	switch phase {
	case phasePreMigrate:
		return "Pre-migrate Artifacts"
	case phaseMigrate:
		if current == phaseMigrate || current == phasePostMigrate {
			return "Migrate Results"
		}
		return "Migrate Preview"
	case phasePostMigrate:
		if current == phasePostMigrate {
			return "Post-migrate Results"
		}
		return "Post-migrate Preview"
	default:
		return "Preview"
	}
}

func metadataIMD(report Report) map[string]any {
	if report.Metadata == nil {
		return nil
	}
	imd, ok := report.Metadata["imd"].(map[string]any)
	if !ok {
		return nil
	}
	return imd
}

func preMigratePreviewLines(imd map[string]any) []string {
	lines := []string{}
	lines = appendPreviewCount(lines, "Programs compared", metadataCollectionCount(imd["programs"]))
	lines = appendPreviewCount(lines, "Plans compared", metadataCollectionCount(imd["plans"]))
	lines = appendPreviewCount(lines, "Teams compared", len(teamMappingsFromValue(imd["teams"])))
	lines = appendPreviewCount(lines, "Memberships compared", metadataCollectionCount(imd["resources"]))
	lines = appendPreviewCount(lines, "Issue/team export rows", len(issueTeamRowsFromValue(imd["issues"])))
	lines = appendPreviewCount(lines, "Parent Link export rows", len(parentLinkRowsFromValue(imd["parentLinks"])))
	lines = appendPreviewCount(lines, "Filter team matches", len(filterTeamClauseRowsFromValue(imd["filters"])))
	return lines
}

func migratePreviewLines(imd map[string]any) []string {
	mappings := teamMappingsFromValue(imd["teams"])
	if len(mappings) == 0 {
		return nil
	}

	createCount := 0
	reuseCount := 0
	skipCount := 0
	for _, mapping := range mappings {
		switch mapping.Decision {
		case "add", "created":
			createCount++
		case "merge":
			reuseCount++
		case "skipped", "conflict":
			skipCount++
		}
	}

	lines := []string{}
	lines = appendPreviewCount(lines, "Teams to create", createCount)
	lines = appendPreviewCount(lines, "Existing teams to reuse", reuseCount)
	lines = appendPreviewCount(lines, "Teams skipped or conflicted", skipCount)
	if len(lines) == 0 {
		return []string{"No destination team creation is required."}
	}
	return lines
}

func postMigratePreviewLines(imd map[string]any) []string {
	mappings := teamMappingsFromValue(imd["teams"])
	issueRows := issueTeamRowsFromValue(imd["issues"])
	parentLinkComparisons := parentLinkComparisonRowsFromValue(imd["parentLinkComparisons"])
	filterMatches := filterMatchRowsFromValue(imd["filterMatches"])
	filterComparisons := filterComparisonRowsFromValue(imd["filterComparisons"])
	targetTeamIDs := targetTeamIDsBySource(mappings)

	issueReadyCount := 0
	for _, row := range issueRows {
		if len(mappedTargetTeamRewritePairs(row.SourceTeamIDs, targetTeamIDs)) > 0 {
			issueReadyCount++
		}
	}

	parentLinkReadyCount := 0
	for _, row := range parentLinkComparisons {
		if row.Status != "ready" && row.Status != "already_rewritten" {
			continue
		}
		if row.TargetParentIssueID == "" {
			continue
		}
		parentLinkReadyCount++
	}

	filterCandidateCount := 0
	filterNoCandidateCount := 0
	filterAmbiguousCount := 0
	if len(filterMatches) > 0 {
		for _, row := range filterMatches {
			switch row.Status {
			case "matched":
				filterCandidateCount++
			case "not_found":
				filterNoCandidateCount++
			case "ambiguous":
				filterAmbiguousCount++
			}
		}
	} else {
		seen := map[string]bool{}
		for _, row := range filterComparisons {
			if seen[row.SourceFilterID] {
				continue
			}
			seen[row.SourceFilterID] = true
			switch row.Status {
			case "ready", "already_rewritten", "same_id":
				filterCandidateCount++
			case "not_found":
				filterNoCandidateCount++
			case "ambiguous":
				filterAmbiguousCount++
			}
		}
	}

	lines := []string{}
	if issueReadyCount > 0 {
		lines = append(lines, fmt.Sprintf("Issue rewrites prepared: %d", issueReadyCount))
	}
	if parentLinkReadyCount > 0 {
		lines = append(lines, fmt.Sprintf("Parent Link rewrites prepared: %d", parentLinkReadyCount))
	}
	if filterCandidateCount > 0 || filterNoCandidateCount > 0 || filterAmbiguousCount > 0 {
		lines = append(lines, fmt.Sprintf("Filter candidates found: %d", filterCandidateCount))
		lines = append(lines, fmt.Sprintf("Filters with no target candidate found: %d", filterNoCandidateCount))
		if filterAmbiguousCount > 0 {
			lines = append(lines, fmt.Sprintf("Filters with multiple target candidates: %d", filterAmbiguousCount))
		}
	}

	if len(lines) == 0 {
		return []string{
			"No post-migrate rewrites are prepared yet.",
		}
	}
	return lines
}

func appendPreviewCount(lines []string, label string, count int) []string {
	if count <= 0 {
		return lines
	}
	return append(lines, fmt.Sprintf("%s: %d", label, count))
}

func metadataCollectionCount(value any) int {
	switch rows := value.(type) {
	case []any:
		return len(rows)
	case []ProgramMapping:
		return len(rows)
	case []PlanMapping:
		return len(rows)
	case []TeamMapping:
		return len(rows)
	case []ResourcePlan:
		return len(rows)
	case []IssueTeamRow:
		return len(rows)
	case []ParentLinkRow:
		return len(rows)
	case []FilterTeamClauseRow:
		return len(rows)
	default:
		return 0
	}
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

func parentLinkPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []ParentLinkRow:
		return limitLines(formatParentLinkRows(rows))
	case []any:
		return limitLines(formatParentLinkRowMaps(rows))
	default:
		return nil
	}
}

func filterPreviewRows(value any) []string {
	switch rows := value.(type) {
	case []FilterTeamClauseRow:
		return limitLines(formatFilterTeamClauseRows(rows))
	case []any:
		return limitLines(formatFilterTeamClauseMaps(rows))
	default:
		return nil
	}
}

func formatParentLinkRows(rows []ParentLinkRow) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s -> %s", row.IssueKey, nonEmptyString(row.SourceParentIssueKey, row.SourceParentIssueID)))
	}
	return lines
}

func formatParentLinkRowMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s -> %s",
			asString(row["issueKey"]),
			nonEmptyString(asString(row["sourceParentIssueKey"]), asString(row["sourceParentIssueId"])),
		))
	}
	return lines
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
		line := fmt.Sprintf("- %s -> %s (%s)", row.SourceTitle, labelWithID(row.TargetTitle, row.TargetTeamID), row.Decision)
		if row.Reason != "" {
			line = fmt.Sprintf("%s: %s", line, row.Reason)
		}
		lines = append(lines, line)
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
		line := fmt.Sprintf("- %s -> %s (%s)",
			asString(row["sourceTitle"]),
			labelWithID(asString(row["targetTitle"]), asString(row["targetTeamId"])),
			asString(row["decision"]),
		)
		if reason := asString(row["reason"]); reason != "" {
			line = fmt.Sprintf("%s: %s", line, reason)
		}
		lines = append(lines, line)
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
		line := fmt.Sprintf("- %s / %s -> %s / %s (%s)", row.SourceTeamName, person, row.TargetTeamName, row.TargetUserID, row.Status)
		if row.WeeklyHours != nil {
			line = fmt.Sprintf("%s weeklyHours=%s", line, formatWeeklyHours(row.WeeklyHours))
		}
		lines = append(lines, line)
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
		line := fmt.Sprintf("- %s / %s -> %s / %s (%s)",
			asString(row["sourceTeamName"]),
			person,
			asString(row["targetTeamName"]),
			asString(row["targetUserId"]),
			asString(row["status"]),
		)
		if weeklyHours := asString(row["weeklyHours"]); weeklyHours != "" {
			line = fmt.Sprintf("%s weeklyHours=%s", line, weeklyHours)
		}
		lines = append(lines, line)
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

func formatFilterTeamClauseRows(rows []FilterTeamClauseRow) []string {
	lines := []string{}
	for _, row := range rows {
		lines = append(lines, fmt.Sprintf("- %s [%s]: %s -> %s", row.FilterName, row.FilterID, row.Clause, labelWithID(row.SourceTeamName, row.SourceTeamID)))
	}
	return lines
}

func formatFilterTeamClauseMaps(rows []any) []string {
	lines := []string{}
	for _, item := range rows {
		row, ok := item.(map[string]any)
		if !ok {
			continue
		}
		lines = append(lines, fmt.Sprintf("- %s [%s]: %s -> %s",
			asString(row["filterName"]),
			asString(row["filterId"]),
			asString(row["clause"]),
			labelWithID(asString(row["sourceTeamName"]), asString(row["sourceTeamId"])),
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

func appendPreviewExamples(lines []string, label string, rows []string, maxItems int) []string {
	if len(rows) == 0 || maxItems <= 0 {
		return lines
	}
	used := 0
	for _, row := range rows {
		if strings.Contains(row, "... and ") {
			continue
		}
		lines = append(lines, fmt.Sprintf("%s: %s", label, strings.TrimPrefix(row, "- ")))
		used++
		if used == maxItems {
			break
		}
	}
	return lines
}

func appendTeamMappingPreview(lines []string, label string, mappings []TeamMapping, include func(TeamMapping) bool, maxItems int) []string {
	if maxItems <= 0 {
		return lines
	}
	used := 0
	for _, mapping := range mappings {
		if !include(mapping) {
			continue
		}
		line := fmt.Sprintf("%s: %s -> %s (%s)", label, mapping.SourceTitle, labelWithID(mapping.TargetTitle, mapping.TargetTeamID), mapping.Decision)
		if reason := firstNonEmpty(mapping.Reason, mapping.ConflictReason); reason != "" {
			line = fmt.Sprintf("%s: %s", line, reason)
		}
		lines = append(lines, line)
		used++
		if used == maxItems {
			break
		}
	}
	return lines
}

func teamMappingsFromValue(value any) []TeamMapping {
	switch rows := value.(type) {
	case []TeamMapping:
		return rows
	case []any:
		out := make([]TeamMapping, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, TeamMapping{
				SourceTeamID:    asInt64(row["sourceTeamId"]),
				SourceTitle:     asString(row["sourceTitle"]),
				SourceShareable: asBool(row["sourceShareable"]),
				TargetTeamID:    asString(row["targetTeamId"]),
				TargetTitle:     asString(row["targetTitle"]),
				Decision:        asString(row["decision"]),
				Reason:          asString(row["reason"]),
				ConflictReason:  asString(row["conflictReason"]),
			})
		}
		return out
	default:
		return nil
	}
}

func issueTeamRowsFromValue(value any) []IssueTeamRow {
	switch rows := value.(type) {
	case []IssueTeamRow:
		return rows
	case []any:
		out := make([]IssueTeamRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, IssueTeamRow{
				IssueKey:        asString(row["issueKey"]),
				ProjectKey:      asString(row["projectKey"]),
				ProjectName:     asString(row["projectName"]),
				ProjectType:     asString(row["projectType"]),
				Summary:         asString(row["summary"]),
				TeamsFieldID:    asString(row["teamsFieldId"]),
				SourceTeamIDs:   asString(row["sourceTeamIds"]),
				SourceTeamNames: asString(row["sourceTeamNames"]),
			})
		}
		return out
	default:
		return nil
	}
}

func parentLinkRowsFromValue(value any) []ParentLinkRow {
	switch rows := value.(type) {
	case []ParentLinkRow:
		return rows
	case []any:
		out := make([]ParentLinkRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, ParentLinkRow{
				IssueKey:               asString(row["issueKey"]),
				IssueID:                asString(row["issueId"]),
				ProjectKey:             asString(row["projectKey"]),
				ProjectName:            asString(row["projectName"]),
				ProjectType:            asString(row["projectType"]),
				Summary:                asString(row["summary"]),
				ParentLinkFieldID:      asString(row["parentLinkFieldId"]),
				SourceParentIssueID:    asString(row["sourceParentIssueId"]),
				SourceParentIssueKey:   asString(row["sourceParentIssueKey"]),
				SourceParentSummary:    asString(row["sourceParentSummary"]),
				SourceParentProjectKey: asString(row["sourceParentProjectKey"]),
			})
		}
		return out
	default:
		return nil
	}
}

func parentLinkComparisonRowsFromValue(value any) []PostMigrationParentLinkComparisonRow {
	switch rows := value.(type) {
	case []PostMigrationParentLinkComparisonRow:
		return rows
	case []any:
		out := make([]PostMigrationParentLinkComparisonRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, PostMigrationParentLinkComparisonRow{
				IssueKey:                asString(row["issueKey"]),
				IssueID:                 asString(row["issueId"]),
				ProjectKey:              asString(row["projectKey"]),
				ProjectName:             asString(row["projectName"]),
				ProjectType:             asString(row["projectType"]),
				Summary:                 asString(row["summary"]),
				SourceParentLinkFieldID: asString(row["sourceParentLinkFieldId"]),
				TargetParentLinkFieldID: asString(row["targetParentLinkFieldId"]),
				SourceParentIssueID:     asString(row["sourceParentIssueId"]),
				SourceParentIssueKey:    asString(row["sourceParentIssueKey"]),
				TargetParentIssueID:     asString(row["targetParentIssueId"]),
				TargetParentIssueKey:    asString(row["targetParentIssueKey"]),
				CurrentParentIssueID:    asString(row["currentParentIssueId"]),
				CurrentParentIssueKey:   asString(row["currentParentIssueKey"]),
				Status:                  asString(row["status"]),
				Reason:                  asString(row["reason"]),
			})
		}
		return out
	default:
		return nil
	}
}

func filterComparisonRowsFromValue(value any) []PostMigrationFilterComparisonRow {
	switch rows := value.(type) {
	case []PostMigrationFilterComparisonRow:
		return rows
	case []any:
		out := make([]PostMigrationFilterComparisonRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, PostMigrationFilterComparisonRow{
				SourceFilterID:     asString(row["sourceFilterId"]),
				SourceFilterName:   asString(row["sourceFilterName"]),
				SourceOwner:        asString(row["sourceOwner"]),
				SourceJQL:          asString(row["sourceJql"]),
				SourceClause:       asString(row["sourceClause"]),
				SourceTeamID:       asString(row["sourceTeamId"]),
				TargetFilterID:     asString(row["targetFilterId"]),
				TargetFilterName:   asString(row["targetFilterName"]),
				TargetOwner:        asString(row["targetOwner"]),
				TargetTeamID:       asString(row["targetTeamId"]),
				CurrentTargetJQL:   asString(row["currentTargetJql"]),
				RewrittenTargetJQL: asString(row["rewrittenTargetJql"]),
				Status:             asString(row["status"]),
				Reason:             asString(row["reason"]),
			})
		}
		return out
	default:
		return nil
	}
}

func filterMatchRowsFromValue(value any) []PostMigrationFilterMatchRow {
	switch rows := value.(type) {
	case []PostMigrationFilterMatchRow:
		return rows
	case []any:
		out := make([]PostMigrationFilterMatchRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, PostMigrationFilterMatchRow{
				SourceFilterID:   asString(row["sourceFilterId"]),
				SourceFilterName: asString(row["sourceFilterName"]),
				SourceOwner:      asString(row["sourceOwner"]),
				TargetFilterID:   asString(row["targetFilterId"]),
				TargetFilterName: asString(row["targetFilterName"]),
				TargetOwner:      asString(row["targetOwner"]),
				MatchMethod:      asString(row["matchMethod"]),
				Status:           asString(row["status"]),
				Reason:           asString(row["reason"]),
			})
		}
		return out
	default:
		return nil
	}
}

func filterTeamClauseRowsFromValue(value any) []FilterTeamClauseRow {
	switch rows := value.(type) {
	case []FilterTeamClauseRow:
		return rows
	case []any:
		out := make([]FilterTeamClauseRow, 0, len(rows))
		for _, item := range rows {
			row, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, FilterTeamClauseRow{
				FilterID:       asString(row["filterId"]),
				FilterName:     asString(row["filterName"]),
				Owner:          asString(row["owner"]),
				MatchType:      asString(row["matchType"]),
				ClauseValue:    asString(row["clauseValue"]),
				SourceTeamID:   asString(row["sourceTeamId"]),
				SourceTeamName: asString(row["sourceTeamName"]),
				Clause:         asString(row["clause"]),
				JQL:            asString(row["jql"]),
			})
		}
		return out
	default:
		return nil
	}
}

func targetTeamIDsBySource(mappings []TeamMapping) map[string]string {
	bySource := map[string]string{}
	for _, mapping := range mappings {
		sourceID := strconv.FormatInt(mapping.SourceTeamID, 10)
		targetID := strings.TrimSpace(mapping.TargetTeamID)
		if sourceID == "" || targetID == "" {
			continue
		}
		bySource[sourceID] = targetID
	}
	return bySource
}

func mappedTargetTeamIDs(raw string, targetTeamIDs map[string]string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		sourceID := strings.TrimSpace(part)
		targetID := strings.TrimSpace(targetTeamIDs[sourceID])
		if targetID == "" {
			continue
		}
		if _, ok := seen[targetID]; ok {
			continue
		}
		seen[targetID] = struct{}{}
		out = append(out, targetID)
	}
	return out
}

func mappedTargetTeamRewritePairs(raw string, targetTeamIDs map[string]string) []string {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		sourceID := strings.TrimSpace(part)
		targetID := strings.TrimSpace(targetTeamIDs[sourceID])
		if sourceID == "" || targetID == "" {
			continue
		}
		pair := fmt.Sprintf("Team = %s -> Team = %s", sourceID, targetID)
		if _, ok := seen[pair]; ok {
			continue
		}
		seen[pair] = struct{}{}
		out = append(out, pair)
	}
	return out
}

func countMappedIssueUpdates(rows []IssueTeamRow, targetTeamIDs map[string]string) int {
	count := 0
	for _, row := range rows {
		if len(mappedTargetTeamIDs(row.SourceTeamIDs, targetTeamIDs)) > 0 {
			count++
		}
	}
	return count
}

func countMappedFilterUpdates(rows []FilterTeamClauseRow, targetTeamIDs map[string]string) int {
	count := 0
	for _, row := range rows {
		if strings.TrimSpace(targetTeamIDs[row.SourceTeamID]) != "" {
			count++
		}
	}
	return count
}

func countReadyFilterUpdates(rows []PostMigrationFilterComparisonRow) int {
	count := 0
	for _, row := range rows {
		if row.Status == "ready" {
			count++
		}
	}
	return count
}

func countReadyParentLinkUpdates(rows []PostMigrationParentLinkComparisonRow) int {
	count := 0
	for _, row := range rows {
		if row.Status == "ready" {
			count++
		}
	}
	return count
}

func countLabel(count int, singular, plural string) string {
	if count <= 0 {
		return ""
	}
	if count == 1 {
		return fmt.Sprintf("1 %s", singular)
	}
	return fmt.Sprintf("%d %s", count, plural)
}

func nonEmptyDetails(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		out = append(out, value)
	}
	return out
}

func formatPhaseDetails(details []string) string {
	if len(details) == 0 {
		return ""
	}
	return fmt.Sprintf(" (%s)", strings.Join(details, ", "))
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

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case string:
		id, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err == nil {
			return id
		}
	}
	return 0
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(v))
		return err == nil && parsed
	default:
		return false
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
	fmt.Fprintln(w, "Usage: teams-migrator <init|migrate|report|version|self-update|uninstall> [flags]")
	fmt.Fprintln(w, "       teams-migrator config <show> [flags]")
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
	fmt.Fprintln(w, "  --filter-source-csv     CSV with source filters that contain team IDs for pre-migrate filter resolution")
	fmt.Fprintln(w, "  --output-dir            Directory for generated reports")
	fmt.Fprintln(w, "  --team-scope            Team migration scope: all, shared-only, or non-shared-only")
	fmt.Fprintln(w, "  --issue-project-scope   Issue correction scope: all or a comma-separated list of Jira project keys")
	fmt.Fprintln(w, "  --post-migrate-issue-workers")
	fmt.Fprintln(w, "                          Initial workers for adaptive post-migrate issue Team-field rewrites")
	fmt.Fprintln(w, "  --post-migrate-issue-fallback-workers")
	fmt.Fprintln(w, "                          Minimum retry workers for issue rewrites after Jira 429 responses")
	fmt.Fprintln(w, "  --config                Path to config.yaml profile store")
	fmt.Fprintln(w, "  --profile               Saved profile name")
	fmt.Fprintln(w, "  --phase                 Migration phase for migrate: pre-migrate, migrate, or post-migrate")
	fmt.Fprintln(w, "  --strict                Exit non-zero on warnings or errors")
	fmt.Fprintln(w, "  --apply                 Execute writes for migrate")
	fmt.Fprintln(w, "  --skip-post-migrate-drift-checks")
	fmt.Fprintln(w, "                          Trust prepared post-migration comparisons and skip per-row rechecks")
	fmt.Fprintln(w, "  --no-input              Disable interactive prompts")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Report command flags:")
	fmt.Fprintln(w, "  --input                 Input report JSON for the report command")
	fmt.Fprintln(w, "  --format                Report output: json or csv")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Environment:")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_BASE_URL")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_SOURCE_USERNAME")
	fmt.Fprintf(w, "  %s\n", credentialEnvName("SOURCE"))
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_BASE_URL")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TARGET_USERNAME")
	fmt.Fprintf(w, "  %s\n", credentialEnvName("TARGET"))
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_IDENTITY_MAPPING_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TEAMS_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_PERSONS_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_RESOURCES_FILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_ISSUES_CSV")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_FILTER_SOURCE_CSV")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_OUTPUT_DIR")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_TEAM_SCOPE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_ISSUE_PROJECT_SCOPE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_POST_MIGRATE_ISSUE_WORKERS")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_POST_MIGRATE_ISSUE_FALLBACK_WORKERS")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_STRICT")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_DRY_RUN")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_REPORT_INPUT")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_CONFIG_PATH")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_PROFILE")
	fmt.Fprintln(w, "  TEAMS_MIGRATOR_PHASE")
}
