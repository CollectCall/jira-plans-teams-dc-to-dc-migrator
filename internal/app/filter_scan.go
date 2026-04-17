package app

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const filterAPIMaxPageSize = 100

var teamEqualsClausePattern = regexp.MustCompile(`(?i)(?:"?team"?|\bteam\b)\s*=\s*(?:"([^"]+)"|'([^']+)'|([A-Za-z0-9_.:-]+))`)

func runScanFilters(cfg Config) Report {
	report := newReport(cfg)
	progress := newProgressTracker(2)
	defer progress.Finish()
	state, findings, actions := executeFilterScanWithProgress(cfg, progress)
	return populateExecutionReport(report, state, findings, actions, "filter_scan_generated", "Filter Team-clause scan generated from source Jira data")
}

func executeFilterScan(cfg Config) (migrationState, []Finding, []Action) {
	return executeFilterScanWithProgress(cfg, nil)
}

func executeFilterScanWithProgress(cfg Config, progress *progressTracker) (migrationState, []Finding, []Action) {
	state := migrationState{}
	findings := cfg.requireFilterScanInputs()
	if hasErrors(findings) {
		return state, findings, nil
	}

	sourceClient, err := newJiraClient(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_client", err.Error()))
		return state, findings, nil
	}

	if progress != nil {
		progress.StartCount("Loading source teams")
	}
	sourceTeams, err := loadTeams(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.TeamsFile, countProgressReporter(progress))
	if progress != nil {
		progress.End()
	}
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_teams_load", err.Error()))
		return state, findings, nil
	}
	state.SourceTeams = sourceTeams

	if progress != nil {
		progress.StartCount("Scanning Jira filters for Team clauses")
		defer progress.End()
	}

	filters, err := loadAllFilters(sourceClient, countProgressReporter(progress))
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_filters_load", err.Error()))
		return state, findings, nil
	}

	rows := buildFilterTeamClauseRows(filters, sourceTeams)
	exportPath, artifact, scanFindings, scanActions, err := finalizeFilterScan(cfg, filters, rows)
	state.FilterTeamClauseRows = rows
	findings = append(findings, scanFindings...)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "filter_scan_export_failed", err.Error()))
		return state, findings, nil
	}
	if artifact != nil {
		state.FilterScanExportPath = exportPath
		state.Artifacts = append(state.Artifacts, *artifact)
	}
	return state, findings, scanActions
}

func scanFiltersWithClient(cfg Config, client *jiraClient, teams []TeamDTO, progress func(current, total int)) ([]FilterTeamClauseRow, string, *Artifact, []Finding, []Action, error) {
	filters, err := loadAllFilters(client, progress)
	if err != nil {
		return nil, "", nil, nil, nil, err
	}
	rows := buildFilterTeamClauseRows(filters, teams)
	path, artifact, findings, actions, err := finalizeFilterScan(cfg, filters, rows)
	return rows, path, artifact, findings, actions, err
}

func loadAllFilters(client *jiraClient, progress func(current, total int)) ([]JiraFilter, error) {
	var all []JiraFilter
	for startAt := 0; ; startAt += filterAPIMaxPageSize {
		page, err := client.SearchFilters(startAt, filterAPIMaxPageSize)
		if err != nil {
			return nil, err
		}
		if len(page.Values) == 0 {
			break
		}
		all = append(all, page.Values...)
		if progress != nil {
			total := page.Total
			if total < len(all) {
				total = len(all)
			}
			progress(len(all), total)
		}
		if len(page.Values) < filterAPIMaxPageSize || startAt+len(page.Values) >= page.Total {
			break
		}
	}
	return all, nil
}

func buildFilterTeamClauseRows(filters []JiraFilter, teams []TeamDTO) []FilterTeamClauseRow {
	teamNameByNormalized := map[string]TeamDTO{}
	teamByID := map[string]TeamDTO{}
	for _, team := range teams {
		teamNameByNormalized[normalizeTeamClauseValue(team.Title)] = team
		teamByID[strconv.FormatInt(team.ID, 10)] = team
	}

	rows := []FilterTeamClauseRow{}
	for _, filter := range filters {
		if strings.TrimSpace(filter.JQL) == "" {
			continue
		}
		matches := teamEqualsClausePattern.FindAllStringSubmatch(filter.JQL, -1)
		for _, match := range matches {
			clause := strings.TrimSpace(match[0])
			value := firstNonEmptyFilterValue(match[1], match[2], match[3])
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}

			row := FilterTeamClauseRow{
				FilterID:    filter.ID,
				FilterName:  filter.Name,
				Owner:       filterOwnerLabel(filter.Owner),
				ClauseValue: value,
				Clause:      clause,
				JQL:         filter.JQL,
			}

			if team, ok := teamByID[value]; ok {
				row.MatchType = "team_id"
				row.SourceTeamID = strconv.FormatInt(team.ID, 10)
				row.SourceTeamName = team.Title
				rows = append(rows, row)
				continue
			}

			if team, ok := teamNameByNormalized[normalizeTeamClauseValue(value)]; ok {
				row.MatchType = "team_name"
				row.SourceTeamID = strconv.FormatInt(team.ID, 10)
				row.SourceTeamName = team.Title
				rows = append(rows, row)
			}
		}
	}

	return rows
}

func writeFilterTeamClauseExport(cfg Config, rows []FilterTeamClauseRow) (string, error) {
	if err := ensureOutputDir(cfg.OutputDir); err != nil {
		return "", err
	}

	const name = "filters-with-team-clauses.pre-migration.csv"
	path := outputPathForName(cfg, name)

	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	records := [][]string{{
		"Filter ID",
		"Filter Name",
		"Owner",
		"Match Type",
		"Clause Value",
		"Source Team ID",
		"Source Team Name",
		"Matched Clause",
		"JQL",
	}}
	for _, row := range rows {
		records = append(records, []string{
			row.FilterID,
			row.FilterName,
			row.Owner,
			row.MatchType,
			row.ClauseValue,
			row.SourceTeamID,
			row.SourceTeamName,
			row.Clause,
			row.JQL,
		})
	}

	if err := writer.WriteAll(records); err != nil {
		return "", err
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	if err := pruneOutputFamily(cfg.OutputDir, name, outputRetentionLimit); err != nil {
		return "", err
	}
	return path, nil
}

func normalizeTeamClauseValue(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func firstNonEmptyFilterValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func filterOwnerLabel(owner *JiraFilterUser) string {
	if owner == nil {
		return ""
	}
	for _, candidate := range []string{owner.DisplayName, owner.Name, owner.Key} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func countDistinctFilterIDs(rows []FilterTeamClauseRow) int {
	seen := map[string]struct{}{}
	for _, row := range rows {
		if row.FilterID == "" {
			continue
		}
		seen[row.FilterID] = struct{}{}
	}
	return len(seen)
}

func finalizeFilterScan(cfg Config, filters []JiraFilter, rows []FilterTeamClauseRow) (string, *Artifact, []Finding, []Action, error) {
	findings := []Finding{
		newFinding(SeverityInfo, "source_filters_scanned", fmt.Sprintf("Scanned %d filters visible to the authenticated user", len(filters))),
		newFinding(SeverityInfo, "team_clause_matches_found", fmt.Sprintf("Found %d Team = {id|name} clause matches across %d filters", len(rows), countDistinctFilterIDs(rows))),
	}
	if len(rows) == 0 {
		findings = append(findings, newFinding(SeverityWarning, "no_team_filter_matches", "No Team = {id|name} clauses matching known source teams were found in visible filters"))
		return "", nil, findings, nil, nil
	}

	exportPath, err := writeFilterTeamClauseExport(cfg, rows)
	if err != nil {
		return "", nil, findings, nil, err
	}

	artifact := &Artifact{
		Key:   "source_filters_with_team_clauses",
		Label: "Filters with Team clauses",
		Path:  exportPath,
		Count: len(rows),
	}
	actions := []Action{{Kind: "scan_filters", Status: "generated", Details: exportPath}}
	return exportPath, artifact, findings, actions, nil
}

func countProgressReporter(progress *progressTracker) func(current, total int) {
	if progress == nil {
		return nil
	}
	return func(current, total int) {
		progress.UpdateCount(current, total)
	}
}
