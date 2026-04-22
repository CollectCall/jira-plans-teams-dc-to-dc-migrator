package app

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const teamFilterScriptRunnerPageSize = 500

type teamFilterScriptRunnerResponse struct {
	Meta struct {
		LastID          int64  `json:"lastId"`
		NextLastID      int64  `json:"nextLastId"`
		Scanned         int    `json:"scanned"`
		Matched         int    `json:"matched"`
		ParseErrorCount int    `json:"parseErrorCount"`
		Limit           int    `json:"limit"`
		DBMode          string `json:"dbMode"`
	} `json:"meta"`
	Results []struct {
		ID    int64  `json:"id"`
		Name  string `json:"name"`
		Owner string `json:"owner"`
		JQL   string `json:"jql"`
	} `json:"results"`
	ParseErrors []teamFilterParseError `json:"parseErrors"`
}

type teamFilterParseError struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Error string `json:"error"`
}

var (
	teamEqualsClausePattern = regexp.MustCompile(`(?i)(?:"?team"?|\bteam\b)\s*=\s*(?:"([^"]+)"|'([^']+)'|([A-Za-z0-9_.:-]+))`)
	teamInClausePattern     = regexp.MustCompile(`(?i)(?:"?team"?|\bteam\b)\s+in\s*\(([^)]*)\)`)
)

type teamClauseMatch struct {
	clause string
	value  string
}

func filterInventoryCSVExampleQuery() string {
	return `SELECT id AS "Filter ID", filtername AS "Filter Name", authorname AS "Owner", reqcontent AS "JQL" FROM searchrequest WHERE reqcontent IS NOT NULL AND (LOWER(reqcontent) LIKE '%team%' OR LOWER(reqcontent) LIKE '%cf[%') ORDER BY id;`
}

func scanFiltersWithConfiguredSource(cfg Config, client *jiraClient, teams []TeamDTO, progress func(current, total int)) ([]FilterTeamClauseRow, string, *Artifact, []Finding, error) {
	switch normalizeFilterDataSource(cfg.FilterDataSource) {
	case filterDataSourceScriptRunner:
		return scanFiltersWithScriptRunner(cfg, client, teams, progress)
	case filterDataSourceDatabaseCSV:
		return scanFiltersWithSourceCSV(cfg, teams)
	default:
		return nil, "", nil, nil, fmt.Errorf("unsupported filter data source %q", cfg.FilterDataSource)
	}
}

func scanFiltersWithScriptRunner(cfg Config, client *jiraClient, teams []TeamDTO, progress func(current, total int)) ([]FilterTeamClauseRow, string, *Artifact, []Finding, error) {
	if client == nil {
		return nil, "", nil, nil, fmt.Errorf("source Jira access is required for the ScriptRunner source filter path")
	}

	filters, scanSummary, findings, err := loadFiltersViaTeamFilterScriptRunner(client, progress)
	if err != nil {
		return nil, "", nil, findings, err
	}

	rows := buildFilterTeamClauseRows(filters, teams)
	findings = append(findings,
		newFinding(SeverityInfo, "source_filters_loaded_scriptrunner", scanSummary),
		newFinding(SeverityInfo, "team_clause_matches_found", fmt.Sprintf("Found %d Team = {id|name} clause matches across %d filters", len(rows), countDistinctFilterIDs(rows))),
	)

	exportPath, artifact, exportFindings, err := finalizeResolvedFilterInventory(cfg, rows, "No Team = {id|name} clauses matching known source teams were found in the ScriptRunner source filter export")
	findings = append(findings, exportFindings...)
	return rows, exportPath, artifact, findings, err
}

func scanFiltersWithSourceCSV(cfg Config, teams []TeamDTO) ([]FilterTeamClauseRow, string, *Artifact, []Finding, error) {
	if strings.TrimSpace(cfg.FilterSourceCSV) == "" {
		return nil, "", nil, nil, fmt.Errorf("CSV with source filters that contain team IDs is required; provide --filter-source-csv or save it in the profile")
	}

	filters, err := loadFiltersFromSourceCSV(cfg.FilterSourceCSV)
	if err != nil {
		return nil, "", nil, nil, fmt.Errorf("loading source filter CSV %s: %w", cfg.FilterSourceCSV, err)
	}

	rows := buildFilterTeamClauseRows(filters, teams)
	findings := []Finding{
		newFinding(SeverityInfo, "source_filters_loaded_csv", fmt.Sprintf("Loaded %d filters from source filter CSV %s", len(filters), cfg.FilterSourceCSV)),
		newFinding(SeverityInfo, "team_clause_matches_found", fmt.Sprintf("Found %d Team = {id|name} clause matches across %d filters", len(rows), countDistinctFilterIDs(rows))),
	}

	exportPath, artifact, exportFindings, err := finalizeResolvedFilterInventory(cfg, rows, "No Team = {id|name} clauses matching known source teams were found in the source filter CSV")
	findings = append(findings, exportFindings...)
	return rows, exportPath, artifact, findings, err
}

func loadFiltersFromSourceCSV(path string) ([]JiraFilter, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("CSV is empty; expected headers like Filter ID, Filter Name, Owner, JQL")
	}

	header := mapCSVHeaderIndexes(records[0])
	jqlIndex := findCSVHeaderIndex(header, "jql", "query", "reqcontent")
	if jqlIndex == -1 {
		return nil, fmt.Errorf("CSV must include a JQL column; example query: %s", filterInventoryCSVExampleQuery())
	}
	idIndex := findCSVHeaderIndex(header, "filter id", "id")
	nameIndex := findCSVHeaderIndex(header, "filter name", "name", "filtername")
	ownerIndex := findCSVHeaderIndex(header, "owner", "author", "authorname")

	filters := make([]JiraFilter, 0, len(records)-1)
	for _, record := range records[1:] {
		jql := csvRecordValue(record, jqlIndex)
		if strings.TrimSpace(jql) == "" {
			continue
		}
		filter := JiraFilter{
			ID:   csvRecordValue(record, idIndex),
			Name: csvRecordValue(record, nameIndex),
			JQL:  jql,
		}
		if owner := csvRecordValue(record, ownerIndex); owner != "" {
			filter.Owner = &JiraFilterUser{DisplayName: owner, Name: owner, Key: owner}
		}
		filters = append(filters, filter)
	}

	return filters, nil
}

func loadFiltersViaTeamFilterScriptRunner(client *jiraClient, progress func(current, total int)) ([]JiraFilter, string, []Finding, error) {
	teamFieldID, fieldLabel, err := resolveTeamsCustomFieldNumericID(client)
	if err != nil {
		return nil, "", nil, err
	}

	var (
		lastID           int64
		totalScanned     int
		totalParseErrors int
		filters          []JiraFilter
	)

	for {
		query := make(url.Values)
		query.Set("enabled", "true")
		query.Set("lastId", strconv.FormatInt(lastID, 10))
		query.Set("limit", strconv.Itoa(teamFilterScriptRunnerPageSize))
		query.Set("teamFieldId", teamFieldID)

		body, err := client.doCoreJSON(http.MethodGet, teamFilterScriptRunnerEndpointPath, query, nil)
		if err != nil {
			endpointURL, buildErr := buildURL(client.instanceBaseURL, teamFilterScriptRunnerEndpointPath, query)
			if buildErr != nil {
				return nil, "", nil, err
			}
			return nil, "", nil, fmt.Errorf("calling ScriptRunner endpoint %s: %w", endpointURL, err)
		}

		var response teamFilterScriptRunnerResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, "", nil, fmt.Errorf("parsing ScriptRunner endpoint response: %w", err)
		}

		for _, result := range response.Results {
			owner := strings.TrimSpace(result.Owner)
			filter := JiraFilter{
				ID:   strconv.FormatInt(result.ID, 10),
				Name: result.Name,
				JQL:  result.JQL,
			}
			if owner != "" {
				filter.Owner = &JiraFilterUser{DisplayName: owner, Name: owner, Key: owner}
			}
			filters = append(filters, filter)
		}

		totalScanned += response.Meta.Scanned
		totalParseErrors += response.Meta.ParseErrorCount
		if progress != nil {
			total := totalScanned
			if response.Meta.Scanned >= teamFilterScriptRunnerPageSize {
				total = totalScanned + 1
			}
			progress(totalScanned, total)
		}

		if response.Meta.Scanned == 0 || response.Meta.NextLastID <= lastID || response.Meta.Scanned < teamFilterScriptRunnerPageSize {
			break
		}
		lastID = response.Meta.NextLastID
	}

	findings := []Finding{}
	if totalParseErrors > 0 {
		findings = append(findings, newFinding(SeverityWarning, "scriptrunner_filter_parse_errors", fmt.Sprintf("ScriptRunner source filter export skipped %d filters because their JQL could not be parsed", totalParseErrors)))
	}

	summary := fmt.Sprintf("ScriptRunner endpoint scanned %d filters and returned %d candidate filters using Teams field %s (%s)", totalScanned, len(filters), fieldLabel, teamFieldID)
	return filters, summary, findings, nil
}

func finalizeResolvedFilterInventory(cfg Config, rows []FilterTeamClauseRow, emptyMessage string) (string, *Artifact, []Finding, error) {
	if len(rows) == 0 {
		return "", nil, []Finding{newFinding(SeverityWarning, "no_team_filter_matches", emptyMessage)}, nil
	}

	exportPath, err := writeFilterTeamClauseExport(cfg, rows)
	if err != nil {
		return "", nil, nil, err
	}

	return exportPath, &Artifact{
		Key:   "source_filters_with_team_clauses",
		Label: "Filters with Team clauses",
		Path:  exportPath,
		Count: len(rows),
	}, nil, nil
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
		matches := extractTeamClauseMatches(filter.JQL)
		for _, match := range matches {
			clause := strings.TrimSpace(match.clause)
			value := strings.TrimSpace(match.value)
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

func extractTeamClauseMatches(jql string) []teamClauseMatch {
	matches := make([]teamClauseMatch, 0)
	for _, match := range teamEqualsClausePattern.FindAllStringSubmatch(jql, -1) {
		value := strings.TrimSpace(firstNonEmptyFilterValue(match[1], match[2], match[3]))
		if value == "" {
			continue
		}
		matches = append(matches, teamClauseMatch{
			clause: strings.TrimSpace(match[0]),
			value:  value,
		})
	}

	for _, match := range teamInClausePattern.FindAllStringSubmatch(jql, -1) {
		clause := strings.TrimSpace(match[0])
		for _, value := range splitJQLListValues(match[1]) {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			matches = append(matches, teamClauseMatch{
				clause: clause,
				value:  value,
			})
		}
	}

	return matches
}

func splitJQLListValues(raw string) []string {
	values := make([]string, 0)
	var current strings.Builder
	var quote rune
	escaped := false

	flush := func() {
		value := strings.TrimSpace(current.String())
		current.Reset()
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		value = strings.TrimSpace(value)
		if value != "" {
			values = append(values, value)
		}
	}

	for _, r := range raw {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			current.WriteRune(r)
			escaped = true
		case quote != 0:
			current.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '"' || r == '\'':
			current.WriteRune(r)
			quote = r
		case r == ',':
			flush()
		default:
			current.WriteRune(r)
		}
	}
	flush()

	return values
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

func mapCSVHeaderIndexes(header []string) map[string]int {
	indexes := make(map[string]int, len(header))
	for i, value := range header {
		indexes[strings.ToLower(strings.TrimSpace(value))] = i
	}
	return indexes
}

func findCSVHeaderIndex(header map[string]int, candidates ...string) int {
	for _, candidate := range candidates {
		if index, ok := header[strings.ToLower(strings.TrimSpace(candidate))]; ok {
			return index
		}
	}
	return -1
}

func csvRecordValue(record []string, index int) string {
	if index < 0 || index >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[index])
}

func countProgressReporter(progress *progressTracker) func(current, total int) {
	if progress == nil {
		return nil
	}
	return func(current, total int) {
		progress.UpdateCount(current, total)
	}
}
