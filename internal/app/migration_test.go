package app

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func float64ptr(v float64) *float64 {
	return &v
}

func TestBuildTeamMappingsSkipsNonSharedTeamWithoutTargetMatch(t *testing.T) {
	cfg := Config{TeamScope: "all"}
	sourceTeams := []TeamDTO{{ID: 10, Title: "Private Alpha", Shareable: false}}
	targetTeams := []TeamDTO{}
	sourcePlans := []PlanDTO{{ID: 100, Title: "Plan A", PlanTeams: []int64{10}}}
	planMappings := []PlanMapping{{
		SourcePlanID:      100,
		SourcePlanTeamIDs: "10",
		Decision:          "merge",
		TargetPlanID:      "200",
		TargetTitle:       "Plan A",
	}}

	mappings, findings := buildTeamMappings(cfg, sourceTeams, targetTeams, sourcePlans, planMappings)
	if len(mappings) != 1 {
		t.Fatalf("expected 1 team mapping, got %d", len(mappings))
	}
	if got := mappings[0].Decision; got != "skipped" {
		t.Fatalf("expected non-shared team to be skipped, got %q", got)
	}
	if mappings[0].Reason == "" {
		t.Fatal("expected skip reason to be populated")
	}
	if len(findings) != 1 || findings[0].Code != "non_shared_team_manual_prerequisite" {
		t.Fatalf("expected manual prerequisite warning, got %#v", findings)
	}
}

func TestBuildTeamMappingsSharedOnlySkipsNonSharedTeams(t *testing.T) {
	cfg := Config{TeamScope: "shared-only"}
	sourceTeams := []TeamDTO{
		{ID: 10, Title: "Shared Alpha", Shareable: true},
		{ID: 11, Title: "Private Beta", Shareable: false},
	}
	targetTeams := []TeamDTO{{ID: 201, Title: "Shared Alpha", Shareable: true}}

	mappings, findings := buildTeamMappings(cfg, sourceTeams, targetTeams, nil, nil)
	if len(mappings) != 2 {
		t.Fatalf("expected 2 team mappings, got %d", len(mappings))
	}
	if got := mappings[0].Decision; got != "merge" {
		t.Fatalf("expected shared team to merge, got %q", got)
	}
	if got := mappings[1].Decision; got != "skipped" {
		t.Fatalf("expected non-shared team to be skipped by scope, got %q", got)
	}
	if mappings[1].Reason == "" {
		t.Fatal("expected scope skip reason for non-shared team")
	}
	if len(findings) != 1 || findings[0].Code != "team_scope_skipped_non_shared" {
		t.Fatalf("expected scope skip finding, got %#v", findings)
	}
}

func TestParseConfigAcceptsTeamScope(t *testing.T) {
	cfg, err := parseConfig([]string{"validate", "--no-input", "--team-scope", "non-shared-only"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.TeamScope != "non-shared-only" {
		t.Fatalf("expected team scope non-shared-only, got %q", cfg.TeamScope)
	}
}

func TestParseConfigAcceptsScanFiltersCommand(t *testing.T) {
	cfg, err := parseConfig([]string{"scan-filters", "--no-input", "--source-base-url", "https://source.example.com/jira"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.Command != "scan-filters" {
		t.Fatalf("expected command scan-filters, got %q", cfg.Command)
	}
}

func TestParseConfigAcceptsScanFiltersFlag(t *testing.T) {
	cfg, err := parseConfig([]string{"plan", "--no-input", "--source-base-url", "https://source.example.com/jira", "--scan-filters"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if !cfg.ScanFilters {
		t.Fatal("expected scan-filters to be enabled")
	}
	if !cfg.ScanFiltersExplicit {
		t.Fatal("expected scan-filters to be marked explicit")
	}
}

func TestRequireCoreInputsErrorsWhenScanFiltersEnabledWithoutSourceURL(t *testing.T) {
	findings := (Config{ScanFilters: true}).requireCoreInputs()
	for _, finding := range findings {
		if finding.Code == "missing_source_base_url_for_filter_scan" && finding.Severity == SeverityError {
			return
		}
	}
	t.Fatalf("expected missing_source_base_url_for_filter_scan error, got %#v", findings)
}

func TestBuildTeamMappingsWarnsOnSameIDDifferentTitle(t *testing.T) {
	cfg := Config{TeamScope: "all"}
	sourceTeams := []TeamDTO{{ID: 10, Title: "Platform Team", Shareable: true}}
	targetTeams := []TeamDTO{{ID: 10, Title: "Operations Team", Shareable: true}}

	mappings, findings := buildTeamMappings(cfg, sourceTeams, targetTeams, nil, nil)
	if len(mappings) != 1 {
		t.Fatalf("expected 1 team mapping, got %d", len(mappings))
	}
	if got := mappings[0].Decision; got != "add" {
		t.Fatalf("expected mapping to remain name-based and result in add, got %q", got)
	}
	if len(findings) != 1 || findings[0].Code != "team_id_title_mismatch" {
		t.Fatalf("expected same-ID different-title warning, got %#v", findings)
	}
}

func TestBuildResourcePlansSkipsExistingDestinationMembership(t *testing.T) {
	state := migrationState{
		IdentityMappings: IdentityMapping{
			"alice@example.com": "alice@example.com",
		},
		SourceTeams: []TeamDTO{
			{ID: 10, Title: "Source Team", Shareable: true},
		},
		SourcePersons: []PersonDTO{
			{ID: 100, JiraUser: &JiraUserDTO{Email: "alice@example.com"}},
		},
		SourceResources: []ResourceDTO{
			{ID: 500, TeamID: 10, WeeklyHours: float64ptr(40), Person: &PersonDTO{ID: 100}},
		},
		TargetPersons: []PersonDTO{
			{ID: 200, JiraUser: &JiraUserDTO{Email: "alice@example.com", JiraUserID: "user-1"}},
		},
		TargetResources: []ResourceDTO{
			{ID: 900, TeamID: 20, WeeklyHours: float64ptr(40), Person: &PersonDTO{ID: 200}},
		},
		TeamMappings: []TeamMapping{
			{SourceTeamID: 10, SourceTitle: "Source Team", TargetTeamID: "20", TargetTitle: "Target Team", Decision: "merge"},
		},
	}

	plans, findings := buildResourcePlans(state)
	if len(plans) != 1 {
		t.Fatalf("expected 1 resource plan, got %d", len(plans))
	}
	if plans[0].Status != "skipped" {
		t.Fatalf("expected existing destination membership to be skipped, got %q", plans[0].Status)
	}
	if plans[0].Reason != "destination membership already exists" {
		t.Fatalf("unexpected skip reason: %q", plans[0].Reason)
	}
	if len(findings) != 1 || findings[0].Code != "destination_membership_exists" {
		t.Fatalf("expected destination membership info finding, got %#v", findings)
	}
}

func TestBuildResourcePlansPreservesUnsetWeeklyHours(t *testing.T) {
	state := migrationState{
		IdentityMappings: IdentityMapping{
			"alice@example.com": "alice@example.com",
		},
		SourceTeams: []TeamDTO{
			{ID: 10, Title: "Source Team", Shareable: true},
		},
		SourcePersons: []PersonDTO{
			{ID: 100, JiraUser: &JiraUserDTO{Email: "alice@example.com"}},
		},
		SourceResources: []ResourceDTO{
			{ID: 500, TeamID: 10, Person: &PersonDTO{ID: 100}},
		},
		TargetPersons: []PersonDTO{
			{ID: 200, JiraUser: &JiraUserDTO{Email: "alice@example.com", JiraUserID: "user-1"}},
		},
		TeamMappings: []TeamMapping{
			{SourceTeamID: 10, SourceTitle: "Source Team", TargetTeamID: "20", TargetTitle: "Target Team", Decision: "merge"},
		},
	}

	plans, findings := buildResourcePlans(state)
	if len(findings) != 0 {
		t.Fatalf("expected no findings, got %#v", findings)
	}
	if len(plans) != 1 {
		t.Fatalf("expected 1 resource plan, got %d", len(plans))
	}
	if plans[0].WeeklyHours != nil {
		t.Fatalf("expected weekly hours to remain unset, got %#v", plans[0].WeeklyHours)
	}
}

func TestCreateResourcePayloadOmitsWeeklyHoursWhenUnset(t *testing.T) {
	payload := createResourcePayload(20, "user-1", nil, false)
	if _, ok := payload["weeklyHours"]; ok {
		t.Fatalf("expected weeklyHours to be omitted, got %#v", payload["weeklyHours"])
	}
}

func TestCreateResourcePayloadIncludesWeeklyHoursWhenSet(t *testing.T) {
	payload := createResourcePayload(20, "user-1", float64ptr(32), false)
	if got, ok := payload["weeklyHours"].(float64); !ok || got != 32 {
		t.Fatalf("expected weeklyHours=32, got %#v", payload["weeklyHours"])
	}
}

func TestBuildIssueTeamRowsIncludesProjectMetadata(t *testing.T) {
	field := JiraField{ID: "customfield_10010", Name: "Team"}
	issues := []JiraIssue{
		{
			Key: "ABC-123",
			Fields: map[string]any{
				"summary": "Migrate team mapping",
				"project": map[string]any{
					"key":            "ABC",
					"name":           "Alpha Beta",
					"projectTypeKey": "software",
				},
				field.ID: []any{
					map[string]any{"id": "42"},
					map[string]any{"teamId": float64(7)},
				},
			},
		},
	}
	sourceTeams := []TeamDTO{
		{ID: 7, Title: "Blue Team"},
		{ID: 42, Title: "Red Team"},
	}

	rows := buildIssueTeamRows(issues, field, sourceTeams)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	want := IssueTeamRow{
		IssueKey:        "ABC-123",
		ProjectKey:      "ABC",
		ProjectName:     "Alpha Beta",
		ProjectType:     "software",
		Summary:         "Migrate team mapping",
		TeamsFieldID:    "customfield_10010",
		SourceTeamIDs:   "42,7",
		SourceTeamNames: "Red Team,Blue Team",
	}
	if !reflect.DeepEqual(rows[0], want) {
		t.Fatalf("unexpected issue row:\nwant: %#v\ngot:  %#v", want, rows[0])
	}
}

func TestWriteIssueTeamExportsWritesDetailedAndImportCSVs(t *testing.T) {
	cfg := Config{OutputDir: t.TempDir(), OutputTimestamp: "20260417-194500"}
	rows := []IssueTeamRow{{
		IssueKey:        "ABC-123",
		ProjectKey:      "ABC",
		ProjectName:     "Alpha Beta",
		ProjectType:     "software",
		Summary:         "Migrate team mapping",
		TeamsFieldID:    "customfield_10010",
		SourceTeamIDs:   "42",
		SourceTeamNames: "Red Team",
	}}

	detailedPath, importPath, err := writeIssueTeamExports(cfg, rows)
	if err != nil {
		t.Fatalf("writeIssueTeamExports returned error: %v", err)
	}

	if filepath.Base(detailedPath) != "issues-with-teams.pre-migration.20260417-194500.csv" {
		t.Fatalf("unexpected detailed path %q", detailedPath)
	}
	if filepath.Base(importPath) != "issues-with-teams.import-ready.20260417-194500.csv" {
		t.Fatalf("unexpected import path %q", importPath)
	}

	detailedRecords := readCSVRecords(t, detailedPath)
	importRecords := readCSVRecords(t, importPath)

	wantDetailed := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Team ID", "Team Name", "Teams Field ID"},
		{"ABC-123", "ABC", "Alpha Beta", "software", "Migrate team mapping", "42", "Red Team", "customfield_10010"},
	}
	if !reflect.DeepEqual(detailedRecords, wantDetailed) {
		t.Fatalf("unexpected detailed CSV:\nwant: %#v\ngot:  %#v", wantDetailed, detailedRecords)
	}

	wantImport := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Team ID"},
		{"ABC-123", "ABC", "Alpha Beta", "software", "Migrate team mapping", "42"},
	}
	if !reflect.DeepEqual(importRecords, wantImport) {
		t.Fatalf("unexpected import CSV:\nwant: %#v\ngot:  %#v", wantImport, importRecords)
	}
}

func TestBuildFilterTeamClauseRowsMatchesByIDAndName(t *testing.T) {
	teams := []TeamDTO{
		{ID: 42, Title: "Red Team"},
		{ID: 7, Title: "Blue Team"},
	}
	filters := []JiraFilter{
		{
			ID:    "10000",
			Name:  "Numeric Team Filter",
			JQL:   "project = ABC AND Team = 42",
			Owner: &JiraFilterUser{DisplayName: "Jane Doe"},
		},
		{
			ID:    "10001",
			Name:  "Named Team Filter",
			JQL:   `project = ABC AND "Team" = "Blue Team"`,
			Owner: &JiraFilterUser{DisplayName: "Jane Doe"},
		},
		{
			ID:   "10002",
			Name: "Unrelated Filter",
			JQL:  `project = ABC AND Team = "Green Team"`,
		},
	}

	rows := buildFilterTeamClauseRows(filters, teams)
	if len(rows) != 2 {
		t.Fatalf("expected 2 matches, got %d", len(rows))
	}

	wantFirst := FilterTeamClauseRow{
		FilterID:       "10000",
		FilterName:     "Numeric Team Filter",
		Owner:          "Jane Doe",
		MatchType:      "team_id",
		ClauseValue:    "42",
		SourceTeamID:   "42",
		SourceTeamName: "Red Team",
		Clause:         "Team = 42",
		JQL:            "project = ABC AND Team = 42",
	}
	if !reflect.DeepEqual(rows[0], wantFirst) {
		t.Fatalf("unexpected first row:\nwant: %#v\ngot:  %#v", wantFirst, rows[0])
	}

	wantSecond := FilterTeamClauseRow{
		FilterID:       "10001",
		FilterName:     "Named Team Filter",
		Owner:          "Jane Doe",
		MatchType:      "team_name",
		ClauseValue:    "Blue Team",
		SourceTeamID:   "7",
		SourceTeamName: "Blue Team",
		Clause:         `"Team" = "Blue Team"`,
		JQL:            `project = ABC AND "Team" = "Blue Team"`,
	}
	if !reflect.DeepEqual(rows[1], wantSecond) {
		t.Fatalf("unexpected second row:\nwant: %#v\ngot:  %#v", wantSecond, rows[1])
	}
}

func TestWriteFilterTeamClauseExportWritesCSV(t *testing.T) {
	cfg := Config{OutputDir: t.TempDir(), OutputTimestamp: "20260417-194500"}
	rows := []FilterTeamClauseRow{{
		FilterID:       "10000",
		FilterName:     "Named Team Filter",
		Owner:          "Jane Doe",
		MatchType:      "team_name",
		ClauseValue:    "Blue Team",
		SourceTeamID:   "7",
		SourceTeamName: "Blue Team",
		Clause:         `"Team" = "Blue Team"`,
		JQL:            `project = ABC AND "Team" = "Blue Team"`,
	}}

	path, err := writeFilterTeamClauseExport(cfg, rows)
	if err != nil {
		t.Fatalf("writeFilterTeamClauseExport returned error: %v", err)
	}

	if filepath.Base(path) != "filters-with-team-clauses.pre-migration.20260417-194500.csv" {
		t.Fatalf("unexpected path %q", path)
	}

	records := readCSVRecords(t, path)
	want := [][]string{
		{"Filter ID", "Filter Name", "Owner", "Match Type", "Clause Value", "Source Team ID", "Source Team Name", "Matched Clause", "JQL"},
		{"10000", "Named Team Filter", "Jane Doe", "team_name", "Blue Team", "7", "Blue Team", `"Team" = "Blue Team"`, `project = ABC AND "Team" = "Blue Team"`},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("unexpected filter scan CSV:\nwant: %#v\ngot:  %#v", want, records)
	}
}

func readCSVRecords(t *testing.T, path string) [][]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatalf("read csv %s: %v", path, err)
	}
	return records
}
