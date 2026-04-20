package app

import (
	"encoding/csv"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	cfg, err := parseConfig([]string{"plan", "--no-input", "--team-scope", "non-shared-only"})
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

func TestParseConfigDefaultsMigratePhase(t *testing.T) {
	cfg, err := parseConfig([]string{"migrate", "--no-input", "--source-base-url", "https://source.example.com/jira", "--target-base-url", "https://target.example.com/jira"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.Phase != phaseMigrate {
		t.Fatalf("expected default migrate phase %q, got %q", phaseMigrate, cfg.Phase)
	}
}

func TestParseConfigAcceptsExplicitMigrationPhase(t *testing.T) {
	cfg, err := parseConfig([]string{"migrate", "--no-input", "--phase", "post-migrate", "--source-base-url", "https://source.example.com/jira", "--target-base-url", "https://target.example.com/jira"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.Phase != phasePostMigrate {
		t.Fatalf("expected explicit phase %q, got %q", phasePostMigrate, cfg.Phase)
	}
}

func TestSourceNeedsAuthWhenPasswordMissing(t *testing.T) {
	if !sourceNeedsAuth(Config{
		SourceBaseURL:  "https://source.example.com/jira",
		SourceUsername: "alice",
	}) {
		t.Fatal("expected source auth prompt when password is missing")
	}
}

func TestTargetNeedsAuthWhenPasswordMissing(t *testing.T) {
	if !targetNeedsAuth(Config{
		TargetBaseURL:  "https://target.example.com/jira",
		TargetUsername: "bob",
	}) {
		t.Fatal("expected target auth prompt when password is missing")
	}
}

func TestInteractivePhaseOutputTimestampIncludesPhaseSlug(t *testing.T) {
	stamp := interactivePhaseOutputTimestamp(phasePostMigrate)
	if !strings.Contains(stamp, "post_migrate") {
		t.Fatalf("expected phase slug in interactive timestamp, got %q", stamp)
	}
}

func TestParseConfigAcceptsIssueProjectScope(t *testing.T) {
	cfg, err := parseConfig([]string{"plan", "--no-input", "--issue-project-scope", "abc,DEF", "--source-base-url", "https://source.example.com/jira", "--target-base-url", "https://target.example.com/jira"})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if cfg.IssueProjectScope != "abc,DEF" {
		t.Fatalf("expected issue project scope to be preserved from flags, got %q", cfg.IssueProjectScope)
	}
}

func TestApplySavedProfileKeepsSkippedIdentityMappingAnswered(t *testing.T) {
	cfg := Config{}
	applySavedProfile(&cfg, SavedProfile{
		IdentityMappingFile: "",
		IdentityMappingSet:  true,
	})
	if !cfg.IdentityMappingSet {
		t.Fatal("expected identity mapping preference to be marked configured")
	}
	if cfg.IdentityMappingFile != "" {
		t.Fatalf("expected empty identity mapping file, got %q", cfg.IdentityMappingFile)
	}
}

func TestSavedProfileFromConfigPersistsSkippedIdentityMappingAnswered(t *testing.T) {
	profile := savedProfileFromConfig(Config{IdentityMappingSet: true}, false)
	if !profile.IdentityMappingSet {
		t.Fatal("expected saved profile to persist identity mapping configured state")
	}
	if profile.IdentityMappingFile != "" {
		t.Fatalf("expected empty identity mapping file, got %q", profile.IdentityMappingFile)
	}
}

func TestSavedProfileFromConfigPersistsParentLinkAndIssueScopeSettings(t *testing.T) {
	profile := savedProfileFromConfig(Config{
		IssueProjectScope:    "ABC,DEF",
		ParentLinkInScope:    true,
		ParentLinkInScopeSet: true,
	}, false)
	if profile.IssueProjectScope != "ABC,DEF" {
		t.Fatalf("expected issue project scope to persist, got %q", profile.IssueProjectScope)
	}
	if !profile.ParentLinkInScope || !profile.ParentLinkInScopeSet {
		t.Fatalf("expected parent link scope settings to persist, got %#v", profile)
	}
}

func TestParseConfigLoadsSkippedIdentityMappingDecisionFromProfileStore(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := strings.Join([]string{
		`current_profile: "default"`,
		`profiles:`,
		`  default:`,
		`    source_base_url: "https://source.example.com/jira"`,
		`    target_base_url: "https://target.example.com/jira"`,
		`    identity_mapping_file: ""`,
		`    identity_mapping_set: "true"`,
		`    team_scope: "all"`,
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config store: %v", err)
	}

	cfg, err := parseConfig([]string{"migrate", "--no-input", "--config", configPath})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if !cfg.IdentityMappingSet {
		t.Fatal("expected identity mapping decision to load from profile store")
	}
	if cfg.IdentityMappingFile != "" {
		t.Fatalf("expected identity mapping file to remain empty, got %q", cfg.IdentityMappingFile)
	}
	if cfg.Profile != "default" {
		t.Fatalf("expected current profile default, got %q", cfg.Profile)
	}
}

func TestParseConfigLoadsSkippedIdentityMappingDecisionFromLegacyProfileStore(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	content := strings.Join([]string{
		`current_profile: "default"`,
		`profiles:`,
		`  default:`,
		`    source_base_url: "https://source.example.com/jira"`,
		`    target_base_url: "https://target.example.com/jira"`,
		`    identity_mapping_file: ""`,
		`    team_scope: "all"`,
	}, "\n") + "\n"
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config store: %v", err)
	}

	cfg, err := parseConfig([]string{"migrate", "--no-input", "--config", configPath})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}
	if !cfg.IdentityMappingSet {
		t.Fatal("expected legacy profile store to treat empty identity mapping as an answered skip")
	}
	if cfg.IdentityMappingFile != "" {
		t.Fatalf("expected identity mapping file to remain empty, got %q", cfg.IdentityMappingFile)
	}
}

func TestApplySavedProfileKeepsSkippedFilterScopeAnswered(t *testing.T) {
	cfg := Config{}
	applySavedProfile(&cfg, SavedProfile{
		FilterTeamIDsInScope:    false,
		FilterTeamIDsInScopeSet: true,
		FilterDataSource:        filterDataSourceDatabaseCSV,
		FilterSourceCSV:         "/tmp/source-filters.csv",
	})
	if !cfg.FilterTeamIDsInScopeSet {
		t.Fatal("expected filter scope preference to be marked configured")
	}
	if cfg.FilterTeamIDsInScope {
		t.Fatal("expected filter scope to remain false")
	}
	if cfg.FilterDataSource != filterDataSourceDatabaseCSV {
		t.Fatalf("expected filter data source %q, got %q", filterDataSourceDatabaseCSV, cfg.FilterDataSource)
	}
	if cfg.FilterSourceCSV != "/tmp/source-filters.csv" {
		t.Fatalf("expected filter source CSV to be applied, got %q", cfg.FilterSourceCSV)
	}
}

func TestSavedProfileFromConfigPersistsFilterScopeSettings(t *testing.T) {
	profile := savedProfileFromConfig(Config{
		FilterTeamIDsInScope:        true,
		FilterTeamIDsInScopeSet:     true,
		FilterDataSource:            filterDataSourceScriptRunner,
		FilterSourceCSV:             "/tmp/source-filters.csv",
		FilterScriptRunnerInstalled: true,
		FilterScriptRunnerEndpoint:  "https://source.example.com/jira/rest/scriptrunner/latest/custom/findTeamFiltersDB?enabled=true&lastId=0&limit=500&teamFieldId=16604",
	}, false)
	if !profile.FilterTeamIDsInScope || !profile.FilterTeamIDsInScopeSet {
		t.Fatalf("expected filter scope settings to persist, got %#v", profile)
	}
	if profile.FilterDataSource != filterDataSourceScriptRunner {
		t.Fatalf("expected filter data source %q, got %q", filterDataSourceScriptRunner, profile.FilterDataSource)
	}
	if !profile.FilterScriptRunnerInstalled {
		t.Fatal("expected ScriptRunner installed flag to persist")
	}
	if profile.FilterScriptRunnerEndpoint == "" {
		t.Fatal("expected ScriptRunner endpoint to persist")
	}
	if profile.FilterSourceCSV != "/tmp/source-filters.csv" {
		t.Fatalf("expected filter source CSV to persist, got %q", profile.FilterSourceCSV)
	}
}

func TestAssignProfileFieldParsesIdentityMappingSet(t *testing.T) {
	profile := SavedProfile{}
	assignProfileField(&profile, "identity_mapping_set", "true")
	if !profile.IdentityMappingSet {
		t.Fatal("expected identity_mapping_set to parse as true")
	}
}

func TestAssignProfileFieldParsesFilterScopeSettings(t *testing.T) {
	profile := SavedProfile{}
	assignProfileField(&profile, "filter_team_ids_in_scope", "true")
	assignProfileField(&profile, "filter_team_ids_in_scope_set", "true")
	assignProfileField(&profile, "filter_data_source", "database")
	assignProfileField(&profile, "filter_source_csv", "/tmp/source-filters.csv")
	assignProfileField(&profile, "filter_scriptrunner_installed", "false")
	if !profile.FilterTeamIDsInScope || !profile.FilterTeamIDsInScopeSet {
		t.Fatalf("expected filter scope flags to parse, got %#v", profile)
	}
	if profile.FilterDataSource != filterDataSourceDatabaseCSV {
		t.Fatalf("expected filter data source %q, got %q", filterDataSourceDatabaseCSV, profile.FilterDataSource)
	}
	if profile.FilterScriptRunnerInstalled {
		t.Fatal("expected ScriptRunner installed flag to parse as false")
	}
	if profile.FilterSourceCSV != "/tmp/source-filters.csv" {
		t.Fatalf("expected filter source CSV to parse, got %q", profile.FilterSourceCSV)
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

func TestRequireCoreInputsErrorsWhenPreMigrateApplyRequested(t *testing.T) {
	findings := (Config{Command: "migrate", Phase: phasePreMigrate, DryRun: false}).requireCoreInputs()
	for _, finding := range findings {
		if finding.Code == "pre_migrate_apply_unsupported" && finding.Severity == SeverityError {
			return
		}
	}
	t.Fatalf("expected pre_migrate_apply_unsupported error, got %#v", findings)
}

func TestRequireCoreInputsErrorsWhenPreMigrateFilterCSVMissing(t *testing.T) {
	findings := (Config{
		Command:              "migrate",
		Phase:                phasePreMigrate,
		DryRun:               true,
		FilterTeamIDsInScope: true,
		FilterDataSource:     filterDataSourceDatabaseCSV,
	}).requireCoreInputs()
	for _, finding := range findings {
		if finding.Code == "pre_migrate_filter_csv_missing" && finding.Severity == SeverityError {
			return
		}
	}
	t.Fatalf("expected pre_migrate_filter_csv_missing error, got %#v", findings)
}

func TestRequireCoreInputsErrorsWhenPreMigrateScriptRunnerNotInstalled(t *testing.T) {
	findings := (Config{
		Command:                     "migrate",
		Phase:                       phasePreMigrate,
		DryRun:                      true,
		FilterTeamIDsInScope:        true,
		FilterDataSource:            filterDataSourceScriptRunner,
		FilterScriptRunnerInstalled: false,
		SourceBaseURL:               "https://source.example.com/jira",
	}).requireCoreInputs()
	for _, finding := range findings {
		if finding.Code == "pre_migrate_filter_endpoint_not_installed" && finding.Severity == SeverityError {
			return
		}
	}
	t.Fatalf("expected pre_migrate_filter_endpoint_not_installed error, got %#v", findings)
}

func TestValidatePostMigratePhaseStateErrorsWhenTeamsStillNeedCreation(t *testing.T) {
	findings := validatePostMigratePhaseState(migrationState{
		TeamMappings: []TeamMapping{
			{SourceTeamID: 10, SourceTitle: "Red Team", Decision: "add"},
		},
	})
	if len(findings) != 1 || findings[0].Code != "post_migrate_phase_blocked" {
		t.Fatalf("expected post_migrate_phase_blocked error, got %#v", findings)
	}
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
	if !strings.Contains(findings[0].Message, `Mitigation: the tool will add "Platform Team" as a separate destination team with a new ID`) {
		t.Fatalf("expected mitigation in warning, got %q", findings[0].Message)
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

func TestWriteTeamIDMappingExportWritesCSV(t *testing.T) {
	cfg := Config{OutputDir: t.TempDir(), OutputTimestamp: "20260420-103000"}
	mappings := []TeamMapping{
		{
			SourceTeamID:    42,
			SourceTitle:     "Red Team",
			SourceShareable: true,
			TargetTeamID:    "1042",
			Decision:        "created",
		},
		{
			SourceTeamID:    7,
			SourceTitle:     "Blue Team",
			SourceShareable: false,
			TargetTeamID:    "7",
			TargetTitle:     "Blue Team",
			Decision:        "merge",
			Reason:          "already exists in destination",
		},
		{
			SourceTeamID:    99,
			SourceTitle:     "Green Team",
			SourceShareable: true,
			Decision:        "skipped",
			Reason:          "manual prerequisite",
		},
	}

	path, err := writeTeamIDMappingExport(cfg, mappings)
	if err != nil {
		t.Fatalf("writeTeamIDMappingExport returned error: %v", err)
	}

	if filepath.Base(path) != "team-id-mapping.migration.20260420-103000.csv" {
		t.Fatalf("unexpected path %q", path)
	}

	records := readCSVRecords(t, path)
	want := [][]string{
		{"Source Team ID", "Source Team Name", "Source Shareable", "Target Team ID", "Target Team Name", "Migration Status", "Reason", "Conflict Reason"},
		{"42", "Red Team", "true", "1042", "Red Team", "created", "", ""},
		{"7", "Blue Team", "false", "7", "Blue Team", "merge", "already exists in destination", ""},
		{"99", "Green Team", "true", "", "", "skipped", "manual prerequisite", ""},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("unexpected team ID mapping CSV:\nwant: %#v\ngot:  %#v", want, records)
	}
}

func TestWritePostMigrationIssueTeamExportWritesCSV(t *testing.T) {
	cfg := Config{OutputDir: t.TempDir(), OutputTimestamp: "20260420-113000"}
	rows := []IssueTeamRow{
		{
			IssueKey:        "ABC-123",
			ProjectKey:      "ABC",
			ProjectName:     "Alpha Beta",
			ProjectType:     "software",
			Summary:         "Migrate team mapping",
			SourceTeamIDs:   "42,7",
			SourceTeamNames: "Red Team,Blue Team",
			TeamsFieldID:    "customfield_10010",
		},
		{
			IssueKey:        "ABC-124",
			ProjectKey:      "ABC",
			ProjectName:     "Alpha Beta",
			ProjectType:     "software",
			Summary:         "Skipped team mapping",
			SourceTeamIDs:   "99",
			SourceTeamNames: "Green Team",
			TeamsFieldID:    "customfield_10010",
		},
	}
	mappings := []TeamMapping{
		{SourceTeamID: 42, TargetTeamID: "1042", Decision: "created"},
		{SourceTeamID: 7, TargetTeamID: "7", Decision: "merge"},
	}

	path, err := writePostMigrationIssueTeamExport(cfg, rows, mappings)
	if err != nil {
		t.Fatalf("writePostMigrationIssueTeamExport returned error: %v", err)
	}

	if filepath.Base(path) != "issues-with-teams.post-migration.20260420-113000.csv" {
		t.Fatalf("unexpected path %q", path)
	}

	records := readCSVRecords(t, path)
	want := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Source Team IDs", "Source Team Names", "Teams Field ID", "Target Team IDs"},
		{"ABC-123", "ABC", "Alpha Beta", "software", "Migrate team mapping", "42,7", "Red Team,Blue Team", "customfield_10010", "1042,7"},
		{"ABC-124", "ABC", "Alpha Beta", "software", "Skipped team mapping", "99", "Green Team", "customfield_10010", ""},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("unexpected post-migration issue mapping CSV:\nwant: %#v\ngot:  %#v", want, records)
	}
}

func TestWritePostMigrationFilterTeamExportWritesCSV(t *testing.T) {
	cfg := Config{OutputDir: t.TempDir(), OutputTimestamp: "20260420-114500"}
	rows := []FilterTeamClauseRow{
		{
			FilterID:       "10000",
			FilterName:     "Numeric Team Filter",
			Owner:          "Jane Doe",
			MatchType:      "team_id",
			ClauseValue:    "42",
			SourceTeamID:   "42",
			SourceTeamName: "Red Team",
			Clause:         "Team = 42",
			JQL:            "project = ABC AND Team = 42",
		},
		{
			FilterID:       "10001",
			FilterName:     "Named Team Filter",
			Owner:          "Jane Doe",
			MatchType:      "team_name",
			ClauseValue:    "Blue Team",
			SourceTeamID:   "7",
			SourceTeamName: "Blue Team",
			Clause:         `Team = "Blue Team"`,
			JQL:            `project = ABC AND Team = "Blue Team"`,
		},
	}
	mappings := []TeamMapping{
		{SourceTeamID: 42, TargetTeamID: "1042", Decision: "created"},
		{SourceTeamID: 7, TargetTeamID: "7", Decision: "merge"},
	}

	path, count, err := writePostMigrationFilterTeamExport(cfg, rows, mappings)
	if err != nil {
		t.Fatalf("writePostMigrationFilterTeamExport returned error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 filter mapping row, got %d", count)
	}
	if filepath.Base(path) != "filters-with-team-clauses.post-migration.20260420-114500.csv" {
		t.Fatalf("unexpected path %q", path)
	}

	records := readCSVRecords(t, path)
	want := [][]string{
		{"Filter ID", "Filter Name", "Owner", "Match Type", "Clause Value", "Source Team ID", "Source Team Name", "Matched Clause", "JQL", "Target Team ID"},
		{"10000", "Numeric Team Filter", "Jane Doe", "team_id", "42", "42", "Red Team", "Team = 42", "project = ABC AND Team = 42", "1042"},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("unexpected post-migration filter mapping CSV:\nwant: %#v\ngot:  %#v", want, records)
	}
}

func TestExecuteMigrationWithStateWritesTeamIDMappingArtifactAfterApply(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/rest/teams-api/1.0/team" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":1234}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	cfg := Config{
		Command:         "migrate",
		Phase:           phaseMigrate,
		TargetBaseURL:   server.URL,
		TargetUsername:  "user",
		TargetPassword:  "pass",
		OutputDir:       t.TempDir(),
		OutputTimestamp: "20260420-111500",
	}
	state := migrationState{
		TeamMappings: []TeamMapping{
			{
				SourceTeamID:    42,
				SourceTitle:     "Red Team",
				SourceShareable: true,
				Decision:        "add",
			},
		},
	}

	state, findings, actions := executeMigrationWithState(cfg, true, state, nil)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	if got := state.TeamMappings[0].TargetTeamID; got != "1234" {
		t.Fatalf("expected created target team ID 1234, got %q", got)
	}
	if got := state.TeamMappings[0].TargetTitle; got != "Red Team" {
		t.Fatalf("expected created target team title Red Team, got %q", got)
	}
	if got := state.TeamMappings[0].Decision; got != "created" {
		t.Fatalf("expected created decision, got %q", got)
	}

	path := artifactPathByKey(state.Artifacts, "migration_team_id_mapping")
	if filepath.Base(path) != "team-id-mapping.migration.20260420-111500.csv" {
		t.Fatalf("unexpected team ID mapping artifact path %q", path)
	}

	records := readCSVRecords(t, path)
	want := [][]string{
		{"Source Team ID", "Source Team Name", "Source Shareable", "Target Team ID", "Target Team Name", "Migration Status", "Reason", "Conflict Reason"},
		{"42", "Red Team", "true", "1234", "Red Team", "created", "", ""},
	}
	if !reflect.DeepEqual(records, want) {
		t.Fatalf("unexpected team ID mapping artifact:\nwant: %#v\ngot:  %#v", want, records)
	}

	foundArtifactAction := false
	for _, action := range actions {
		if action.Kind == "migration_team_id_mapping" && action.Status == "generated" {
			foundArtifactAction = true
			break
		}
	}
	if !foundArtifactAction {
		t.Fatalf("expected migration team ID mapping generated action, got %#v", actions)
	}
}

func TestExecuteMigrationWithStateWritesPostMigrationPreparationArtifactsAfterApply(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method %s", r.Method)
		}
		if r.URL.Path != "/rest/teams-api/1.0/team" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":1234}`))
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	outputDir := t.TempDir()
	preCfg := Config{OutputDir: outputDir, OutputTimestamp: "20260420-120000"}
	_, _, err := writeIssueTeamExports(preCfg, []IssueTeamRow{{
		IssueKey:        "ABC-123",
		ProjectKey:      "ABC",
		ProjectName:     "Alpha Beta",
		ProjectType:     "software",
		Summary:         "Migrate team mapping",
		SourceTeamIDs:   "42",
		SourceTeamNames: "Red Team",
		TeamsFieldID:    "customfield_10010",
	}})
	if err != nil {
		t.Fatalf("writeIssueTeamExports returned error: %v", err)
	}
	_, err = writeFilterTeamClauseExport(preCfg, []FilterTeamClauseRow{{
		FilterID:       "10000",
		FilterName:     "Numeric Team Filter",
		Owner:          "Jane Doe",
		MatchType:      "team_id",
		ClauseValue:    "42",
		SourceTeamID:   "42",
		SourceTeamName: "Red Team",
		Clause:         "Team = 42",
		JQL:            "project = ABC AND Team = 42",
	}})
	if err != nil {
		t.Fatalf("writeFilterTeamClauseExport returned error: %v", err)
	}

	cfg := Config{
		Command:                 "migrate",
		Phase:                   phaseMigrate,
		TargetBaseURL:           server.URL,
		TargetUsername:          "user",
		TargetPassword:          "pass",
		OutputDir:               outputDir,
		OutputTimestamp:         "20260420-121500",
		FilterTeamIDsInScope:    true,
		FilterTeamIDsInScopeSet: true,
	}
	state := migrationState{
		TeamMappings: []TeamMapping{
			{
				SourceTeamID:    42,
				SourceTitle:     "Red Team",
				SourceShareable: true,
				Decision:        "add",
			},
		},
	}

	state, findings, actions := executeMigrationWithState(cfg, true, state, nil)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	issuePath := artifactPathByKey(state.Artifacts, "post_migrate_issue_team_mapping")
	if filepath.Base(issuePath) != "issues-with-teams.post-migration.20260420-121500.csv" {
		t.Fatalf("unexpected post-migration issue mapping path %q", issuePath)
	}
	issueRecords := readCSVRecords(t, issuePath)
	wantIssue := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Source Team IDs", "Source Team Names", "Teams Field ID", "Target Team IDs"},
		{"ABC-123", "ABC", "Alpha Beta", "software", "Migrate team mapping", "42", "Red Team", "customfield_10010", "1234"},
	}
	if !reflect.DeepEqual(issueRecords, wantIssue) {
		t.Fatalf("unexpected post-migration issue artifact:\nwant: %#v\ngot:  %#v", wantIssue, issueRecords)
	}

	filterPath := artifactPathByKey(state.Artifacts, "post_migrate_filter_team_mapping")
	if filepath.Base(filterPath) != "filters-with-team-clauses.post-migration.20260420-121500.csv" {
		t.Fatalf("unexpected post-migration filter mapping path %q", filterPath)
	}
	filterRecords := readCSVRecords(t, filterPath)
	wantFilter := [][]string{
		{"Filter ID", "Filter Name", "Owner", "Match Type", "Clause Value", "Source Team ID", "Source Team Name", "Matched Clause", "JQL", "Target Team ID"},
		{"10000", "Numeric Team Filter", "Jane Doe", "team_id", "42", "42", "Red Team", "Team = 42", "project = ABC AND Team = 42", "1234"},
	}
	if !reflect.DeepEqual(filterRecords, wantFilter) {
		t.Fatalf("unexpected post-migration filter artifact:\nwant: %#v\ngot:  %#v", wantFilter, filterRecords)
	}

	expectedActions := map[string]bool{
		"migration_team_id_mapping":        false,
		"post_migrate_issue_team_mapping":  false,
		"post_migrate_filter_team_mapping": false,
	}
	for _, action := range actions {
		if _, ok := expectedActions[action.Kind]; ok && action.Status == "generated" {
			expectedActions[action.Kind] = true
		}
	}
	for kind, ok := range expectedActions {
		if !ok {
			t.Fatalf("expected generated action for %s, got %#v", kind, actions)
		}
	}
}

func TestPreparePostMigrationTargetFilterArtifactsWritesSnapshotMatchAndComparisonExports(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_16604","name":"Team","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-team"}}]`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB":
			if got := r.URL.Query().Get("filterName"); got != "Numeric Team Filter" {
				t.Fatalf("unexpected filterName query %q", got)
			}
			if got := r.URL.Query().Get("owner"); got != "Jane Doe" {
				t.Fatalf("unexpected owner query %q", got)
			}
			if got := r.URL.Query().Get("teamFieldId"); got != "16604" {
				t.Fatalf("unexpected teamFieldId query %q", got)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"meta":{"lastId":0,"nextLastId":9001,"scanned":1,"matched":1,"parseErrorCount":0,"limit":500,"dbMode":"local","durationMs":1},"results":[{"id":9001,"name":"Numeric Team Filter","owner":"Jane Doe","jql":"project = ABC AND Team = 42"}],"parseErrors":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/filter/9001":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"9001","name":"Numeric Team Filter","description":"demo","jql":"project = ABC AND Team = 42","owner":{"displayName":"Jane Doe"},"viewUrl":"https://example.test/filters/9001","searchUrl":"https://example.test/issues/?filter=9001"}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{
		TargetBaseURL:           server.URL,
		TargetUsername:          "user",
		TargetPassword:          "pass",
		OutputDir:               t.TempDir(),
		OutputTimestamp:         "20260420-133000",
		FilterTeamIDsInScope:    true,
		FilterTeamIDsInScopeSet: true,
	}
	state := migrationState{
		FilterTeamClauseRows: []FilterTeamClauseRow{
			{
				FilterID:       "10000",
				FilterName:     "Numeric Team Filter",
				Owner:          "Jane Doe",
				MatchType:      "team_id",
				ClauseValue:    "42",
				SourceTeamID:   "42",
				SourceTeamName: "Red Team",
				Clause:         "Team = 42",
				JQL:            "project = ABC AND Team = 42",
			},
		},
		TeamMappings: []TeamMapping{
			{SourceTeamID: 42, TargetTeamID: "142", TargetTitle: "Red Team", Decision: "created"},
		},
	}

	findings := preparePostMigrationTargetFilterArtifacts(cfg, &state)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	snapshotPath := artifactPathByKey(state.Artifacts, "post_migrate_target_filter_snapshot")
	if filepath.Base(snapshotPath) != "target-filters.snapshot.post-migration.20260420-133000.csv" {
		t.Fatalf("unexpected target snapshot path %q", snapshotPath)
	}
	snapshotRecords := readCSVRecords(t, snapshotPath)
	wantSnapshot := [][]string{
		{"Target Filter ID", "Target Filter Name", "Target Owner", "Description", "JQL", "View URL", "Search URL"},
		{"9001", "Numeric Team Filter", "Jane Doe", "demo", "project = ABC AND Team = 42", "https://example.test/filters/9001", "https://example.test/issues/?filter=9001"},
	}
	if !reflect.DeepEqual(snapshotRecords, wantSnapshot) {
		t.Fatalf("unexpected target snapshot CSV:\nwant: %#v\ngot:  %#v", wantSnapshot, snapshotRecords)
	}

	matchPath := artifactPathByKey(state.Artifacts, "post_migrate_filter_target_match")
	if filepath.Base(matchPath) != "filter-target-match.post-migration.20260420-133000.csv" {
		t.Fatalf("unexpected filter target match path %q", matchPath)
	}
	matchRecords := readCSVRecords(t, matchPath)
	wantMatch := [][]string{
		{"Source Filter ID", "Source Filter Name", "Source Owner", "Target Filter ID", "Target Filter Name", "Target Owner", "Status", "Reason"},
		{"10000", "Numeric Team Filter", "Jane Doe", "9001", "Numeric Team Filter", "Jane Doe", "matched", ""},
	}
	if !reflect.DeepEqual(matchRecords, wantMatch) {
		t.Fatalf("unexpected filter target match CSV:\nwant: %#v\ngot:  %#v", wantMatch, matchRecords)
	}

	comparisonPath := artifactPathByKey(state.Artifacts, "post_migrate_filter_comparison")
	if filepath.Base(comparisonPath) != "filter-jql-comparison.post-migration.20260420-133000.csv" {
		t.Fatalf("unexpected filter comparison path %q", comparisonPath)
	}
	comparisonRecords := readCSVRecords(t, comparisonPath)
	wantComparison := [][]string{
		{"Source Filter ID", "Source Filter Name", "Source Owner", "Source Clause", "Source Team ID", "Target Filter ID", "Target Filter Name", "Target Owner", "Target Team ID", "Current Target JQL", "Rewritten Target JQL", "Status", "Reason"},
		{"10000", "Numeric Team Filter", "Jane Doe", "Team = 42", "42", "9001", "Numeric Team Filter", "Jane Doe", "142", "project = ABC AND Team = 42", "project = ABC AND Team = 142", "ready", ""},
	}
	if !reflect.DeepEqual(comparisonRecords, wantComparison) {
		t.Fatalf("unexpected filter comparison CSV:\nwant: %#v\ngot:  %#v", wantComparison, comparisonRecords)
	}
}

func TestPreparePostMigrationTargetIssueArtifactsWritesSnapshotAndComparisonExports(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_18888","name":"Team","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-team"}}]`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/ABC-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"10001","key":"ABC-1","fields":{"summary":"Demo issue","project":{"key":"ABC","name":"Demo Project","projectTypeKey":"software"},"customfield_18888":[{"id":42},{"id":7}]}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{
		TargetBaseURL:   server.URL,
		TargetUsername:  "user",
		TargetPassword:  "pass",
		OutputDir:       t.TempDir(),
		OutputTimestamp: "20260420-134500",
	}
	state := migrationState{
		IssueTeamRows: []IssueTeamRow{
			{
				IssueKey:        "ABC-1",
				ProjectKey:      "ABC",
				ProjectName:     "Demo Project",
				ProjectType:     "software",
				Summary:         "Demo issue",
				TeamsFieldID:    "customfield_16604",
				SourceTeamIDs:   "42,7",
				SourceTeamNames: "Red Team,Shared Team",
			},
		},
		TeamMappings: []TeamMapping{
			{SourceTeamID: 42, TargetTeamID: "142", TargetTitle: "Red Team", Decision: "created"},
			{SourceTeamID: 7, TargetTeamID: "7", TargetTitle: "Shared Team", Decision: "merge"},
		},
	}

	findings := preparePostMigrationTargetIssueArtifacts(cfg, &state)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	snapshotPath := artifactPathByKey(state.Artifacts, "post_migrate_target_issue_snapshot")
	if filepath.Base(snapshotPath) != "target-issues.snapshot.post-migration.20260420-134500.csv" {
		t.Fatalf("unexpected target issue snapshot path %q", snapshotPath)
	}
	snapshotRecords := readCSVRecords(t, snapshotPath)
	wantSnapshot := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Target Teams Field ID", "Current Target Team IDs"},
		{"ABC-1", "ABC", "Demo Project", "software", "Demo issue", "customfield_18888", "42,7"},
	}
	if !reflect.DeepEqual(snapshotRecords, wantSnapshot) {
		t.Fatalf("unexpected target issue snapshot CSV:\nwant: %#v\ngot:  %#v", wantSnapshot, snapshotRecords)
	}

	comparisonPath := artifactPathByKey(state.Artifacts, "post_migrate_issue_comparison")
	if filepath.Base(comparisonPath) != "issue-team-comparison.post-migration.20260420-134500.csv" {
		t.Fatalf("unexpected issue comparison path %q", comparisonPath)
	}
	comparisonRecords := readCSVRecords(t, comparisonPath)
	wantComparison := [][]string{
		{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Source Teams Field ID", "Target Teams Field ID", "Source Team IDs", "Source Team Names", "Target Team IDs", "Current Target Team IDs", "Status", "Reason"},
		{"ABC-1", "ABC", "Demo Project", "software", "Demo issue", "customfield_16604", "customfield_18888", "42,7", "Red Team,Shared Team", "142,7", "42,7", "ready", ""},
	}
	if !reflect.DeepEqual(comparisonRecords, wantComparison) {
		t.Fatalf("unexpected issue comparison CSV:\nwant: %#v\ngot:  %#v", wantComparison, comparisonRecords)
	}
}

func TestPreparePostMigrationTargetParentLinkArtifactsWritesSnapshotAndComparisonExports(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[
				{"id":"customfield_18888","name":"Team","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-team"}},
				{"id":"customfield_19999","name":"Parent Link","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-parent"}}
			]`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/ABC-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"10001","key":"ABC-1","fields":{"summary":"Child issue","project":{"key":"ABC","name":"Demo Project","projectTypeKey":"software"},"customfield_19999":{"id":"5001"}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/INIT-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"6001","key":"INIT-1","fields":{"summary":"Parent issue","project":{"key":"INIT","name":"Initiatives","projectTypeKey":"software"}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/5001":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"5001","key":"INIT-1","fields":{"summary":"Parent issue","project":{"key":"INIT","name":"Initiatives","projectTypeKey":"software"}}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{
		TargetBaseURL:        server.URL,
		TargetUsername:       "user",
		TargetPassword:       "pass",
		OutputDir:            t.TempDir(),
		OutputTimestamp:      "20260420-141500",
		ParentLinkInScope:    true,
		ParentLinkInScopeSet: true,
	}
	state := migrationState{
		ParentLinkRows: []ParentLinkRow{
			{
				IssueKey:             "ABC-1",
				IssueID:              "10001",
				ProjectKey:           "ABC",
				ProjectName:          "Demo Project",
				ProjectType:          "software",
				Summary:              "Child issue",
				ParentLinkFieldID:    "customfield_16605",
				SourceParentIssueID:  "5001",
				SourceParentIssueKey: "INIT-1",
				SourceParentSummary:  "Parent issue",
			},
		},
	}

	findings := preparePostMigrationTargetParentLinkArtifacts(cfg, &state)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	snapshotPath := artifactPathByKey(state.Artifacts, "post_migrate_target_parent_link_snapshot")
	if filepath.Base(snapshotPath) != "target-parent-link-issues.snapshot.post-migration.20260420-141500.csv" {
		t.Fatalf("unexpected target parent-link snapshot path %q", snapshotPath)
	}
	snapshotRecords := readCSVRecords(t, snapshotPath)
	wantSnapshot := [][]string{
		{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Target Parent Link Field ID", "Current Parent Issue ID", "Current Parent Issue Key"},
		{"ABC-1", "10001", "ABC", "Demo Project", "software", "Child issue", "customfield_19999", "5001", "INIT-1"},
	}
	if !reflect.DeepEqual(snapshotRecords, wantSnapshot) {
		t.Fatalf("unexpected target parent-link snapshot CSV:\nwant: %#v\ngot:  %#v", wantSnapshot, snapshotRecords)
	}

	comparisonPath := artifactPathByKey(state.Artifacts, "post_migrate_parent_link_comparison")
	if filepath.Base(comparisonPath) != "parent-link-comparison.post-migration.20260420-141500.csv" {
		t.Fatalf("unexpected parent-link comparison path %q", comparisonPath)
	}
	comparisonRecords := readCSVRecords(t, comparisonPath)
	wantComparison := [][]string{
		{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Source Parent Link Field ID", "Target Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Target Parent Issue ID", "Target Parent Issue Key", "Current Parent Issue ID", "Current Parent Issue Key", "Status", "Reason"},
		{"ABC-1", "10001", "ABC", "Demo Project", "software", "Child issue", "customfield_16605", "customfield_19999", "5001", "INIT-1", "6001", "INIT-1", "5001", "INIT-1", "ready", ""},
	}
	if !reflect.DeepEqual(comparisonRecords, wantComparison) {
		t.Fatalf("unexpected parent-link comparison CSV:\nwant: %#v\ngot:  %#v", wantComparison, comparisonRecords)
	}
}

func TestGetIssueAndUpdateIssueFields(t *testing.T) {
	var updateBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/ABC-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"10001","key":"ABC-1","fields":{"customfield_18888":[{"id":42}]}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/rest/api/2/issue/ABC-1":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read update body: %v", err)
			}
			updateBody = string(data)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := newJiraClient(server.URL, "user", "pass")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}

	issue, err := client.GetIssue("ABC-1", []string{"customfield_18888"})
	if err != nil {
		t.Fatalf("GetIssue returned error: %v", err)
	}
	if issue.Key != "ABC-1" {
		t.Fatalf("unexpected issue from GetIssue: %#v", issue)
	}

	err = client.UpdateIssueFields("ABC-1", map[string]any{
		"customfield_18888": []any{map[string]any{"id": float64(142)}},
	})
	if err != nil {
		t.Fatalf("UpdateIssueFields returned error: %v", err)
	}
	if !strings.Contains(updateBody, `"customfield_18888":[{"id":142}]`) {
		t.Fatalf("expected update payload to contain rewritten team field, got %s", updateBody)
	}
}

func TestJiraAPIErrorFormatsUnauthorizedClearly(t *testing.T) {
	err := (&jiraAPIError{
		Method:     http.MethodGet,
		Endpoint:   "/team",
		StatusCode: http.StatusUnauthorized,
		Message:    "<html><title>Unauthorized</title></html>",
	}).Error()

	want := "GET /team returned 401: Jira authentication failed; check the username/password you entered for this instance"
	if err != want {
		t.Fatalf("unexpected unauthorized error message:\nwant: %q\ngot:  %q", want, err)
	}
}

func TestJiraAPIErrorFormatsForbiddenClearly(t *testing.T) {
	err := (&jiraAPIError{
		Method:     http.MethodGet,
		Endpoint:   "/rest/api/2/search",
		StatusCode: http.StatusForbidden,
		Message:    "<html><title>Forbidden</title></html>",
	}).Error()

	want := "GET /rest/api/2/search returned 403: Jira authenticated the request but denied access; check the permissions for this instance"
	if err != want {
		t.Fatalf("unexpected forbidden error message:\nwant: %q\ngot:  %q", want, err)
	}
}

func TestGetFilterAndUpdateFilter(t *testing.T) {
	var updateBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/filter/9001":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"9001","name":"Numeric Team Filter","jql":"project = ABC AND Team = 42","owner":{"displayName":"Jane Doe"}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/rest/api/2/filter/9001":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read update body: %v", err)
			}
			updateBody = string(data)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"9001","name":"Numeric Team Filter","jql":"project = ABC AND Team = 142","owner":{"displayName":"Jane Doe"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := newJiraClient(server.URL, "user", "pass")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}

	filter, err := client.GetFilter("9001")
	if err != nil {
		t.Fatalf("GetFilter returned error: %v", err)
	}
	if filter.ID != "9001" || filter.JQL != "project = ABC AND Team = 42" {
		t.Fatalf("unexpected filter from GetFilter: %#v", filter)
	}

	updated, err := client.UpdateFilter("9001", JiraFilterUpdatePayload{
		Name:        "Numeric Team Filter",
		Description: "demo",
		JQL:         "project = ABC AND Team = 142",
	})
	if err != nil {
		t.Fatalf("UpdateFilter returned error: %v", err)
	}
	if !strings.Contains(updateBody, `"jql":"project = ABC AND Team = 142"`) {
		t.Fatalf("expected update payload to contain rewritten JQL, got %s", updateBody)
	}
	if updated == nil || updated.JQL != "project = ABC AND Team = 142" {
		t.Fatalf("unexpected updated filter: %#v", updated)
	}
}

func TestExecuteMigrationWithStateAppliesPostMigrationCorrections(t *testing.T) {
	var (
		issueUpdateBody  string
		filterUpdateBody string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_18888","name":"Team","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-team"}}]`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/ABC-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"10001","key":"ABC-1","fields":{"summary":"Demo issue","project":{"key":"ABC","name":"Demo Project","projectTypeKey":"software"},"customfield_18888":[{"id":42}]}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/rest/api/2/issue/ABC-1":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read issue update body: %v", err)
			}
			issueUpdateBody = string(data)
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"meta":{"lastId":0,"nextLastId":9001,"scanned":1,"matched":1,"parseErrorCount":0,"limit":500,"dbMode":"local","durationMs":1},"results":[{"id":9001,"name":"Numeric Team Filter","owner":"Jane Doe","jql":"project = ABC AND Team = 42"}],"parseErrors":[]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/filter/9001":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"9001","name":"Numeric Team Filter","description":"demo","jql":"project = ABC AND Team = 42","owner":{"displayName":"Jane Doe"}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/rest/api/2/filter/9001":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read filter update body: %v", err)
			}
			filterUpdateBody = string(data)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"9001","name":"Numeric Team Filter","description":"demo","jql":"project = ABC AND Team = 142","owner":{"displayName":"Jane Doe"}}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{
		Command:                 "migrate",
		Phase:                   phasePostMigrate,
		TargetBaseURL:           server.URL,
		TargetUsername:          "user",
		TargetPassword:          "pass",
		OutputDir:               t.TempDir(),
		OutputTimestamp:         "20260420-140500",
		DryRun:                  false,
		FilterTeamIDsInScope:    true,
		FilterTeamIDsInScopeSet: true,
	}
	state := migrationState{
		IssueTeamRows: []IssueTeamRow{
			{
				IssueKey:        "ABC-1",
				ProjectKey:      "ABC",
				ProjectName:     "Demo Project",
				ProjectType:     "software",
				Summary:         "Demo issue",
				TeamsFieldID:    "customfield_16604",
				SourceTeamIDs:   "42",
				SourceTeamNames: "Red Team",
			},
		},
		FilterTeamClauseRows: []FilterTeamClauseRow{
			{
				FilterID:       "10000",
				FilterName:     "Numeric Team Filter",
				Owner:          "Jane Doe",
				MatchType:      "team_id",
				ClauseValue:    "42",
				SourceTeamID:   "42",
				SourceTeamName: "Red Team",
				Clause:         "Team = 42",
				JQL:            "project = ABC AND Team = 42",
			},
		},
		TeamMappings: []TeamMapping{
			{SourceTeamID: 42, TargetTeamID: "142", TargetTitle: "Red Team", Decision: "created"},
		},
	}

	state, findings, actions := executeMigrationWithState(cfg, true, state, nil)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	if !strings.Contains(issueUpdateBody, `"customfield_18888":[{"id":142}]`) {
		t.Fatalf("expected issue update payload to contain rewritten team ID, got %s", issueUpdateBody)
	}
	if !strings.Contains(filterUpdateBody, `"jql":"project = ABC AND Team = 142"`) {
		t.Fatalf("expected filter update payload to contain rewritten JQL, got %s", filterUpdateBody)
	}

	issueResultsPath := artifactPathByKey(state.Artifacts, "post_migrate_issue_update_results")
	if filepath.Base(issueResultsPath) != "issue-update-results.post-migration.20260420-140500.csv" {
		t.Fatalf("unexpected issue results path %q", issueResultsPath)
	}
	issueResultRecords := readCSVRecords(t, issueResultsPath)
	wantIssueResults := [][]string{
		{"Issue Key", "Source Teams Field ID", "Target Teams Field ID", "Source Team IDs", "Target Team IDs", "Current Target Team IDs", "Status", "Message"},
		{"ABC-1", "customfield_16604", "customfield_18888", "42", "142", "142", "updated", "Updated target issue Teams field to the mapped destination team IDs"},
	}
	if !reflect.DeepEqual(issueResultRecords, wantIssueResults) {
		t.Fatalf("unexpected issue update results CSV:\nwant: %#v\ngot:  %#v", wantIssueResults, issueResultRecords)
	}

	filterResultsPath := artifactPathByKey(state.Artifacts, "post_migrate_filter_update_results")
	if filepath.Base(filterResultsPath) != "filter-update-results.post-migration.20260420-140500.csv" {
		t.Fatalf("unexpected filter results path %q", filterResultsPath)
	}
	filterResultRecords := readCSVRecords(t, filterResultsPath)
	wantFilterResults := [][]string{
		{"Source Filter ID", "Source Filter Name", "Target Filter ID", "Target Filter Name", "Current Target JQL", "Rewritten Target JQL", "Status", "Message"},
		{"10000", "Numeric Team Filter", "9001", "Numeric Team Filter", "project = ABC AND Team = 142", "project = ABC AND Team = 142", "updated", "Updated target filter JQL to the mapped destination team IDs"},
	}
	if !reflect.DeepEqual(filterResultRecords, wantFilterResults) {
		t.Fatalf("unexpected filter update results CSV:\nwant: %#v\ngot:  %#v", wantFilterResults, filterResultRecords)
	}

	if !containsAction(actions, "post_migrate_issue_update", "updated") {
		t.Fatalf("expected post-migrate issue update action, got %#v", actions)
	}
	if !containsAction(actions, "post_migrate_filter_update", "updated") {
		t.Fatalf("expected post-migrate filter update action, got %#v", actions)
	}
}

func TestExecuteMigrationWithStateAppliesPostMigrationParentLinkCorrections(t *testing.T) {
	var updateBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_19999","name":"Parent Link","custom":true,"schema":{"custom":"com.atlassian.jpo:jpo-custom-field-parent"}}]`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/ABC-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"10001","key":"ABC-1","fields":{"summary":"Child issue","project":{"key":"ABC","name":"Demo Project","projectTypeKey":"software"},"customfield_19999":{"id":"5001"}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/INIT-1":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"6001","key":"INIT-1","fields":{"summary":"Parent issue","project":{"key":"INIT","name":"Initiatives","projectTypeKey":"software"}}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/rest/api/2/issue/5001":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"5001","key":"INIT-1","fields":{"summary":"Parent issue","project":{"key":"INIT","name":"Initiatives","projectTypeKey":"software"}}}`))
		case r.Method == http.MethodPut && r.URL.Path == "/rest/api/2/issue/ABC-1":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read parent link update body: %v", err)
			}
			updateBodies = append(updateBodies, string(data))
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{
		Command:              "migrate",
		Phase:                phasePostMigrate,
		TargetBaseURL:        server.URL,
		TargetUsername:       "user",
		TargetPassword:       "pass",
		OutputDir:            t.TempDir(),
		OutputTimestamp:      "20260420-142500",
		DryRun:               false,
		ParentLinkInScope:    true,
		ParentLinkInScopeSet: true,
	}
	state := migrationState{
		ParentLinkRows: []ParentLinkRow{
			{
				IssueKey:             "ABC-1",
				IssueID:              "10001",
				ProjectKey:           "ABC",
				ProjectName:          "Demo Project",
				ProjectType:          "software",
				Summary:              "Child issue",
				ParentLinkFieldID:    "customfield_16605",
				SourceParentIssueID:  "5001",
				SourceParentIssueKey: "INIT-1",
				SourceParentSummary:  "Parent issue",
			},
		},
	}

	state, findings, actions := executeMigrationWithState(cfg, true, state, nil)
	for _, finding := range findings {
		if finding.Severity == SeverityError {
			t.Fatalf("unexpected error finding: %#v", findings)
		}
	}

	if len(updateBodies) != 1 {
		t.Fatalf("expected exactly one parent-link update body, got %d", len(updateBodies))
	}
	if !strings.Contains(updateBodies[0], `"customfield_19999":{"id":"6001"}`) {
		t.Fatalf("expected parent-link update payload to contain mapped target parent ID, got %s", updateBodies[0])
	}

	resultsPath := artifactPathByKey(state.Artifacts, "post_migrate_parent_link_update_results")
	if filepath.Base(resultsPath) != "parent-link-update-results.post-migration.20260420-142500.csv" {
		t.Fatalf("unexpected parent-link results path %q", resultsPath)
	}
	resultRecords := readCSVRecords(t, resultsPath)
	wantResults := [][]string{
		{"Issue Key", "Source Parent Link Field ID", "Target Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Target Parent Issue ID", "Target Parent Issue Key", "Current Parent Issue ID", "Current Parent Issue Key", "Status", "Message"},
		{"ABC-1", "customfield_16605", "customfield_19999", "5001", "INIT-1", "6001", "INIT-1", "6001", "INIT-1", "updated", "Updated target issue Parent Link to the mapped target parent issue"},
	}
	if !reflect.DeepEqual(resultRecords, wantResults) {
		t.Fatalf("unexpected parent-link update results CSV:\nwant: %#v\ngot:  %#v", wantResults, resultRecords)
	}

	if !containsAction(actions, "post_migrate_parent_link_update", "updated") {
		t.Fatalf("expected post-migrate parent-link update action, got %#v", actions)
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

func containsAction(actions []Action, kind, status string) bool {
	for _, action := range actions {
		if action.Kind == kind && action.Status == status {
			return true
		}
	}
	return false
}
