package app

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestPrintSummaryShowsPhaseSectionsForTypedMetadata(t *testing.T) {
	report := samplePhaseReport()

	var out bytes.Buffer
	printSummary(&out, report, nil)
	rendered := out.String()

	for _, want := range []string{
		"Execution Phases",
		"Generated artifacts: 1",
		"Pre-migrate [completed]",
		"Migrate [up next]",
		"Post-migrate [up next]",
		"Planned actions: 1",
		"Pre-migrate Preview",
		"Migrate Preview",
		"Post-migrate Preview",
		"Programs compared: 1",
		"Plans compared: 1",
		"Teams compared: 2",
		"Memberships compared: 1",
		"Teams to create: 1",
		"Existing teams to reuse: 1",
		"Issue rewrites prepared: 1",
		"Parent Link rewrites prepared: 1",
		"Filter candidates found: 1",
		"Filters with no target candidate found: 1",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("summary did not contain %q:\n%s", want, rendered)
		}
	}
	for _, unwanted := range []string{
		"Create: Red Team",
		"Team mapping comparison: out/team-mapping.pre-migration.csv",
		"Teams: Red Team",
		"Memberships: Red Team",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("summary contained row sample %q:\n%s", unwanted, rendered)
		}
	}
}

func TestPrintSummaryShowsPhaseSectionsForDecodedJSONMetadata(t *testing.T) {
	report := samplePhaseReport()

	data, err := json.Marshal(report.Metadata)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	report.Metadata = decoded

	var out bytes.Buffer
	printSummary(&out, report, nil)
	rendered := out.String()

	for _, want := range []string{
		"Execution Phases",
		"Generated artifacts: 1",
		"Pre-migrate Preview",
		"Migrate Preview",
		"Post-migrate Preview",
		"Programs compared: 1",
		"Plans compared: 1",
		"Teams compared: 2",
		"Memberships compared: 1",
		"Teams to create: 1",
		"Existing teams to reuse: 1",
		"Issue rewrites prepared: 1",
		"Parent Link rewrites prepared: 1",
		"Filter candidates found: 1",
		"Filters with no target candidate found: 1",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("summary did not contain %q after JSON decode:\n%s", want, rendered)
		}
	}
}

func TestPrintPreMigrateReviewChecklistShowsOnlyUsefulArtifactPaths(t *testing.T) {
	report := samplePhaseReport()
	report.Metadata["artifacts"] = []Artifact{
		{Label: "Team mapping comparison", Path: "out/team-mapping.pre-migration.csv"},
		{Label: "Source team memberships", Path: "out/source-team-memberships.pre-migration.csv"},
		{Label: "Team membership mapping comparison", Path: "out/team-membership-mapping.pre-migration.csv"},
	}

	var out bytes.Buffer
	printPreMigrateReviewChecklist(&out, report)
	rendered := out.String()

	for _, want := range []string{
		"Review Checklist",
		"Team mapping file: out/team-mapping.pre-migration.csv",
		"Membership mapping file: out/team-membership-mapping.pre-migration.csv",
		"Resume later with: teams-migrator migrate --phase migrate",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("checklist did not contain %q:\n%s", want, rendered)
		}
	}
	for _, unwanted := range []string{
		"Team mapping file: Team mapping comparison:",
		"Membership mapping file: Team membership mapping comparison:",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("checklist contained noisy label %q:\n%s", unwanted, rendered)
		}
	}
}

func TestPrintInteractivePreMigrateSummaryIsCompactAndCompleted(t *testing.T) {
	report := samplePhaseReport()
	report.Source = Endpoint{BaseURL: "https://source.example.com", Mode: "api"}
	report.Target = Endpoint{BaseURL: "https://target.example.com", Mode: "api"}
	report.Stats = ReportStats{Actions: 1, Warnings: 2}
	report.Findings = []Finding{
		{Severity: SeverityWarning, Code: "same_id_conflict", Message: "A long warning that belongs in the report"},
		{Severity: SeverityInfo, Code: "pre_migrate_phase_complete", Message: "Pre-migrate phase completed; no remote writes were sent"},
	}
	report.Metadata["artifacts"] = []Artifact{
		{Label: "Team mapping comparison", Path: "out/team-mapping.pre-migration.csv"},
		{Label: "Source team memberships", Path: "out/source-team-memberships.pre-migration.csv"},
		{Label: "Team membership mapping comparison", Path: "out/team-membership-mapping.pre-migration.csv"},
	}

	var out bytes.Buffer
	printInteractivePreMigrateSummary(&out, report, []string{"out/report.json", "out/report.csv"})
	rendered := out.String()

	for _, want := range []string{
		"Pre-migrate phase completed",
		"Reports: out/report.json, out/report.csv",
		"Prepared",
		"Review artifacts: 3",
		"Migrate Readiness",
		"Warnings: 2",
		"Warning details are in the report files.",
		"Next Steps",
		"Team mapping: out/team-mapping.pre-migration.csv",
		"Membership mapping: out/team-membership-mapping.pre-migration.csv",
		"Resume later with: teams-migrator migrate --phase migrate",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("compact summary did not contain %q:\n%s", want, rendered)
		}
	}
	for _, unwanted := range []string{
		"Pre-migrate Preview",
		"Execution Phases",
		"Action Summary",
		"Post-migrate Readiness",
		"Notes",
		"Warnings\n- A long warning",
		"Team mapping comparison: out/team-mapping.pre-migration.csv",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("compact summary contained noisy text %q:\n%s", unwanted, rendered)
		}
	}
}

func TestPrintInteractiveMigratePreviewSummaryOnlyShowsMigratePlan(t *testing.T) {
	report := samplePhaseReport()
	report.Phase = phaseMigrate
	report.DryRun = true
	report.Metadata["artifacts"] = []Artifact{
		{Label: "Team mapping comparison", Path: "out/team-mapping.pre-migration.csv"},
		{Label: "Team membership mapping comparison", Path: "out/team-membership-mapping.pre-migration.csv"},
	}

	var out bytes.Buffer
	printInteractivePhasePreviewSummary(&out, report, []string{"out/migrate-report.json"})
	rendered := out.String()

	for _, want := range []string{
		"Migrate preview ready",
		"Report: out/migrate-report.json",
		"Plan",
		"Teams to create: 1",
		"Existing teams to reuse: 1",
		"Status",
		"Planned actions: 1",
		"Review Files",
		"Team mapping: out/team-mapping.pre-migration.csv",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("migrate preview summary did not contain %q:\n%s", want, rendered)
		}
	}
	for _, unwanted := range []string{
		"Execution Phases",
		"Pre-migrate Preview",
		"Post-migrate Preview",
		"Programs compared",
		"Issue rewrites prepared",
		"Notes",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("migrate preview summary contained noisy text %q:\n%s", unwanted, rendered)
		}
	}
}

func TestPrintInteractivePostMigratePreviewSummaryOnlyShowsCorrectionPlan(t *testing.T) {
	report := samplePhaseReport()
	report.Phase = phasePostMigrate
	report.DryRun = true
	report.Metadata["artifacts"] = []Artifact{
		{Label: "Issue comparison export", Path: "out/issue-team-comparison.post-migration.csv"},
		{Label: "Parent-link comparison export", Path: "out/parent-link-comparison.post-migration.csv"},
		{Label: "Filter JQL comparison export", Path: "out/filter-jql-comparison.post-migration.csv"},
	}

	var out bytes.Buffer
	printInteractivePhasePreviewSummary(&out, report, []string{"out/post-report.json"})
	rendered := out.String()

	for _, want := range []string{
		"Post-migrate preview ready",
		"Report: out/post-report.json",
		"Correction Plan",
		"Issue rewrites prepared: 1",
		"Parent Link rewrites prepared: 1",
		"Filter candidates found: 1",
		"Status",
		"Review Files",
		"out/issue-team-comparison.post-migration.csv",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("post-migrate preview summary did not contain %q:\n%s", want, rendered)
		}
	}
	for _, unwanted := range []string{
		"Execution Phases",
		"Pre-migrate Preview",
		"Migrate Preview",
		"Programs compared",
		"Existing teams to reuse",
		"Action Summary",
		"Notes",
	} {
		if strings.Contains(rendered, unwanted) {
			t.Fatalf("post-migrate preview summary contained noisy text %q:\n%s", unwanted, rendered)
		}
	}
}

func TestPrintSummaryShowsConnectivityHintBeforeRawErrors(t *testing.T) {
	report := Report{
		Command: "migrate",
		Phase:   phasePreMigrate,
		DryRun:  true,
		Source:  Endpoint{BaseURL: "https://source.example.com/jira", Mode: "api"},
		Target:  Endpoint{BaseURL: "https://target.example.com/jira", Mode: "api"},
		Findings: []Finding{
			{Severity: SeverityError, Code: "source_teams_load", Message: `loading source teams: Get "https://source.example.com/jira/rest/teams-api/1.0/team": dial tcp: lookup source.example.com: no such host`},
		},
		Stats: ReportStats{Errors: 1},
	}

	var out bytes.Buffer
	printSummary(&out, report, nil)
	rendered := out.String()

	hint := "Could not reach source/target Jira. Check base URL, VPN/DNS, and credentials."
	if !strings.Contains(rendered, "Connectivity") || !strings.Contains(rendered, hint) {
		t.Fatalf("summary did not contain connectivity hint:\n%s", rendered)
	}
	if !strings.Contains(rendered, `https://source.example.com/jira/rest/teams-api/1.0/team`) {
		t.Fatalf("summary did not keep raw endpoint error:\n%s", rendered)
	}
	if strings.Index(rendered, hint) > strings.LastIndex(rendered, "Errors") {
		t.Fatalf("connectivity hint should appear before raw errors:\n%s", rendered)
	}
}

func samplePhaseReport() Report {
	return Report{
		Command: "migrate",
		DryRun:  true,
		Actions: []Action{
			{Kind: "team", Status: "planned", Details: "Red Team"},
		},
		Metadata: map[string]any{
			"artifacts": []Artifact{
				{Label: "Team mapping comparison", Path: "out/team-mapping.pre-migration.csv"},
			},
			"imd": map[string]any{
				"programs": []ProgramMapping{
					{SourceTitle: "Portfolio", TargetTitle: "Portfolio", TargetProgramID: "100", Decision: "merge"},
				},
				"plans": []PlanMapping{
					{SourceTitle: "Plan Alpha", SourceProgramTitle: "Portfolio", TargetTitle: "Plan Alpha", TargetPlanID: "200", Decision: "merge"},
				},
				"teams": []TeamMapping{
					{SourceTeamID: 42, SourceTitle: "Red Team", TargetTeamID: "142", TargetTitle: "Red Team", Decision: "add"},
					{SourceTeamID: 7, SourceTitle: "Blue Team", TargetTeamID: "207", TargetTitle: "Blue Team", Decision: "merge"},
				},
				"resources": []ResourcePlan{
					{SourceTeamName: "Red Team", SourceEmail: "red@example.com", TargetTeamName: "Red Team", TargetUserID: "jira-user-1", Status: "planned"},
				},
				"issues": []IssueTeamRow{
					{IssueKey: "ABC-123", Summary: "Move team field", SourceTeamIDs: "42", SourceTeamNames: "Red Team"},
				},
				"parentLinkComparisons": []PostMigrationParentLinkComparisonRow{
					{IssueKey: "ABC-123", SourceParentIssueID: "10001", TargetParentIssueID: "20001", Status: "ready"},
				},
				"filterComparisons": []PostMigrationFilterComparisonRow{
					{SourceFilterID: "10001", SourceFilterName: "Numeric Team Filter", SourceClause: "Team = 42", SourceTeamID: "42", TargetTeamID: "142", Status: "ready"},
				},
				"filterMatches": []PostMigrationFilterMatchRow{
					{SourceFilterID: "10001", SourceFilterName: "Numeric Team Filter", Status: "matched"},
					{SourceFilterID: "10002", SourceFilterName: "Missing Filter", Status: "not_found"},
				},
				"filters": []FilterTeamClauseRow{
					{FilterID: "10000", FilterName: "Red Filter", MatchType: "team_name", SourceTeamID: "42", SourceTeamName: "Red Team", Clause: `Team = "Red Team"`},
				},
			},
		},
	}
}
