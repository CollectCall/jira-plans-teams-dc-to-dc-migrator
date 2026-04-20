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
		"Pre-migrate [skipped]",
		"Migrate [planned]",
		"Post-migrate [up next]",
		"Pre-migrate Preview",
		"Migrate Preview",
		"Post-migrate Preview",
		"Issue update: ABC-123 [Red Team] Team = 42 -> Team = 142",
		"Parent link update: ABC-123 Parent Link = 10001 -> Parent Link = 20001",
		`Filter update: Numeric Team Filter [10001] Team = 42 -> Team = 142`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("summary did not contain %q:\n%s", want, rendered)
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
		"Pre-migrate Preview",
		"Migrate Preview",
		"Post-migrate Preview",
		"Issue update: ABC-123 [Red Team] Team = 42 -> Team = 142",
		"Parent link update: ABC-123 Parent Link = 10001 -> Parent Link = 20001",
		`Filter update: Numeric Team Filter [10001] Team = 42 -> Team = 142`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("summary did not contain %q after JSON decode:\n%s", want, rendered)
		}
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
				"filters": []FilterTeamClauseRow{
					{FilterID: "10000", FilterName: "Red Filter", MatchType: "team_name", SourceTeamID: "42", SourceTeamName: "Red Team", Clause: `Team = "Red Team"`},
				},
			},
		},
	}
}
