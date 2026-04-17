package app

import "testing"

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
