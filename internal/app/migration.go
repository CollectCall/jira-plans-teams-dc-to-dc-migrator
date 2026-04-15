package app

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type migrationState struct {
	IdentityMappings IdentityMapping
	SourceTeams      []TeamDTO
	SourcePersons    []PersonDTO
	SourceResources  []ResourceDTO
	TargetTeams      []TeamDTO
	TargetPersons    []PersonDTO
	TeamMappings     []TeamMapping
	ResourcePlans    []ResourcePlan
}

func executeMigration(cfg Config, apply bool) (migrationState, []Finding, []Action) {
	state, findings := loadMigrationState(cfg)
	if hasErrors(findings) {
		return state, findings, nil
	}

	actions := planTeamActions(state)
	resourceActions, resourceFindings := planResourceActions(state)
	actions = append(actions, resourceActions...)
	findings = append(findings, resourceFindings...)

	if cfg.IssuesCSV != "" {
		outputPath, err := enrichIssuesCSV(cfg, state.TeamMappings, state.SourceTeams)
		if err != nil {
			findings = append(findings, newFinding(SeverityError, "issues_csv_enrichment_failed", err.Error()))
		} else {
			actions = append(actions, Action{Kind: "enrich_issues_csv", Status: "generated", Details: outputPath})
		}
	}

	if !apply {
		return state, findings, actions
	}

	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetAuthToken, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "target_client", err.Error()))
		return state, findings, actions
	}

	execActions, execFindings := applyMigration(targetClient, &state)
	actions = append(actions, execActions...)
	findings = append(findings, execFindings...)
	return state, findings, actions
}

func loadMigrationState(cfg Config) (migrationState, []Finding) {
	var findings []Finding
	mapping, err := loadIdentityMappings(cfg.IdentityMappingFile)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "identity_mapping_load", err.Error()))
		return migrationState{}, findings
	}

	sourceTeams, err := loadTeams(cfg.SourceBaseURL, cfg.SourceAuthToken, cfg.SourceUsername, cfg.SourcePassword, cfg.TeamsFile)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_teams_load", err.Error()))
	}
	sourcePersons, err := loadPersons(cfg.SourceBaseURL, cfg.SourceAuthToken, cfg.SourceUsername, cfg.SourcePassword, cfg.PersonsFile)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_persons_load", err.Error()))
	}
	sourceResources, err := loadResources(cfg.SourceBaseURL, cfg.SourceAuthToken, cfg.SourceUsername, cfg.SourcePassword, cfg.ResourcesFile)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "source_resources_load", err.Error()))
	}
	targetTeams, err := loadTeams(cfg.TargetBaseURL, cfg.TargetAuthToken, cfg.TargetUsername, cfg.TargetPassword, "")
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "target_teams_load", err.Error()))
	}
	targetPersons, err := loadPersons(cfg.TargetBaseURL, cfg.TargetAuthToken, cfg.TargetUsername, cfg.TargetPassword, "")
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "target_persons_load", err.Error()))
	}

	if hasErrors(findings) {
		return migrationState{}, findings
	}

	state := migrationState{
		IdentityMappings: mapping,
		SourceTeams:      sourceTeams,
		SourcePersons:    sourcePersons,
		SourceResources:  sourceResources,
		TargetTeams:      targetTeams,
		TargetPersons:    targetPersons,
	}

	state.TeamMappings, findings = append(state.TeamMappings, buildTeamMappings(sourceTeams, targetTeams)...), findings
	resourcePlans, resourceFindings := buildResourcePlans(state)
	state.ResourcePlans = resourcePlans
	findings = append(findings, resourceFindings...)

	return state, findings
}

func buildTeamMappings(sourceTeams, targetTeams []TeamDTO) []TeamMapping {
	targetByTitle := map[string][]TeamDTO{}
	for _, team := range targetTeams {
		targetByTitle[normalizeTitle(team.Title)] = append(targetByTitle[normalizeTitle(team.Title)], team)
	}

	mappings := make([]TeamMapping, 0, len(sourceTeams))
	for _, source := range sourceTeams {
		matches := targetByTitle[normalizeTitle(source.Title)]
		switch len(matches) {
		case 0:
			mappings = append(mappings, TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				TargetTeamID:    fmt.Sprintf("planned:%d", source.ID),
				TargetTitle:     source.Title,
				Decision:        "create",
			})
		case 1:
			mappings = append(mappings, TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				TargetTeamID:    strconv.FormatInt(matches[0].ID, 10),
				TargetTitle:     matches[0].Title,
				Decision:        "reuse",
			})
		default:
			mappings = append(mappings, TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				Decision:        "conflict",
				ConflictReason:  "multiple destination teams match normalized title",
			})
		}
	}
	return mappings
}

func buildResourcePlans(state migrationState) ([]ResourcePlan, []Finding) {
	var findings []Finding
	personByID := map[int64]PersonDTO{}
	for _, person := range state.SourcePersons {
		personByID[person.ID] = person
	}

	targetUserByEmail := map[string]string{}
	for _, person := range state.TargetPersons {
		if person.JiraUser == nil || person.JiraUser.Email == "" || person.JiraUser.JiraUserID == "" {
			continue
		}
		targetUserByEmail[strings.ToLower(strings.TrimSpace(person.JiraUser.Email))] = person.JiraUser.JiraUserID
	}

	mappingsBySourceTeam := map[int64]TeamMapping{}
	for _, mapping := range state.TeamMappings {
		mappingsBySourceTeam[mapping.SourceTeamID] = mapping
		if mapping.Decision == "conflict" {
			findings = append(findings, newFinding(SeverityWarning, "team_conflict", fmt.Sprintf("Source team %d has a duplicate destination title match", mapping.SourceTeamID)))
		}
	}

	plans := make([]ResourcePlan, 0, len(state.SourceResources))
	for _, resource := range state.SourceResources {
		plan := ResourcePlan{
			SourceResourceID: resource.ID,
			SourceTeamID:     resource.TeamID,
			WeeklyHours:      resource.WeeklyHours,
			Status:           "planned",
		}
		if plan.WeeklyHours == 0 {
			plan.WeeklyHours = 40
		}

		sourcePersonID := int64(0)
		if resource.Person != nil {
			sourcePersonID = resource.Person.ID
		}
		plan.SourcePersonID = sourcePersonID

		teamMapping, ok := mappingsBySourceTeam[resource.TeamID]
		if !ok || teamMapping.Decision == "conflict" {
			plan.Status = "skipped"
			plan.Reason = "team mapping missing or conflicted"
			plans = append(plans, plan)
			continue
		}
		plan.TargetTeamID = teamMapping.TargetTeamID

		sourcePerson, ok := personByID[sourcePersonID]
		if !ok && resource.Person != nil {
			sourcePerson = *resource.Person
			ok = true
		}
		if !ok || sourcePerson.JiraUser == nil || sourcePerson.JiraUser.Email == "" {
			plan.Status = "skipped"
			plan.Reason = "source person email missing"
			plans = append(plans, plan)
			findings = append(findings, newFinding(SeverityWarning, "missing_source_email", fmt.Sprintf("Resource %d has no portable source email", resource.ID)))
			continue
		}

		targetEmail, ok := state.IdentityMappings[strings.ToLower(strings.TrimSpace(sourcePerson.JiraUser.Email))]
		if !ok {
			plan.Status = "skipped"
			plan.Reason = "identity mapping missing"
			plans = append(plans, plan)
			findings = append(findings, newFinding(SeverityWarning, "missing_identity_mapping", fmt.Sprintf("No identity mapping for %s", sourcePerson.JiraUser.Email)))
			continue
		}

		targetUserID, ok := targetUserByEmail[targetEmail]
		if !ok {
			plan.Status = "skipped"
			plan.Reason = "destination user missing"
			plans = append(plans, plan)
			findings = append(findings, newFinding(SeverityWarning, "missing_destination_user", fmt.Sprintf("Target user not found for %s", targetEmail)))
			continue
		}

		plan.TargetUserID = targetUserID
		plans = append(plans, plan)
	}
	return plans, findings
}

func planTeamActions(state migrationState) []Action {
	actions := make([]Action, 0, len(state.TeamMappings))
	for _, mapping := range state.TeamMappings {
		status := "planned"
		switch mapping.Decision {
		case "reuse":
			status = "reused"
		case "create":
			status = "planned"
		case "conflict":
			status = "skipped"
		}
		actions = append(actions, Action{
			Kind:     "team",
			SourceID: strconv.FormatInt(mapping.SourceTeamID, 10),
			TargetID: mapping.TargetTeamID,
			Status:   status,
			Details:  fmt.Sprintf("%s -> %s (%s)", mapping.SourceTitle, mapping.TargetTitle, mapping.Decision),
		})
	}
	return actions
}

func planResourceActions(state migrationState) ([]Action, []Finding) {
	var findings []Finding
	actions := make([]Action, 0, len(state.ResourcePlans))
	for _, resource := range state.ResourcePlans {
		status := resource.Status
		details := resource.Reason
		if details == "" {
			details = fmt.Sprintf("team=%s user=%s weeklyHours=%.2f", resource.TargetTeamID, resource.TargetUserID, resource.WeeklyHours)
		}
		actions = append(actions, Action{
			Kind:     "resource",
			SourceID: strconv.FormatInt(resource.SourceResourceID, 10),
			TargetID: resource.TargetTeamID,
			Status:   status,
			Details:  details,
		})
	}
	return actions, findings
}

func applyMigration(client *jiraClient, state *migrationState) ([]Action, []Finding) {
	var actions []Action
	var findings []Finding

	teamIDs := map[int64]int64{}
	for i := range state.TeamMappings {
		mapping := &state.TeamMappings[i]
		switch mapping.Decision {
		case "reuse":
			targetID, _ := strconv.ParseInt(mapping.TargetTeamID, 10, 64)
			teamIDs[mapping.SourceTeamID] = targetID
		case "create":
			createdID, err := client.CreateTeam(TeamDTO{Title: mapping.SourceTitle, Shareable: mapping.SourceShareable})
			if err != nil {
				findings = append(findings, newFinding(SeverityError, "team_create_failed", fmt.Sprintf("Creating team %s failed: %v", mapping.SourceTitle, err)))
				continue
			}
			mapping.TargetTeamID = strconv.FormatInt(createdID, 10)
			mapping.Decision = "created"
			teamIDs[mapping.SourceTeamID] = createdID
			actions = append(actions, Action{
				Kind:     "team",
				SourceID: strconv.FormatInt(mapping.SourceTeamID, 10),
				TargetID: mapping.TargetTeamID,
				Status:   "created",
				Details:  mapping.SourceTitle,
			})
		}
	}

	for i := range state.ResourcePlans {
		resource := &state.ResourcePlans[i]
		if resource.Status == "skipped" {
			continue
		}
		targetTeamID, ok := teamIDs[resource.SourceTeamID]
		if !ok {
			findings = append(findings, newFinding(SeverityWarning, "resource_team_missing", fmt.Sprintf("Resource %d could not resolve target team", resource.SourceResourceID)))
			continue
		}
		createdID, err := client.CreateResource(targetTeamID, resource.TargetUserID, resource.WeeklyHours)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "resource_create_failed", fmt.Sprintf("Creating resource %d failed: %v", resource.SourceResourceID, err)))
			continue
		}
		actions = append(actions, Action{
			Kind:     "resource",
			SourceID: strconv.FormatInt(resource.SourceResourceID, 10),
			TargetID: strconv.FormatInt(createdID, 10),
			Status:   "created",
			Details:  fmt.Sprintf("team=%d user=%s", targetTeamID, resource.TargetUserID),
		})
	}

	return actions, findings
}

func loadIdentityMappings(path string) (IdentityMapping, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	mapping := IdentityMapping{}
	for i, row := range rows {
		if i == 0 {
			continue
		}
		if len(row) < 2 {
			continue
		}
		source := strings.ToLower(strings.TrimSpace(row[0]))
		target := strings.ToLower(strings.TrimSpace(row[1]))
		if source == "" || target == "" {
			continue
		}
		mapping[source] = target
	}
	return mapping, nil
}

func loadTeams(baseURL, token, username, password, file string) ([]TeamDTO, error) {
	if file != "" {
		return loadJSONFile[TeamDTO](file)
	}
	client, err := newJiraClient(baseURL, token, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListTeams()
}

func loadPersons(baseURL, token, username, password, file string) ([]PersonDTO, error) {
	if file != "" {
		return loadJSONFile[PersonDTO](file)
	}
	client, err := newJiraClient(baseURL, token, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPersons()
}

func loadResources(baseURL, token, username, password, file string) ([]ResourceDTO, error) {
	if file != "" {
		return loadJSONFile[ResourceDTO](file)
	}
	client, err := newJiraClient(baseURL, token, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListResources()
}

func loadJSONFile[T any](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeCollection[T](data)
}

func normalizeTitle(title string) string {
	return strings.ToLower(strings.TrimSpace(title))
}

func enrichIssuesCSV(cfg Config, mappings []TeamMapping, teams []TeamDTO) (string, error) {
	if err := ensureOutputDir(cfg.OutputDir); err != nil {
		return "", err
	}
	file, err := os.Open(cfg.IssuesCSV)
	if err != nil {
		return "", err
	}
	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", fmt.Errorf("issues CSV is empty")
	}

	sourceTeamName := map[int64]string{}
	for _, team := range teams {
		sourceTeamName[team.ID] = team.Title
	}
	mappingBySourceID := map[string]TeamMapping{}
	for _, mapping := range mappings {
		mappingBySourceID[strconv.FormatInt(mapping.SourceTeamID, 10)] = mapping
	}

	header := rows[0]
	teamColumn := -1
	for i, column := range header {
		if strings.EqualFold(strings.TrimSpace(column), "Teams") {
			teamColumn = i
			break
		}
	}
	if teamColumn == -1 {
		return "", fmt.Errorf("issues CSV missing Teams column")
	}

	header = append(header, "teamName", "mappedTeamId")
	outputRows := [][]string{header}
	for _, row := range rows[1:] {
		if len(row) <= teamColumn {
			row = append(row, "", "")
			outputRows = append(outputRows, row)
			continue
		}

		sourceTeamID := strings.TrimSpace(row[teamColumn])
		mapping, ok := mappingBySourceID[sourceTeamID]
		if !ok {
			row = append(row, "", "")
			outputRows = append(outputRows, row)
			continue
		}

		teamName := ""
		if id, err := strconv.ParseInt(sourceTeamID, 10, 64); err == nil {
			teamName = sourceTeamName[id]
		}
		row = append(row, teamName, mapping.TargetTeamID)
		outputRows = append(outputRows, row)
	}

	outputPath := filepath.Join(cfg.OutputDir, "enriched-issues.csv")
	outFile, err := os.Create(outputPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	if err := writer.WriteAll(outputRows); err != nil {
		return "", err
	}
	writer.Flush()
	return outputPath, writer.Error()
}

func migrationMetadata(state migrationState) map[string]any {
	return map[string]any{
		"imd": map[string]any{
			"teams":     state.TeamMappings,
			"resources": state.ResourcePlans,
		},
		"counts": map[string]int{
			"sourceTeams":     len(state.SourceTeams),
			"sourcePersons":   len(state.SourcePersons),
			"sourceResources": len(state.SourceResources),
			"targetTeams":     len(state.TargetTeams),
			"targetPersons":   len(state.TargetPersons),
		},
	}
}

func writeMetadataSnapshot(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
