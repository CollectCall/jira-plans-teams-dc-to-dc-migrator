package app

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type migrationState struct {
	IdentityMappings     IdentityMapping
	SourcePrograms       []ProgramDTO
	TargetPrograms       []ProgramDTO
	ProgramMappings      []ProgramMapping
	SourcePlans          []PlanDTO
	TargetPlans          []PlanDTO
	PlanMappings         []PlanMapping
	SourceTeams          []TeamDTO
	SourcePersons        []PersonDTO
	SourceResources      []ResourceDTO
	TargetTeams          []TeamDTO
	TargetPersons        []PersonDTO
	TargetResources      []ResourceDTO
	TeamMappings         []TeamMapping
	ResourcePlans        []ResourcePlan
	TeamsField           *TeamsFieldSelection
	IssueTeamRows        []IssueTeamRow
	IssueExportPath      string
	MembershipExportPath string
	Artifacts            []Artifact
}

func executeMigration(cfg Config, apply bool) (migrationState, []Finding, []Action) {
	state, findings := loadMigrationState(cfg)
	return executeMigrationWithState(cfg, apply, state, findings)
}

func executeMigrationWithState(cfg Config, apply bool, state migrationState, findings []Finding) (migrationState, []Finding, []Action) {
	if hasErrors(findings) {
		return state, findings, nil
	}

	actions := planTeamActions(state)
	resourceActions, resourceFindings := planResourceActions(state)
	actions = append(actions, resourceActions...)
	findings = append(findings, resourceFindings...)
	for _, artifact := range state.Artifacts {
		actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
	}

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

	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
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
	progress := newProgressTracker(6)
	defer progress.Finish()

	progress.Start("Loading source and target datasets")
	mapping, err := loadIdentityMappings(cfg.IdentityMappingFile)
	if err != nil {
		progress.End()
		findings = append(findings, newFinding(SeverityError, "identity_mapping_load", err.Error()))
		return migrationState{}, findings
	}

	var (
		sourceTeams     []TeamDTO
		sourcePrograms  []ProgramDTO
		sourcePlans     []PlanDTO
		sourcePersons   []PersonDTO
		sourceResources []ResourceDTO
		targetTeams     []TeamDTO
		targetPrograms  []ProgramDTO
		targetPlans     []PlanDTO
		targetPersons   []PersonDTO
		targetResources []ResourceDTO
	)

	type loadResult struct {
		code     string
		severity Severity
		message  string
	}
	results := make(chan loadResult, 10)
	var wg sync.WaitGroup
	runLoad := func(code string, severity Severity, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				results <- loadResult{code: code, severity: severity, message: err.Error()}
			}
		}()
	}

	runLoad("source_teams_load", SeverityError, func() error {
		var loadErr error
		sourceTeams, loadErr = loadTeams(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.TeamsFile)
		return loadErr
	})
	runLoad("source_programs_load", SeverityWarning, func() error {
		var loadErr error
		sourcePrograms, loadErr = loadPrograms(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword)
		return loadErr
	})
	runLoad("source_plans_load", SeverityWarning, func() error {
		var loadErr error
		sourcePlans, loadErr = loadPlans(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword)
		return loadErr
	})
	runLoad("source_persons_load", SeverityError, func() error {
		var loadErr error
		sourcePersons, loadErr = loadPersons(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.PersonsFile)
		return loadErr
	})
	runLoad("source_resources_load", SeverityError, func() error {
		var loadErr error
		sourceResources, loadErr = loadResources(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.ResourcesFile)
		return loadErr
	})
	runLoad("target_teams_load", SeverityError, func() error {
		var loadErr error
		targetTeams, loadErr = loadTeams(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "")
		return loadErr
	})
	runLoad("target_programs_load", SeverityWarning, func() error {
		var loadErr error
		targetPrograms, loadErr = loadPrograms(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
		return loadErr
	})
	runLoad("target_plans_load", SeverityWarning, func() error {
		var loadErr error
		targetPlans, loadErr = loadPlans(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
		return loadErr
	})
	runLoad("target_persons_load", SeverityError, func() error {
		var loadErr error
		targetPersons, loadErr = loadPersons(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "")
		return loadErr
	})
	runLoad("target_resources_load", SeverityWarning, func() error {
		var loadErr error
		targetResources, loadErr = loadResources(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "")
		return loadErr
	})

	wg.Wait()
	close(results)
	for result := range results {
		findings = append(findings, newFinding(result.severity, result.code, result.message))
	}
	sourceClient, sourceClientErr := sourceIssueClient(cfg)
	if sourceClientErr != nil && cfg.SourceBaseURL != "" {
		findings = append(findings, newFinding(SeverityWarning, "source_issue_client", sourceClientErr.Error()))
	} else if sourceClient == nil {
		findings = append(findings, newFinding(SeverityWarning, "issue_export_skipped", "Issue Teams-field export was skipped because no source Jira base URL was provided"))
	}
	progress.End()

	if hasErrors(findings) {
		return migrationState{}, findings
	}

	progress.Start("Hydrating resource-linked persons")
	if strings.TrimSpace(cfg.SourceBaseURL) != "" {
		if sourceClient, err := newJiraClient(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword); err == nil {
			sourcePersons, findings = hydrateResourceLinkedPersons(sourceClient, sourcePersons, sourceResources, "source", findings)
		}
	}
	progress.End()

	progress.Start("Resolving target Jira users")
	if strings.TrimSpace(cfg.TargetBaseURL) != "" {
		if targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword); err == nil {
			mapping, targetPersons, findings = resolveTargetUsersForResourcePersons(targetClient, mapping, sourcePersons, sourceResources, targetPersons, findings)
		}
	}
	progress.End()

	state := migrationState{
		IdentityMappings: mapping,
		SourcePrograms:   sourcePrograms,
		TargetPrograms:   targetPrograms,
		SourcePlans:      sourcePlans,
		TargetPlans:      targetPlans,
		SourceTeams:      sourceTeams,
		SourcePersons:    sourcePersons,
		SourceResources:  sourceResources,
		TargetTeams:      targetTeams,
		TargetPersons:    targetPersons,
		TargetResources:  targetResources,
	}

	autoMappings, autoMappingFindings := deriveAutomaticIdentityMappings(mapping, sourcePersons, targetPersons)
	state.IdentityMappings = autoMappings
	findings = append(findings, autoMappingFindings...)

	progress.Start("Building mapping plans")
	state.ProgramMappings = buildProgramMappings(sourcePrograms, targetPrograms)
	state.PlanMappings = buildPlanMappings(sourcePlans, targetPlans, state.ProgramMappings, sourcePrograms, targetPrograms, sourceTeams, targetTeams)
	state.TeamMappings, findings = append(state.TeamMappings, buildTeamMappings(sourceTeams, targetTeams)...), findings
	resourcePlans, resourceFindings := buildResourcePlans(state)
	state.ResourcePlans = resourcePlans
	findings = append(findings, resourceFindings...)
	progress.End()

	progress.Start("Writing pre-migration artifacts")
	if artifacts, err := writeEntityExports(cfg, state); err == nil {
		state.Artifacts = artifacts
		state.MembershipExportPath = artifactPathByKey(artifacts, "source_team_memberships")
		state.IssueExportPath = artifactPathByKey(artifacts, "source_issues_with_team_values")
		for _, artifact := range artifacts {
			findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
		}
	} else {
		findings = append(findings, newFinding(SeverityWarning, "artifact_export_failed", err.Error()))
	}
	progress.End()

	progress.StartCount("Exporting issues with team values")
	if sourceClient != nil {
		selection, issueRows, issuePath, issueFindings := exportIssuesWithTeamsField(cfg, sourceClient, sourceTeams, progress)
		state.TeamsField = selection
		state.IssueTeamRows = issueRows
		if issuePath != "" {
			state.IssueExportPath = issuePath
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "source_issues_with_team_values",
				Label: "Source issues with team values",
				Path:  issuePath,
				Count: len(issueRows),
			})
		}
		findings = append(findings, issueFindings...)
	}
	progress.End()

	progress.Start("Writing generated identity mapping")
	if generatedPath, err := writeGeneratedIdentityMapping(cfg, state.IdentityMappings); err == nil && generatedPath != "" {
		findings = append(findings, newFinding(SeverityInfo, "identity_mapping_generated", fmt.Sprintf("Generated reviewable identity mapping artifact: %s", generatedPath)))
	}
	progress.End()

	return state, findings
}

func sourceIssueClient(cfg Config) (*jiraClient, error) {
	if strings.TrimSpace(cfg.SourceBaseURL) == "" {
		return nil, nil
	}
	return newJiraClient(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword)
}

func buildTeamMappings(sourceTeams, targetTeams []TeamDTO) []TeamMapping {
	targetByTitle := map[string][]TeamDTO{}
	for _, team := range targetTeams {
		targetByTitle[normalizeTitle(team.Title)] = append(targetByTitle[normalizeTitle(team.Title)], team)
	}

	mappings := make([]TeamMapping, 0, len(sourceTeams))
	nextCreateOffset := 0
	for _, source := range sourceTeams {
		matches := targetByTitle[normalizeTitle(source.Title)]
		switch len(matches) {
		case 0:
			nextCreateOffset++
			mappings = append(mappings, TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				TargetTeamID:    expectedSequentialID(len(targetTeams), nextCreateOffset),
				TargetTitle:     source.Title,
				Decision:        "add",
			})
		case 1:
			mappings = append(mappings, TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				TargetTeamID:    strconv.FormatInt(matches[0].ID, 10),
				TargetTitle:     matches[0].Title,
				Decision:        "merge",
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
	teamNameByID := map[int64]string{}
	for _, team := range state.SourceTeams {
		teamNameByID[team.ID] = team.Title
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
			SourceTeamName:   teamNameByID[resource.TeamID],
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
		plan.TargetTeamName = teamMapping.TargetTitle

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
		plan.SourceEmail = strings.ToLower(strings.TrimSpace(sourcePerson.JiraUser.Email))

		targetEmail, ok := state.IdentityMappings[plan.SourceEmail]
		if !ok {
			plan.Status = "skipped"
			plan.Reason = "identity mapping missing"
			plans = append(plans, plan)
			findings = append(findings, newFinding(SeverityWarning, "missing_identity_mapping", fmt.Sprintf("No identity mapping for %s", sourcePerson.JiraUser.Email)))
			continue
		}
		plan.TargetEmail = targetEmail

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
		case "merge":
			status = "reused"
		case "add":
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
		case "merge":
			targetID, _ := strconv.ParseInt(mapping.TargetTeamID, 10, 64)
			teamIDs[mapping.SourceTeamID] = targetID
		case "add":
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
	if strings.TrimSpace(path) == "" {
		return IdentityMapping{}, nil
	}
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

func deriveAutomaticIdentityMappings(existing IdentityMapping, sourcePersons, targetPersons []PersonDTO) (IdentityMapping, []Finding) {
	mappings := IdentityMapping{}
	for source, target := range existing {
		mappings[source] = target
	}

	targetEmails := map[string]struct{}{}
	for _, person := range targetPersons {
		if person.JiraUser == nil || person.JiraUser.Email == "" {
			continue
		}
		email := strings.ToLower(strings.TrimSpace(person.JiraUser.Email))
		targetEmails[email] = struct{}{}
	}

	autoResolved := 0
	for _, person := range sourcePersons {
		if person.JiraUser == nil || person.JiraUser.Email == "" {
			continue
		}
		sourceEmail := strings.ToLower(strings.TrimSpace(person.JiraUser.Email))
		if _, ok := mappings[sourceEmail]; ok {
			continue
		}
		if _, ok := targetEmails[sourceEmail]; ok {
			mappings[sourceEmail] = sourceEmail
			autoResolved++
		}
	}

	var findings []Finding
	if autoResolved > 0 {
		findings = append(findings, newFinding(SeverityInfo, "identity_mapping_auto_resolved", fmt.Sprintf("Auto-resolved %d identity mappings by matching identical source and target emails", autoResolved)))
	}
	return mappings, findings
}

func writeGeneratedIdentityMapping(cfg Config, mappings IdentityMapping) (string, error) {
	if len(mappings) == 0 {
		return "", nil
	}
	if err := ensureOutputDir(cfg.OutputDir); err != nil {
		return "", err
	}

	path := filepath.Join(cfg.OutputDir, "identity-mapping.generated.csv")
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"sourceEmail", "targetEmail"}); err != nil {
		return "", err
	}

	keys := make([]string, 0, len(mappings))
	for key := range mappings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := writer.Write([]string{key, mappings[key]}); err != nil {
			return "", err
		}
	}
	writer.Flush()
	return path, writer.Error()
}

func loadTeams(baseURL, username, password, file string) ([]TeamDTO, error) {
	if file != "" {
		return loadJSONFile[TeamDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListTeams()
}

func loadPersons(baseURL, username, password, file string) ([]PersonDTO, error) {
	if file != "" {
		return loadJSONFile[PersonDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPersons()
}

func loadResources(baseURL, username, password, file string) ([]ResourceDTO, error) {
	if file != "" {
		return loadJSONFile[ResourceDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListResources()
}

func loadPrograms(baseURL, username, password string) ([]ProgramDTO, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPrograms()
}

func loadPlans(baseURL, username, password string) ([]PlanDTO, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPlans()
}

func hydrateResourceLinkedPersons(client *jiraClient, persons []PersonDTO, resources []ResourceDTO, side string, findings []Finding) ([]PersonDTO, []Finding) {
	personByID := map[int64]PersonDTO{}
	for _, person := range persons {
		personByID[person.ID] = person
	}

	needed := map[int64]struct{}{}
	for _, resource := range resources {
		if resource.Person == nil || resource.Person.ID == 0 {
			continue
		}
		person, ok := personByID[resource.Person.ID]
		if !ok || person.JiraUser == nil || strings.TrimSpace(person.JiraUser.Email) == "" {
			needed[resource.Person.ID] = struct{}{}
		}
	}
	if len(needed) == 0 {
		return persons, findings
	}

	type hydration struct {
		person *PersonDTO
	}
	results := make(chan hydration, len(needed))
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for personID := range needed {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			sem <- struct{}{}
			person, err := client.GetPerson(id)
			<-sem
			if err != nil || person == nil {
				return
			}
			results <- hydration{person: person}
		}(personID)
	}
	wg.Wait()
	close(results)

	hydrated := 0
	for result := range results {
		if result.person == nil {
			continue
		}
		personByID[result.person.ID] = *result.person
		hydrated++
	}
	if hydrated > 0 {
		findings = append(findings, newFinding(SeverityInfo, side+"_persons_hydrated", fmt.Sprintf("Hydrated %d %s person records from the Teams person API", hydrated, side)))
	}

	out := make([]PersonDTO, 0, len(personByID))
	for _, person := range personByID {
		out = append(out, person)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, findings
}

func resolveTargetUsersForResourcePersons(client *jiraClient, mapping IdentityMapping, sourcePersons []PersonDTO, resources []ResourceDTO, targetPersons []PersonDTO, findings []Finding) (IdentityMapping, []PersonDTO, []Finding) {
	if mapping == nil {
		mapping = IdentityMapping{}
	}

	personByID := map[int64]PersonDTO{}
	for _, person := range sourcePersons {
		personByID[person.ID] = person
	}
	targetByEmail := map[string]struct{}{}
	for _, person := range targetPersons {
		if person.JiraUser == nil || strings.TrimSpace(person.JiraUser.Email) == "" {
			continue
		}
		targetByEmail[strings.ToLower(strings.TrimSpace(person.JiraUser.Email))] = struct{}{}
	}

	type targetResolution struct {
		sourceEmail string
		user        *CoreJiraUser
	}

	needed := map[string]JiraUserDTO{}
	for _, resource := range resources {
		if resource.Person == nil {
			continue
		}
		person, ok := personByID[resource.Person.ID]
		if !ok || person.JiraUser == nil || strings.TrimSpace(person.JiraUser.Email) == "" {
			continue
		}
		sourceEmail := strings.ToLower(strings.TrimSpace(person.JiraUser.Email))
		if _, ok := mapping[sourceEmail]; ok {
			continue
		}
		if _, ok := targetByEmail[sourceEmail]; ok {
			mapping[sourceEmail] = sourceEmail
			continue
		}
		needed[sourceEmail] = *person.JiraUser
	}
	if len(needed) == 0 {
		return mapping, targetPersons, findings
	}

	results := make(chan targetResolution, len(needed))
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for sourceEmail, ref := range needed {
		wg.Add(1)
		go func(email string, userRef JiraUserDTO) {
			defer wg.Done()
			sem <- struct{}{}
			user, err := resolveTargetUser(client, email, userRef)
			<-sem
			if err != nil || user == nil {
				return
			}
			results <- targetResolution{sourceEmail: email, user: user}
		}(sourceEmail, ref)
	}
	wg.Wait()
	close(results)

	resolved := 0
	for result := range results {
		if result.user == nil {
			continue
		}
		targetEmail := strings.ToLower(strings.TrimSpace(result.user.EmailAddress))
		if targetEmail == "" {
			targetEmail = result.sourceEmail
		}
		mapping[result.sourceEmail] = targetEmail
		if _, ok := targetByEmail[targetEmail]; !ok {
			targetPersons = append(targetPersons, PersonDTO{
				ID: 0,
				JiraUser: &JiraUserDTO{
					JiraUserID:   firstNonEmpty(result.user.Key, result.user.Name),
					JiraUsername: result.user.Name,
					Email:        targetEmail,
					Title:        result.user.DisplayName,
				},
			})
			targetByEmail[targetEmail] = struct{}{}
		}
		resolved++
	}
	if resolved > 0 {
		findings = append(findings, newFinding(SeverityInfo, "target_users_resolved", fmt.Sprintf("Resolved %d target Jira users for resource-linked identities", resolved)))
	}
	return mapping, targetPersons, findings
}

func resolveTargetUser(client *jiraClient, sourceEmail string, ref JiraUserDTO) (*CoreJiraUser, error) {
	for _, candidate := range []string{sourceEmail, ref.JiraUserID, ref.JiraUsername, ref.Title} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		users, err := client.SearchCoreUsers(candidate)
		if err != nil {
			continue
		}
		if user := selectExactTargetUser(users, sourceEmail, ref); user != nil {
			return user, nil
		}
	}
	return nil, fmt.Errorf("no exact target Jira user match")
}

func selectExactTargetUser(users []CoreJiraUser, sourceEmail string, ref JiraUserDTO) *CoreJiraUser {
	wantEmail := strings.ToLower(strings.TrimSpace(sourceEmail))
	wantID := strings.ToLower(strings.TrimSpace(ref.JiraUserID))
	wantUsername := strings.ToLower(strings.TrimSpace(ref.JiraUsername))
	for _, user := range users {
		if !user.Active {
			continue
		}
		if wantEmail != "" && strings.EqualFold(strings.TrimSpace(user.EmailAddress), wantEmail) {
			copy := user
			return &copy
		}
		if wantID != "" && (strings.EqualFold(strings.TrimSpace(user.Key), wantID) || strings.EqualFold(strings.TrimSpace(user.Name), wantID)) {
			copy := user
			return &copy
		}
		if wantUsername != "" && strings.EqualFold(strings.TrimSpace(user.Name), wantUsername) {
			copy := user
			return &copy
		}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func buildProgramMappings(sourcePrograms, targetPrograms []ProgramDTO) []ProgramMapping {
	targetByTitle := map[string][]ProgramDTO{}
	for _, program := range targetPrograms {
		targetByTitle[normalizeTitle(program.Title)] = append(targetByTitle[normalizeTitle(program.Title)], program)
	}

	mappings := make([]ProgramMapping, 0, len(sourcePrograms))
	nextCreateOffset := 0
	for _, source := range sourcePrograms {
		matches := targetByTitle[normalizeTitle(source.Title)]
		switch len(matches) {
		case 0:
			nextCreateOffset++
			mappings = append(mappings, ProgramMapping{
				SourceProgramID: source.ID,
				SourceTitle:     source.Title,
				SourceOwner:     source.Owner,
				TargetProgramID: expectedSequentialID(len(targetPrograms), nextCreateOffset),
				TargetTitle:     source.Title,
				Decision:        "add",
			})
		case 1:
			mappings = append(mappings, ProgramMapping{
				SourceProgramID: source.ID,
				SourceTitle:     source.Title,
				SourceOwner:     source.Owner,
				TargetProgramID: strconv.FormatInt(matches[0].ID, 10),
				TargetTitle:     matches[0].Title,
				Decision:        "merge",
			})
		default:
			mappings = append(mappings, ProgramMapping{
				SourceProgramID: source.ID,
				SourceTitle:     source.Title,
				SourceOwner:     source.Owner,
				Decision:        "conflict",
				ConflictReason:  "multiple destination programs match normalized title",
			})
		}
	}
	return mappings
}

func buildPlanMappings(sourcePlans, targetPlans []PlanDTO, programMappings []ProgramMapping, sourcePrograms, targetPrograms []ProgramDTO, sourceTeams, targetTeams []TeamDTO) []PlanMapping {
	targetByTitle := map[string][]PlanDTO{}
	for _, plan := range targetPlans {
		targetByTitle[normalizeTitle(plan.Title)] = append(targetByTitle[normalizeTitle(plan.Title)], plan)
	}

	mappedProgramIDs := map[int64]string{}
	for _, mapping := range programMappings {
		if mapping.Decision == "conflict" {
			continue
		}
		mappedProgramIDs[mapping.SourceProgramID] = mapping.TargetProgramID
	}

	sourceProgramTitles := programTitlesByID(sourcePrograms)
	targetProgramTitles := programTitlesByID(targetPrograms)
	sourceTeamTitles := teamTitlesByID(sourceTeams)
	targetTeamTitles := teamTitlesByID(targetTeams)

	mappings := make([]PlanMapping, 0, len(sourcePlans))
	nextCreateOffset := 0
	for _, source := range sourcePlans {
		matches := targetByTitle[normalizeTitle(source.Title)]
		if len(matches) > 1 {
			if mappedID, ok := mappedProgramIDs[source.ProgramID]; ok {
				filtered := make([]PlanDTO, 0, len(matches))
				for _, match := range matches {
					if strconv.FormatInt(match.ProgramID, 10) == mappedID {
						filtered = append(filtered, match)
					}
				}
				if len(filtered) > 0 {
					matches = filtered
				}
			}
		}

		mapping := PlanMapping{
			SourcePlanID:         source.ID,
			SourceTitle:          source.Title,
			SourceProgramID:      source.ProgramID,
			SourceProgramTitle:   sourceProgramTitles[source.ProgramID],
			SourcePlanTeamIDs:    joinInt64s(source.PlanTeams),
			SourcePlanTeamTitles: joinMappedTitles(source.PlanTeams, sourceTeamTitles),
		}

		switch len(matches) {
		case 0:
			nextCreateOffset++
			mapping.TargetPlanID = expectedSequentialID(len(targetPlans), nextCreateOffset)
			mapping.TargetTitle = source.Title
			mapping.Decision = "add"
		case 1:
			mapping.TargetPlanID = strconv.FormatInt(matches[0].ID, 10)
			mapping.TargetTitle = matches[0].Title
			mapping.TargetProgramID = strconv.FormatInt(matches[0].ProgramID, 10)
			mapping.TargetProgramTitle = targetProgramTitles[matches[0].ProgramID]
			mapping.TargetPlanTeamIDs = joinInt64s(matches[0].PlanTeams)
			mapping.TargetPlanTeamTitles = joinMappedTitles(matches[0].PlanTeams, targetTeamTitles)
			mapping.Decision = "merge"
		default:
			mapping.Decision = "conflict"
			mapping.ConflictReason = "multiple destination plans match normalized title"
		}
		mappings = append(mappings, mapping)
	}
	return mappings
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

func expectedSequentialID(existingCount, createOffset int) string {
	return strconv.Itoa(existingCount + createOffset)
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
	metadata := map[string]any{
		"imd": map[string]any{
			"programs":  state.ProgramMappings,
			"plans":     state.PlanMappings,
			"teams":     state.TeamMappings,
			"resources": state.ResourcePlans,
			"issues":    state.IssueTeamRows,
		},
		"counts": map[string]int{
			"sourcePrograms":  len(state.SourcePrograms),
			"targetPrograms":  len(state.TargetPrograms),
			"sourcePlans":     len(state.SourcePlans),
			"targetPlans":     len(state.TargetPlans),
			"sourceTeams":     len(state.SourceTeams),
			"sourcePersons":   len(state.SourcePersons),
			"sourceResources": len(state.SourceResources),
			"targetTeams":     len(state.TargetTeams),
			"targetPersons":   len(state.TargetPersons),
			"targetResources": len(state.TargetResources),
		},
	}
	if len(state.Artifacts) > 0 {
		metadata["artifacts"] = state.Artifacts
	}
	if state.TeamsField != nil {
		metadata["teamsField"] = state.TeamsField
	}
	if state.IssueExportPath != "" {
		metadata["issueExport"] = map[string]any{
			"path":  state.IssueExportPath,
			"count": len(state.IssueTeamRows),
		}
	}
	if state.MembershipExportPath != "" {
		metadata["membershipExport"] = map[string]any{
			"path":  state.MembershipExportPath,
			"count": len(state.ResourcePlans),
		}
	}
	return metadata
}

func writeMetadataSnapshot(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func writeEntityExports(cfg Config, state migrationState) ([]Artifact, error) {
	if err := ensureOutputDir(cfg.OutputDir); err != nil {
		return nil, err
	}

	artifacts := []Artifact{}
	add := func(key, label, name string, header []string, rows [][]string) error {
		if len(rows) == 0 {
			return nil
		}
		path, err := writeCSVExport(filepath.Join(cfg.OutputDir, name), header, rows)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, Artifact{Key: key, Label: label, Path: path, Count: len(rows)})
		return nil
	}

	if err := add("source_programs", "Source programs", "source-programs.pre-migration.csv",
		[]string{"sourceProgramId", "title", "owner", "description"},
		sourceProgramRows(state.SourcePrograms),
	); err != nil {
		return nil, err
	}
	if err := add("destination_programs", "Destination programs", "destination-programs.pre-migration.csv",
		[]string{"destinationProgramId", "title", "owner", "description"},
		destinationProgramRows(state.TargetPrograms),
	); err != nil {
		return nil, err
	}
	if err := add("program_mapping", "Program mapping comparison", "program-mapping.pre-migration.csv",
		[]string{"sourceProgramId", "sourceTitle", "sourceOwner", "destinationProgramId", "destinationTitle", "decision", "reason"},
		programMappingRows(state.ProgramMappings),
	); err != nil {
		return nil, err
	}
	if err := add("source_plans", "Source plans", "source-plans.pre-migration.csv",
		[]string{"sourcePlanId", "title", "programId", "programTitle", "planTeamIds", "planTeamTitles", "defaultTeamWeeklyCapacity", "hoursPerDay", "timeZone"},
		sourcePlanRows(state.SourcePlans, state.SourcePrograms, state.SourceTeams),
	); err != nil {
		return nil, err
	}
	if err := add("destination_plans", "Destination plans", "destination-plans.pre-migration.csv",
		[]string{"destinationPlanId", "title", "programId", "programTitle", "planTeamIds", "planTeamTitles", "defaultTeamWeeklyCapacity", "hoursPerDay", "timeZone"},
		destinationPlanRows(state.TargetPlans, state.TargetPrograms, state.TargetTeams),
	); err != nil {
		return nil, err
	}
	if err := add("plan_mapping", "Plan mapping comparison", "plan-mapping.pre-migration.csv",
		[]string{"sourcePlanId", "sourceTitle", "sourceProgramId", "sourceProgramTitle", "sourcePlanTeamIds", "sourcePlanTeamTitles", "destinationPlanId", "destinationTitle", "destinationProgramId", "destinationProgramTitle", "destinationPlanTeamIds", "destinationPlanTeamTitles", "decision", "reason"},
		planMappingRows(state.PlanMappings),
	); err != nil {
		return nil, err
	}
	if err := add("source_teams", "Source teams", "source-teams.pre-migration.csv",
		[]string{"sourceTeamId", "title", "shareable", "planIds", "planTitles"},
		sourceTeamRows(state.SourceTeams, state.SourcePlans),
	); err != nil {
		return nil, err
	}
	if err := add("destination_teams", "Destination teams", "destination-teams.pre-migration.csv",
		[]string{"destinationTeamId", "title", "shareable", "planIds", "planTitles"},
		destinationTeamRows(state.TargetTeams, state.TargetPlans),
	); err != nil {
		return nil, err
	}
	if err := add("team_mapping", "Team mapping comparison", "team-mapping.pre-migration.csv",
		[]string{"sourceTeamId", "sourceTitle", "sourceShareable", "destinationTeamId", "destinationTitle", "decision", "reason"},
		teamMappingRows(state.TeamMappings),
	); err != nil {
		return nil, err
	}
	if err := add("source_team_memberships", "Source team memberships", "source-team-memberships.pre-migration.csv",
		[]string{"sourceResourceId", "sourceTeamId", "sourceTeamName", "sourcePersonId", "sourceEmail", "weeklyHours"},
		sourceMembershipRows(state.SourceResources, state.SourcePersons, state.SourceTeams),
	); err != nil {
		return nil, err
	}
	if err := add("destination_team_memberships", "Destination team memberships", "destination-team-memberships.pre-migration.csv",
		[]string{"destinationResourceId", "destinationTeamId", "destinationTeamName", "destinationPersonId", "destinationEmail", "destinationUserId", "weeklyHours"},
		destinationMembershipRows(state.TargetResources, state.TargetPersons, state.TargetTeams),
	); err != nil {
		return nil, err
	}
	if err := add("team_membership_mapping", "Team membership mapping comparison", "team-membership-mapping.pre-migration.csv",
		[]string{"sourceResourceId", "sourceTeamId", "sourceTeamName", "sourcePersonId", "sourceEmail", "destinationEmail", "destinationTeamId", "destinationTeamName", "destinationUserId", "weeklyHours", "status", "reason"},
		membershipMappingRows(state.ResourcePlans),
	); err != nil {
		return nil, err
	}

	return artifacts, nil
}

func artifactPathByKey(artifacts []Artifact, key string) string {
	for _, artifact := range artifacts {
		if artifact.Key == key {
			return artifact.Path
		}
	}
	return ""
}

func replaceArtifact(artifacts []Artifact, replacement Artifact) []Artifact {
	for i := range artifacts {
		if artifacts[i].Key == replacement.Key {
			artifacts[i] = replacement
			return artifacts
		}
	}
	return append(artifacts, replacement)
}

func writeCSVExport(path string, header []string, rows [][]string) (string, error) {
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		return "", err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return "", err
		}
	}
	writer.Flush()
	return path, writer.Error()
}

func sourceProgramRows(programs []ProgramDTO) [][]string {
	rows := make([][]string, 0, len(programs))
	for _, program := range programs {
		rows = append(rows, []string{
			strconv.FormatInt(program.ID, 10),
			program.Title,
			program.Owner,
			program.Description,
		})
	}
	return rows
}

func destinationProgramRows(programs []ProgramDTO) [][]string {
	rows := make([][]string, 0, len(programs))
	for _, program := range programs {
		rows = append(rows, []string{
			strconv.FormatInt(program.ID, 10),
			program.Title,
			program.Owner,
			program.Description,
		})
	}
	return rows
}

func programMappingRows(mappings []ProgramMapping) [][]string {
	rows := make([][]string, 0, len(mappings))
	for _, mapping := range mappings {
		rows = append(rows, []string{
			strconv.FormatInt(mapping.SourceProgramID, 10),
			mapping.SourceTitle,
			mapping.SourceOwner,
			mapping.TargetProgramID,
			mapping.TargetTitle,
			mapping.Decision,
			mapping.ConflictReason,
		})
	}
	return rows
}

func sourcePlanRows(plans []PlanDTO, programs []ProgramDTO, teams []TeamDTO) [][]string {
	return planRowsWithLabel(plans, programs, teams, false)
}

func destinationPlanRows(plans []PlanDTO, programs []ProgramDTO, teams []TeamDTO) [][]string {
	return planRowsWithLabel(plans, programs, teams, true)
}

func planRowsWithLabel(plans []PlanDTO, programs []ProgramDTO, teams []TeamDTO, _ bool) [][]string {
	programTitles := programTitlesByID(programs)
	teamTitles := teamTitlesByID(teams)
	rows := make([][]string, 0, len(plans))
	for _, plan := range plans {
		rows = append(rows, []string{
			strconv.FormatInt(plan.ID, 10),
			plan.Title,
			strconv.FormatInt(plan.ProgramID, 10),
			programTitles[plan.ProgramID],
			joinInt64s(plan.PlanTeams),
			joinMappedTitles(plan.PlanTeams, teamTitles),
			strconv.FormatFloat(plan.DefaultTeamWeeklyCapacity, 'f', -1, 64),
			strconv.FormatFloat(plan.HoursPerDay, 'f', -1, 64),
			plan.TimeZone,
		})
	}
	return rows
}

func planMappingRows(mappings []PlanMapping) [][]string {
	rows := make([][]string, 0, len(mappings))
	for _, mapping := range mappings {
		rows = append(rows, []string{
			strconv.FormatInt(mapping.SourcePlanID, 10),
			mapping.SourceTitle,
			strconv.FormatInt(mapping.SourceProgramID, 10),
			mapping.SourceProgramTitle,
			mapping.SourcePlanTeamIDs,
			mapping.SourcePlanTeamTitles,
			mapping.TargetPlanID,
			mapping.TargetTitle,
			mapping.TargetProgramID,
			mapping.TargetProgramTitle,
			mapping.TargetPlanTeamIDs,
			mapping.TargetPlanTeamTitles,
			mapping.Decision,
			mapping.ConflictReason,
		})
	}
	return rows
}

func sourceTeamRows(teams []TeamDTO, plans []PlanDTO) [][]string {
	return teamRowsWithPlanUsage(teams, plans)
}

func destinationTeamRows(teams []TeamDTO, plans []PlanDTO) [][]string {
	return teamRowsWithPlanUsage(teams, plans)
}

func teamRowsWithPlanUsage(teams []TeamDTO, plans []PlanDTO) [][]string {
	planIDsByTeamID := map[int64][]string{}
	planTitlesByTeamID := map[int64][]string{}
	for _, plan := range plans {
		planID := strconv.FormatInt(plan.ID, 10)
		for _, teamID := range plan.PlanTeams {
			planIDsByTeamID[teamID] = append(planIDsByTeamID[teamID], planID)
			planTitlesByTeamID[teamID] = append(planTitlesByTeamID[teamID], plan.Title)
		}
	}

	rows := make([][]string, 0, len(teams))
	for _, team := range teams {
		rows = append(rows, []string{
			strconv.FormatInt(team.ID, 10),
			team.Title,
			strconv.FormatBool(team.Shareable),
			strings.Join(planIDsByTeamID[team.ID], ","),
			strings.Join(planTitlesByTeamID[team.ID], ","),
		})
	}
	return rows
}

func teamMappingRows(mappings []TeamMapping) [][]string {
	rows := make([][]string, 0, len(mappings))
	for _, mapping := range mappings {
		rows = append(rows, []string{
			strconv.FormatInt(mapping.SourceTeamID, 10),
			mapping.SourceTitle,
			strconv.FormatBool(mapping.SourceShareable),
			mapping.TargetTeamID,
			mapping.TargetTitle,
			mapping.Decision,
			mapping.ConflictReason,
		})
	}
	return rows
}

func sourceMembershipRows(resources []ResourceDTO, persons []PersonDTO, teams []TeamDTO) [][]string {
	personByID := map[int64]PersonDTO{}
	for _, person := range persons {
		personByID[person.ID] = person
	}
	teamTitles := teamTitlesByID(teams)
	rows := make([][]string, 0, len(resources))
	for _, resource := range resources {
		personID, email, _ := resourcePersonDetails(resource, personByID)
		rows = append(rows, []string{
			strconv.FormatInt(resource.ID, 10),
			strconv.FormatInt(resource.TeamID, 10),
			teamTitles[resource.TeamID],
			strconv.FormatInt(personID, 10),
			email,
			strconv.FormatFloat(defaultWeeklyHours(resource.WeeklyHours), 'f', -1, 64),
		})
	}
	return rows
}

func destinationMembershipRows(resources []ResourceDTO, persons []PersonDTO, teams []TeamDTO) [][]string {
	personByID := map[int64]PersonDTO{}
	for _, person := range persons {
		personByID[person.ID] = person
	}
	teamTitles := teamTitlesByID(teams)
	rows := make([][]string, 0, len(resources))
	for _, resource := range resources {
		personID, email, userID := resourcePersonDetails(resource, personByID)
		rows = append(rows, []string{
			strconv.FormatInt(resource.ID, 10),
			strconv.FormatInt(resource.TeamID, 10),
			teamTitles[resource.TeamID],
			strconv.FormatInt(personID, 10),
			email,
			userID,
			strconv.FormatFloat(defaultWeeklyHours(resource.WeeklyHours), 'f', -1, 64),
		})
	}
	return rows
}

func membershipMappingRows(plans []ResourcePlan) [][]string {
	rows := make([][]string, 0, len(plans))
	for _, plan := range plans {
		rows = append(rows, []string{
			strconv.FormatInt(plan.SourceResourceID, 10),
			strconv.FormatInt(plan.SourceTeamID, 10),
			plan.SourceTeamName,
			strconv.FormatInt(plan.SourcePersonID, 10),
			plan.SourceEmail,
			plan.TargetEmail,
			plan.TargetTeamID,
			plan.TargetTeamName,
			plan.TargetUserID,
			strconv.FormatFloat(plan.WeeklyHours, 'f', -1, 64),
			plan.Status,
			plan.Reason,
		})
	}
	return rows
}

func programTitlesByID(programs []ProgramDTO) map[int64]string {
	titles := map[int64]string{}
	for _, program := range programs {
		titles[program.ID] = program.Title
	}
	return titles
}

func teamTitlesByID(teams []TeamDTO) map[int64]string {
	titles := map[int64]string{}
	for _, team := range teams {
		titles[team.ID] = team.Title
	}
	return titles
}

func joinInt64s(values []int64) string {
	if len(values) == 0 {
		return ""
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, strconv.FormatInt(value, 10))
	}
	return strings.Join(out, ";")
}

func joinMappedTitles(ids []int64, titles map[int64]string) string {
	if len(ids) == 0 {
		return ""
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, titles[id])
	}
	return strings.Join(out, ";")
}

func resourcePersonDetails(resource ResourceDTO, persons map[int64]PersonDTO) (int64, string, string) {
	personID := int64(0)
	var person *PersonDTO
	if resource.Person != nil {
		personID = resource.Person.ID
		person = resource.Person
	}
	if stored, ok := persons[personID]; ok {
		person = &stored
	}
	if person == nil || person.JiraUser == nil {
		return personID, "", ""
	}
	return personID, strings.ToLower(strings.TrimSpace(person.JiraUser.Email)), person.JiraUser.JiraUserID
}

func defaultWeeklyHours(hours float64) float64 {
	if hours == 0 {
		return 40
	}
	return hours
}

func exportIssuesWithTeamsField(cfg Config, client *jiraClient, sourceTeams []TeamDTO, progress *progressTracker) (*TeamsFieldSelection, []IssueTeamRow, string, []Finding) {
	fields, err := client.ListFields()
	if err != nil {
		return nil, nil, "", []Finding{newFinding(SeverityWarning, "teams_field_discovery_failed", fmt.Sprintf("Could not load Jira fields: %v", err))}
	}

	selection, findings := selectTeamsField(fields)
	if selection == nil {
		return nil, nil, "", findings
	}

	jql := fmt.Sprintf(`"%s" is not EMPTY`, selection.Field.Name)
	issues, err := client.SearchIssues(jql, []string{"summary", "project", selection.Field.ID}, func(current, total int) {
		if progress != nil {
			progress.UpdateCount(current, total)
		}
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "teams_field_issue_search_failed", fmt.Sprintf("Could not search issues for teams field %s: %v", selection.Field.ID, err)))
		return selection, nil, "", findings
	}

	rows := buildIssueTeamRows(issues, selection.Field, sourceTeams)
	if len(rows) == 0 {
		findings = append(findings, newFinding(SeverityInfo, "teams_field_no_issues", fmt.Sprintf("No issues found with a value for %s", selection.Field.Name)))
		return selection, rows, "", findings
	}

	path, err := writeIssueTeamExport(cfg, selection.Field, rows)
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "teams_field_issue_export_failed", err.Error()))
		return selection, rows, "", findings
	}

	findings = append(findings, newFinding(SeverityInfo, "teams_field_issue_exported", fmt.Sprintf("Exported %d issues with a value for %s to %s", len(rows), selection.Field.Name, path)))
	return selection, rows, path, findings
}

func selectTeamsField(fields []JiraField) (*TeamsFieldSelection, []Finding) {
	candidates := make([]JiraField, 0)
	for _, field := range fields {
		if looksLikeTeamsField(field) {
			candidates = append(candidates, field)
		}
	}
	if len(candidates) == 0 {
		return nil, []Finding{newFinding(SeverityWarning, "teams_field_missing", "Could not find a Jira issue field that looks like the Teams field")}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return scoreTeamsFieldCandidate(candidates[i]) > scoreTeamsFieldCandidate(candidates[j])
	})

	names := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		names = append(names, fmt.Sprintf("%s (%s)", candidate.Name, candidate.ID))
	}

	selected := candidates[0]
	selection := &TeamsFieldSelection{
		Field:      selected,
		Decision:   "selected",
		Candidates: names,
	}

	var findings []Finding
	if len(candidates) > 1 {
		findings = append(findings, newFinding(SeverityInfo, "teams_field_multiple_candidates", fmt.Sprintf("Multiple Teams-like issue fields found; selected %s (%s)", selected.Name, selected.ID)))
	}
	return selection, findings
}

func looksLikeTeamsField(field JiraField) bool {
	name := strings.ToLower(strings.TrimSpace(field.Name))
	if field.ID == "" || !field.Custom {
		return false
	}
	if field.Schema != nil {
		custom := strings.ToLower(field.Schema.Custom)
		if strings.Contains(custom, "portfolio") && strings.Contains(custom, "team") {
			return true
		}
		if strings.Contains(custom, "plans") && strings.Contains(custom, "team") {
			return true
		}
	}
	return name == "team" || name == "teams" || strings.Contains(name, "team")
}

func scoreTeamsFieldCandidate(field JiraField) int {
	score := 0
	name := strings.ToLower(strings.TrimSpace(field.Name))
	switch name {
	case "team":
		score += 100
	case "teams":
		score += 95
	}
	if strings.Contains(name, "team") {
		score += 20
	}
	if field.Schema != nil {
		custom := strings.ToLower(field.Schema.Custom)
		if strings.Contains(custom, "portfolio") && strings.Contains(custom, "team") {
			score += 80
		}
		if strings.Contains(custom, "plans") && strings.Contains(custom, "team") {
			score += 70
		}
		if field.Schema.Type == "array" {
			score += 10
		}
	}
	return score
}

func buildIssueTeamRows(issues []JiraIssue, field JiraField, sourceTeams []TeamDTO) []IssueTeamRow {
	sourceTeamNames := map[string]string{}
	for _, team := range sourceTeams {
		sourceTeamNames[strconv.FormatInt(team.ID, 10)] = team.Title
	}

	rows := make([]IssueTeamRow, 0, len(issues))
	for _, issue := range issues {
		raw := issue.Fields[field.ID]
		teamIDs := extractTeamFieldIDs(raw)
		if len(teamIDs) == 0 {
			continue
		}
		names := make([]string, 0, len(teamIDs))
		for _, id := range teamIDs {
			if name, ok := sourceTeamNames[id]; ok {
				names = append(names, name)
			}
		}
		projectKey := ""
		if project, ok := issue.Fields["project"].(map[string]any); ok {
			if key, ok := project["key"].(string); ok {
				projectKey = key
			}
		}
		summary := ""
		if rawSummary, ok := issue.Fields["summary"].(string); ok {
			summary = rawSummary
		}
		rows = append(rows, IssueTeamRow{
			IssueKey:        issue.Key,
			ProjectKey:      projectKey,
			Summary:         summary,
			TeamsFieldID:    field.ID,
			TeamsFieldName:  field.Name,
			SourceTeamIDs:   strings.Join(teamIDs, ","),
			SourceTeamNames: strings.Join(names, ","),
		})
	}
	return rows
}

func extractTeamFieldIDs(raw any) []string {
	ids := map[string]struct{}{}
	var collect func(any)
	collect = func(value any) {
		switch v := value.(type) {
		case string:
			if trimmed := strings.TrimSpace(v); trimmed != "" {
				ids[trimmed] = struct{}{}
			}
		case float64:
			ids[strconv.FormatInt(int64(v), 10)] = struct{}{}
		case []any:
			for _, item := range v {
				collect(item)
			}
		case map[string]any:
			for _, key := range []string{"id", "teamId", "value"} {
				if nested, ok := v[key]; ok {
					collect(nested)
				}
			}
		}
	}
	collect(raw)

	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func writeIssueTeamExport(cfg Config, field JiraField, rows []IssueTeamRow) (string, error) {
	if err := ensureOutputDir(cfg.OutputDir); err != nil {
		return "", err
	}
	path := filepath.Join(cfg.OutputDir, "issues-with-teams.pre-migration.csv")
	file, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"issueKey", "projectKey", "summary", "teamsFieldId", "teamsFieldName", "sourceTeamIds", "sourceTeamNames"}); err != nil {
		return "", err
	}
	for _, row := range rows {
		if err := writer.Write([]string{row.IssueKey, row.ProjectKey, row.Summary, row.TeamsFieldID, row.TeamsFieldName, row.SourceTeamIDs, row.SourceTeamNames}); err != nil {
			return "", err
		}
	}
	writer.Flush()
	return path, writer.Error()
}
