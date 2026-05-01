package app

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	postMigrationIssueApplyWorkers         = 8
	postMigrationIssueApplyFallbackWorkers = 3
	postMigrationIssueApplyMaxWorkers      = 40
)

type migrationState struct {
	IdentityMappings          IdentityMapping
	SourcePrograms            []ProgramDTO
	TargetPrograms            []ProgramDTO
	ProgramMappings           []ProgramMapping
	SourcePlans               []PlanDTO
	TargetPlans               []PlanDTO
	PlanMappings              []PlanMapping
	SourceTeams               []TeamDTO
	SourcePersons             []PersonDTO
	SourceResources           []ResourceDTO
	TargetTeams               []TeamDTO
	TargetPersons             []PersonDTO
	TargetResources           []ResourceDTO
	TeamMappings              []TeamMapping
	ResourcePlans             []ResourcePlan
	TeamsField                *TeamsFieldSelection
	IssueTeamRows             []IssueTeamRow
	ParentLinkRows            []ParentLinkRow
	TargetParentLinkField     *ParentLinkFieldRow
	FilterTeamClauseRows      []FilterTeamClauseRow
	TargetIssueSnapshots      []TargetIssueSnapshotRow
	IssueComparisons          []PostMigrationIssueComparisonRow
	IssueUpdateResults        []PostMigrationIssueResultRow
	TargetParentLinkSnapshots []TargetParentLinkSnapshotRow
	ParentLinkComparisons     []PostMigrationParentLinkComparisonRow
	ParentLinkUpdateResults   []PostMigrationParentLinkResultRow
	TargetFilters             []JiraFilter
	TargetFilterSnapshots     []TargetFilterSnapshotRow
	FilterTargetMatches       []PostMigrationFilterMatchRow
	FilterComparisons         []PostMigrationFilterComparisonRow
	FilterUpdateResults       []PostMigrationFilterResultRow
	IssueExportPath           string
	IssueImportExportPath     string
	MembershipExportPath      string
	FilterScanExportPath      string
	Artifacts                 []Artifact
}

func executeMigration(cfg Config, apply bool) (migrationState, []Finding, []Action) {
	state, findings := loadMigrationState(cfg)
	return executeMigrationWithState(cfg, apply, state, findings)
}

func executeMigrationWithState(cfg Config, apply bool, state migrationState, findings []Finding) (migrationState, []Finding, []Action) {
	if hasErrors(findings) {
		return state, findings, nil
	}

	actions := []Action{}
	if !runsPostMigratePhase(cfg.Command, cfg.Phase) {
		actions = planTeamActions(state)
		resourceActions, resourceFindings := planResourceActions(state)
		actions = append(actions, resourceActions...)
		findings = append(findings, resourceFindings...)
	}
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

	if runsPostMigratePhase(cfg.Command, cfg.Phase) {
		if needsPostMigrationTargetArtifactsPreparation(cfg, state) {
			progress := newProgressTracker(0)
			findings = append(findings, preparePostMigrationTargetArtifacts(cfg, &state, progress)...)
			progress.Finish()
		}
		execActions, execFindings := applyPostMigrationCorrections(cfg, targetClient, &state)
		actions = append(actions, execActions...)
		findings = append(findings, execFindings...)
		return state, findings, actions
	}

	execActions, execFindings := applyMigration(targetClient, &state)
	actions = append(actions, execActions...)
	findings = append(findings, execFindings...)
	if runsMigratePhase(cfg.Command, cfg.Phase) {
		exportPath, err := writeTeamIDMappingExport(cfg, state.TeamMappings)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "migration_team_id_mapping_export_failed", err.Error()))
		} else if exportPath != "" {
			artifact := Artifact{
				Key:   "migration_team_id_mapping",
				Label: "Migration team ID mapping",
				Path:  exportPath,
				Count: len(state.TeamMappings),
			}
			state.Artifacts = replaceArtifact(state.Artifacts, artifact)
			actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
			findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
		}
		prepFindings, prepActions := preparePostMigrationCorrectionArtifacts(cfg, &state)
		findings = append(findings, prepFindings...)
		actions = append(actions, prepActions...)
	}
	return state, findings, actions
}

func preparePostMigrationCorrectionArtifacts(cfg Config, state *migrationState) ([]Finding, []Action) {
	var findings []Finding
	var actions []Action

	if issueTeamCorrectionsInScope(cfg) {
		if len(state.IssueTeamRows) == 0 {
			if issuePath, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-teams.pre-migration.csv"); ok {
				var err error
				issuePath, err = cleanInputFilePath("issue/team export", issuePath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_issue_mapping_input_load_failed", err.Error()))
					return findings, actions
				}
				rows, err := loadIssueTeamRowsFromExport(issuePath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_issue_mapping_input_load_failed", fmt.Sprintf("Could not load issue/team export %s: %v", issuePath, err)))
				} else {
					state.IssueTeamRows = rows
					state.IssueExportPath = issuePath
				}
			}
		}
		if len(state.IssueTeamRows) > 0 {
			rows := issueTeamRowsInProjectScope(state.IssueTeamRows, cfg.IssueProjectScope)
			exportPath, err := writePostMigrationIssueTeamExport(cfg, rows, state.TeamMappings)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "migration_issue_mapping_export_failed", err.Error()))
			} else if exportPath != "" {
				artifact := Artifact{
					Key:   "post_migrate_issue_team_mapping",
					Label: "Post-migration issue/team mapping",
					Path:  exportPath,
					Count: len(rows),
				}
				state.Artifacts = replaceArtifact(state.Artifacts, artifact)
				actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
				findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
			}
		}
	}

	if cfg.ParentLinkInScope {
		if len(state.ParentLinkRows) == 0 {
			if parentPath, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-parent-link.pre-migration.csv"); ok {
				var err error
				parentPath, err = cleanInputFilePath("parent link export", parentPath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_parent_link_input_load_failed", err.Error()))
					return findings, actions
				}
				rows, err := loadParentLinkRowsFromExport(parentPath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_parent_link_input_load_failed", fmt.Sprintf("Could not load parent link export %s: %v", parentPath, err)))
				} else {
					state.ParentLinkRows = rows
				}
			} else {
				findings = append(findings, newFinding(SeverityWarning, "migration_parent_link_input_missing", "Could not prepare post-migration parent-link mapping because no pre-migrate parent-link export was found. Run pre-migrate first."))
			}
		}
		if len(state.ParentLinkRows) > 0 {
			rows := parentLinkRowsInProjectScope(state.ParentLinkRows, cfg.IssueProjectScope)
			exportPath, err := writePostMigrationParentLinkExport(cfg, rows)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "migration_parent_link_export_failed", err.Error()))
			} else if exportPath != "" {
				artifact := Artifact{
					Key:   "post_migrate_parent_link_mapping",
					Label: "Post-migration parent-link mapping",
					Path:  exportPath,
					Count: len(rows),
				}
				state.Artifacts = replaceArtifact(state.Artifacts, artifact)
				actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
				findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
			}
		}
	}

	if cfg.FilterTeamIDsInScope {
		if len(state.FilterTeamClauseRows) == 0 {
			if filterPath, ok := latestOutputFamilyPath(cfg.OutputDir, "filters-with-team-clauses.pre-migration.csv"); ok {
				var err error
				filterPath, err = cleanInputFilePath("filter export", filterPath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_filter_mapping_input_load_failed", err.Error()))
					return findings, actions
				}
				rows, err := loadFilterTeamClauseRowsFromExport(filterPath)
				if err != nil {
					findings = append(findings, newFinding(SeverityWarning, "migration_filter_mapping_input_load_failed", fmt.Sprintf("Could not load filter export %s: %v", filterPath, err)))
				} else {
					state.FilterTeamClauseRows = rows
					state.FilterScanExportPath = filterPath
				}
			} else {
				findings = append(findings, newFinding(SeverityWarning, "migration_filter_mapping_input_missing", "Could not prepare post-migration filter mapping because no pre-migrate filter export was found. Run pre-migrate first."))
			}
		}
		if len(state.FilterTeamClauseRows) > 0 {
			exportPath, rowCount, err := writePostMigrationFilterTeamExport(cfg, state.FilterTeamClauseRows, state.TeamMappings)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "migration_filter_mapping_export_failed", err.Error()))
			} else if exportPath != "" {
				artifact := Artifact{
					Key:   "post_migrate_filter_team_mapping",
					Label: "Post-migration filter/team mapping",
					Path:  exportPath,
					Count: rowCount,
				}
				state.Artifacts = replaceArtifact(state.Artifacts, artifact)
				actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
				findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
			}
		}
	}

	return findings, actions
}

func preparePostMigrationTargetArtifacts(cfg Config, state *migrationState, progress *progressTracker) []Finding {
	var findings []Finding
	if issueTeamCorrectionsInScope(cfg) {
		findings = append(findings, preparePostMigrationTargetIssueArtifacts(cfg, state, progress)...)
	}
	findings = append(findings, preparePostMigrationTargetParentLinkArtifacts(cfg, state, progress)...)
	return append(findings, preparePostMigrationTargetFilterArtifacts(cfg, state, progress)...)
}

func needsPostMigrationTargetArtifactsPreparation(cfg Config, state migrationState) bool {
	if issueTeamCorrectionsInScope(cfg) && len(state.IssueTeamRows) > 0 && state.TargetIssueSnapshots == nil && state.IssueComparisons == nil {
		return true
	}
	if cfg.ParentLinkInScope && len(state.ParentLinkRows) > 0 && state.TargetParentLinkSnapshots == nil && state.ParentLinkComparisons == nil {
		return true
	}
	if cfg.FilterTeamIDsInScope && len(state.FilterTeamClauseRows) > 0 && state.FilterComparisons == nil {
		return true
	}
	return false
}

func preparePostMigrationTargetIssueArtifacts(cfg Config, state *migrationState, progress *progressTracker) []Finding {
	sourceRows := issueTeamRowsInProjectScope(state.IssueTeamRows, cfg.IssueProjectScope)
	if len(sourceRows) == 0 {
		return nil
	}

	state.TargetIssueSnapshots = nil
	state.IssueComparisons = nil

	progressStart(progress, "Resolving target Teams field for issue comparison")
	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		progressEnd(progress)
		return []Finding{newFinding(SeverityWarning, "post_migrate_target_issue_client", fmt.Sprintf("Could not create target Jira client for issue lookup: %v", err))}
	}

	fields, err := targetClient.ListFields()
	if err != nil {
		progressEnd(progress)
		return []Finding{newFinding(SeverityWarning, "post_migrate_target_issue_field_lookup_failed", fmt.Sprintf("Could not load target Jira fields for issue comparison: %v", err))}
	}

	selection, selectionFindings := selectTeamsField(fields)
	findings := append([]Finding{}, selectionFindings...)
	if selection == nil {
		progressEnd(progress)
		return append(findings, newFinding(SeverityWarning, "post_migrate_target_issue_field_missing", "Could not resolve the target Jira Teams field for issue comparison"))
	}
	progressEnd(progress)

	findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_issue_lookup_started", fmt.Sprintf("Resolving target issues for %d issue/team rows using Teams field %s (%s)", len(sourceRows), selection.Field.Name, selection.Field.ID)))

	issueKeys := uniqueIssueKeysFromTeamRows(sourceRows)
	progressStartCount(progress, "Fetching target issues for team comparison")
	fetchedIssues, err := targetClient.SearchIssuesByKeys(issueKeys, cfg.IssueProjectScope, []string{"summary", "project", "projectType", selection.Field.ID}, func(current, total int) {
		progressUpdateCount(progress, current, total)
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_issue_fetch_failed", fmt.Sprintf("Could not fetch target issues for team comparison: %v", err)))
	}
	progressEnd(progress)
	for _, issueKey := range missingIssueKeys(issueKeys, fetchedIssues) {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_issue_fetch_failed", fmt.Sprintf("Could not fetch target issue %s: issue was not returned by target Jira search", issueKey)))
	}

	snapshotRows := buildTargetIssueSnapshotRows(selection.Field.ID, fetchedIssues)
	state.TargetIssueSnapshots = snapshotRows
	if path, err := writeTargetIssueSnapshotExport(cfg, snapshotRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_issue_snapshot_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_target_issue_snapshot",
			Label: "Target issue snapshot",
			Path:  path,
			Count: len(snapshotRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_issue_snapshot_generated", fmt.Sprintf("Generated target issue snapshot: %s", path)))
	}

	comparisonRows := buildPostMigrationIssueComparisonRows(sourceRows, selection.Field.ID, fetchedIssues, state.TeamMappings)
	state.IssueComparisons = comparisonRows
	if path, err := writePostMigrationIssueComparisonExport(cfg, comparisonRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_issue_comparison_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_issue_comparison",
			Label: "Post-migration issue comparison",
			Path:  path,
			Count: len(comparisonRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_issue_comparison_generated", fmt.Sprintf("Generated issue comparison export: %s", path)))
	}

	return findings
}

func preparePostMigrationTargetParentLinkArtifacts(cfg Config, state *migrationState, progress *progressTracker) []Finding {
	sourceRows := parentLinkRowsInProjectScope(state.ParentLinkRows, cfg.IssueProjectScope)
	if !cfg.ParentLinkInScope || len(sourceRows) == 0 {
		return nil
	}

	state.TargetParentLinkSnapshots = nil
	state.ParentLinkComparisons = nil

	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		return []Finding{newFinding(SeverityWarning, "post_migrate_target_parent_link_client", fmt.Sprintf("Could not create target Jira client for Parent Link lookup: %v", err))}
	}

	field, findings := loadOrResolveTargetParentLinkField(cfg, state, targetClient, progress)
	if field == nil {
		return findings
	}

	findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_parent_link_lookup_started", fmt.Sprintf("Resolving target Parent Link state for %d source rows using field %s (%s)", len(sourceRows), field.Name, field.ID)))

	childIssueKeys := uniqueChildIssueKeysFromParentLinkRows(sourceRows)
	progressStartCount(progress, "Fetching target child issues for Parent Link comparison")
	childIssues, err := targetClient.SearchIssuesByKeys(childIssueKeys, cfg.IssueProjectScope, []string{"summary", "project", "projectType", field.ID}, func(current, total int) {
		progressUpdateCount(progress, current, total)
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_link_child_fetch_failed", fmt.Sprintf("Could not fetch target child issues for Parent Link comparison: %v", err)))
	}
	progressEnd(progress)
	for _, issueKey := range missingIssueKeys(childIssueKeys, childIssues) {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_link_child_fetch_failed", fmt.Sprintf("Could not fetch target child issue %s: issue was not returned by target Jira search", issueKey)))
	}

	parentIssueKeys := uniqueParentIssueKeysFromParentLinkRows(sourceRows)
	progressStartCount(progress, "Fetching target parent issues for Parent Link comparison")
	targetParents, err := targetClient.SearchIssuesByKeys(parentIssueKeys, cfg.IssueProjectScope, []string{"summary", "project"}, func(current, total int) {
		progressUpdateCount(progress, current, total)
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_lookup_failed", fmt.Sprintf("Could not fetch target parent issues for Parent Link comparison: %v", err)))
	}
	progressEnd(progress)
	for _, parentKey := range missingIssueKeys(parentIssueKeys, targetParents) {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_lookup_failed", fmt.Sprintf("Could not fetch target parent issue %s: issue was not returned by target Jira search", parentKey)))
	}

	currentParentCache := map[string]JiraIssue{}
	snapshotRows := buildTargetParentLinkSnapshotRows(targetClient, field.ID, childIssues, currentParentCache)
	state.TargetParentLinkSnapshots = snapshotRows
	if path, err := writeTargetParentLinkSnapshotExport(cfg, snapshotRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_link_snapshot_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_target_parent_link_snapshot",
			Label: "Target Parent Link snapshot",
			Path:  path,
			Count: len(snapshotRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_parent_link_snapshot_generated", fmt.Sprintf("Generated target Parent Link snapshot: %s", path)))
	}

	comparisonRows, comparisonFindings := buildPostMigrationParentLinkComparisonRows(targetClient, sourceRows, field.ID, childIssues, targetParents, currentParentCache)
	findings = append(findings, comparisonFindings...)
	state.ParentLinkComparisons = comparisonRows
	if path, err := writePostMigrationParentLinkComparisonExport(cfg, comparisonRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_comparison_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_parent_link_comparison",
			Label: "Post-migration parent-link comparison",
			Path:  path,
			Count: len(comparisonRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_parent_link_comparison_generated", fmt.Sprintf("Generated parent-link comparison export: %s", path)))
	}

	return findings
}

func loadOrResolveTargetParentLinkField(cfg Config, state *migrationState, targetClient *jiraClient, progress *progressTracker) (*JiraField, []Finding) {
	if state.TargetParentLinkField != nil && strings.TrimSpace(state.TargetParentLinkField.FieldID) != "" {
		return parentLinkFieldRowToJiraField(*state.TargetParentLinkField), nil
	}

	if path, ok := latestOutputFamilyPath(cfg.OutputDir, "target-parent-link-field.pre-migration.csv"); ok {
		progressStart(progress, "Loading pre-migration target Parent Link field")
		cleanPath, cleanErr := cleanInputFilePath("target parent link field export", path)
		if cleanErr != nil {
			progressEnd(progress)
			return nil, []Finding{newFinding(SeverityWarning, "post_migrate_target_parent_link_field_reuse_failed", cleanErr.Error())}
		}
		path = cleanPath
		row, err := loadParentLinkFieldFromExport(path)
		progressEnd(progress)
		if err != nil {
			return nil, []Finding{newFinding(SeverityWarning, "post_migrate_target_parent_link_field_reuse_failed", fmt.Sprintf("Could not reuse target Parent Link field export %s: %v", path, err))}
		}
		if row != nil && strings.TrimSpace(row.FieldID) != "" {
			state.TargetParentLinkField = row
			return parentLinkFieldRowToJiraField(*row), []Finding{newFinding(SeverityInfo, "post_migrate_target_parent_link_field_reused", fmt.Sprintf("Reused target Parent Link field export: %s", path))}
		}
	}

	progressStart(progress, "Resolving target Parent Link field")
	fields, err := targetClient.ListFields()
	if err != nil {
		progressEnd(progress)
		return nil, []Finding{newFinding(SeverityWarning, "post_migrate_target_parent_link_field_lookup_failed", fmt.Sprintf("Could not load target Jira fields for Parent Link comparison: %v", err))}
	}

	field, selectionFindings := selectParentLinkField(fields)
	findings := append([]Finding{}, selectionFindings...)
	if field == nil {
		progressEnd(progress)
		return nil, append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_link_field_missing", "Could not resolve the target Jira Parent Link field for comparison"))
	}
	progressEnd(progress)
	state.TargetParentLinkField = parentLinkFieldRowFromJiraField(*field)
	return field, findings
}

func resolveTargetParentLinkFieldForExport(cfg Config, targetClient *jiraClient) (*ParentLinkFieldRow, string, []Finding) {
	fields, err := targetClient.ListFields()
	if err != nil {
		return nil, "", []Finding{newFinding(SeverityWarning, "target_parent_link_field_lookup_failed", fmt.Sprintf("Could not load target Jira fields for Parent Link field export: %v", err))}
	}

	field, findings := selectParentLinkField(fields)
	if field == nil {
		return nil, "", append(findings, newFinding(SeverityWarning, "target_parent_link_field_missing", "Could not resolve the target Jira Parent Link field for pre-migration export"))
	}

	row := parentLinkFieldRowFromJiraField(*field)
	path, err := writeParentLinkFieldExport(cfg, "target-parent-link-field.pre-migration.csv", *row)
	if err != nil {
		return row, "", append(findings, newFinding(SeverityWarning, "target_parent_link_field_export_failed", err.Error()))
	}
	return row, path, append(findings, newFinding(SeverityInfo, "target_parent_link_field_exported", fmt.Sprintf("Exported target Parent Link field %s (%s) to %s", field.Name, field.ID, path)))
}

func parentLinkFieldRowFromJiraField(field JiraField) *ParentLinkFieldRow {
	row := &ParentLinkFieldRow{
		FieldID:   field.ID,
		FieldName: field.Name,
	}
	if field.Schema != nil {
		row.SchemaCustom = field.Schema.Custom
		row.SchemaType = field.Schema.Type
	}
	return row
}

func parentLinkFieldRowToJiraField(row ParentLinkFieldRow) *JiraField {
	field := &JiraField{
		ID:     row.FieldID,
		Name:   nonEmptyString(row.FieldName, "Parent Link"),
		Custom: true,
	}
	if row.SchemaCustom != "" || row.SchemaType != "" {
		field.Schema = &JiraFieldSchema{Custom: row.SchemaCustom, Type: row.SchemaType}
	}
	return field
}

func preparePreMigrationFilterTargetMatchArtifacts(cfg Config, state *migrationState, progress *progressTracker) []Finding {
	if !cfg.FilterTeamIDsInScope || len(state.FilterTeamClauseRows) == 0 {
		return nil
	}

	progressStart(progress, "Resolving target Teams field for filter mapping")
	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		progressEnd(progress)
		return []Finding{newFinding(SeverityWarning, "pre_migrate_target_filter_client", fmt.Sprintf("Could not create target Jira client for filter lookup: %v", err))}
	}

	teamFieldID, fieldLabel, err := resolveTeamsCustomFieldNumericID(targetClient)
	if err != nil {
		progressEnd(progress)
		return []Finding{newFinding(SeverityWarning, "pre_migrate_target_filter_field", fmt.Sprintf("Could not resolve target Teams field ID for filter lookup: %v", err))}
	}
	progressEnd(progress)

	findings, candidatesBySource, targetFilterIDs, matchMethodsBySource := resolveTargetFilterCandidates(targetClient, teamFieldID, fieldLabel, state.FilterTeamClauseRows, progress)
	if len(candidatesBySource) == 0 {
		return findings
	}

	fetchedFilters, fetchFindings := fetchTargetFiltersByID(targetClient, targetFilterIDs, state, progress)
	findings = append(findings, fetchFindings...)

	matchRows, _ := buildPostMigrationFilterMatchAndComparisonRows(state.FilterTeamClauseRows, candidatesBySource, fetchedFilters, nil, matchMethodsBySource)
	state.FilterTargetMatches = matchRows

	if path, err := writePreMigrationFilterTargetMatchExport(cfg, matchRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "pre_migrate_filter_target_match_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "pre_migrate_filter_target_match",
			Label: "Pre-migration filter target match",
			Path:  path,
			Count: len(matchRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "pre_migrate_filter_target_match_generated", fmt.Sprintf("Generated filter target match export: %s", path)))
	}

	return findings
}

func preparePostMigrationTargetFilterArtifacts(cfg Config, state *migrationState, progress *progressTracker) []Finding {
	if !cfg.FilterTeamIDsInScope || len(state.FilterTeamClauseRows) == 0 {
		return nil
	}

	preparedMatchRows := state.FilterTargetMatches
	state.TargetFilters = nil
	state.TargetFilterSnapshots = nil
	state.FilterComparisons = nil

	targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		return []Finding{newFinding(SeverityWarning, "post_migrate_target_filter_client", fmt.Sprintf("Could not create target Jira client for filter lookup: %v", err))}
	}

	candidatesBySource := map[string][]JiraFilter{}
	targetFilterIDs := map[string]struct{}{}
	matchMethodsBySource := map[string]string{}
	findings := []Finding{}
	if len(preparedMatchRows) > 0 {
		candidatesBySource, targetFilterIDs = targetFilterCandidatesFromMatchRows(preparedMatchRows)
		matchMethodsBySource = filterMatchMethodsFromRows(preparedMatchRows)
		state.FilterTargetMatches = preparedMatchRows
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_target_match_reused_for_comparison", fmt.Sprintf("Using %d prepared filter target match rows for post-migration comparison", len(preparedMatchRows))))
	} else {
		progressStart(progress, "Resolving target Teams field for filter comparison")
		teamFieldID, fieldLabel, err := resolveTeamsCustomFieldNumericID(targetClient)
		if err != nil {
			progressEnd(progress)
			return []Finding{newFinding(SeverityWarning, "post_migrate_target_filter_field", fmt.Sprintf("Could not resolve target Teams field ID for filter lookup: %v", err))}
		}
		progressEnd(progress)

		var lookupFindings []Finding
		lookupFindings, candidatesBySource, targetFilterIDs, matchMethodsBySource = resolveTargetFilterCandidates(targetClient, teamFieldID, fieldLabel, state.FilterTeamClauseRows, progress)
		findings = append(findings, lookupFindings...)
	}

	fetchedFilters, fetchFindings := fetchTargetFiltersByID(targetClient, targetFilterIDs, state, progress)
	findings = append(findings, fetchFindings...)

	snapshotRows := buildTargetFilterSnapshotRows(fetchedFilters)
	state.TargetFilterSnapshots = snapshotRows
	if path, err := writeTargetFilterSnapshotExport(cfg, snapshotRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_snapshot_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_target_filter_snapshot",
			Label: "Target filter snapshot",
			Path:  path,
			Count: len(snapshotRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_snapshot_generated", fmt.Sprintf("Generated target filter snapshot: %s", path)))
	}

	matchRows, comparisonRows := buildPostMigrationFilterMatchAndComparisonRows(state.FilterTeamClauseRows, candidatesBySource, fetchedFilters, state.TeamMappings, matchMethodsBySource)
	state.FilterTargetMatches = matchRows
	state.FilterComparisons = comparisonRows

	if path, err := writePostMigrationFilterTargetMatchExport(cfg, matchRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_target_match_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_filter_target_match",
			Label: "Post-migration filter target match",
			Path:  path,
			Count: len(matchRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_target_match_generated", fmt.Sprintf("Generated filter target match export: %s", path)))
	}

	if path, err := writePostMigrationFilterComparisonExport(cfg, comparisonRows); err != nil {
		findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_comparison_failed", err.Error()))
	} else if path != "" {
		state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
			Key:   "post_migrate_filter_comparison",
			Label: "Post-migration filter JQL comparison",
			Path:  path,
			Count: len(comparisonRows),
		})
		findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_comparison_generated", fmt.Sprintf("Generated filter JQL comparison export: %s", path)))
	}

	return findings
}

const (
	filterMatchMethodOwnerEmail    = "matched_by_owner_email"
	filterMatchMethodOwner         = "matched_by_owner"
	filterMatchMethodExactName     = "matched_by_exact_name"
	filterMatchMethodExactJQL      = "matched_by_exact_jql"
	filterMatchMethodAmbiguousName = "ambiguous_exact_name"
	filterMatchMethodNotFound      = "not_found"
)

func resolveTargetFilterCandidates(client *jiraClient, teamFieldID, fieldLabel string, sourceRows []FilterTeamClauseRow, progress *progressTracker) ([]Finding, map[string][]JiraFilter, map[string]struct{}, map[string]string) {
	sourceFilters := uniqueSourceTeamIDFilters(sourceRows)
	if len(sourceFilters) == 0 {
		return []Finding{newFinding(SeverityInfo, "post_migrate_target_filter_lookup_skipped", "No source filter rows using team IDs were found for target filter lookup")}, nil, nil, nil
	}

	findings := []Finding{
		newFinding(SeverityInfo, "post_migrate_target_filter_lookup_started", fmt.Sprintf("Resolving target filters for %d source filters using Teams field %s", len(sourceFilters), fieldLabel)),
	}

	candidatesBySource := map[string][]JiraFilter{}
	targetFilterIDs := map[string]struct{}{}
	matchMethodsBySource := map[string]string{}
	var resolveFiltersTask *progressTask
	if progress != nil {
		resolveFiltersTask = progress.BeginTask("Resolving target filters by name and owner")
	}
	for i, sourceFilter := range sourceFilters {
		if resolveFiltersTask != nil {
			resolveFiltersTask.Update(i+1, len(sourceFilters))
			resolveFiltersTask.Detail(fmt.Sprintf("scanning %s", sourceFilter.FilterName))
		}
		candidates, matchMethod, candidateFindings, err := loadTargetFiltersForSourceFilter(client, teamFieldID, sourceFilter, func(scanned, matched int) {
			if resolveFiltersTask != nil {
				resolveFiltersTask.Detail(fmt.Sprintf("scanning %s: %d scanned, %d matched", sourceFilter.FilterName, scanned, matched))
			}
		})
		findings = append(findings, candidateFindings...)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_lookup_failed", fmt.Sprintf("Could not resolve target filter candidates for %q: %v", sourceFilter.FilterName, err)))
			continue
		}
		candidatesBySource[sourceFilter.FilterID] = candidates
		if matchMethod != "" {
			matchMethodsBySource[sourceFilter.FilterID] = matchMethod
		}
		for _, filter := range candidates {
			targetFilterIDs[filter.ID] = struct{}{}
		}
	}
	unresolvedSourceFilters := make([]FilterTeamClauseRow, 0)
	for _, sourceFilter := range sourceFilters {
		if len(candidatesBySource[sourceFilter.FilterID]) == 0 {
			unresolvedSourceFilters = append(unresolvedSourceFilters, sourceFilter)
		}
	}
	if len(unresolvedSourceFilters) > 0 {
		allFilters, allFindings, err := loadAllTargetTeamFilters(client, teamFieldID, progress)
		findings = append(findings, allFindings...)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_jql_fallback_failed", fmt.Sprintf("Could not scan target filters for exact-JQL fallback: %v", err)))
		} else {
			for _, sourceFilter := range unresolvedSourceFilters {
				fallbackFilters := targetFiltersMatchingSourceJQL(allFilters, sourceFilter)
				if len(fallbackFilters) == 0 {
					continue
				}
				candidatesBySource[sourceFilter.FilterID] = fallbackFilters
				matchMethodsBySource[sourceFilter.FilterID] = filterMatchMethodExactJQL
				for _, filter := range fallbackFilters {
					targetFilterIDs[filter.ID] = struct{}{}
				}
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_jql_fallback", fmt.Sprintf("Target filter lookup matched %d candidate(s) for %q by exact JQL after exact name/owner lookup found none", len(fallbackFilters), sourceFilter.FilterName)))
			}
		}
	}
	if resolveFiltersTask != nil {
		resolveFiltersTask.Done()
	}
	for _, sourceFilter := range sourceFilters {
		if matchMethodsBySource[sourceFilter.FilterID] == "" {
			matchMethodsBySource[sourceFilter.FilterID] = filterMatchMethodNotFound
		}
	}
	return findings, candidatesBySource, targetFilterIDs, matchMethodsBySource
}

func targetFilterCandidatesFromMatchRows(rows []PostMigrationFilterMatchRow) (map[string][]JiraFilter, map[string]struct{}) {
	candidatesBySource := map[string][]JiraFilter{}
	targetFilterIDs := map[string]struct{}{}
	for _, row := range rows {
		targetID := strings.TrimSpace(row.TargetFilterID)
		if targetID == "" || (row.Status != "matched" && row.Status != "ambiguous") {
			continue
		}
		filter := JiraFilter{ID: targetID, Name: row.TargetFilterName}
		if strings.TrimSpace(row.TargetOwner) != "" {
			filter.Owner = &JiraFilterUser{DisplayName: row.TargetOwner, Name: row.TargetOwner, Key: row.TargetOwner}
		}
		candidatesBySource[row.SourceFilterID] = append(candidatesBySource[row.SourceFilterID], filter)
		targetFilterIDs[targetID] = struct{}{}
	}
	return candidatesBySource, targetFilterIDs
}

func filterMatchMethodsFromRows(rows []PostMigrationFilterMatchRow) map[string]string {
	methods := map[string]string{}
	for _, row := range rows {
		method := strings.TrimSpace(row.MatchMethod)
		if method == "" {
			method = filterMatchMethodForStatus(row.Status)
		}
		if method == "" {
			continue
		}
		methods[row.SourceFilterID] = method
	}
	return methods
}

func filterMatchMethodForStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "not_found":
		return filterMatchMethodNotFound
	case "ambiguous":
		return filterMatchMethodAmbiguousName
	default:
		return ""
	}
}

func fetchTargetFiltersByID(client *jiraClient, targetFilterIDs map[string]struct{}, state *migrationState, progress *progressTracker) (map[string]JiraFilter, []Finding) {
	findings := []Finding{}
	fetchedFilters := map[string]JiraFilter{}
	filterIDs := make([]string, 0, len(targetFilterIDs))
	for id := range targetFilterIDs {
		filterIDs = append(filterIDs, id)
	}
	sort.Strings(filterIDs)
	progressStartCount(progress, "Fetching matched target filters")
	for i, id := range filterIDs {
		progressUpdateCount(progress, i+1, len(filterIDs))
		filter, err := client.GetFilter(id)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_fetch_failed", fmt.Sprintf("Could not fetch target filter %s: %v", id, err)))
			continue
		}
		fetchedFilters[id] = *filter
		if state != nil {
			state.TargetFilters = append(state.TargetFilters, *filter)
		}
	}
	progressEnd(progress)
	return fetchedFilters, findings
}

func uniqueSourceTeamIDFilters(rows []FilterTeamClauseRow) []FilterTeamClauseRow {
	byFilterID := map[string]FilterTeamClauseRow{}
	for _, row := range rows {
		if row.MatchType != "team_id" {
			continue
		}
		if _, ok := byFilterID[row.FilterID]; ok {
			continue
		}
		byFilterID[row.FilterID] = row
	}

	ids := make([]string, 0, len(byFilterID))
	for id := range byFilterID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]FilterTeamClauseRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, byFilterID[id])
	}
	return out
}

func uniqueIssueKeysFromTeamRows(rows []IssueTeamRow) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.IssueKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func uniqueChildIssueKeysFromParentLinkRows(rows []ParentLinkRow) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.IssueKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func uniqueParentIssueKeysFromParentLinkRows(rows []ParentLinkRow) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.SourceParentIssueKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func missingIssueKeys(keys []string, issues map[string]JiraIssue) []string {
	missing := make([]string, 0)
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		if _, ok := issues[trimmed]; ok {
			continue
		}
		missing = append(missing, trimmed)
	}
	return missing
}

func loadTargetFiltersForSourceFilter(client *jiraClient, teamFieldID string, sourceFilter FilterTeamClauseRow, progress func(scanned, matched int)) ([]JiraFilter, string, []Finding, error) {
	findings := []Finding{}
	if strings.TrimSpace(sourceFilter.OwnerEmail) != "" {
		filters, emailFindings, err := loadTargetFiltersForSourceFilterQuery(client, teamFieldID, sourceFilter, "", strings.TrimSpace(sourceFilter.OwnerEmail), progress)
		findings = append(findings, emailFindings...)
		if err != nil || len(filters) > 0 {
			return filters, filterMatchMethodOwnerEmail, findings, err
		}
	}
	if strings.TrimSpace(sourceFilter.Owner) != "" {
		filters, ownerFindings, err := loadTargetFiltersForSourceFilterQuery(client, teamFieldID, sourceFilter, strings.TrimSpace(sourceFilter.Owner), "", progress)
		findings = append(findings, ownerFindings...)
		if err != nil || len(filters) > 0 {
			return filters, filterMatchMethodOwner, findings, err
		}
	}

	findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_owner_retry", fmt.Sprintf("Owner-scoped target filter lookup found no candidates for %q; retrying by exact name without owner", sourceFilter.FilterName)))
	fallbackFilters, fallbackFindings, fallbackErr := loadTargetFiltersForSourceFilterQuery(client, teamFieldID, sourceFilter, "", "", progress)
	findings = append(findings, fallbackFindings...)
	if fallbackErr != nil {
		return nil, "", findings, fallbackErr
	}
	if len(fallbackFilters) == 0 {
		return nil, filterMatchMethodNotFound, findings, nil
	}
	return fallbackFilters, filterMatchMethodExactName, findings, nil
}

func loadTargetFiltersForSourceFilterQuery(client *jiraClient, teamFieldID string, sourceFilter FilterTeamClauseRow, owner, ownerEmail string, progress func(scanned, matched int)) ([]JiraFilter, []Finding, error) {
	var (
		lastID           int64
		totalScanned     int
		totalParseErrors int
		filters          []JiraFilter
		parseErrors      []teamFilterParseError
	)

	for {
		query := make(url.Values)
		query.Set("enabled", "true")
		query.Set("lastId", strconv.FormatInt(lastID, 10))
		query.Set("limit", strconv.Itoa(teamFilterScriptRunnerPageSize))
		query.Set("teamFieldId", teamFieldID)
		query.Set("filterName", sourceFilter.FilterName)
		if strings.TrimSpace(owner) != "" {
			query.Set("owner", owner)
		}
		if strings.TrimSpace(ownerEmail) != "" {
			query.Set("ownerEmail", ownerEmail)
		}

		body, err := client.doCoreJSON(http.MethodGet, targetTeamFilterScriptRunnerEndpointPath, query, nil)
		if err != nil {
			endpointURL, buildErr := buildURL(client.instanceBaseURL, targetTeamFilterScriptRunnerEndpointPath, query)
			if buildErr != nil {
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("calling target ScriptRunner endpoint %s: %w", endpointURL, err)
		}

		var response teamFilterScriptRunnerResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, nil, fmt.Errorf("parsing target ScriptRunner endpoint response: %w", err)
		}

		for _, result := range response.Results {
			resultOwner := strings.TrimSpace(result.Owner)
			resultOwnerEmail := strings.TrimSpace(result.OwnerEmail)
			filter := JiraFilter{
				ID:   strconv.FormatInt(result.ID, 10),
				Name: result.Name,
				JQL:  result.JQL,
			}
			if resultOwner != "" || resultOwnerEmail != "" {
				filter.Owner = &JiraFilterUser{DisplayName: resultOwner, Name: resultOwner, Key: resultOwner, EmailAddress: resultOwnerEmail}
			}
			if targetFilterMatchesSourceWithOwnerScope(filter, sourceFilter, strings.TrimSpace(owner) != "" || strings.TrimSpace(ownerEmail) != "") {
				filters = append(filters, filter)
			}
		}

		totalScanned += response.Meta.Scanned
		totalParseErrors += response.Meta.ParseErrorCount
		parseErrors = append(parseErrors, response.ParseErrors...)
		if progress != nil {
			progress(totalScanned, len(filters))
		}
		if response.Meta.Scanned == 0 || response.Meta.NextLastID <= lastID || response.Meta.Scanned < teamFilterScriptRunnerPageSize {
			break
		}
		lastID = response.Meta.NextLastID
	}

	deduped := deduplicateFiltersByID(filters)
	findings := []Finding{}
	if totalParseErrors > 0 {
		parseErrorSamples := summarizeTeamFilterParseErrors(parseErrors, 3)
		if parseErrorSamples != "" {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_parse_errors", fmt.Sprintf("Target filter lookup for %q skipped %d filters because their JQL could not be parsed: %s", sourceFilter.FilterName, totalParseErrors, parseErrorSamples)))
		} else {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_parse_errors", fmt.Sprintf("Target filter lookup for %q skipped %d filters because their JQL could not be parsed", sourceFilter.FilterName, totalParseErrors)))
		}
	}
	findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_lookup_summary", fmt.Sprintf("Target filter lookup scanned %d rows and found %d exact candidate filters for %q", totalScanned, len(deduped), sourceFilter.FilterName)))
	return deduped, findings, nil
}

func loadAllTargetTeamFilters(client *jiraClient, teamFieldID string, progress *progressTracker) ([]JiraFilter, []Finding, error) {
	var (
		lastID           int64
		totalScanned     int
		totalParseErrors int
		filters          []JiraFilter
		parseErrors      []teamFilterParseError
		task             *progressTask
	)
	if progress != nil {
		task = progress.BeginTask("Scanning target filters for exact-JQL fallback")
	}
	defer func() {
		if task != nil {
			task.Done()
		}
	}()

	for {
		query := make(url.Values)
		query.Set("enabled", "true")
		query.Set("lastId", strconv.FormatInt(lastID, 10))
		query.Set("limit", strconv.Itoa(teamFilterScriptRunnerPageSize))
		query.Set("teamFieldId", teamFieldID)

		body, err := client.doCoreJSON(http.MethodGet, targetTeamFilterScriptRunnerEndpointPath, query, nil)
		if err != nil {
			endpointURL, buildErr := buildURL(client.instanceBaseURL, targetTeamFilterScriptRunnerEndpointPath, query)
			if buildErr != nil {
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("calling target ScriptRunner endpoint %s: %w", endpointURL, err)
		}

		var response teamFilterScriptRunnerResponse
		if err := json.Unmarshal(body, &response); err != nil {
			return nil, nil, fmt.Errorf("parsing target ScriptRunner endpoint response: %w", err)
		}

		for _, result := range response.Results {
			resultOwner := strings.TrimSpace(result.Owner)
			resultOwnerEmail := strings.TrimSpace(result.OwnerEmail)
			filter := JiraFilter{
				ID:   strconv.FormatInt(result.ID, 10),
				Name: result.Name,
				JQL:  result.JQL,
			}
			if resultOwner != "" || resultOwnerEmail != "" {
				filter.Owner = &JiraFilterUser{DisplayName: resultOwner, Name: resultOwner, Key: resultOwner, EmailAddress: resultOwnerEmail}
			}
			filters = append(filters, filter)
		}

		totalScanned += response.Meta.Scanned
		totalParseErrors += response.Meta.ParseErrorCount
		parseErrors = append(parseErrors, response.ParseErrors...)
		if task != nil {
			task.Update(totalScanned, totalScanned)
			task.Detail(fmt.Sprintf("%d matched Team filters scanned", len(filters)))
		}
		if response.Meta.Scanned == 0 || response.Meta.NextLastID <= lastID || response.Meta.Scanned < teamFilterScriptRunnerPageSize {
			break
		}
		lastID = response.Meta.NextLastID
	}

	deduped := deduplicateFiltersByID(filters)
	findings := []Finding{}
	if totalParseErrors > 0 {
		parseErrorSamples := summarizeTeamFilterParseErrors(parseErrors, 3)
		if parseErrorSamples != "" {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_jql_fallback_parse_errors", fmt.Sprintf("Target filter exact-JQL fallback skipped %d filters because their JQL could not be parsed: %s", totalParseErrors, parseErrorSamples)))
		} else {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_jql_fallback_parse_errors", fmt.Sprintf("Target filter exact-JQL fallback skipped %d filters because their JQL could not be parsed", totalParseErrors)))
		}
	}
	findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_jql_fallback_summary", fmt.Sprintf("Target filter exact-JQL fallback scanned %d rows and found %d Team filter candidate(s)", totalScanned, len(deduped))))
	return deduped, findings, nil
}

func targetFiltersMatchingSourceJQL(filters []JiraFilter, sourceFilter FilterTeamClauseRow) []JiraFilter {
	sourceJQL := normalizeFilterJQL(sourceFilter.JQL)
	if sourceJQL == "" {
		return nil
	}
	matches := make([]JiraFilter, 0)
	for _, filter := range filters {
		if normalizeFilterJQL(filter.JQL) == sourceJQL {
			matches = append(matches, filter)
		}
	}
	return deduplicateFiltersByID(matches)
}

func summarizeTeamFilterParseErrors(parseErrors []teamFilterParseError, maxSamples int) string {
	if len(parseErrors) == 0 {
		return ""
	}
	if maxSamples <= 0 {
		maxSamples = 1
	}
	limit := len(parseErrors)
	if limit > maxSamples {
		limit = maxSamples
	}
	samples := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		parseError := parseErrors[i]
		name := strings.TrimSpace(parseError.Name)
		if name != "" {
			if parseError.ID > 0 {
				samples = append(samples, fmt.Sprintf("%q (id=%d)", name, parseError.ID))
			} else {
				samples = append(samples, fmt.Sprintf("%q", name))
			}
			continue
		}
		if parseError.ID > 0 {
			samples = append(samples, fmt.Sprintf("Filter ID %d", parseError.ID))
		} else {
			samples = append(samples, "unknown filter")
		}
	}
	return strings.Join(samples, ", ")
}

func targetFilterMatchesSource(filter JiraFilter, sourceFilter FilterTeamClauseRow) bool {
	return targetFilterMatchesSourceWithOwnerScope(filter, sourceFilter, true)
}

func targetFilterMatchesSourceWithOwnerScope(filter JiraFilter, sourceFilter FilterTeamClauseRow, requireOwner bool) bool {
	if normalizeTitle(filter.Name) != normalizeTitle(sourceFilter.FilterName) {
		return false
	}
	if !requireOwner {
		return true
	}
	sourceOwnerEmail := normalizeEmail(sourceFilter.OwnerEmail)
	sourceOwner := normalizeTitle(sourceFilter.Owner)
	if sourceOwnerEmail == "" && sourceOwner == "" {
		return true
	}
	if sourceOwnerEmail != "" && normalizeEmail(filterOwnerEmail(filter.Owner)) == sourceOwnerEmail {
		return true
	}
	if normalizeTitle(filterOwnerLabel(filter.Owner)) == sourceOwner {
		return true
	}
	return normalizeFilterJQL(filter.JQL) != "" && normalizeFilterJQL(filter.JQL) == normalizeFilterJQL(sourceFilter.JQL)
}

func normalizeFilterJQL(jql string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(jql))), " ")
}

func deduplicateFiltersByID(filters []JiraFilter) []JiraFilter {
	byID := map[string]JiraFilter{}
	for _, filter := range filters {
		if strings.TrimSpace(filter.ID) == "" {
			continue
		}
		byID[filter.ID] = filter
	}

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]JiraFilter, 0, len(ids))
	for _, id := range ids {
		out = append(out, byID[id])
	}
	return out
}

func buildTargetFilterSnapshotRows(filters map[string]JiraFilter) []TargetFilterSnapshotRow {
	ids := make([]string, 0, len(filters))
	for id := range filters {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	rows := make([]TargetFilterSnapshotRow, 0, len(ids))
	for _, id := range ids {
		filter := filters[id]
		rows = append(rows, TargetFilterSnapshotRow{
			TargetFilterID:   filter.ID,
			TargetFilterName: filter.Name,
			TargetOwner:      filterOwnerLabel(filter.Owner),
			Description:      filter.Description,
			JQL:              filter.JQL,
			ViewURL:          filter.ViewURL,
			SearchURL:        filter.SearchURL,
		})
	}
	return rows
}

func buildTargetIssueSnapshotRows(targetTeamsFieldID string, issues map[string]JiraIssue) []TargetIssueSnapshotRow {
	keys := make([]string, 0, len(issues))
	for key := range issues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rows := make([]TargetIssueSnapshotRow, 0, len(keys))
	for _, key := range keys {
		issue := issues[key]
		projectKey, projectName, projectType := issueProjectDetails(issue.Fields)
		summary := ""
		if rawSummary, ok := issue.Fields["summary"].(string); ok {
			summary = rawSummary
		}
		rows = append(rows, TargetIssueSnapshotRow{
			IssueKey:             issue.Key,
			ProjectKey:           projectKey,
			ProjectName:          projectName,
			ProjectType:          projectType,
			Summary:              summary,
			TargetTeamsFieldID:   targetTeamsFieldID,
			CurrentTargetTeamIDs: strings.Join(extractTeamFieldIDs(issue.Fields[targetTeamsFieldID]), ","),
		})
	}
	return rows
}

func buildTargetParentLinkSnapshotRows(client *jiraClient, targetParentLinkFieldID string, issues map[string]JiraIssue, currentParentCache map[string]JiraIssue) []TargetParentLinkSnapshotRow {
	keys := make([]string, 0, len(issues))
	for key := range issues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	rows := make([]TargetParentLinkSnapshotRow, 0, len(keys))
	for _, key := range keys {
		issue := issues[key]
		projectKey, projectName, projectType := issueProjectDetails(issue.Fields)
		summary := ""
		if rawSummary, ok := issue.Fields["summary"].(string); ok {
			summary = rawSummary
		}
		currentParentID, currentParentKey, _ := resolveIssueReference(client, issue.Fields[targetParentLinkFieldID], currentParentCache)
		rows = append(rows, TargetParentLinkSnapshotRow{
			IssueKey:                issue.Key,
			IssueID:                 issue.ID,
			ProjectKey:              projectKey,
			ProjectName:             projectName,
			ProjectType:             projectType,
			Summary:                 summary,
			TargetParentLinkFieldID: targetParentLinkFieldID,
			CurrentParentIssueID:    currentParentID,
			CurrentParentIssueKey:   currentParentKey,
		})
	}
	return rows
}

func buildPostMigrationIssueComparisonRows(sourceRows []IssueTeamRow, targetTeamsFieldID string, fetchedIssues map[string]JiraIssue, teamMappings []TeamMapping) []PostMigrationIssueComparisonRow {
	rows := make([]PostMigrationIssueComparisonRow, 0, len(sourceRows))
	targetTeamIDs := teamTargetIDsBySourceID(teamMappings)
	for _, sourceRow := range sourceRows {
		rows = append(rows, buildPostMigrationIssueComparisonRow(sourceRow, targetTeamsFieldID, fetchedIssues[sourceRow.IssueKey], targetTeamIDs))
	}
	return rows
}

func buildPostMigrationIssueComparisonRow(sourceRow IssueTeamRow, targetTeamsFieldID string, targetIssue JiraIssue, targetTeamIDs map[string]string) PostMigrationIssueComparisonRow {
	row := PostMigrationIssueComparisonRow{
		IssueKey:           sourceRow.IssueKey,
		ProjectKey:         sourceRow.ProjectKey,
		ProjectName:        sourceRow.ProjectName,
		ProjectType:        sourceRow.ProjectType,
		Summary:            sourceRow.Summary,
		SourceTeamsFieldID: sourceRow.TeamsFieldID,
		TargetTeamsFieldID: targetTeamsFieldID,
		SourceTeamIDs:      sourceRow.SourceTeamIDs,
		SourceTeamNames:    sourceRow.SourceTeamNames,
		TargetTeamIDs:      strings.Join(mappedTargetTeamIDsForExport(sourceRow.SourceTeamIDs, targetTeamIDs), ","),
	}

	if strings.TrimSpace(targetIssue.Key) == "" {
		row.Status = "not_found"
		row.Reason = "no target issue with the same issue key was found"
		return row
	}
	currentIDs := extractTeamFieldIDs(targetIssue.Fields[targetTeamsFieldID])
	row.CurrentTargetTeamIDs = strings.Join(currentIDs, ",")

	sourceIDs := splitDelimitedValues(sourceRow.SourceTeamIDs)
	if len(sourceIDs) == 0 {
		row.Status = "no_source_team_ids"
		row.Reason = "no source team IDs were exported for this issue"
		return row
	}

	changedSourceIDs := make([]string, 0, len(sourceIDs))
	targetIDs := make([]string, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		targetID := strings.TrimSpace(targetTeamIDs[sourceID])
		if targetID == "" {
			row.Status = "unresolved_team_mapping"
			row.Reason = "no destination team ID was resolved for one or more source team IDs on this issue"
			return row
		}
		targetIDs = append(targetIDs, targetID)
		if targetID != sourceID {
			changedSourceIDs = append(changedSourceIDs, sourceID)
		}
	}

	if len(changedSourceIDs) == 0 {
		row.Status = "same_id"
		row.Reason = "source and target team IDs are identical; no issue update is needed"
		return row
	}

	currentSet := toSet(currentIDs)
	targetSet := toSet(targetIDs)
	if setEquals(currentSet, targetSet) {
		row.Status = "already_rewritten"
		row.Reason = "the target issue already contains the mapped destination team IDs"
		return row
	}

	if len(currentIDs) > 0 {
		for _, sourceID := range changedSourceIDs {
			if _, ok := currentSet[sourceID]; !ok {
				row.Status = "source_team_ids_not_found_in_target_issue"
				row.Reason = "the current target issue Teams field does not contain all source team IDs that need rewriting"
				return row
			}
		}
	}

	row.Status = "ready"
	return row
}

func buildPostMigrationParentLinkComparisonRows(client *jiraClient, sourceRows []ParentLinkRow, targetParentLinkFieldID string, childIssues map[string]JiraIssue, targetParents map[string]JiraIssue, currentParentCache map[string]JiraIssue) ([]PostMigrationParentLinkComparisonRow, []Finding) {
	rows := make([]PostMigrationParentLinkComparisonRow, 0, len(sourceRows))
	findings := make([]Finding, 0)
	for _, sourceRow := range sourceRows {
		row, finding := buildPostMigrationParentLinkComparisonRow(client, sourceRow, targetParentLinkFieldID, childIssues[sourceRow.IssueKey], targetParents[sourceRow.SourceParentIssueKey], currentParentCache)
		rows = append(rows, row)
		if finding != nil {
			findings = append(findings, *finding)
		}
	}
	return rows, findings
}

func buildPostMigrationParentLinkComparisonRow(client *jiraClient, sourceRow ParentLinkRow, targetParentLinkFieldID string, targetChild JiraIssue, targetParent JiraIssue, currentParentCache map[string]JiraIssue) (PostMigrationParentLinkComparisonRow, *Finding) {
	row := PostMigrationParentLinkComparisonRow{
		IssueKey:                sourceRow.IssueKey,
		IssueID:                 sourceRow.IssueID,
		ProjectKey:              sourceRow.ProjectKey,
		ProjectName:             sourceRow.ProjectName,
		ProjectType:             sourceRow.ProjectType,
		Summary:                 sourceRow.Summary,
		SourceParentLinkFieldID: sourceRow.ParentLinkFieldID,
		TargetParentLinkFieldID: targetParentLinkFieldID,
		SourceParentIssueID:     sourceRow.SourceParentIssueID,
		SourceParentIssueKey:    sourceRow.SourceParentIssueKey,
		TargetParentIssueID:     targetParent.ID,
		TargetParentIssueKey:    targetParent.Key,
	}

	if strings.TrimSpace(targetChild.Key) == "" {
		row.Status = "not_found_child"
		row.Reason = "no target child issue with the same issue key was found"
		return row, nil
	}
	if strings.TrimSpace(targetParent.Key) == "" {
		row.Status = "not_found_parent"
		row.Reason = "no target parent issue with the same issue key was found"
		return row, nil
	}

	currentParentID, currentParentKey, err := resolveIssueReference(client, targetChild.Fields[targetParentLinkFieldID], currentParentCache)
	if err != nil {
		row.Status = "current_parent_lookup_failed"
		row.Reason = "could not resolve the current target Parent Link issue reference"
		return row, &Finding{Severity: SeverityWarning, Code: "post_migrate_current_parent_lookup_failed", Message: fmt.Sprintf("Could not resolve current Parent Link for child %s: %v", sourceRow.IssueKey, err)}
	}
	row.CurrentParentIssueID = currentParentID
	row.CurrentParentIssueKey = currentParentKey

	if parentReferenceMatches(row.CurrentParentIssueID, row.CurrentParentIssueKey, targetParent.ID, targetParent.Key) {
		row.Status = "already_rewritten"
		row.Reason = "the target issue already points to the mapped target parent issue"
		return row, nil
	}

	row.Status = "ready"
	return row, nil
}

func resolveIssueReference(client *jiraClient, raw any, cache map[string]JiraIssue) (string, string, error) {
	ref := extractParentIssueReference(raw)
	issueID := strings.TrimSpace(ref.idOrKey)
	issueKey := strings.TrimSpace(ref.key)
	if issueID == "" && issueKey == "" {
		return "", "", nil
	}
	if issueKey != "" && issueID != "" {
		if issueID == issueKey || looksLikeIssueKey(issueID) {
			return "", issueKey, nil
		}
		return issueID, issueKey, nil
	}

	lookupKey := nonEmptyString(issueKey, issueID)
	if cached, ok := cache[lookupKey]; ok {
		return nonEmptyString(issueID, cached.ID), nonEmptyString(issueKey, cached.Key), nil
	}
	issue, err := client.GetIssue(lookupKey, []string{"summary", "project"})
	if err != nil {
		return issueID, issueKey, err
	}
	cache[lookupKey] = *issue
	return nonEmptyString(issueID, issue.ID), nonEmptyString(issueKey, issue.Key), nil
}

func parentReferenceMatches(currentID, currentKey, targetID, targetKey string) bool {
	currentID = strings.TrimSpace(currentID)
	currentKey = strings.TrimSpace(currentKey)
	targetID = strings.TrimSpace(targetID)
	targetKey = strings.TrimSpace(targetKey)
	if currentID != "" && targetID != "" && currentID == targetID {
		return true
	}
	return currentKey != "" && targetKey != "" && currentKey == targetKey
}

func looksLikeIssueKey(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, " ") || !strings.Contains(value, "-") {
		return false
	}
	parts := strings.Split(value, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	for _, r := range parts[0] {
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' {
			return false
		}
	}
	for _, r := range parts[1] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func buildPostMigrationFilterMatchAndComparisonRows(sourceRows []FilterTeamClauseRow, candidatesBySource map[string][]JiraFilter, fetchedFilters map[string]JiraFilter, teamMappings []TeamMapping, matchMethodsBySource map[string]string) ([]PostMigrationFilterMatchRow, []PostMigrationFilterComparisonRow) {
	groupedSourceRows := map[string][]FilterTeamClauseRow{}
	for _, row := range sourceRows {
		if row.MatchType != "team_id" {
			continue
		}
		groupedSourceRows[row.FilterID] = append(groupedSourceRows[row.FilterID], row)
	}

	filterIDs := make([]string, 0, len(groupedSourceRows))
	for id := range groupedSourceRows {
		filterIDs = append(filterIDs, id)
	}
	sort.Strings(filterIDs)

	targetTeamIDs := teamTargetIDsBySourceID(teamMappings)
	matchRows := make([]PostMigrationFilterMatchRow, 0)
	comparisonRows := make([]PostMigrationFilterComparisonRow, 0)

	for _, filterID := range filterIDs {
		sourceGroup := groupedSourceRows[filterID]
		representative := sourceGroup[0]
		candidates := candidatesBySource[filterID]
		matchMethod := strings.TrimSpace(matchMethodsBySource[filterID])

		switch len(candidates) {
		case 0:
			if matchMethod == "" {
				matchMethod = filterMatchMethodNotFound
			}
			matchRows = append(matchRows, PostMigrationFilterMatchRow{
				SourceFilterID:   representative.FilterID,
				SourceFilterName: representative.FilterName,
				SourceOwner:      representative.Owner,
				MatchMethod:      matchMethod,
				Status:           "not_found",
				Reason:           "no target filter matched by exact name/owner",
			})
			for _, row := range sourceGroup {
				comparisonRows = append(comparisonRows, PostMigrationFilterComparisonRow{
					SourceFilterID:   row.FilterID,
					SourceFilterName: row.FilterName,
					SourceOwner:      row.Owner,
					SourceJQL:        row.JQL,
					SourceClause:     row.Clause,
					SourceTeamID:     row.SourceTeamID,
					Status:           "not_found",
					Reason:           "no target filter matched by exact name/owner",
				})
			}
		case 1:
			if matchMethod == "" {
				matchMethod = filterMatchMethodExactName
			}
			targetFilter := fetchedFilters[candidates[0].ID]
			targetOwner := filterOwnerLabel(targetFilter.Owner)
			matchRows = append(matchRows, PostMigrationFilterMatchRow{
				SourceFilterID:   representative.FilterID,
				SourceFilterName: representative.FilterName,
				SourceOwner:      representative.Owner,
				TargetFilterID:   targetFilter.ID,
				TargetFilterName: targetFilter.Name,
				TargetOwner:      targetOwner,
				MatchMethod:      matchMethod,
				Status:           "matched",
			})
			for _, row := range sourceGroup {
				comparisonRows = append(comparisonRows, buildPostMigrationFilterComparisonRow(row, targetFilter, targetTeamIDs))
			}
		default:
			if matchMethod == "" || matchMethod == filterMatchMethodExactName {
				matchMethod = filterMatchMethodAmbiguousName
			}
			for _, candidate := range candidates {
				targetFilter := fetchedFilters[candidate.ID]
				matchRows = append(matchRows, PostMigrationFilterMatchRow{
					SourceFilterID:   representative.FilterID,
					SourceFilterName: representative.FilterName,
					SourceOwner:      representative.Owner,
					TargetFilterID:   targetFilter.ID,
					TargetFilterName: targetFilter.Name,
					TargetOwner:      filterOwnerLabel(targetFilter.Owner),
					MatchMethod:      matchMethod,
					Status:           "ambiguous",
					Reason:           "multiple target filters matched by exact name/owner",
				})
			}
			for _, row := range sourceGroup {
				comparisonRows = append(comparisonRows, PostMigrationFilterComparisonRow{
					SourceFilterID:   row.FilterID,
					SourceFilterName: row.FilterName,
					SourceOwner:      row.Owner,
					SourceJQL:        row.JQL,
					SourceClause:     row.Clause,
					SourceTeamID:     row.SourceTeamID,
					Status:           "ambiguous",
					Reason:           "multiple target filters matched by exact name/owner",
				})
			}
		}
	}

	return matchRows, comparisonRows
}

func buildPostMigrationFilterComparisonRow(sourceRow FilterTeamClauseRow, targetFilter JiraFilter, targetTeamIDs map[string]string) PostMigrationFilterComparisonRow {
	row := PostMigrationFilterComparisonRow{
		SourceFilterID:   sourceRow.FilterID,
		SourceFilterName: sourceRow.FilterName,
		SourceOwner:      sourceRow.Owner,
		SourceJQL:        sourceRow.JQL,
		SourceClause:     sourceRow.Clause,
		SourceTeamID:     sourceRow.SourceTeamID,
		TargetFilterID:   targetFilter.ID,
		TargetFilterName: targetFilter.Name,
		TargetOwner:      filterOwnerLabel(targetFilter.Owner),
		CurrentTargetJQL: targetFilter.JQL,
	}

	targetTeamID := strings.TrimSpace(targetTeamIDs[strings.TrimSpace(sourceRow.SourceTeamID)])
	row.TargetTeamID = targetTeamID
	if targetTeamID == "" {
		row.Status = "unresolved_team_mapping"
		row.Reason = "no destination team ID was resolved for this source team"
		return row
	}

	if sourceRow.SourceTeamID == targetTeamID {
		row.Status = "same_id"
		row.RewrittenTargetJQL = targetFilter.JQL
		row.Reason = "source and target team IDs are identical; no filter JQL change is needed"
		return row
	}

	replacements := map[string]string{sourceRow.SourceTeamID: targetTeamID}
	rewrittenClause := rewriteTeamIDNumericLiterals(sourceRow.Clause, replacements)
	rewrittenJQL, changed := rewriteNumericTeamClausesInJQL(targetFilter.JQL, replacements)
	if !changed {
		if containsLiteralFold(targetFilter.JQL, rewrittenClause) || jqlHasNumericTeamClauseID(targetFilter.JQL, targetTeamID) {
			row.Status = "already_rewritten"
			row.RewrittenTargetJQL = targetFilter.JQL
			row.Reason = "the target filter already contains the mapped destination team ID"
			return row
		}
		row.Status = "source_clause_not_found_in_target_jql"
		row.Reason = "the exact source clause was not found in the current target filter JQL"
		return row
	}

	row.RewrittenTargetJQL = rewrittenJQL
	if row.RewrittenTargetJQL == targetFilter.JQL {
		row.Status = "no_change"
		row.Reason = "rewriting the target filter JQL produced no change"
		return row
	}

	row.Status = "ready"
	return row
}

func containsLiteralFold(s, substr string) bool {
	if substr == "" {
		return true
	}
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}

func replaceFirstLiteralFold(s, old, replacement string) (string, bool) {
	if old == "" {
		return s, false
	}
	idx := strings.Index(strings.ToLower(s), strings.ToLower(old))
	if idx < 0 {
		return s, false
	}
	return s[:idx] + replacement + s[idx+len(old):], true
}

func loadMigrationState(cfg Config) (migrationState, []Finding) {
	var findings []Finding
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "invalid_output_dir", err.Error()))
		return migrationState{}, findings
	}
	cfg.OutputDir = outputDir

	progressTotal := 13
	if runsPreMigratePhase(cfg.Command, cfg.Phase) {
		progressTotal += 2
		if issueTeamCorrectionsInScope(cfg) {
			progressTotal++
		}
		if cfg.ParentLinkInScope {
			progressTotal++
		}
		if cfg.FilterTeamIDsInScope {
			progressTotal++
		}
	}
	if runsPostMigratePhase(cfg.Command, cfg.Phase) {
		progressTotal += 12
	}
	progress := newProgressTracker(progressTotal)
	defer progress.Finish()
	mapping, err := loadIdentityMappings(cfg.IdentityMappingFile)
	if err != nil {
		findings = append(findings, newFinding(SeverityError, "identity_mapping_load", err.Error()))
		return migrationState{}, findings
	}

	if runsPostMigratePhase(cfg.Command, cfg.Phase) && postMigrateCanUsePreparedArtifacts(cfg) {
		state, artifactFindings := loadPostMigrateStateFromPreparedArtifacts(cfg, progress)
		state.IdentityMappings = mapping
		findings = append(findings, artifactFindings...)
		return state, findings
	}
	if runsMigratePhase(cfg.Command, cfg.Phase) && migrateCanUsePreparedArtifacts(cfg) {
		state, artifactFindings := loadMigrateStateFromPreparedArtifacts(cfg, progress)
		state.IdentityMappings = mapping
		findings = append(findings, artifactFindings...)
		return state, findings
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
		err      error
	}
	results := make(chan loadResult, 10)
	var wg sync.WaitGroup
	runLoad := func(label, code string, severity Severity, fn func(progress func(current, total int)) error) {
		task := progress.BeginTask(label)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer task.Done()
			err := fn(func(current, total int) {
				task.Update(current, total)
			})
			if err != nil {
				results <- loadResult{code: code, severity: severity, message: fmt.Sprintf("%s: %s", strings.ToLower(label), err.Error()), err: err}
			}
		}()
	}

	runLoad("Loading source teams", "source_teams_load", SeverityError, func(progressFn func(current, total int)) error {
		var loadErr error
		sourceTeams, loadErr = loadTeams(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.TeamsFile, progressFn)
		return loadErr
	})
	runLoad("Loading source programs", "source_programs_load", SeverityWarning, func(progressFn func(current, total int)) error {
		var loadErr error
		sourcePrograms, loadErr = loadPrograms(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, progressFn)
		return loadErr
	})
	runLoad("Loading source plans", "source_plans_load", SeverityWarning, func(progressFn func(current, total int)) error {
		var loadErr error
		sourcePlans, loadErr = loadPlans(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, progressFn)
		return loadErr
	})
	runLoad("Loading source persons", "source_persons_load", SeverityError, func(progressFn func(current, total int)) error {
		var loadErr error
		sourcePersons, loadErr = loadPersons(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.PersonsFile, progressFn)
		return loadErr
	})
	runLoad("Loading source resources", "source_resources_load", SeverityError, func(progressFn func(current, total int)) error {
		var loadErr error
		sourceResources, loadErr = loadResources(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword, cfg.ResourcesFile, progressFn)
		return loadErr
	})
	runLoad("Loading target teams", "target_teams_load", SeverityError, func(progressFn func(current, total int)) error {
		var loadErr error
		targetTeams, loadErr = loadTeams(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "", progressFn)
		return loadErr
	})
	runLoad("Loading target programs", "target_programs_load", SeverityWarning, func(progressFn func(current, total int)) error {
		var loadErr error
		targetPrograms, loadErr = loadPrograms(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, progressFn)
		return loadErr
	})
	runLoad("Loading target plans", "target_plans_load", SeverityWarning, func(progressFn func(current, total int)) error {
		var loadErr error
		targetPlans, loadErr = loadPlans(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, progressFn)
		return loadErr
	})
	runLoad("Loading target persons", "target_persons_load", SeverityError, func(progressFn func(current, total int)) error {
		var loadErr error
		targetPersons, loadErr = loadPersons(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "", progressFn)
		return loadErr
	})
	runLoad("Loading target resources", "target_resources_load", SeverityWarning, func(progressFn func(current, total int)) error {
		var loadErr error
		targetResources, loadErr = loadResources(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword, "", progressFn)
		return loadErr
	})

	wg.Wait()
	close(results)
	targetForbidden := false
	for result := range results {
		findings = append(findings, newFinding(result.severity, result.code, result.message))
		var apiErr *jiraAPIError
		if strings.HasPrefix(result.code, "target_") && errors.As(result.err, &apiErr) && apiErr.StatusCode == http.StatusForbidden {
			targetForbidden = true
		}
	}
	if targetForbidden {
		if finding := targetAuthDiagnosticFinding(cfg); finding != nil {
			findings = append(findings, *finding)
		}
	}

	sourceClient, sourceClientErr := sourceIssueClient(cfg)
	needsSourceIssueInputs := runsPreMigratePhase(cfg.Command, cfg.Phase) || runsPostMigratePhase(cfg.Command, cfg.Phase)
	if needsSourceIssueInputs && sourceClientErr != nil && cfg.SourceBaseURL != "" {
		findings = append(findings, newFinding(SeverityWarning, "source_issue_client", sourceClientErr.Error()))
	} else if runsPreMigratePhase(cfg.Command, cfg.Phase) && sourceClient == nil {
		if issueTeamCorrectionsInScope(cfg) {
			findings = append(findings, newFinding(SeverityWarning, "issue_export_skipped", "Issue Teams-field export was skipped because no source Jira base URL was provided"))
		}
		if cfg.ParentLinkInScope {
			findings = append(findings, newFinding(SeverityWarning, "parent_link_export_skipped", "Parent Link export was skipped because no source Jira base URL was provided"))
		}
	}

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
	teamMappings, teamFindings := buildTeamMappings(cfg, sourceTeams, targetTeams, sourcePlans, state.PlanMappings)
	state.TeamMappings = teamMappings
	findings = append(findings, teamFindings...)
	resourcePlans, resourceFindings := buildResourcePlans(state)
	state.ResourcePlans = resourcePlans
	findings = append(findings, resourceFindings...)
	progress.End()

	if runsPostMigratePhase(cfg.Command, cfg.Phase) {
		findings = append(findings, validatePostMigratePhaseState(state)...)
		if hasErrors(findings) {
			return state, findings
		}
	}

	if runsPreMigratePhase(cfg.Command, cfg.Phase) {
		progress.Start("Writing pre-migration artifacts")
		if artifacts, err := writeEntityExports(cfg, state); err == nil {
			state.Artifacts = artifacts
			state.MembershipExportPath = artifactPathByKey(artifacts, "source_team_memberships")
			state.IssueExportPath = artifactPathByKey(artifacts, "source_issues_with_team_values_detailed")
			state.IssueImportExportPath = artifactPathByKey(artifacts, "source_issues_with_team_values_import")
			for _, artifact := range artifacts {
				findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
			}
		} else {
			findings = append(findings, newFinding(SeverityWarning, "artifact_export_failed", err.Error()))
		}
		progress.End()

		if issueTeamCorrectionsInScope(cfg) {
			progress.StartCount("Exporting issues with team values")
			if sourceClient != nil {
				selection, issueRows, issuePath, issueImportPath, issueFindings := exportIssuesWithTeamsField(cfg, sourceClient, sourceTeams, progress)
				state.TeamsField = selection
				state.IssueTeamRows = issueRows
				if issuePath != "" {
					state.IssueExportPath = issuePath
					state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
						Key:   "source_issues_with_team_values_detailed",
						Label: "Detailed pre-migration issue/team export",
						Path:  issuePath,
						Count: len(issueRows),
					})
					if issueImportPath != "" {
						state.IssueImportExportPath = issueImportPath
						state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
							Key:   "source_issues_with_team_values_import",
							Label: "Import-ready issue/team CSV",
							Path:  issueImportPath,
							Count: len(issueRows),
						})
					}
				}
				findings = append(findings, issueFindings...)
			}
			progress.End()
		}

		if cfg.ParentLinkInScope {
			progress.StartCount("Exporting issues with Parent Link values")
			if sourceClient == nil {
				findings = append(findings, newFinding(SeverityError, "parent_link_export_skipped", "Parent Link export is in scope but no source Jira base URL is available"))
			} else {
				field, rows, exportPath, outOfScopePath, parentFindings := exportIssuesWithParentLink(cfg, sourceClient, progress)
				state.ParentLinkRows = rows
				findings = append(findings, parentFindings...)
				if exportPath != "" {
					state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
						Key:   "source_issues_with_parent_link",
						Label: "Issues with Parent Link",
						Path:  exportPath,
						Count: len(rows),
					})
					findings = append(findings, newFinding(SeverityInfo, "source_issues_with_parent_link_generated", fmt.Sprintf("Generated issues with Parent Link export: %s", exportPath)))
				}
				if outOfScopePath != "" {
					state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
						Key:   "source_parent_link_out_of_scope",
						Label: "Issues with out-of-scope Parent Link",
						Path:  outOfScopePath,
					})
					findings = append(findings, newFinding(SeverityInfo, "source_parent_link_out_of_scope_generated", fmt.Sprintf("Generated out-of-scope Parent Link reference export: %s", outOfScopePath)))
				}
				_ = field
			}
			progress.End()

			progress.Start("Resolving target Parent Link field")
			if targetClient, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword); err != nil {
				findings = append(findings, newFinding(SeverityWarning, "target_parent_link_field_client", fmt.Sprintf("Could not create target Jira client for Parent Link field lookup: %v", err)))
			} else if field, fieldPath, fieldFindings := resolveTargetParentLinkFieldForExport(cfg, targetClient); len(fieldFindings) > 0 {
				findings = append(findings, fieldFindings...)
				if field != nil && fieldPath != "" {
					state.TargetParentLinkField = field
					state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
						Key:   "target_parent_link_field",
						Label: "Target Parent Link field",
						Path:  fieldPath,
						Count: 1,
					})
				}
			}
			progress.End()
		}

		if cfg.FilterTeamIDsInScope {
			progress.StartCount("Resolving source filters with team IDs")
			rows, exportPath, artifact, filterFindings, err := scanFiltersWithConfiguredSource(cfg, sourceClient, sourceTeams, func(current, total int) {
				progress.UpdateCount(current, total)
			})
			state.FilterTeamClauseRows = rows
			findings = append(findings, filterFindings...)
			if err != nil {
				findings = append(findings, newFinding(SeverityError, "filter_inventory_failed", err.Error()))
			} else if artifact != nil {
				state.FilterScanExportPath = exportPath
				state.Artifacts = replaceArtifact(state.Artifacts, *artifact)
				findings = append(findings, newFinding(SeverityInfo, artifact.Key+"_generated", fmt.Sprintf("Generated %s: %s", strings.ToLower(artifact.Label), artifact.Path)))
			}
			progress.End()

			progress.StartCount("Resolving target filters for source filter mapping")
			filterMatchFindings := preparePreMigrationFilterTargetMatchArtifacts(cfg, &state, progress)
			findings = append(findings, filterMatchFindings...)
			progress.End()
		}

		progress.Start("Writing generated identity mapping")
		if generatedPath, err := writeGeneratedIdentityMapping(cfg, state.IdentityMappings); err == nil && generatedPath != "" {
			findings = append(findings, newFinding(SeverityInfo, "identity_mapping_generated", fmt.Sprintf("Generated reviewable identity mapping artifact: %s", generatedPath)))
		}
		progress.End()
	}

	if runsPostMigratePhase(cfg.Command, cfg.Phase) {
		postState, postFindings := loadPostMigrateInputs(cfg, state, sourceClient, progress)
		state = postState
		findings = append(findings, postFindings...)
		if !cfg.SkipPostMigrateArtifactReuse {
			postState, postFindings = loadPostMigrateTargetArtifactsFromExports(cfg, state, progress)
			state = postState
			findings = append(findings, postFindings...)
		}
		if needsPostMigrationTargetArtifactsPreparation(cfg, state) {
			findings = append(findings, preparePostMigrationTargetArtifacts(cfg, &state, progress)...)
		}
	}

	return state, findings
}

func sourceIssueClient(cfg Config) (*jiraClient, error) {
	if strings.TrimSpace(cfg.SourceBaseURL) == "" {
		return nil, nil
	}
	return newJiraClient(cfg.SourceBaseURL, cfg.SourceUsername, cfg.SourcePassword)
}

func postMigrateCanUsePreparedArtifacts(cfg Config) bool {
	if !runsPostMigratePhase(cfg.Command, cfg.Phase) {
		return false
	}
	if _, ok := latestPostMigrateTeamMappingPath(cfg.OutputDir); !ok {
		return false
	}
	if issueTeamCorrectionsInScope(cfg) {
		if _, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-teams.pre-migration.csv"); !ok {
			return false
		}
	}
	if cfg.ParentLinkInScope {
		if _, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-parent-link.pre-migration.csv"); !ok {
			return false
		}
	}
	if cfg.FilterTeamIDsInScope {
		if _, ok := latestOutputFamilyPath(cfg.OutputDir, "filters-with-team-clauses.pre-migration.csv"); !ok {
			return false
		}
	}
	return true
}

func migrateCanUsePreparedArtifacts(cfg Config) bool {
	if !runsMigratePhase(cfg.Command, cfg.Phase) {
		return false
	}
	if _, ok := latestOutputFamilyPath(cfg.OutputDir, "team-mapping.pre-migration.csv"); !ok {
		return false
	}
	if _, ok := latestOutputFamilyPath(cfg.OutputDir, "team-membership-mapping.pre-migration.csv"); !ok {
		return false
	}
	return true
}

func latestPostMigrateTeamMappingPath(outputDir string) (string, bool) {
	if path, ok := latestOutputFamilyPath(outputDir, "team-id-mapping.migration.csv"); ok {
		return path, true
	}
	return latestOutputFamilyPath(outputDir, "team-mapping.pre-migration.csv")
}

func latestFilterTargetMatchPath(outputDir string) (string, bool) {
	if path, ok := latestOutputFamilyPath(outputDir, "filter-target-match.post-migration.csv"); ok {
		return path, true
	}
	return latestOutputFamilyPath(outputDir, "filter-target-match.pre-migration.csv")
}

func loadMigrateStateFromPreparedArtifacts(cfg Config, progress *progressTracker) (migrationState, []Finding) {
	var findings []Finding
	state := migrationState{}

	if mappingPath, ok := latestOutputFamilyPath(cfg.OutputDir, "team-mapping.pre-migration.csv"); ok {
		progressStart(progress, "Loading pre-migration team mapping")
		cleanPath, cleanErr := cleanInputFilePath("team mapping export", mappingPath)
		if cleanErr != nil {
			findings = append(findings, newFinding(SeverityError, "migrate_team_mapping_load_failed", cleanErr.Error()))
			progressEnd(progress)
			return state, findings
		}
		mappingPath = cleanPath
		rows, err := loadTeamMappingsFromExport(mappingPath)
		if err != nil {
			findings = append(findings, newFinding(SeverityError, "migrate_team_mapping_load_failed", fmt.Sprintf("Could not load team mapping export %s: %v", mappingPath, err)))
		} else {
			state.TeamMappings = rows
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "team_mapping",
				Label: "Team mapping comparison",
				Path:  mappingPath,
				Count: len(rows),
			})
			findings = append(findings, newFinding(SeverityInfo, "migrate_team_mapping_reused", fmt.Sprintf("Reused team mapping export: %s", mappingPath)))
		}
		progressEnd(progress)
	}
	if membershipPath, ok := latestOutputFamilyPath(cfg.OutputDir, "team-membership-mapping.pre-migration.csv"); ok {
		progressStart(progress, "Loading pre-migration team membership mapping")
		cleanPath, cleanErr := cleanInputFilePath("team membership mapping export", membershipPath)
		if cleanErr != nil {
			findings = append(findings, newFinding(SeverityError, "migrate_membership_mapping_load_failed", cleanErr.Error()))
			progressEnd(progress)
			return state, findings
		}
		membershipPath = cleanPath
		rows, err := loadResourcePlansFromExport(membershipPath)
		if err != nil {
			findings = append(findings, newFinding(SeverityError, "migrate_membership_mapping_load_failed", fmt.Sprintf("Could not load team membership mapping export %s: %v", membershipPath, err)))
		} else {
			state.ResourcePlans = rows
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "team_membership_mapping",
				Label: "Team membership mapping comparison",
				Path:  membershipPath,
				Count: len(rows),
			})
			findings = append(findings, newFinding(SeverityInfo, "migrate_membership_mapping_reused", fmt.Sprintf("Reused team membership mapping export: %s", membershipPath)))
		}
		progressEnd(progress)
	}
	return state, findings
}

func loadPostMigrateStateFromPreparedArtifacts(cfg Config, progress *progressTracker) (migrationState, []Finding) {
	var findings []Finding
	state := migrationState{}

	if mappingPath, ok := latestPostMigrateTeamMappingPath(cfg.OutputDir); ok {
		progressStart(progress, "Loading team ID mapping export")
		cleanPath, cleanErr := cleanInputFilePath("team ID mapping export", mappingPath)
		if cleanErr != nil {
			findings = append(findings, newFinding(SeverityError, "post_migrate_team_mapping_load_failed", cleanErr.Error()))
			progressEnd(progress)
			return state, findings
		}
		mappingPath = cleanPath
		rows, err := loadTeamMappingsFromExport(mappingPath)
		if err != nil {
			findings = append(findings, newFinding(SeverityError, "post_migrate_team_mapping_load_failed", fmt.Sprintf("Could not load team mapping export %s: %v", mappingPath, err)))
		} else {
			state.TeamMappings = rows
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "migration_team_id_mapping",
				Label: "Migration team ID mapping",
				Path:  mappingPath,
				Count: len(rows),
			})
			findings = append(findings, newFinding(SeverityInfo, "post_migrate_team_mapping_reused", fmt.Sprintf("Reused team mapping export: %s", mappingPath)))
		}
		progressEnd(progress)
	}
	if hasErrors(findings) {
		return state, findings
	}

	findings = append(findings, validatePostMigratePhaseState(state)...)
	if hasErrors(findings) {
		return state, findings
	}

	var inputFindings []Finding
	state, inputFindings = loadPostMigrateInputs(cfg, state, nil, progress)
	findings = append(findings, inputFindings...)
	if !cfg.SkipPostMigrateArtifactReuse {
		state, inputFindings = loadPostMigrateTargetArtifactsFromExports(cfg, state, progress)
		findings = append(findings, inputFindings...)
	}
	if needsPostMigrationTargetArtifactsPreparation(cfg, state) {
		findings = append(findings, preparePostMigrationTargetArtifacts(cfg, &state, progress)...)
	}
	return state, findings
}

func loadPostMigrateTargetArtifactsFromExports(cfg Config, state migrationState, progress *progressTracker) (migrationState, []Finding) {
	findings := []Finding{}

	if issueTeamCorrectionsInScope(cfg) {
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "target-issues.snapshot.post-migration.csv"); ok {
			progressStart(progress, "Loading target issue snapshot")
			rows, err := loadTargetIssueSnapshotRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_issue_snapshot_reuse_failed", fmt.Sprintf("Could not reuse target issue snapshot %s: %v", path, err)))
			} else {
				state.TargetIssueSnapshots = rows
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_target_issue_snapshot", Label: "Target issue snapshot", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_issue_snapshot_reused", fmt.Sprintf("Reused target issue snapshot: %s", path)))
			}
			progressEnd(progress)
		}
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "issue-team-comparison.post-migration.csv"); ok {
			progressStart(progress, "Loading issue team comparison")
			rows, err := loadPostMigrationIssueComparisonRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_issue_comparison_reuse_failed", fmt.Sprintf("Could not reuse issue comparison %s: %v", path, err)))
			} else {
				state.IssueComparisons = rows
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_issue_comparison", Label: "Post-migration issue comparison", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_issue_comparison_reused", fmt.Sprintf("Reused issue comparison: %s", path)))
			}
			progressEnd(progress)
		}
	}

	if cfg.ParentLinkInScope {
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "target-parent-link-issues.snapshot.post-migration.csv"); ok {
			progressStart(progress, "Loading target Parent Link snapshot")
			rows, err := loadTargetParentLinkSnapshotRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_parent_link_snapshot_reuse_failed", fmt.Sprintf("Could not reuse target Parent Link snapshot %s: %v", path, err)))
			} else {
				state.TargetParentLinkSnapshots = rows
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_target_parent_link_snapshot", Label: "Target Parent Link snapshot", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_parent_link_snapshot_reused", fmt.Sprintf("Reused target Parent Link snapshot: %s", path)))
			}
			progressEnd(progress)
		}
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "parent-link-comparison.post-migration.csv"); ok {
			progressStart(progress, "Loading Parent Link comparison")
			rows, err := loadPostMigrationParentLinkComparisonRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_comparison_reuse_failed", fmt.Sprintf("Could not reuse Parent Link comparison %s: %v", path, err)))
			} else {
				state.ParentLinkComparisons = rows
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_parent_link_comparison", Label: "Post-migration parent-link comparison", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_parent_link_comparison_reused", fmt.Sprintf("Reused Parent Link comparison: %s", path)))
			}
			progressEnd(progress)
		}
	}

	if cfg.FilterTeamIDsInScope {
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "target-filters.snapshot.post-migration.csv"); ok {
			progressStart(progress, "Loading target filter snapshot")
			rows, err := loadTargetFilterSnapshotRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_target_filter_snapshot_reuse_failed", fmt.Sprintf("Could not reuse target filter snapshot %s: %v", path, err)))
			} else {
				state.TargetFilterSnapshots = rows
				state.TargetFilters = jiraFiltersFromTargetFilterSnapshots(rows)
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_target_filter_snapshot", Label: "Target filter snapshot", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_target_filter_snapshot_reused", fmt.Sprintf("Reused target filter snapshot: %s", path)))
			}
			progressEnd(progress)
		}
		var matchFindings []Finding
		state, matchFindings = loadFilterTargetMatchFromExports(cfg, state, progress)
		findings = append(findings, matchFindings...)
		if path, ok := latestOutputFamilyPath(cfg.OutputDir, "filter-jql-comparison.post-migration.csv"); ok {
			progressStart(progress, "Loading filter JQL comparison")
			rows, err := loadPostMigrationFilterComparisonRowsFromExport(path)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_comparison_reuse_failed", fmt.Sprintf("Could not reuse filter JQL comparison %s: %v", path, err)))
			} else {
				state.FilterComparisons = rows
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_filter_comparison", Label: "Post-migration filter JQL comparison", Path: path, Count: len(rows)})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_comparison_reused", fmt.Sprintf("Reused filter JQL comparison: %s", path)))
			}
			progressEnd(progress)
		}
	}

	return state, findings
}

func loadFilterTargetMatchFromExports(cfg Config, state migrationState, progress *progressTracker) (migrationState, []Finding) {
	path, ok := latestFilterTargetMatchPath(cfg.OutputDir)
	if !ok {
		return state, nil
	}

	progressStart(progress, "Loading filter target match")
	defer progressEnd(progress)

	rows, err := loadPostMigrationFilterMatchRowsFromExport(path)
	if err != nil {
		return state, []Finding{newFinding(SeverityWarning, "post_migrate_filter_target_match_reuse_failed", fmt.Sprintf("Could not reuse filter target match %s: %v", path, err))}
	}

	state.FilterTargetMatches = rows
	state.Artifacts = replaceArtifact(state.Artifacts, Artifact{Key: "post_migrate_filter_target_match", Label: "Post-migration filter target match", Path: path, Count: len(rows)})
	return state, []Finding{newFinding(SeverityInfo, "post_migrate_filter_target_match_reused", fmt.Sprintf("Reused filter target match: %s", path))}
}

func jiraFiltersFromTargetFilterSnapshots(rows []TargetFilterSnapshotRow) []JiraFilter {
	filters := make([]JiraFilter, 0, len(rows))
	for _, row := range rows {
		filter := JiraFilter{
			ID:          row.TargetFilterID,
			Name:        row.TargetFilterName,
			Description: row.Description,
			JQL:         row.JQL,
			ViewURL:     row.ViewURL,
			SearchURL:   row.SearchURL,
		}
		if strings.TrimSpace(row.TargetOwner) != "" {
			filter.Owner = &JiraFilterUser{DisplayName: row.TargetOwner, Name: row.TargetOwner, Key: row.TargetOwner}
		}
		filters = append(filters, filter)
	}
	return filters
}

func validatePostMigratePhaseState(state migrationState) []Finding {
	pending := make([]string, 0)
	for _, mapping := range state.TeamMappings {
		if mapping.Decision == "add" {
			pending = append(pending, mapping.SourceTitle)
		}
	}
	if len(pending) == 0 {
		return []Finding{newFinding(SeverityInfo, "post_migrate_phase_ready", "Post-migrate phase is ready because all source teams already resolve to destination team IDs")}
	}

	const maxTitles = 5
	display := pending
	if len(display) > maxTitles {
		display = display[:maxTitles]
	}
	message := fmt.Sprintf("Post-migrate phase cannot start because %d team(s) still need destination creation: %s", len(pending), strings.Join(display, ", "))
	if len(display) < len(pending) {
		message = fmt.Sprintf("%s, and %d more", message, len(pending)-len(display))
	}
	return []Finding{newFinding(SeverityError, "post_migrate_phase_blocked", message)}
}

func loadPostMigrateInputs(cfg Config, state migrationState, sourceClient *jiraClient, progress *progressTracker) (migrationState, []Finding) {
	findings := []Finding{}

	if issueTeamCorrectionsInScope(cfg) {
		if issuePath, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-teams.pre-migration.csv"); ok {
			progressStart(progress, "Loading pre-migration issue/team export")
			cleanPath, cleanErr := cleanInputFilePath("issue/team export", issuePath)
			if cleanErr != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_issue_export_reuse_failed", cleanErr.Error()))
				progressEnd(progress)
				return state, findings
			}
			issuePath = cleanPath
			rows, err := loadIssueTeamRowsFromExport(issuePath)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_issue_export_reuse_failed", fmt.Sprintf("Could not reuse existing issue/team export %s: %v", issuePath, err)))
			} else {
				rows = issueTeamRowsInProjectScope(rows, cfg.IssueProjectScope)
				state.IssueTeamRows = rows
				state.IssueExportPath = issuePath
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
					Key:   "source_issues_with_team_values_detailed",
					Label: "Detailed pre-migration issue/team export",
					Path:  issuePath,
					Count: len(rows),
				})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_issue_export_reused", fmt.Sprintf("Reused existing issue/team export: %s", issuePath)))
			}
			progressEnd(progress)
		}

		if state.IssueExportPath == "" && sourceClient != nil {
			progressStartCount(progress, "Rebuilding source issue/team export")
			selection, issueRows, issuePath, issueImportPath, issueFindings := exportIssuesWithTeamsField(cfg, sourceClient, state.SourceTeams, progress)
			state.TeamsField = selection
			issueRows = issueTeamRowsInProjectScope(issueRows, cfg.IssueProjectScope)
			state.IssueTeamRows = issueRows
			findings = append(findings, issueFindings...)
			if issuePath != "" {
				state.IssueExportPath = issuePath
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
					Key:   "source_issues_with_team_values_detailed",
					Label: "Detailed pre-migration issue/team export",
					Path:  issuePath,
					Count: len(issueRows),
				})
			}
			if issueImportPath != "" {
				state.IssueImportExportPath = issueImportPath
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
					Key:   "source_issues_with_team_values_import",
					Label: "Import-ready issue/team CSV",
					Path:  issueImportPath,
					Count: len(issueRows),
				})
			}
			progressEnd(progress)
		}

		if state.IssueExportPath == "" {
			findings = append(findings, newFinding(SeverityError, "post_migrate_issue_input_missing", "Post-migrate phase needs issue/team source data. Run pre-migrate first or provide a source Jira base URL so the tool can rebuild it."))
		}
	}

	if parentPath, ok := latestOutputFamilyPath(cfg.OutputDir, "issues-with-parent-link.pre-migration.csv"); ok {
		progressStart(progress, "Loading pre-migration Parent Link export")
		cleanPath, cleanErr := cleanInputFilePath("parent link export", parentPath)
		if cleanErr != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_export_reuse_failed", cleanErr.Error()))
			progressEnd(progress)
			return state, findings
		}
		parentPath = cleanPath
		rows, err := loadParentLinkRowsFromExport(parentPath)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_export_reuse_failed", fmt.Sprintf("Could not reuse existing parent-link export %s: %v", parentPath, err)))
		} else {
			rows = parentLinkRowsInProjectScope(rows, cfg.IssueProjectScope)
			state.ParentLinkRows = rows
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "source_issues_with_parent_link",
				Label: "Issues with Parent Link",
				Path:  parentPath,
				Count: len(rows),
			})
			findings = append(findings, newFinding(SeverityInfo, "post_migrate_parent_link_export_reused", fmt.Sprintf("Reused existing parent-link export: %s", parentPath)))
		}
		progressEnd(progress)
	}

	if len(state.ParentLinkRows) == 0 && cfg.ParentLinkInScope && sourceClient != nil {
		progressStartCount(progress, "Rebuilding source Parent Link export")
		_, rows, exportPath, outOfScopePath, parentFindings := exportIssuesWithParentLink(cfg, sourceClient, progress)
		rows = parentLinkRowsInProjectScope(rows, cfg.IssueProjectScope)
		state.ParentLinkRows = rows
		findings = append(findings, parentFindings...)
		if exportPath != "" {
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "source_issues_with_parent_link",
				Label: "Issues with Parent Link",
				Path:  exportPath,
				Count: len(rows),
			})
		}
		if outOfScopePath != "" {
			state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
				Key:   "source_parent_link_out_of_scope",
				Label: "Issues with out-of-scope Parent Link",
				Path:  outOfScopePath,
			})
		}
		progressEnd(progress)
	}

	if cfg.ParentLinkInScope && len(state.ParentLinkRows) == 0 {
		findings = append(findings, newFinding(SeverityError, "post_migrate_parent_link_input_missing", "Post-migrate phase needs the pre-migrate Parent Link export. Run pre-migrate first so source parent references are exported."))
	}

	if cfg.FilterTeamIDsInScope {
		if filterPath, ok := latestOutputFamilyPath(cfg.OutputDir, "filters-with-team-clauses.pre-migration.csv"); ok {
			progressStart(progress, "Loading pre-migration filter Team-clause export")
			cleanPath, cleanErr := cleanInputFilePath("filter export", filterPath)
			if cleanErr != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_export_reuse_failed", cleanErr.Error()))
				progressEnd(progress)
				return state, findings
			}
			filterPath = cleanPath
			rows, err := loadFilterTeamClauseRowsFromExport(filterPath)
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_export_reuse_failed", fmt.Sprintf("Could not reuse existing filter export %s: %v", filterPath, err)))
			} else {
				state.FilterTeamClauseRows = rows
				state.FilterScanExportPath = filterPath
				state.Artifacts = replaceArtifact(state.Artifacts, Artifact{
					Key:   "source_filters_with_team_clauses",
					Label: "Filters with Team clauses",
					Path:  filterPath,
					Count: len(rows),
				})
				findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_export_reused", fmt.Sprintf("Reused existing filter Team-clause export: %s", filterPath)))
			}
			progressEnd(progress)
		}

		if len(state.FilterTeamClauseRows) == 0 {
			findings = append(findings, newFinding(SeverityError, "post_migrate_filter_input_missing", "Post-migrate phase needs the pre-migrate source filter export. Run pre-migrate first so the source list of filters with team IDs is resolved and exported."))
		} else if len(state.FilterTargetMatches) == 0 && !cfg.SkipPostMigrateArtifactReuse {
			var matchFindings []Finding
			state, matchFindings = loadFilterTargetMatchFromExports(cfg, state, progress)
			findings = append(findings, matchFindings...)
		}
	}

	return state, findings
}

func progressStart(progress *progressTracker, label string) {
	if progress != nil {
		progress.Start(label)
	}
}

func progressStartCount(progress *progressTracker, label string) {
	if progress != nil {
		progress.StartCount(label)
	}
}

func progressUpdateCount(progress *progressTracker, current, total int) {
	if progress != nil {
		progress.UpdateCount(current, total)
	}
}

func progressEnd(progress *progressTracker) {
	if progress != nil {
		progress.End()
	}
}

func issueTeamCorrectionsInScope(cfg Config) bool {
	if cfg.IssueTeamIDsInScopeSet {
		return cfg.IssueTeamIDsInScope
	}
	return true
}

func targetAuthDiagnosticFinding(cfg Config) *Finding {
	client, err := newJiraClient(cfg.TargetBaseURL, cfg.TargetUsername, cfg.TargetPassword)
	if err != nil {
		finding := newFinding(SeverityWarning, "target_auth_diagnostic_failed", fmt.Sprintf("Could not create target Jira client for auth diagnostic: %v", err))
		return &finding
	}
	user, err := client.CurrentUser()
	if err != nil {
		finding := newFinding(SeverityWarning, "target_auth_diagnostic_failed", fmt.Sprintf("Target core Jira auth diagnostic failed at /rest/api/2/myself: %v", err))
		return &finding
	}

	identity := jiraUserIdentity(*user)
	finding := newFinding(SeverityInfo, "target_auth_diagnostic", fmt.Sprintf("Target core Jira /rest/api/2/myself authenticated as %s; the target Teams/JPO 403 happened after authentication. Verify this exact user has Advanced Roadmaps and team-management access on %s.", identity, normalizeInstanceBaseURL(cfg.TargetBaseURL)))
	return &finding
}

func jiraUserIdentity(user CoreJiraUser) string {
	parts := make([]string, 0, 4)
	if strings.TrimSpace(user.Name) != "" {
		parts = append(parts, fmt.Sprintf("name=%q", user.Name))
	}
	if strings.TrimSpace(user.Key) != "" && user.Key != user.Name {
		parts = append(parts, fmt.Sprintf("key=%q", user.Key))
	}
	if strings.TrimSpace(user.DisplayName) != "" {
		parts = append(parts, fmt.Sprintf("displayName=%q", user.DisplayName))
	}
	parts = append(parts, fmt.Sprintf("active=%t", user.Active))
	if len(parts) == 0 {
		return "an unnamed active Jira user"
	}
	return strings.Join(parts, ", ")
}

func buildTeamMappings(cfg Config, sourceTeams, targetTeams []TeamDTO, sourcePlans []PlanDTO, planMappings []PlanMapping) ([]TeamMapping, []Finding) {
	targetByTitle := map[string][]TeamDTO{}
	targetByID := map[int64]TeamDTO{}
	for _, team := range targetTeams {
		targetByTitle[normalizeTitle(team.Title)] = append(targetByTitle[normalizeTitle(team.Title)], team)
		targetByID[team.ID] = team
	}

	planTitlesByTeamID := map[int64][]string{}
	for _, plan := range sourcePlans {
		for _, teamID := range plan.PlanTeams {
			planTitlesByTeamID[teamID] = append(planTitlesByTeamID[teamID], plan.Title)
		}
	}

	targetPlanExistsBySourceTeamID := map[int64]bool{}
	for _, mapping := range planMappings {
		hasTargetPlan := mapping.Decision == "merge"
		for _, teamID := range parseInt64List(mapping.SourcePlanTeamIDs) {
			if hasTargetPlan {
				targetPlanExistsBySourceTeamID[teamID] = true
			}
		}
	}

	var (
		findings                 []Finding
		skippedSharedCount       int
		skippedNonSharedCount    int
		manualPrerequisiteTitles []string
	)
	mappings := make([]TeamMapping, 0, len(sourceTeams))
	nextCreateOffset := 0
	for _, source := range sourceTeams {
		sameIDTarget, sameIDTargetExists := targetByID[source.ID]
		sameIDTitleMismatch := sameIDTargetExists && normalizeTitle(source.Title) != normalizeTitle(sameIDTarget.Title)
		matches := targetByTitle[normalizeTitle(source.Title)]
		planUsage := strings.Join(planTitlesByTeamID[source.ID], ", ")
		scopeReason := teamScopeSkipReason(cfg.TeamScope, source.Shareable)
		var mapping TeamMapping
		if scopeReason != "" {
			mapping = TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				Decision:        "skipped",
				Reason:          scopeReason,
			}
			if len(matches) == 1 {
				mapping.TargetTeamID = strconv.FormatInt(matches[0].ID, 10)
				mapping.TargetTitle = matches[0].Title
			}
			if source.Shareable {
				skippedSharedCount++
			} else {
				skippedNonSharedCount++
			}
		} else if !source.Shareable && len(matches) == 0 {
			reason := "non-shared team must be created manually in the destination plan before migration"
			if !targetPlanExistsBySourceTeamID[source.ID] {
				reason = "non-shared team requires a destination plan to exist first, then manual team creation before migration"
			}
			if planUsage != "" {
				reason = fmt.Sprintf("%s; source plan usage: %s", reason, planUsage)
			}
			mapping = TeamMapping{
				SourceTeamID:    source.ID,
				SourceTitle:     source.Title,
				SourceShareable: source.Shareable,
				TargetTitle:     source.Title,
				Decision:        "skipped",
				Reason:          reason,
			}
			manualPrerequisiteTitles = append(manualPrerequisiteTitles, source.Title)
		} else {
			switch len(matches) {
			case 0:
				nextCreateOffset++
				mapping = TeamMapping{
					SourceTeamID:    source.ID,
					SourceTitle:     source.Title,
					SourceShareable: source.Shareable,
					TargetTeamID:    expectedSequentialID(len(targetTeams), nextCreateOffset),
					TargetTitle:     source.Title,
					Decision:        "add",
				}
			case 1:
				mapping = TeamMapping{
					SourceTeamID:    source.ID,
					SourceTitle:     source.Title,
					SourceShareable: source.Shareable,
					TargetTeamID:    strconv.FormatInt(matches[0].ID, 10),
					TargetTitle:     matches[0].Title,
					Decision:        "merge",
				}
			default:
				mapping = TeamMapping{
					SourceTeamID:    source.ID,
					SourceTitle:     source.Title,
					SourceShareable: source.Shareable,
					Decision:        "conflict",
					Reason:          "multiple destination teams match normalized title",
					ConflictReason:  "multiple destination teams match normalized title",
				}
			}
		}

		if sameIDTitleMismatch {
			findings = append(findings, newFinding(SeverityWarning, "team_id_title_mismatch", fmt.Sprintf(
				"Source team %q (%d) has the same ID as destination team %q but a different title. Mitigation: %s",
				source.Title,
				source.ID,
				sameIDTarget.Title,
				teamIDTitleMismatchMitigation(mapping),
			)))
		}
		mappings = append(mappings, mapping)
	}
	if skippedSharedCount > 0 {
		findings = append(findings, newFinding(SeverityInfo, "team_scope_skipped_shared", fmt.Sprintf("Skipped %d shared teams because team scope is %s", skippedSharedCount, cfg.TeamScope)))
	}
	if skippedNonSharedCount > 0 {
		findings = append(findings, newFinding(SeverityInfo, "team_scope_skipped_non_shared", fmt.Sprintf("Skipped %d non-shared teams because team scope is %s", skippedNonSharedCount, cfg.TeamScope)))
	}
	if len(manualPrerequisiteTitles) > 0 {
		findings = append(findings, newFinding(SeverityWarning, "non_shared_team_manual_prerequisite", fmt.Sprintf("Skipped %d non-shared teams that must already exist in destination plans before migration: %s", len(manualPrerequisiteTitles), strings.Join(manualPrerequisiteTitles, ", "))))
	}
	return mappings, findings
}

func teamIDTitleMismatchMitigation(mapping TeamMapping) string {
	switch mapping.Decision {
	case "add", "created":
		return fmt.Sprintf("the tool will add %q as a separate destination team with a new ID instead of reusing the existing numeric ID match", mapping.SourceTitle)
	case "merge":
		return fmt.Sprintf("title-based matching will reuse destination team %q (%s), not the conflicting same-ID team", mapping.TargetTitle, mapping.TargetTeamID)
	case "skipped":
		if strings.TrimSpace(mapping.Reason) != "" {
			return fmt.Sprintf("this team is currently skipped: %s", mapping.Reason)
		}
		return "this team is currently skipped and will not reuse the conflicting same-ID destination team"
	case "conflict":
		return "automatic migration is blocked until the team mapping is resolved manually"
	default:
		return "the tool will use title-based mapping rather than the conflicting same numeric ID"
	}
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
	existingMemberships := map[string]struct{}{}
	targetPersonByID := map[int64]PersonDTO{}
	for _, person := range state.TargetPersons {
		targetPersonByID[person.ID] = person
	}
	for _, resource := range state.TargetResources {
		_, _, userID := resourcePersonDetails(resource, targetPersonByID)
		if userID == "" {
			continue
		}
		existingMemberships[fmt.Sprintf("%d:%s", resource.TeamID, userID)] = struct{}{}
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

		sourcePersonID := int64(0)
		if resource.Person != nil {
			sourcePersonID = resource.Person.ID
		}
		plan.SourcePersonID = sourcePersonID

		teamMapping, ok := mappingsBySourceTeam[resource.TeamID]
		if !ok || teamMapping.Decision == "conflict" || teamMapping.Decision == "skipped" {
			plan.Status = "skipped"
			plan.Reason = "team mapping missing or conflicted"
			if teamMapping.Reason != "" {
				plan.Reason = teamMapping.Reason
			}
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
		if _, exists := existingMemberships[fmt.Sprintf("%s:%s", plan.TargetTeamID, plan.TargetUserID)]; exists {
			plan.Status = "skipped"
			plan.Reason = "destination membership already exists"
			plans = append(plans, plan)
			findings = append(findings, newFinding(SeverityInfo, "destination_membership_exists", fmt.Sprintf("Skipping resource %d because %s is already a member of destination team %s", resource.ID, targetEmail, plan.TargetTeamName)))
			continue
		}
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
		case "skipped":
			status = "skipped"
		case "conflict":
			status = "skipped"
		}
		details := fmt.Sprintf("%s -> %s (%s)", mapping.SourceTitle, mapping.TargetTitle, mapping.Decision)
		if mapping.Reason != "" {
			details = fmt.Sprintf("%s -> %s (%s: %s)", mapping.SourceTitle, mapping.TargetTitle, mapping.Decision, mapping.Reason)
		}
		actions = append(actions, Action{
			Kind:     "team",
			SourceID: strconv.FormatInt(mapping.SourceTeamID, 10),
			TargetID: mapping.TargetTeamID,
			Status:   status,
			Details:  details,
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
			details = fmt.Sprintf("team=%s user=%s weeklyHours=%s", resource.TargetTeamID, resource.TargetUserID, formatWeeklyHours(resource.WeeklyHours))
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
	progress := newProgressTracker(0)
	defer progress.Finish()

	teamIDs := map[int64]int64{}
	teamTask := progress.BeginTask("Applying team migrations")
	for i := range state.TeamMappings {
		mapping := &state.TeamMappings[i]
		teamTask.Update(i+1, len(state.TeamMappings))
		teamTask.Detail(mapping.SourceTitle)
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
			mapping.TargetTitle = mapping.SourceTitle
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
	teamTask.Detail(fmt.Sprintf("%d processed, %d created", len(state.TeamMappings), countActionsByKindStatus(actions, "team", "created")))
	teamTask.Done()

	resourceTask := progress.BeginTask("Applying team memberships")
	for i := range state.ResourcePlans {
		resource := &state.ResourcePlans[i]
		resourceTask.Update(i+1, len(state.ResourcePlans))
		resourceTask.Detail(fmt.Sprintf("resource %d", resource.SourceResourceID))
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
	resourceTask.Detail(fmt.Sprintf("%d processed, %d created", len(state.ResourcePlans), countActionsByKindStatus(actions, "resource", "created")))
	resourceTask.Done()

	return actions, findings
}

func countActionsByKindStatus(actions []Action, kind, status string) int {
	count := 0
	for _, action := range actions {
		if action.Kind == kind && action.Status == status {
			count++
		}
	}
	return count
}

func applyPostMigrationCorrections(cfg Config, client *jiraClient, state *migrationState) ([]Action, []Finding) {
	var actions []Action
	var findings []Finding
	progress := newProgressTracker(countPostMigrationApplyTasks(cfg))
	defer progress.Finish()

	if issueTeamCorrectionsInScope(cfg) {
		issueTask := progress.BeginTask("Applying issue team rewrites")
		issueActions, issueFindings, issueResults := applyPostMigrationIssueCorrections(cfg, client, state, issueTask)
		actions = append(actions, issueActions...)
		findings = append(findings, issueFindings...)
		state.IssueUpdateResults = issueResults
		actions = append(actions, issueSkippedCorrectionActions(issueResults)...)
		if path, err := writePostMigrationIssueUpdateResultsExport(cfg, issueResults); err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_issue_results_export_failed", err.Error()))
		} else if path != "" {
			artifact := Artifact{
				Key:   "post_migrate_issue_update_results",
				Label: "Post-migration issue update results",
				Path:  path,
				Count: len(issueResults),
			}
			state.Artifacts = replaceArtifact(state.Artifacts, artifact)
			actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
			findings = append(findings, newFinding(SeverityInfo, "post_migrate_issue_results_export_generated", fmt.Sprintf("Generated post-migration issue update results: %s", path)))
		}
	}

	if cfg.ParentLinkInScope {
		parentLinkTask := progress.BeginTask("Applying Parent Link rewrites")
		parentLinkActions, parentLinkFindings, parentLinkResults := applyPostMigrationParentLinkCorrections(cfg, client, state, parentLinkTask)
		actions = append(actions, parentLinkActions...)
		findings = append(findings, parentLinkFindings...)
		state.ParentLinkUpdateResults = parentLinkResults
		actions = append(actions, parentLinkSkippedCorrectionActions(parentLinkResults)...)
		if path, err := writePostMigrationParentLinkUpdateResultsExport(cfg, parentLinkResults); err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_results_export_failed", err.Error()))
		} else if path != "" {
			artifact := Artifact{
				Key:   "post_migrate_parent_link_update_results",
				Label: "Post-migration parent-link update results",
				Path:  path,
				Count: len(parentLinkResults),
			}
			state.Artifacts = replaceArtifact(state.Artifacts, artifact)
			actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
			findings = append(findings, newFinding(SeverityInfo, "post_migrate_parent_link_results_export_generated", fmt.Sprintf("Generated post-migration parent-link update results: %s", path)))
		}
	}

	if cfg.FilterTeamIDsInScope {
		filterTask := progress.BeginTask("Applying filter rewrites")
		filterActions, filterFindings, filterResults := applyPostMigrationFilterCorrections(cfg, client, state, filterTask)
		actions = append(actions, filterActions...)
		findings = append(findings, filterFindings...)
		state.FilterUpdateResults = filterResults
		actions = append(actions, filterSkippedCorrectionActions(filterResults)...)
		if path, err := writePostMigrationFilterUpdateResultsExport(cfg, filterResults); err != nil {
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_results_export_failed", err.Error()))
		} else if path != "" {
			artifact := Artifact{
				Key:   "post_migrate_filter_update_results",
				Label: "Post-migration filter update results",
				Path:  path,
				Count: len(filterResults),
			}
			state.Artifacts = replaceArtifact(state.Artifacts, artifact)
			actions = append(actions, Action{Kind: artifact.Key, Status: "generated", Details: artifact.Path})
			findings = append(findings, newFinding(SeverityInfo, "post_migrate_filter_results_export_generated", fmt.Sprintf("Generated post-migration filter update results: %s", path)))
		}
	}

	return actions, findings
}

func countPostMigrationApplyTasks(cfg Config) int {
	total := 0
	if issueTeamCorrectionsInScope(cfg) {
		total++
	}
	if cfg.ParentLinkInScope {
		total++
	}
	if cfg.FilterTeamIDsInScope {
		total++
	}
	return total
}

func issueSkippedCorrectionActions(rows []PostMigrationIssueResultRow) []Action {
	actions := make([]Action, 0)
	for _, row := range rows {
		if row.Status == "" || row.Status == "updated" {
			continue
		}
		actions = append(actions, Action{
			Kind:     "post_migrate_issue_update",
			SourceID: row.IssueKey,
			Status:   "skipped",
			Details:  postMigrationSkippedCorrectionDetails(row.Status, row.Message),
		})
	}
	return actions
}

func parentLinkSkippedCorrectionActions(rows []PostMigrationParentLinkResultRow) []Action {
	actions := make([]Action, 0)
	for _, row := range rows {
		if row.Status == "" || row.Status == "updated" {
			continue
		}
		actions = append(actions, Action{
			Kind:     "post_migrate_parent_link_update",
			SourceID: row.IssueKey,
			TargetID: row.TargetParentIssueID,
			Status:   "skipped",
			Details:  postMigrationSkippedCorrectionDetails(row.Status, row.Message),
		})
	}
	return actions
}

func filterSkippedCorrectionActions(rows []PostMigrationFilterResultRow) []Action {
	actions := make([]Action, 0)
	for _, row := range rows {
		if row.Status == "" || row.Status == "updated" {
			continue
		}
		actions = append(actions, Action{
			Kind:     "post_migrate_filter_update",
			SourceID: row.SourceFilterID,
			TargetID: row.TargetFilterID,
			Status:   "skipped",
			Details:  postMigrationSkippedCorrectionDetails(row.Status, row.Message),
		})
	}
	return actions
}

func postMigrationSkippedCorrectionDetails(status, message string) string {
	status = strings.TrimSpace(status)
	message = strings.TrimSpace(message)
	if status == "" {
		return message
	}
	if message == "" {
		return status
	}
	return fmt.Sprintf("%s: %s", status, message)
}

func applyPostMigrationIssueCorrections(cfg Config, client *jiraClient, state *migrationState, progress *progressTask) ([]Action, []Finding, []PostMigrationIssueResultRow) {
	if len(state.IssueComparisons) == 0 {
		progress.Done()
		return nil, nil, nil
	}

	targetTeamIDs := teamTargetIDsBySourceID(state.TeamMappings)
	sourceIssueKeys := issueTeamSourceIssueKeySet(state.IssueTeamRows)
	rowResults := applyPostMigrationIssueCorrectionsWithFallback(cfg, client, state.IssueComparisons, targetTeamIDs, sourceIssueKeys, progress)
	results := make([]PostMigrationIssueResultRow, 0, len(rowResults))
	actions := make([]Action, 0)
	findings := make([]Finding, 0)
	for _, rowResult := range rowResults {
		results = append(results, rowResult.result)
		actions = append(actions, rowResult.actions...)
		findings = append(findings, rowResult.findings...)
	}

	updated := countIssueResultStatus(results, "updated")
	progress.Detail(fmt.Sprintf("%d processed, %d updated, %d skipped", len(results), updated, len(results)-updated))
	progress.Done()
	return actions, findings, results
}

type postMigrationIssueApplyResult struct {
	result   PostMigrationIssueResultRow
	actions  []Action
	findings []Finding
	retry429 bool
}

func applyPostMigrationIssueCorrectionsWithFallback(cfg Config, client *jiraClient, comparisons []PostMigrationIssueComparisonRow, targetTeamIDs map[string]string, sourceIssueKeys map[string]struct{}, progress *progressTask) []postMigrationIssueApplyResult {
	applyDefaultPostMigrateIssueWorkers(&cfg)
	results := make([]postMigrationIssueApplyResult, len(comparisons))
	allIndexes := make([]int, len(comparisons))
	for index := range comparisons {
		allIndexes[index] = index
	}

	workerCount := cfg.PostMigrateIssueWorkers
	processed := 0
	for len(allIndexes) > 0 {
		batchSize := adaptiveIssueApplyBatchSize(workerCount, len(allIndexes))
		batchIndexes := allIndexes[:batchSize]
		allIndexes = allIndexes[batchSize:]

		batchResults := applyPostMigrationIssueCorrectionsConcurrent(cfg, client, comparisons, targetTeamIDs, sourceIssueKeys, progress, batchIndexes, workerCount, false)
		retryIndexes := postMigrationIssueRetryIndexes(batchResults, batchIndexes)
		batchHadFailure := postMigrationIssueBatchHadFailure(batchResults, batchIndexes)
		if len(retryIndexes) > 0 {
			var retryResults []postMigrationIssueApplyResult
			retryResults, workerCount = retryPostMigrationIssue429s(cfg, client, comparisons, targetTeamIDs, sourceIssueKeys, progress, retryIndexes, workerCount)
			for _, index := range retryIndexes {
				batchResults[index] = retryResults[index]
			}
			batchHadFailure = batchHadFailure || postMigrationIssueBatchHadFailure(batchResults, retryIndexes)
		} else if !batchHadFailure && len(allIndexes) > 0 {
			nextWorkerCount := nextPostMigrationIssueWorkerCount(workerCount)
			if nextWorkerCount != workerCount {
				workerCount = nextWorkerCount
				progress.Detail(fmt.Sprintf("increasing issue apply concurrency to %d workers", workerCount))
			}
		}

		for _, index := range batchIndexes {
			results[index] = batchResults[index]
		}
		processed += len(batchIndexes)
		progress.Update(processed, len(comparisons))
	}
	return results
}

func adaptiveIssueApplyBatchSize(workerCount, remaining int) int {
	if workerCount < 1 {
		workerCount = 1
	}
	size := workerCount * 10
	if size < 50 {
		size = 50
	}
	if size > remaining {
		return remaining
	}
	return size
}

func nextPostMigrationIssueWorkerCount(workerCount int) int {
	if workerCount < 1 {
		return 1
	}
	if workerCount >= postMigrationIssueApplyMaxWorkers {
		return postMigrationIssueApplyMaxWorkers
	}
	increase := workerCount / 2
	if increase < 1 {
		increase = 1
	}
	next := workerCount + increase
	if next > postMigrationIssueApplyMaxWorkers {
		return postMigrationIssueApplyMaxWorkers
	}
	return next
}

func reducedPostMigrationIssueWorkerCount(current, minimum int) int {
	if current <= minimum {
		return minimum
	}
	reduced := current / 2
	if reduced < minimum {
		return minimum
	}
	return reduced
}

func postMigrationIssueRetryIndexes(results []postMigrationIssueApplyResult, indexes []int) []int {
	retryIndexes := make([]int, 0)
	for _, index := range indexes {
		if results[index].retry429 {
			retryIndexes = append(retryIndexes, index)
		}
	}
	return retryIndexes
}

func postMigrationIssueBatchHadFailure(results []postMigrationIssueApplyResult, indexes []int) bool {
	for _, index := range indexes {
		if strings.Contains(results[index].result.Status, "failed") {
			return true
		}
	}
	return false
}

func retryPostMigrationIssue429s(cfg Config, client *jiraClient, comparisons []PostMigrationIssueComparisonRow, targetTeamIDs map[string]string, sourceIssueKeys map[string]struct{}, progress *progressTask, retryIndexes []int, workerCount int) ([]postMigrationIssueApplyResult, int) {
	results := make([]postMigrationIssueApplyResult, len(comparisons))
	originalWorkerCount := workerCount
	for len(retryIndexes) > 0 {
		nextWorkerCount := reducedPostMigrationIssueWorkerCount(workerCount, cfg.PostMigrateIssueFallbackWorkers)
		progress.Detail(fmt.Sprintf("retrying %d issue updates with %d workers after Jira 429 responses", len(retryIndexes), nextWorkerCount))
		retryResults := applyPostMigrationIssueCorrectionsConcurrent(cfg, client, comparisons, targetTeamIDs, sourceIssueKeys, progress, retryIndexes, nextWorkerCount, false)
		nextRetryIndexes := make([]int, 0)
		for _, index := range retryIndexes {
			result := retryResults[index]
			if result.retry429 && nextWorkerCount > cfg.PostMigrateIssueFallbackWorkers {
				nextRetryIndexes = append(nextRetryIndexes, index)
				continue
			}
			if result.result.Status == "updated" {
				result.result.Message = postMigrationIssueRetrySuccessMessage(cfg, originalWorkerCount, nextWorkerCount)
			} else if result.result.Message != "" {
				result.result.Message = fmt.Sprintf("%s; retried after reducing issue apply concurrency from %d to %d", result.result.Message, originalWorkerCount, nextWorkerCount)
			}
			results[index] = result
		}
		workerCount = nextWorkerCount
		retryIndexes = nextRetryIndexes
	}
	return results, workerCount
}

func applyPostMigrationIssueCorrectionsConcurrent(cfg Config, client *jiraClient, comparisons []PostMigrationIssueComparisonRow, targetTeamIDs map[string]string, sourceIssueKeys map[string]struct{}, progress *progressTask, indexes []int, workerCount int, updateProgress bool) []postMigrationIssueApplyResult {
	results := make([]postMigrationIssueApplyResult, len(comparisons))
	if len(indexes) == 0 {
		return results
	}
	if workerCount > len(indexes) {
		workerCount = len(indexes)
	}
	if workerCount < 1 {
		workerCount = 1
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	var progressMu sync.Mutex
	processed := 0

	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = applyPostMigrationIssueCorrection(cfg, client, comparisons[index], targetTeamIDs, sourceIssueKeys, progress)
				if updateProgress {
					progressMu.Lock()
					processed++
					progress.Update(processed, len(comparisons))
					progressMu.Unlock()
				}
			}
		}()
	}
	for _, index := range indexes {
		jobs <- index
	}
	close(jobs)
	wg.Wait()
	return results
}

func postMigrationIssueRetrySuccessMessage(cfg Config, fromWorkers, toWorkers int) string {
	if cfg.SkipPostMigrateDriftChecks {
		return fmt.Sprintf("Updated target issue from prepared comparison after retrying with reduced concurrency from %d to %d", fromWorkers, toWorkers)
	}
	return fmt.Sprintf("Updated target issue Teams field after retrying with reduced concurrency from %d to %d", fromWorkers, toWorkers)
}

func applyPostMigrationIssueCorrection(cfg Config, client *jiraClient, comparison PostMigrationIssueComparisonRow, targetTeamIDs map[string]string, sourceIssueKeys map[string]struct{}, progress *progressTask) postMigrationIssueApplyResult {
	progress.Detail(fmt.Sprintf("preparing %s", comparison.IssueKey))
	result := PostMigrationIssueResultRow{
		IssueKey:           comparison.IssueKey,
		SourceTeamsFieldID: comparison.SourceTeamsFieldID,
		TargetTeamsFieldID: comparison.TargetTeamsFieldID,
		SourceTeamIDs:      comparison.SourceTeamIDs,
		TargetTeamIDs:      comparison.TargetTeamIDs,
		Status:             comparison.Status,
		Message:            comparison.Reason,
	}

	if comparison.Status != "ready" {
		return postMigrationIssueApplyResult{result: result}
	}
	if len(sourceIssueKeys) == 0 {
		result.Status = "source_export_missing"
		result.Message = "No source issue/team export rows are available for this issue correction"
		return postMigrationIssueApplyResult{result: result}
	}
	if _, ok := sourceIssueKeys[strings.TrimSpace(comparison.IssueKey)]; !ok {
		result.Status = "not_in_source_export"
		result.Message = "This issue was not present in the source issue/team export"
		return postMigrationIssueApplyResult{result: result}
	}
	if !issueProjectInScope(cfg.IssueProjectScope, comparison.ProjectKey) {
		result.Status = "out_of_scope_project"
		result.Message = "This issue project is outside the configured issue project scope"
		return postMigrationIssueApplyResult{result: result}
	}

	sourceIDs := splitDelimitedValues(comparison.SourceTeamIDs)
	targetIDs := make([]string, 0, len(sourceIDs))
	replacements := map[string]string{}
	changedSourceIDs := make([]string, 0, len(sourceIDs))
	unresolved := false
	for _, sourceID := range sourceIDs {
		targetID := strings.TrimSpace(targetTeamIDs[sourceID])
		if targetID == "" {
			unresolved = true
			break
		}
		targetIDs = append(targetIDs, targetID)
		replacements[sourceID] = targetID
		if targetID != sourceID {
			changedSourceIDs = append(changedSourceIDs, sourceID)
		}
	}
	if unresolved {
		result.Status = "unresolved_team_mapping"
		result.Message = "No destination team ID was resolved for one or more source team IDs on this issue"
		return postMigrationIssueApplyResult{result: result}
	}

	if len(changedSourceIDs) == 0 {
		result.Status = "same_id"
		result.Message = "Source and target team IDs are identical; no issue update is needed"
		return postMigrationIssueApplyResult{result: result}
	}

	if cfg.SkipPostMigrateDriftChecks {
		progress.Detail(fmt.Sprintf("updating %s from prepared comparison", comparison.IssueKey))
		if err := client.UpdateIssueFieldsNoRetry429(comparison.IssueKey, map[string]any{comparison.TargetTeamsFieldID: issueTeamFieldValueFromIDs(targetIDs)}); err != nil {
			result.Status = "update_failed"
			result.Message = fmt.Sprintf("Could not update target issue from prepared comparison: %v", err)
			return postMigrationIssueApplyResult{
				result:   result,
				retry429: isJiraTooManyRequestsError(err),
				findings: []Finding{
					newFinding(SeverityWarning, "post_migrate_issue_update_failed", fmt.Sprintf("Could not update issue %s from prepared comparison: %v", comparison.IssueKey, err)),
				},
			}
		}
		result.Status = "updated"
		result.Message = "Updated target issue from prepared comparison without rechecking current target state"
		result.CurrentTargetTeamIDs = strings.Join(targetIDs, ",")
		return postMigrationIssueApplyResult{
			result: result,
			actions: []Action{{
				Kind:     "post_migrate_issue_update",
				SourceID: comparison.IssueKey,
				Status:   "updated",
				Details:  fmt.Sprintf("teams field %s -> %s", comparison.SourceTeamsFieldID, comparison.TargetTeamsFieldID),
			}},
		}
	}

	progress.Detail(fmt.Sprintf("checking %s", comparison.IssueKey))
	targetIssue, err := client.GetIssueNoRetry429(comparison.IssueKey, []string{comparison.TargetTeamsFieldID})
	if err != nil {
		result.Status = "fetch_failed"
		result.Message = fmt.Sprintf("Could not load current target issue state: %v", err)
		return postMigrationIssueApplyResult{
			result:   result,
			retry429: isJiraTooManyRequestsError(err),
			findings: []Finding{
				newFinding(SeverityWarning, "post_migrate_issue_fetch_failed", fmt.Sprintf("Could not fetch target issue %s before applying corrections: %v", comparison.IssueKey, err)),
			},
		}
	}
	raw := targetIssue.Fields[comparison.TargetTeamsFieldID]
	currentIDs := extractTeamFieldIDs(raw)
	result.CurrentTargetTeamIDs = strings.Join(currentIDs, ",")

	currentSet := toSet(currentIDs)
	targetSet := toSet(targetIDs)
	if setEquals(currentSet, targetSet) {
		result.Status = "already_rewritten"
		result.Message = "The target issue already contains the mapped destination team IDs"
		return postMigrationIssueApplyResult{result: result}
	}

	if len(currentIDs) > 0 {
		missingSource := false
		for _, sourceID := range changedSourceIDs {
			if _, ok := currentSet[sourceID]; !ok {
				missingSource = true
				break
			}
		}
		if missingSource {
			result.Status = "source_team_ids_not_found_in_target_issue"
			result.Message = "The current target issue Teams field does not contain all source team IDs that need rewriting"
			return postMigrationIssueApplyResult{result: result}
		}
	}

	var updateValue any
	if len(currentIDs) == 0 {
		updateValue = issueTeamFieldValueFromIDs(targetIDs)
	} else {
		rewrittenRaw, changed := rewriteTeamFieldIDs(raw, replacements)
		if !changed || reflect.DeepEqual(rewrittenRaw, raw) {
			result.Status = "no_change"
			result.Message = "Rewriting the target issue Teams field produced no change"
			return postMigrationIssueApplyResult{result: result}
		}
		updateValue = issueTeamFieldUpdateValue(raw, rewrittenRaw)
	}

	progress.Detail(fmt.Sprintf("updating %s", comparison.IssueKey))
	if err := client.UpdateIssueFieldsNoRetry429(comparison.IssueKey, map[string]any{comparison.TargetTeamsFieldID: updateValue}); err != nil {
		result.Status = "update_failed"
		result.Message = fmt.Sprintf("Could not update target issue: %v", err)
		return postMigrationIssueApplyResult{
			result:   result,
			retry429: isJiraTooManyRequestsError(err),
			findings: []Finding{
				newFinding(SeverityWarning, "post_migrate_issue_update_failed", fmt.Sprintf("Could not update issue %s: %v", comparison.IssueKey, err)),
			},
		}
	}

	result.Status = "updated"
	result.Message = "Updated target issue Teams field to the mapped destination team IDs"
	result.CurrentTargetTeamIDs = strings.Join(targetIDs, ",")
	return postMigrationIssueApplyResult{
		result: result,
		actions: []Action{{
			Kind:     "post_migrate_issue_update",
			SourceID: comparison.IssueKey,
			Status:   "updated",
			Details:  fmt.Sprintf("teams field %s -> %s", comparison.SourceTeamsFieldID, comparison.TargetTeamsFieldID),
		}},
	}
}

func isJiraTooManyRequestsError(err error) bool {
	var apiErr *jiraAPIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusTooManyRequests
}

func countIssueResultStatus(rows []PostMigrationIssueResultRow, status string) int {
	count := 0
	for _, row := range rows {
		if row.Status == status {
			count++
		}
	}
	return count
}

func applyPostMigrationParentLinkCorrections(cfg Config, client *jiraClient, state *migrationState, progress *progressTask) ([]Action, []Finding, []PostMigrationParentLinkResultRow) {
	if len(state.ParentLinkComparisons) == 0 {
		progress.Done()
		return nil, nil, nil
	}

	results := make([]PostMigrationParentLinkResultRow, 0, len(state.ParentLinkComparisons))
	actions := make([]Action, 0)
	findings := make([]Finding, 0)
	currentParentCache := map[string]JiraIssue{}
	sourceIssueKeys := parentLinkSourceIssueKeySet(state.ParentLinkRows)

	for i, comparison := range state.ParentLinkComparisons {
		progress.Update(i+1, len(state.ParentLinkComparisons))
		progress.Detail(fmt.Sprintf("preparing %s", comparison.IssueKey))
		result := PostMigrationParentLinkResultRow{
			IssueKey:                comparison.IssueKey,
			SourceParentLinkFieldID: comparison.SourceParentLinkFieldID,
			TargetParentLinkFieldID: comparison.TargetParentLinkFieldID,
			SourceParentIssueID:     comparison.SourceParentIssueID,
			SourceParentIssueKey:    comparison.SourceParentIssueKey,
			TargetParentIssueID:     comparison.TargetParentIssueID,
			TargetParentIssueKey:    comparison.TargetParentIssueKey,
			CurrentParentIssueID:    comparison.CurrentParentIssueID,
			CurrentParentIssueKey:   comparison.CurrentParentIssueKey,
			Status:                  comparison.Status,
			Message:                 comparison.Reason,
		}

		if comparison.Status != "ready" {
			results = append(results, result)
			continue
		}
		if len(sourceIssueKeys) == 0 {
			result.Status = "source_export_missing"
			result.Message = "No source Parent Link export rows are available for this correction"
			results = append(results, result)
			continue
		}
		if _, ok := sourceIssueKeys[strings.TrimSpace(comparison.IssueKey)]; !ok {
			result.Status = "not_in_source_export"
			result.Message = "This issue was not present in the source Parent Link export"
			results = append(results, result)
			continue
		}
		if !issueProjectInScope(cfg.IssueProjectScope, comparison.ProjectKey) {
			result.Status = "out_of_scope_project"
			result.Message = "This issue project is outside the configured issue project scope"
			results = append(results, result)
			continue
		}

		if cfg.SkipPostMigrateDriftChecks {
			progress.Detail(fmt.Sprintf("updating %s from prepared comparison", comparison.IssueKey))
			if err := client.UpdateIssueFields(comparison.IssueKey, map[string]any{
				comparison.TargetParentLinkFieldID: comparison.TargetParentIssueKey,
			}); err != nil {
				result.Status = "update_failed"
				result.Message = fmt.Sprintf("Could not update target issue Parent Link from prepared comparison: %v", err)
				results = append(results, result)
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_update_failed", fmt.Sprintf("Could not update Parent Link on issue %s from prepared comparison: %v", comparison.IssueKey, err)))
				continue
			}
			result.Status = "updated"
			result.Message = "Updated target issue Parent Link from prepared comparison without rechecking current target state"
			result.CurrentParentIssueID = comparison.TargetParentIssueID
			result.CurrentParentIssueKey = comparison.TargetParentIssueKey
			results = append(results, result)
			actions = append(actions, Action{
				Kind:     "post_migrate_parent_link_update",
				SourceID: comparison.IssueKey,
				TargetID: comparison.TargetParentIssueID,
				Status:   "updated",
				Details:  fmt.Sprintf("%s -> %s", comparison.SourceParentIssueKey, comparison.TargetParentIssueKey),
			})
			continue
		}

		progress.Detail(fmt.Sprintf("checking %s", comparison.IssueKey))
		targetChild, err := client.GetIssue(comparison.IssueKey, []string{comparison.TargetParentLinkFieldID})
		if err != nil {
			result.Status = "fetch_failed"
			result.Message = fmt.Sprintf("Could not load current target issue state: %v", err)
			results = append(results, result)
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_child_fetch_failed", fmt.Sprintf("Could not fetch target child issue %s before applying Parent Link correction: %v", comparison.IssueKey, err)))
			continue
		}

		currentParentID, currentParentKey, err := resolveIssueReference(client, targetChild.Fields[comparison.TargetParentLinkFieldID], currentParentCache)
		if err != nil {
			result.Status = "current_parent_lookup_failed"
			result.Message = fmt.Sprintf("Could not resolve the current target Parent Link issue reference: %v", err)
			results = append(results, result)
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_current_lookup_failed", fmt.Sprintf("Could not resolve current Parent Link for child %s: %v", comparison.IssueKey, err)))
			continue
		}
		result.CurrentParentIssueID = currentParentID
		result.CurrentParentIssueKey = currentParentKey

		if parentReferenceMatches(currentParentID, currentParentKey, comparison.TargetParentIssueID, comparison.TargetParentIssueKey) {
			result.Status = "already_rewritten"
			result.Message = "The target child issue already points to the mapped target parent issue"
			results = append(results, result)
			continue
		}

		progress.Detail(fmt.Sprintf("updating %s", comparison.IssueKey))
		if err := client.UpdateIssueFields(comparison.IssueKey, map[string]any{
			comparison.TargetParentLinkFieldID: comparison.TargetParentIssueKey,
		}); err != nil {
			result.Status = "update_failed"
			result.Message = fmt.Sprintf("Could not update target issue Parent Link: %v", err)
			results = append(results, result)
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_parent_link_update_failed", fmt.Sprintf("Could not update Parent Link on issue %s: %v", comparison.IssueKey, err)))
			continue
		}

		result.Status = "updated"
		result.Message = "Updated target issue Parent Link to the mapped target parent issue"
		result.CurrentParentIssueID = comparison.TargetParentIssueID
		result.CurrentParentIssueKey = comparison.TargetParentIssueKey
		results = append(results, result)
		actions = append(actions, Action{
			Kind:     "post_migrate_parent_link_update",
			SourceID: comparison.IssueKey,
			TargetID: comparison.TargetParentIssueID,
			Status:   "updated",
			Details:  fmt.Sprintf("%s -> %s", comparison.SourceParentIssueKey, comparison.TargetParentIssueKey),
		})
	}

	updated := countParentLinkResultStatus(results, "updated")
	progress.Detail(fmt.Sprintf("%d processed, %d updated, %d skipped", len(results), updated, len(results)-updated))
	progress.Done()
	return actions, findings, results
}

func countParentLinkResultStatus(rows []PostMigrationParentLinkResultRow, status string) int {
	count := 0
	for _, row := range rows {
		if row.Status == status {
			count++
		}
	}
	return count
}

type postMigrationFilterRewritePlan struct {
	SourceFilterID     string
	SourceFilterName   string
	SourceJQL          string
	TargetFilterID     string
	TargetFilterName   string
	CurrentTargetJQL   string
	RewrittenTargetJQL string
	Status             string
	Message            string
}

func applyPostMigrationFilterCorrections(cfg Config, client *jiraClient, state *migrationState, progress *progressTask) ([]Action, []Finding, []PostMigrationFilterResultRow) {
	if len(state.FilterComparisons) == 0 {
		progress.Done()
		return nil, nil, nil
	}

	filterByID := map[string]JiraFilter{}
	for _, filter := range state.TargetFilters {
		filterByID[filter.ID] = filter
	}

	plans := buildPostMigrationFilterRewritePlans(state.FilterComparisons, filterByID)
	results := make([]PostMigrationFilterResultRow, 0, len(plans))
	actions := make([]Action, 0, len(plans))
	findings := make([]Finding, 0)

	for i, plan := range plans {
		progress.Update(i+1, len(plans))
		progress.Detail(fmt.Sprintf("preparing filter %s", nonEmptyString(plan.TargetFilterID, plan.SourceFilterID)))
		result := PostMigrationFilterResultRow{
			SourceFilterID:     plan.SourceFilterID,
			SourceFilterName:   plan.SourceFilterName,
			SourceJQL:          plan.SourceJQL,
			TargetFilterID:     plan.TargetFilterID,
			TargetFilterName:   plan.TargetFilterName,
			CurrentTargetJQL:   plan.CurrentTargetJQL,
			RewrittenTargetJQL: plan.RewrittenTargetJQL,
			TargetJQLBefore:    plan.CurrentTargetJQL,
			TargetJQLAfter:     plan.RewrittenTargetJQL,
			Status:             plan.Status,
			Message:            plan.Message,
		}

		if plan.Status != "ready" {
			results = append(results, result)
			continue
		}

		if cfg.SkipPostMigrateDriftChecks {
			filter := filterByID[plan.TargetFilterID]
			name := nonEmptyString(filter.Name, plan.TargetFilterName)
			progress.Detail(fmt.Sprintf("updating filter %s from prepared comparison", plan.TargetFilterID))
			updated, err := client.UpdateFilter(plan.TargetFilterID, JiraFilterUpdatePayload{
				Name:        name,
				Description: filter.Description,
				JQL:         plan.RewrittenTargetJQL,
			})
			if err != nil {
				result.Status = "update_failed"
				result.Message = fmt.Sprintf("Could not update target filter from prepared comparison: %v", err)
				results = append(results, result)
				findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_update_failed", fmt.Sprintf("Could not update filter %s from prepared comparison: %v", plan.TargetFilterID, err)))
				continue
			}
			result.Status = "updated"
			result.Message = "Updated target filter from prepared comparison without rechecking current target state"
			result.TargetJQLBefore = plan.CurrentTargetJQL
			if updated != nil && strings.TrimSpace(updated.JQL) != "" {
				result.TargetJQLAfter = updated.JQL
			} else {
				result.TargetJQLAfter = plan.RewrittenTargetJQL
			}
			results = append(results, result)
			actions = append(actions, Action{
				Kind:     "post_migrate_filter_update",
				SourceID: plan.SourceFilterID,
				TargetID: plan.TargetFilterID,
				Status:   "updated",
				Details:  plan.TargetFilterName,
			})
			continue
		}

		progress.Detail(fmt.Sprintf("checking filter %s", plan.TargetFilterID))
		filter, err := client.GetFilter(plan.TargetFilterID)
		if err != nil {
			result.Status = "fetch_failed"
			result.Message = fmt.Sprintf("Could not load current target filter state: %v", err)
			results = append(results, result)
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_fetch_failed", fmt.Sprintf("Could not fetch target filter %s before applying corrections: %v", plan.TargetFilterID, err)))
			continue
		}

		if filter.JQL == plan.RewrittenTargetJQL {
			result.Status = "already_rewritten"
			result.Message = "The target filter already contains the rewritten destination team IDs"
			result.CurrentTargetJQL = filter.JQL
			result.TargetJQLBefore = filter.JQL
			result.TargetJQLAfter = filter.JQL
			results = append(results, result)
			continue
		}
		if filter.JQL != plan.CurrentTargetJQL {
			result.Status = "drifted"
			result.Message = "The current target filter JQL has changed since the comparison artifact was generated"
			result.CurrentTargetJQL = filter.JQL
			result.TargetJQLBefore = filter.JQL
			results = append(results, result)
			continue
		}

		progress.Detail(fmt.Sprintf("updating filter %s", plan.TargetFilterID))
		updated, err := client.UpdateFilter(plan.TargetFilterID, JiraFilterUpdatePayload{
			Name:        filter.Name,
			Description: filter.Description,
			JQL:         plan.RewrittenTargetJQL,
		})
		if err != nil {
			result.Status = "update_failed"
			result.Message = fmt.Sprintf("Could not update target filter: %v", err)
			results = append(results, result)
			findings = append(findings, newFinding(SeverityWarning, "post_migrate_filter_update_failed", fmt.Sprintf("Could not update filter %s: %v", plan.TargetFilterID, err)))
			continue
		}

		result.Status = "updated"
		result.Message = "Updated target filter JQL to the mapped destination team IDs"
		result.TargetJQLBefore = plan.CurrentTargetJQL
		if updated != nil && strings.TrimSpace(updated.JQL) != "" {
			result.TargetJQLAfter = updated.JQL
		} else {
			result.TargetJQLAfter = plan.RewrittenTargetJQL
		}
		result.CurrentTargetJQL = plan.CurrentTargetJQL
		results = append(results, result)
		actions = append(actions, Action{
			Kind:     "post_migrate_filter_update",
			SourceID: plan.SourceFilterID,
			TargetID: plan.TargetFilterID,
			Status:   "updated",
			Details:  plan.TargetFilterName,
		})
	}

	updated := countFilterResultStatus(results, "updated")
	progress.Detail(fmt.Sprintf("%d processed, %d updated, %d skipped", len(results), updated, len(results)-updated))
	progress.Done()
	return actions, findings, results
}

func countFilterResultStatus(rows []PostMigrationFilterResultRow, status string) int {
	count := 0
	for _, row := range rows {
		if row.Status == status {
			count++
		}
	}
	return count
}

func buildPostMigrationFilterRewritePlans(rows []PostMigrationFilterComparisonRow, filters map[string]JiraFilter) []postMigrationFilterRewritePlan {
	grouped := map[string][]PostMigrationFilterComparisonRow{}
	blockedWithoutTarget := make([]postMigrationFilterRewritePlan, 0)
	for _, row := range rows {
		targetID := strings.TrimSpace(row.TargetFilterID)
		if targetID == "" {
			blockedWithoutTarget = append(blockedWithoutTarget, postMigrationFilterRewritePlan{
				SourceFilterID:     row.SourceFilterID,
				SourceFilterName:   row.SourceFilterName,
				SourceJQL:          row.SourceJQL,
				CurrentTargetJQL:   row.CurrentTargetJQL,
				RewrittenTargetJQL: row.RewrittenTargetJQL,
				Status:             row.Status,
				Message:            row.Reason,
			})
			continue
		}
		grouped[targetID] = append(grouped[targetID], row)
	}

	targetIDs := make([]string, 0, len(grouped))
	for targetID := range grouped {
		targetIDs = append(targetIDs, targetID)
	}
	sort.Strings(targetIDs)

	plans := append([]postMigrationFilterRewritePlan{}, blockedWithoutTarget...)
	for _, targetID := range targetIDs {
		group := grouped[targetID]
		representative := group[0]
		filter := filters[targetID]
		currentJQL := representative.CurrentTargetJQL
		if strings.TrimSpace(filter.JQL) != "" {
			currentJQL = filter.JQL
		}

		blockingReason := ""
		rewrittenJQL := currentJQL
		readyRows := make([]PostMigrationFilterComparisonRow, 0, len(group))
		noChangeRows := 0
		alreadyRewrittenRows := 0
		sort.SliceStable(group, func(i, j int) bool {
			return len(group[i].SourceClause) > len(group[j].SourceClause)
		})

		for _, row := range group {
			switch row.Status {
			case "ready":
				readyRows = append(readyRows, row)
			case "already_rewritten":
				noChangeRows++
				alreadyRewrittenRows++
				continue
			case "same_id", "no_change":
				noChangeRows++
				continue
			default:
				blockingReason = row.Reason
			}
			if blockingReason != "" {
				break
			}
		}

		if blockingReason != "" {
			plans = append(plans, postMigrationFilterRewritePlan{
				SourceFilterID:     representative.SourceFilterID,
				SourceFilterName:   representative.SourceFilterName,
				SourceJQL:          representative.SourceJQL,
				TargetFilterID:     representative.TargetFilterID,
				TargetFilterName:   representative.TargetFilterName,
				CurrentTargetJQL:   currentJQL,
				RewrittenTargetJQL: currentJQL,
				Status:             "blocked",
				Message:            blockingReason,
			})
			continue
		}

		for _, clauseGroup := range groupReadyFilterRowsByClause(readyRows) {
			sourceClause := clauseGroup[0].SourceClause
			replacements := map[string]string{}
			for _, row := range clauseGroup {
				replacements[row.SourceTeamID] = row.TargetTeamID
			}
			if updated, changed := rewriteNumericTeamClausesInJQL(rewrittenJQL, replacements); changed {
				rewrittenJQL = updated
				continue
			}
			rewrittenClause := rewriteTeamIDNumericLiterals(sourceClause, replacements)
			if containsLiteralFold(rewrittenJQL, rewrittenClause) {
				continue
			}
			blockingReason = "the current target filter JQL no longer contains the expected source clause"
			break
		}

		status := "ready"
		message := "Target filter JQL is ready to be rewritten"
		if blockingReason != "" {
			status = "blocked"
			message = blockingReason
			rewrittenJQL = currentJQL
		} else if len(readyRows) == 0 && noChangeRows == len(group) && alreadyRewrittenRows > 0 {
			status = "already_rewritten"
			message = "The target filter already contains the rewritten destination team IDs"
		} else if rewrittenJQL == currentJQL {
			status = "no_change"
			message = "Rewriting the target filter JQL produced no change"
		}

		plans = append(plans, postMigrationFilterRewritePlan{
			SourceFilterID:     representative.SourceFilterID,
			SourceFilterName:   representative.SourceFilterName,
			SourceJQL:          representative.SourceJQL,
			TargetFilterID:     representative.TargetFilterID,
			TargetFilterName:   representative.TargetFilterName,
			CurrentTargetJQL:   currentJQL,
			RewrittenTargetJQL: rewrittenJQL,
			Status:             status,
			Message:            message,
		})
	}

	sort.SliceStable(plans, func(i, j int) bool {
		left := nonEmptyString(plans[i].TargetFilterID, plans[i].SourceFilterID)
		right := nonEmptyString(plans[j].TargetFilterID, plans[j].SourceFilterID)
		if left == right {
			return plans[i].SourceFilterName < plans[j].SourceFilterName
		}
		return left < right
	})
	return plans
}

func rewriteTeamIDNumericLiterals(value string, replacements map[string]string) string {
	if len(replacements) == 0 || value == "" {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	for i := 0; i < len(value); {
		if value[i] < '0' || value[i] > '9' {
			out.WriteByte(value[i])
			i++
			continue
		}
		start := i
		for i < len(value) && value[i] >= '0' && value[i] <= '9' {
			i++
		}
		token := value[start:i]
		if replacement, ok := replacements[token]; ok {
			out.WriteString(replacement)
		} else {
			out.WriteString(token)
		}
	}
	return out.String()
}

func rewriteNumericTeamClausesInJQL(jql string, replacements map[string]string) (string, bool) {
	if len(replacements) == 0 || jql == "" {
		return jql, false
	}

	var out strings.Builder
	out.Grow(len(jql))
	changed := false
	for i := 0; i < len(jql); {
		if isInsideQuotedText(jql, i) && !isLikelyQuotedFunctionArgumentClause(jql, i) {
			out.WriteByte(jql[i])
			i++
			continue
		}
		fieldEnd, ok := scanTeamField(jql, i)
		if !ok {
			out.WriteByte(jql[i])
			i++
			continue
		}

		operatorStart := skipASCIIWhitespace(jql, fieldEnd)
		if operatorStart >= len(jql) {
			out.WriteString(jql[i:])
			break
		}

		if jql[operatorStart] == '=' {
			valueStart := skipASCIIWhitespace(jql, operatorStart+1)
			valueEnd, replacement, replaced := scanReplaceableNumericOperand(jql, valueStart, replacements)
			if valueEnd == valueStart {
				out.WriteString(jql[i:fieldEnd])
				i = fieldEnd
				continue
			}
			out.WriteString(jql[i:valueStart])
			if replaced {
				out.WriteString(replacement)
				changed = true
			} else {
				out.WriteString(jql[valueStart:valueEnd])
			}
			i = valueEnd
			continue
		}

		if !hasWordAtFold(jql, operatorStart, "in") || !isJQLWordBoundary(jql, operatorStart+2) {
			out.WriteString(jql[i:fieldEnd])
			i = fieldEnd
			continue
		}

		openParen := skipASCIIWhitespace(jql, operatorStart+2)
		if openParen >= len(jql) || jql[openParen] != '(' {
			out.WriteString(jql[i:fieldEnd])
			i = fieldEnd
			continue
		}

		listEnd, rewrittenList, listChanged, ok := rewriteTeamInOperandList(jql, openParen, replacements)
		if !ok {
			out.WriteString(jql[i:fieldEnd])
			i = fieldEnd
			continue
		}
		out.WriteString(jql[i:openParen])
		out.WriteString(rewrittenList)
		changed = changed || listChanged
		i = listEnd
	}

	return out.String(), changed
}

func jqlHasNumericTeamClauseID(jql, id string) bool {
	if strings.TrimSpace(id) == "" {
		return false
	}
	_, changed := rewriteNumericTeamClausesInJQL(jql, map[string]string{id: id})
	return changed
}

func scanTeamField(s string, start int) (int, bool) {
	if start >= len(s) {
		return start, false
	}
	if s[start] == '"' || s[start] == '\'' {
		quote := s[start]
		fieldStart := start + 1
		fieldEnd := fieldStart
		for fieldEnd < len(s) && s[fieldEnd] != quote {
			fieldEnd++
		}
		if fieldEnd >= len(s) {
			return start, false
		}
		if isSupportedTeamFieldName(s[fieldStart:fieldEnd]) {
			return fieldEnd + 1, true
		}
		return start, false
	}

	if !isJQLWordBoundary(s, start-1) {
		return start, false
	}
	for _, field := range []string{"team", "teams"} {
		end := start + len(field)
		if hasWordAtFold(s, start, field) && isJQLWordBoundary(s, end) {
			return end, true
		}
	}
	if hasPrefixFold(s[start:], "cf[") {
		end := start + 3
		digitStart := end
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
		if end > digitStart && end < len(s) && s[end] == ']' {
			return end + 1, true
		}
	}
	return start, false
}

func isSupportedTeamFieldName(field string) bool {
	normalized := strings.ToLower(strings.TrimSpace(field))
	if normalized == "team" || normalized == "teams" {
		return true
	}
	if len(normalized) < 5 || !strings.HasPrefix(normalized, "cf[") || normalized[len(normalized)-1] != ']' {
		return false
	}
	for _, r := range normalized[3 : len(normalized)-1] {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func rewriteTeamInOperandList(s string, openParen int, replacements map[string]string) (int, string, bool, bool) {
	var out strings.Builder
	out.WriteByte('(')
	changed := false
	i := openParen + 1
	for i < len(s) {
		switch s[i] {
		case ')':
			out.WriteByte(')')
			return i + 1, out.String(), changed, true
		case '"', '\'':
			end, replacement, replaced := scanReplaceableNumericOperand(s, i, replacements)
			if end == i {
				out.WriteByte(s[i])
				i++
				continue
			}
			if replaced {
				out.WriteString(replacement)
				changed = true
			} else {
				out.WriteString(s[i:end])
			}
			i = end
		default:
			if s[i] >= '0' && s[i] <= '9' {
				if i > openParen+1 && !isNumericTeamOperandBoundary(s[i-1]) {
					out.WriteByte(s[i])
					i++
					continue
				}
				end, replacement, replaced := scanReplaceableNumericOperand(s, i, replacements)
				if end == i {
					out.WriteByte(s[i])
					i++
					continue
				}
				if replaced {
					out.WriteString(replacement)
					changed = true
				} else {
					out.WriteString(s[i:end])
				}
				i = end
				continue
			}
			out.WriteByte(s[i])
			i++
		}
	}
	return openParen, "", false, false
}

func scanReplaceableNumericOperand(s string, start int, replacements map[string]string) (int, string, bool) {
	if start >= len(s) {
		return start, "", false
	}
	quote := byte(0)
	digitStart := start
	if s[start] == '"' || s[start] == '\'' {
		quote = s[start]
		digitStart = start + 1
	}
	digitEnd := digitStart
	for digitEnd < len(s) && s[digitEnd] >= '0' && s[digitEnd] <= '9' {
		digitEnd++
	}
	if digitEnd == digitStart {
		return start, "", false
	}
	end := digitEnd
	if quote != 0 {
		if end >= len(s) || s[end] != quote {
			return start, "", false
		}
		end++
	} else if end < len(s) && !isNumericTeamOperandBoundary(s[end]) {
		return start, "", false
	}
	replacement, ok := replacements[s[digitStart:digitEnd]]
	if !ok {
		return end, "", false
	}
	if quote == 0 {
		return end, replacement, true
	}
	return end, string(quote) + replacement + string(quote), true
}

func skipASCIIWhitespace(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r', '\f':
			i++
		default:
			return i
		}
	}
	return i
}

func hasWordAtFold(s string, start int, word string) bool {
	end := start + len(word)
	return start >= 0 && end <= len(s) && strings.EqualFold(s[start:end], word)
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

func isJQLWordBoundary(s string, i int) bool {
	if i < 0 || i >= len(s) {
		return true
	}
	c := s[i]
	return !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')
}

func isNumericTeamOperandBoundary(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', ',', ')', '(', '"', '\'':
		return true
	default:
		return false
	}
}

func isInsideQuotedText(s string, pos int) bool {
	var quote byte
	escaped := false
	for i := 0; i < pos && i < len(s); i++ {
		c := s[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
		}
	}
	return quote != 0
}

func isLikelyQuotedFunctionArgumentClause(s string, pos int) bool {
	openingQuote := activeQuoteStart(s, pos)
	if openingQuote < 0 {
		return false
	}
	escapedQuote := activeEscapedQuoteStart(s, openingQuote+1, pos)
	if escapedQuote >= 0 {
		return quoteStartsFunctionArgument(s, escapedQuote)
	}
	return quoteStartsFunctionArgument(s, openingQuote)
}

func activeQuoteStart(s string, pos int) int {
	var quote byte
	quoteStart := -1
	escaped := false
	for i := 0; i < pos && i < len(s); i++ {
		c := s[i]
		if escaped {
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if c == quote {
				quote = 0
				quoteStart = -1
			}
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
			quoteStart = i
		}
	}
	return quoteStart
}

func activeEscapedQuoteStart(s string, start, pos int) int {
	quoteStart := -1
	for i := start; i+1 < pos && i+1 < len(s); i++ {
		if s[i] != '\\' || (s[i+1] != '"' && s[i+1] != '\'') {
			continue
		}
		if quoteStart >= 0 {
			quoteStart = -1
		} else {
			quoteStart = i + 1
		}
		i++
	}
	return quoteStart
}

func quoteStartsFunctionArgument(s string, quote int) bool {
	i := quote - 1
	if i >= 0 && s[i] == '\\' {
		i--
	}
	for i >= 0 {
		switch s[i] {
		case ' ', '\t', '\n', '\r', '\f':
			i--
			continue
		case '(':
			return true
		default:
			return false
		}
	}
	return false
}

func groupReadyFilterRowsByClause(rows []PostMigrationFilterComparisonRow) [][]PostMigrationFilterComparisonRow {
	grouped := map[string][]PostMigrationFilterComparisonRow{}
	for _, row := range rows {
		grouped[row.SourceClause] = append(grouped[row.SourceClause], row)
	}

	clauses := make([]string, 0, len(grouped))
	for clause := range grouped {
		clauses = append(clauses, clause)
	}
	sort.SliceStable(clauses, func(i, j int) bool {
		if len(clauses[i]) == len(clauses[j]) {
			return clauses[i] < clauses[j]
		}
		return len(clauses[i]) > len(clauses[j])
	})

	out := make([][]PostMigrationFilterComparisonRow, 0, len(clauses))
	for _, clause := range clauses {
		group := grouped[clause]
		sort.SliceStable(group, func(i, j int) bool {
			if len(group[i].SourceTeamID) != len(group[j].SourceTeamID) {
				return len(group[i].SourceTeamID) > len(group[j].SourceTeamID)
			}
			if group[i].SourceTeamID != group[j].SourceTeamID {
				return group[i].SourceTeamID < group[j].SourceTeamID
			}
			return group[i].TargetTeamID < group[j].TargetTeamID
		})
		out = append(out, group)
	}
	return out
}

func rewriteTeamFieldIDs(raw any, replacements map[string]string) (any, bool) {
	switch v := raw.(type) {
	case nil:
		return nil, false
	case string:
		trimmed := strings.TrimSpace(v)
		if targetID, ok := replacements[trimmed]; ok {
			return targetID, targetID != v
		}
		return v, false
	case float64:
		current := strconv.FormatInt(int64(v), 10)
		targetID, ok := replacements[current]
		if !ok {
			return v, false
		}
		targetNumeric, err := strconv.ParseInt(targetID, 10, 64)
		if err != nil {
			return v, false
		}
		return float64(targetNumeric), targetNumeric != int64(v)
	case []any:
		out := make([]any, len(v))
		changed := false
		for i, item := range v {
			rewritten, itemChanged := rewriteTeamFieldIDs(item, replacements)
			out[i] = rewritten
			changed = changed || itemChanged
		}
		if !changed {
			return v, false
		}
		return out, true
	case map[string]any:
		out := make(map[string]any, len(v))
		changed := false
		for key, value := range v {
			rewritten, valueChanged := rewriteTeamFieldIDs(value, replacements)
			out[key] = rewritten
			changed = changed || valueChanged
		}
		if !changed {
			return v, false
		}
		return out, true
	default:
		return v, false
	}
}

func issueTeamFieldUpdateValue(raw, rewritten any) any {
	rawIDs := extractTeamFieldIDs(raw)
	rewrittenIDs := extractTeamFieldIDs(rewritten)
	if len(rawIDs) == 1 && len(rewrittenIDs) == 1 && rawIDs[0] != rewrittenIDs[0] {
		return rewrittenIDs[0]
	}
	switch v := rewritten.(type) {
	case float64:
		return strconv.FormatInt(int64(v), 10)
	case string:
		return v
	default:
		return rewritten
	}
}

func issueTeamFieldValueFromIDs(ids []string) any {
	cleanIDs := uniqueTrimmedStrings(ids)
	if len(cleanIDs) == 1 {
		return cleanIDs[0]
	}
	return cleanIDs
}

func loadIdentityMappings(path string) (IdentityMapping, error) {
	if strings.TrimSpace(path) == "" {
		return IdentityMapping{}, nil
	}
	cleanPath, err := cleanInputFilePath("identity mapping", path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenInRoot(filepath.Dir(cleanPath), filepath.Base(cleanPath))
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
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return "", err
	}
	cfg.OutputDir = outputDir
	if err := ensureOutputDir(outputDir); err != nil {
		return "", err
	}

	const name = "identity-mapping.generated.csv"
	path := outputPathForName(cfg, name)
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
	if err := writer.Error(); err != nil {
		return "", err
	}
	if err := pruneOutputFamily(outputDir, name, outputRetentionLimit); err != nil {
		return "", err
	}
	return path, nil
}

func writeTeamIDMappingExport(cfg Config, mappings []TeamMapping) (string, error) {
	if len(mappings) == 0 {
		return "", nil
	}
	return writeCSVExport(
		cfg,
		"team-id-mapping.migration.csv",
		[]string{"Source Team ID", "Source Team Name", "Source Shareable", "Target Team ID", "Target Team Name", "Migration Status", "Reason", "Conflict Reason"},
		teamIDMappingRows(mappings),
	)
}

func teamIDMappingRows(mappings []TeamMapping) [][]string {
	rows := make([][]string, 0, len(mappings))
	for _, mapping := range mappings {
		targetTitle := strings.TrimSpace(mapping.TargetTitle)
		if targetTitle == "" && strings.TrimSpace(mapping.TargetTeamID) != "" {
			targetTitle = mapping.SourceTitle
		}
		rows = append(rows, []string{
			strconv.FormatInt(mapping.SourceTeamID, 10),
			mapping.SourceTitle,
			strconv.FormatBool(mapping.SourceShareable),
			mapping.TargetTeamID,
			targetTitle,
			mapping.Decision,
			mapping.Reason,
			mapping.ConflictReason,
		})
	}
	return rows
}

func writePostMigrationIssueTeamExport(cfg Config, rows []IssueTeamRow, mappings []TeamMapping) (string, error) {
	rows = issueTeamRowsInProjectScope(rows, cfg.IssueProjectScope)
	if len(rows) == 0 {
		return "", nil
	}
	return writeCSVExport(
		cfg,
		"issues-with-teams.post-migration.csv",
		[]string{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Source Team IDs", "Source Team Names", "Teams Field ID", "Target Team IDs"},
		postMigrationIssueTeamRows(rows, mappings),
	)
}

func writeTargetIssueSnapshotExport(cfg Config, rows []TargetIssueSnapshotRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.TargetTeamsFieldID,
			row.CurrentTargetTeamIDs,
		})
	}
	return writeCSVExport(
		cfg,
		"target-issues.snapshot.post-migration.csv",
		[]string{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Target Teams Field ID", "Current Target Team IDs"},
		records,
	)
}

func writePostMigrationIssueComparisonExport(cfg Config, rows []PostMigrationIssueComparisonRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.SourceTeamsFieldID,
			row.TargetTeamsFieldID,
			row.SourceTeamIDs,
			row.SourceTeamNames,
			row.TargetTeamIDs,
			row.CurrentTargetTeamIDs,
			row.Status,
			row.Reason,
		})
	}
	return writeCSVExport(
		cfg,
		"issue-team-comparison.post-migration.csv",
		[]string{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Source Teams Field ID", "Target Teams Field ID", "Source Team IDs", "Source Team Names", "Target Team IDs", "Current Target Team IDs", "Status", "Reason"},
		records,
	)
}

func writePostMigrationIssueUpdateResultsExport(cfg Config, rows []PostMigrationIssueResultRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.SourceTeamsFieldID,
			row.TargetTeamsFieldID,
			row.SourceTeamIDs,
			row.TargetTeamIDs,
			row.CurrentTargetTeamIDs,
			row.Status,
			row.Message,
		})
	}
	return writeCSVExport(
		cfg,
		"issue-update-results.post-migration.csv",
		[]string{"Issue Key", "Source Teams Field ID", "Target Teams Field ID", "Source Team IDs", "Target Team IDs", "Current Target Team IDs", "Status", "Message"},
		records,
	)
}

func writePostMigrationParentLinkExport(cfg Config, rows []ParentLinkRow) (string, error) {
	rows = parentLinkRowsInProjectScope(rows, cfg.IssueProjectScope)
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.IssueID,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.ParentLinkFieldID,
			row.SourceParentIssueID,
			row.SourceParentIssueKey,
			row.SourceParentSummary,
			row.SourceParentProjectKey,
			row.SourceParentIssueKey,
			"",
		})
	}
	return writeCSVExport(
		cfg,
		"issues-with-parent-link.post-migration.csv",
		[]string{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Source Parent Summary", "Source Parent Project Key", "Target Parent Issue Key", "Target Parent Issue ID"},
		records,
	)
}

func writeTargetParentLinkSnapshotExport(cfg Config, rows []TargetParentLinkSnapshotRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.IssueID,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.TargetParentLinkFieldID,
			row.CurrentParentIssueID,
			row.CurrentParentIssueKey,
		})
	}
	return writeCSVExport(
		cfg,
		"target-parent-link-issues.snapshot.post-migration.csv",
		[]string{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Target Parent Link Field ID", "Current Parent Issue ID", "Current Parent Issue Key"},
		records,
	)
}

func writePostMigrationParentLinkComparisonExport(cfg Config, rows []PostMigrationParentLinkComparisonRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.IssueID,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.SourceParentLinkFieldID,
			row.TargetParentLinkFieldID,
			row.SourceParentIssueID,
			row.SourceParentIssueKey,
			row.TargetParentIssueID,
			row.TargetParentIssueKey,
			row.CurrentParentIssueID,
			row.CurrentParentIssueKey,
			row.Status,
			row.Reason,
		})
	}
	return writeCSVExport(
		cfg,
		"parent-link-comparison.post-migration.csv",
		[]string{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Source Parent Link Field ID", "Target Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Target Parent Issue ID", "Target Parent Issue Key", "Current Parent Issue ID", "Current Parent Issue Key", "Status", "Reason"},
		records,
	)
}

func writePostMigrationParentLinkUpdateResultsExport(cfg Config, rows []PostMigrationParentLinkResultRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.SourceParentLinkFieldID,
			row.TargetParentLinkFieldID,
			row.SourceParentIssueID,
			row.SourceParentIssueKey,
			row.TargetParentIssueID,
			row.TargetParentIssueKey,
			row.CurrentParentIssueID,
			row.CurrentParentIssueKey,
			row.Status,
			row.Message,
		})
	}
	return writeCSVExport(
		cfg,
		"parent-link-update-results.post-migration.csv",
		[]string{"Issue Key", "Source Parent Link Field ID", "Target Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Target Parent Issue ID", "Target Parent Issue Key", "Current Parent Issue ID", "Current Parent Issue Key", "Status", "Message"},
		records,
	)
}

func postMigrationIssueTeamRows(rows []IssueTeamRow, mappings []TeamMapping) [][]string {
	targetTeamIDs := teamTargetIDsBySourceID(mappings)
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.IssueKey,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.SourceTeamIDs,
			row.SourceTeamNames,
			row.TeamsFieldID,
			strings.Join(mappedTargetTeamIDsForExport(row.SourceTeamIDs, targetTeamIDs), ","),
		})
	}
	return out
}

func writePostMigrationFilterTeamExport(cfg Config, rows []FilterTeamClauseRow, mappings []TeamMapping) (string, int, error) {
	records := postMigrationFilterTeamRows(rows, mappings)
	if len(records) == 0 {
		return "", 0, nil
	}
	path, err := writeCSVExport(
		cfg,
		"filters-with-team-clauses.post-migration.csv",
		[]string{"Filter ID", "Filter Name", "Owner", "Owner Email", "Match Type", "Clause Value", "Source Team ID", "Source Team Name", "Matched Clause", "JQL", "Target Team ID"},
		records,
	)
	if err != nil {
		return "", 0, err
	}
	return path, len(records), nil
}

func writeTargetFilterSnapshotExport(cfg Config, rows []TargetFilterSnapshotRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.TargetFilterID,
			row.TargetFilterName,
			row.TargetOwner,
			row.Description,
			row.JQL,
			row.ViewURL,
			row.SearchURL,
		})
	}
	return writeCSVExport(
		cfg,
		"target-filters.snapshot.post-migration.csv",
		[]string{"Target Filter ID", "Target Filter Name", "Target Owner", "Description", "JQL", "View URL", "Search URL"},
		records,
	)
}

func writePostMigrationFilterTargetMatchExport(cfg Config, rows []PostMigrationFilterMatchRow) (string, error) {
	return writeFilterTargetMatchExport(cfg, "filter-target-match.post-migration.csv", rows)
}

func writePreMigrationFilterTargetMatchExport(cfg Config, rows []PostMigrationFilterMatchRow) (string, error) {
	return writeFilterTargetMatchExport(cfg, "filter-target-match.pre-migration.csv", rows)
}

func writeFilterTargetMatchExport(cfg Config, name string, rows []PostMigrationFilterMatchRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.SourceFilterID,
			row.SourceFilterName,
			row.SourceOwner,
			row.TargetFilterID,
			row.TargetFilterName,
			row.TargetOwner,
			row.MatchMethod,
			row.Status,
			row.Reason,
		})
	}
	return writeCSVExport(
		cfg,
		name,
		[]string{"Source Filter ID", "Source Filter Name", "Source Owner", "Target Filter ID", "Target Filter Name", "Target Owner", "Match Method", "Status", "Reason"},
		records,
	)
}

func writePostMigrationFilterComparisonExport(cfg Config, rows []PostMigrationFilterComparisonRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.SourceFilterID,
			row.SourceFilterName,
			row.SourceOwner,
			row.SourceJQL,
			row.SourceClause,
			row.SourceTeamID,
			row.TargetFilterID,
			row.TargetFilterName,
			row.TargetOwner,
			row.TargetTeamID,
			row.CurrentTargetJQL,
			row.RewrittenTargetJQL,
			row.Status,
			row.Reason,
		})
	}
	return writeCSVExport(
		cfg,
		"filter-jql-comparison.post-migration.csv",
		[]string{"Source Filter ID", "Source Filter Name", "Source Owner", "Source JQL", "Source Clause", "Source Team ID", "Target Filter ID", "Target Filter Name", "Target Owner", "Target Team ID", "Current Target JQL", "Rewritten Target JQL", "Status", "Reason"},
		records,
	)
}

func writePostMigrationFilterUpdateResultsExport(cfg Config, rows []PostMigrationFilterResultRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.SourceFilterID,
			row.SourceFilterName,
			row.SourceJQL,
			row.TargetFilterID,
			row.TargetFilterName,
			row.TargetJQLBefore,
			row.RewrittenTargetJQL,
			row.TargetJQLAfter,
			row.Status,
			row.Message,
		})
	}
	return writeCSVExport(
		cfg,
		"filter-update-results.post-migration.csv",
		[]string{"Source Filter ID", "Source Filter Name", "Source JQL", "Target Filter ID", "Target Filter Name", "Target JQL Before", "Planned Rewritten Target JQL", "Target JQL After", "Status", "Message"},
		records,
	)
}

func postMigrationFilterTeamRows(rows []FilterTeamClauseRow, mappings []TeamMapping) [][]string {
	targetTeamIDs := teamTargetIDsBySourceID(mappings)
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		if row.MatchType != "team_id" {
			continue
		}
		out = append(out, []string{
			row.FilterID,
			row.FilterName,
			row.Owner,
			row.OwnerEmail,
			row.MatchType,
			row.ClauseValue,
			row.SourceTeamID,
			row.SourceTeamName,
			row.Clause,
			row.JQL,
			strings.TrimSpace(targetTeamIDs[strings.TrimSpace(row.SourceTeamID)]),
		})
	}
	return out
}

func teamTargetIDsBySourceID(mappings []TeamMapping) map[string]string {
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

func mappedTargetTeamIDsForExport(raw string, targetTeamIDs map[string]string) []string {
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

func loadTeams(baseURL, username, password, file string, progress func(current, total int)) ([]TeamDTO, error) {
	if file != "" {
		return loadJSONFile[TeamDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListTeams(progress)
}

func loadPersons(baseURL, username, password, file string, progress func(current, total int)) ([]PersonDTO, error) {
	if file != "" {
		return loadJSONFile[PersonDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPersons(progress)
}

func loadResources(baseURL, username, password, file string, progress func(current, total int)) ([]ResourceDTO, error) {
	if file != "" {
		return loadJSONFile[ResourceDTO](file)
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListResources(progress)
}

func loadPrograms(baseURL, username, password string, progress func(current, total int)) ([]ProgramDTO, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPrograms(progress)
}

func loadPlans(baseURL, username, password string, progress func(current, total int)) ([]PlanDTO, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, nil
	}
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return nil, err
	}
	return client.ListPlans(progress)
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

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func parseInt64List(value string) []int64 {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := splitDelimitedValues(value)
	out := make([]int64, 0, len(parts))
	for _, part := range parts {
		id, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err == nil {
			out = append(out, id)
		}
	}
	return out
}

func splitDelimitedValues(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ';'
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}
	return out
}

func toSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out[trimmed] = struct{}{}
	}
	return out
}

func setEquals(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func nonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func teamScopeSkipReason(scope string, shareable bool) string {
	switch scope {
	case "shared-only":
		if !shareable {
			return "skipped because this run is limited to shared teams"
		}
	case "non-shared-only":
		if shareable {
			return "skipped because this run is limited to non-shared teams"
		}
	}
	return ""
}

func expectedSequentialID(existingCount, createOffset int) string {
	return strconv.Itoa(existingCount + createOffset)
}

func enrichIssuesCSV(cfg Config, mappings []TeamMapping, teams []TeamDTO) (string, error) {
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return "", err
	}
	cfg.OutputDir = outputDir
	if err := ensureOutputDir(outputDir); err != nil {
		return "", err
	}
	issuesCSV, err := cleanInputFilePath("issues CSV", cfg.IssuesCSV)
	if err != nil {
		return "", err
	}
	file, err := os.OpenInRoot(filepath.Dir(issuesCSV), filepath.Base(issuesCSV))
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

	const name = "enriched-issues.csv"
	outputPath := outputPathForName(cfg, name)
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
	if err := writer.Error(); err != nil {
		return "", err
	}
	if err := pruneOutputFamily(outputDir, name, outputRetentionLimit); err != nil {
		return "", err
	}
	return outputPath, nil
}

func migrationMetadata(state migrationState) map[string]any {
	metadata := map[string]any{
		"imd": map[string]any{
			"programs":              state.ProgramMappings,
			"plans":                 state.PlanMappings,
			"teams":                 state.TeamMappings,
			"resources":             state.ResourcePlans,
			"issues":                state.IssueTeamRows,
			"parentLinks":           state.ParentLinkRows,
			"filters":               state.FilterTeamClauseRows,
			"targetIssues":          state.TargetIssueSnapshots,
			"issueComparisons":      state.IssueComparisons,
			"issueResults":          state.IssueUpdateResults,
			"targetParentLinks":     state.TargetParentLinkSnapshots,
			"parentLinkComparisons": state.ParentLinkComparisons,
			"parentLinkResults":     state.ParentLinkUpdateResults,
			"targetFilters":         state.TargetFilterSnapshots,
			"filterMatches":         state.FilterTargetMatches,
			"filterComparisons":     state.FilterComparisons,
			"filterResults":         state.FilterUpdateResults,
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
			"label": "Detailed pre-migration issue/team export",
			"path":  state.IssueExportPath,
			"count": len(state.IssueTeamRows),
		}
	}
	if state.IssueImportExportPath != "" {
		metadata["issueImportExport"] = map[string]any{
			"label": "Import-ready issue/team CSV",
			"path":  state.IssueImportExportPath,
			"count": len(state.IssueTeamRows),
		}
	}
	if state.MembershipExportPath != "" {
		metadata["membershipExport"] = map[string]any{
			"path":  state.MembershipExportPath,
			"count": len(state.ResourcePlans),
		}
	}
	if state.FilterScanExportPath != "" {
		metadata["filterScanExport"] = map[string]any{
			"label": "Filters with Team clauses",
			"path":  state.FilterScanExportPath,
			"count": len(state.FilterTeamClauseRows),
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
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return nil, err
	}
	cfg.OutputDir = outputDir
	if err := ensureOutputDir(outputDir); err != nil {
		return nil, err
	}

	artifacts := []Artifact{}
	add := func(key, label, name string, header []string, rows [][]string) error {
		if len(rows) == 0 {
			return nil
		}
		path, err := writeCSVExport(cfg, name, header, rows)
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

func findArtifactByKey(artifacts []Artifact, key string) (Artifact, bool) {
	for _, artifact := range artifacts {
		if artifact.Key == key {
			return artifact, true
		}
	}
	return Artifact{}, false
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

func writeCSVExport(cfg Config, name string, header []string, rows [][]string) (string, error) {
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return "", err
	}
	cfg.OutputDir = outputDir
	path := outputPathForName(cfg, name)
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
	if err := writer.Error(); err != nil {
		return "", err
	}
	if err := pruneOutputFamily(outputDir, name, outputRetentionLimit); err != nil {
		return "", err
	}
	return path, nil
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
		reason := mapping.Reason
		if reason == "" {
			reason = mapping.ConflictReason
		}
		rows = append(rows, []string{
			strconv.FormatInt(mapping.SourceTeamID, 10),
			mapping.SourceTitle,
			strconv.FormatBool(mapping.SourceShareable),
			mapping.TargetTeamID,
			mapping.TargetTitle,
			mapping.Decision,
			reason,
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
			formatWeeklyHours(resource.WeeklyHours),
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
			formatWeeklyHours(resource.WeeklyHours),
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
			formatWeeklyHours(plan.WeeklyHours),
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

func formatWeeklyHours(hours *float64) string {
	if hours == nil {
		return ""
	}
	return strconv.FormatFloat(*hours, 'f', -1, 64)
}

func exportIssuesWithTeamsField(cfg Config, client *jiraClient, sourceTeams []TeamDTO, progress *progressTracker) (*TeamsFieldSelection, []IssueTeamRow, string, string, []Finding) {
	fields, err := client.ListFields()
	if err != nil {
		return nil, nil, "", "", []Finding{newFinding(SeverityWarning, "teams_field_discovery_failed", fmt.Sprintf("Could not load Jira fields: %v", err))}
	}

	selection, findings := selectTeamsField(fields)
	if selection == nil {
		return nil, nil, "", "", findings
	}

	jql := scopedIssueJQL(cfg.IssueProjectScope, fmt.Sprintf(`"%s" is not EMPTY`, selection.Field.Name))
	issues, err := client.SearchIssues(jql, []string{"summary", "project", "projectType", selection.Field.ID}, func(current, total int) {
		if progress != nil {
			progress.UpdateCount(current, total)
		}
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "teams_field_issue_search_failed", fmt.Sprintf("Could not search issues for teams field %s: %v", selection.Field.ID, err)))
		return selection, nil, "", "", findings
	}

	rows := buildIssueTeamRows(issues, selection.Field, sourceTeams)
	if len(rows) == 0 {
		findings = append(findings, newFinding(SeverityInfo, "teams_field_no_issues", fmt.Sprintf("No issues found with a value for %s", selection.Field.Name)))
		return selection, rows, "", "", findings
	}

	detailedPath, importPath, err := writeIssueTeamExports(cfg, rows)
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "teams_field_issue_export_failed", err.Error()))
		return selection, rows, "", "", findings
	}

	findings = append(findings,
		newFinding(SeverityInfo, "teams_field_issue_exported", fmt.Sprintf("Exported %d issues with a value for %s to detailed CSV %s", len(rows), selection.Field.Name, detailedPath)),
		newFinding(SeverityInfo, "teams_field_issue_import_exported", fmt.Sprintf("Exported %d issues with a value for %s to import-ready CSV %s", len(rows), selection.Field.Name, importPath)),
	)
	return selection, rows, detailedPath, importPath, findings
}

func exportIssuesWithParentLink(cfg Config, client *jiraClient, progress *progressTracker) (JiraField, []ParentLinkRow, string, string, []Finding) {
	fields, err := client.ListFields()
	if err != nil {
		return JiraField{}, nil, "", "", []Finding{newFinding(SeverityWarning, "parent_link_field_discovery_failed", fmt.Sprintf("Could not load Jira fields: %v", err))}
	}

	field, findings := selectParentLinkField(fields)
	if field == nil {
		return JiraField{}, nil, "", "", findings
	}

	parentLinkIssueTypes, typeFindings := resolveParentLinkIssueTypes(client)
	findings = append(findings, typeFindings...)

	jql := buildParentLinkIssueJQL(cfg.IssueProjectScope, field.Name, parentLinkIssueTypes)
	issues, err := client.SearchIssues(jql, []string{"summary", "project", "projectType", field.ID}, func(current, total int) {
		if progress != nil {
			progress.UpdateCount(current, total)
		}
	})
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "parent_link_issue_search_failed", fmt.Sprintf("Could not search issues for parent link field %s: %v", field.ID, err)))
		return *field, nil, "", "", findings
	}

	scopeProjects, err := normalizeIssueProjectScope(cfg.IssueProjectScope)
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "parent_link_scope_invalid", fmt.Sprintf("Could not apply Parent Link parent project scope: %v", err)))
	}

	rows, outOfScopeRows, rowFindings := buildParentLinkRows(client, issues, *field, scopeProjects)
	findings = append(findings, rowFindings...)
	outOfScopePath := ""
	if len(outOfScopeRows) > 0 {
		path, err := writeParentLinkOutOfScopeExport(cfg, outOfScopeRows)
		if err != nil {
			findings = append(findings, newFinding(SeverityWarning, "parent_link_out_of_scope_export_failed", err.Error()))
		} else if path != "" {
			outOfScopePath = path
			findings = append(findings, newFinding(SeverityInfo, "parent_link_out_of_scope_exported", fmt.Sprintf("Exported %d issues with out-of-scope Parent Link parents to %s", len(outOfScopeRows), path)))
		}
	}
	if len(rows) == 0 {
		findings = append(findings, newFinding(SeverityInfo, "parent_link_no_issues", fmt.Sprintf("No issues found with a value for %s", field.Name)))
		return *field, rows, "", outOfScopePath, findings
	}

	exportPath, err := writeParentLinkExport(cfg, rows)
	if err != nil {
		findings = append(findings, newFinding(SeverityWarning, "parent_link_export_failed", err.Error()))
		return *field, rows, "", outOfScopePath, findings
	}

	findings = append(findings, newFinding(SeverityInfo, "parent_link_exported", fmt.Sprintf("Exported %d issues with a value for %s to %s", len(rows), field.Name, exportPath)))
	return *field, rows, exportPath, outOfScopePath, findings
}

func resolveParentLinkIssueTypes(client *jiraClient) ([]string, []Finding) {
	const epicName = "Epic"

	issueTypes, err := client.ListIssueTypes()
	if err != nil {
		return []string{epicName}, []Finding{newFinding(SeverityWarning, "parent_link_issue_types_lookup_failed", fmt.Sprintf("Could not load Jira issue types for Parent Link recovery scope; falling back to %s only: %v", epicName, err))}
	}

	hierarchyLevels, err := client.ListHierarchyLevels(nil)
	if err != nil {
		return []string{epicName}, []Finding{newFinding(SeverityWarning, "parent_link_hierarchy_lookup_failed", fmt.Sprintf("Could not load Advanced Roadmaps hierarchy for Parent Link recovery scope; falling back to %s only: %v", epicName, err))}
	}

	issueTypeNamesByID := make(map[string]string, len(issueTypes))
	for _, issueType := range issueTypes {
		id := strings.TrimSpace(issueType.ID)
		name := strings.TrimSpace(issueType.Name)
		if id == "" || name == "" {
			continue
		}
		issueTypeNamesByID[id] = name
	}

	epicLevelID := int64(-1)
	for _, level := range hierarchyLevels {
		if strings.EqualFold(strings.TrimSpace(level.Title), epicName) {
			epicLevelID = level.ID
			break
		}
		for _, issueTypeID := range level.IssueTypeIDs {
			if strings.EqualFold(issueTypeNamesByID[strings.TrimSpace(issueTypeID)], epicName) {
				epicLevelID = level.ID
				break
			}
		}
		if epicLevelID >= 0 {
			break
		}
	}

	if epicLevelID < 0 {
		return []string{epicName}, []Finding{newFinding(SeverityWarning, "parent_link_hierarchy_epic_missing", fmt.Sprintf("Advanced Roadmaps hierarchy does not identify an %s level; falling back to %s only", epicName, epicName))}
	}

	names := []string{epicName}
	seen := map[string]struct{}{strings.ToUpper(epicName): struct{}{}}
	for _, level := range hierarchyLevels {
		if level.ID <= epicLevelID {
			continue
		}
		for _, issueTypeID := range level.IssueTypeIDs {
			name := strings.TrimSpace(issueTypeNamesByID[strings.TrimSpace(issueTypeID)])
			if name == "" {
				continue
			}
			key := strings.ToUpper(name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			names = append(names, name)
		}
	}

	return names, nil
}

func buildParentLinkIssueJQL(scope, fieldName string, issueTypes []string) string {
	typeClause := `type = Epic`
	cleanIssueTypes := uniqueTrimmedStrings(issueTypes)
	if len(cleanIssueTypes) > 0 {
		typeClause = fmt.Sprintf("type IN (%s)", quoteJQLValues(cleanIssueTypes))
	}
	return scopedIssueJQL(scope, fmt.Sprintf(`%s AND "%s" is not EMPTY`, typeClause, fieldName))
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

func selectParentLinkField(fields []JiraField) (*JiraField, []Finding) {
	candidates := make([]JiraField, 0)
	for _, field := range fields {
		if looksLikeParentLinkField(field) {
			candidates = append(candidates, field)
		}
	}
	if len(candidates) == 0 {
		return nil, []Finding{newFinding(SeverityWarning, "parent_link_field_missing", "Could not find a Jira issue field that looks like the Parent Link field")}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return scoreParentLinkFieldCandidate(candidates[i]) > scoreParentLinkFieldCandidate(candidates[j])
	})

	selected := candidates[0]
	var findings []Finding
	if len(candidates) > 1 {
		findings = append(findings, newFinding(SeverityInfo, "parent_link_multiple_candidates", fmt.Sprintf("Multiple Parent Link-like issue fields found; selected %s (%s)", selected.Name, selected.ID)))
	}
	return &selected, findings
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

func looksLikeParentLinkField(field JiraField) bool {
	name := strings.ToLower(strings.TrimSpace(field.Name))
	if field.ID == "" || !field.Custom {
		return false
	}
	if field.Schema != nil {
		custom := strings.ToLower(field.Schema.Custom)
		if strings.Contains(custom, "parent") && (strings.Contains(custom, "portfolio") || strings.Contains(custom, "plans")) {
			return true
		}
		if strings.Contains(custom, "parentlink") || strings.Contains(custom, "parent-link") {
			return true
		}
	}
	return name == "parent link" || strings.Contains(name, "parent link")
}

func scoreParentLinkFieldCandidate(field JiraField) int {
	score := 0
	name := strings.ToLower(strings.TrimSpace(field.Name))
	if name == "parent link" {
		score += 100
	}
	if strings.Contains(name, "parent link") {
		score += 40
	}
	if field.Schema != nil {
		custom := strings.ToLower(field.Schema.Custom)
		if strings.Contains(custom, "portfolio") && strings.Contains(custom, "parent") {
			score += 80
		}
		if strings.Contains(custom, "plans") && strings.Contains(custom, "parent") {
			score += 70
		}
		if strings.Contains(custom, "parentlink") || strings.Contains(custom, "parent-link") {
			score += 60
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
		projectKey, projectName, projectType := issueProjectDetails(issue.Fields)
		summary := ""
		if rawSummary, ok := issue.Fields["summary"].(string); ok {
			summary = rawSummary
		}
		rows = append(rows, IssueTeamRow{
			IssueKey:        issue.Key,
			ProjectKey:      projectKey,
			ProjectName:     projectName,
			ProjectType:     projectType,
			Summary:         summary,
			TeamsFieldID:    field.ID,
			SourceTeamIDs:   strings.Join(teamIDs, ","),
			SourceTeamNames: strings.Join(names, ","),
		})
	}
	return rows
}

func buildParentLinkRows(client *jiraClient, issues []JiraIssue, field JiraField, scopeProjects []string) ([]ParentLinkRow, []ParentLinkRow, []Finding) {
	rows := make([]ParentLinkRow, 0, len(issues))
	outOfScopeRows := make([]ParentLinkRow, 0)
	findings := make([]Finding, 0)
	parentCache := map[string]JiraIssue{}
	parentScope := projectScopeSet(scopeProjects)

	for _, issue := range issues {
		ref := extractParentIssueReference(issue.Fields[field.ID])
		if ref.idOrKey == "" && ref.key == "" {
			continue
		}

		parentLookupKey := nonEmptyString(ref.key, ref.idOrKey)
		parentIssue, ok := parentCache[parentLookupKey]
		if !ok {
			fetched, err := client.GetIssue(parentLookupKey, []string{"summary", "project"})
			if err != nil {
				findings = append(findings, newFinding(SeverityWarning, "parent_link_parent_issue_fetch_failed", fmt.Sprintf("Could not fetch parent issue %s for child %s: %v", parentLookupKey, issue.Key, err)))
				continue
			}
			parentIssue = *fetched
			parentCache[parentLookupKey] = parentIssue
		}

		projectKey, projectName, projectType := issueProjectDetails(issue.Fields)
		parentProjectKey, _, _ := issueProjectDetails(parentIssue.Fields)
		summary := ""
		if rawSummary, ok := issue.Fields["summary"].(string); ok {
			summary = rawSummary
		}
		parentSummary := ""
		if rawSummary, ok := parentIssue.Fields["summary"].(string); ok {
			parentSummary = rawSummary
		}

		row := ParentLinkRow{
			IssueKey:               issue.Key,
			IssueID:                issue.ID,
			ProjectKey:             projectKey,
			ProjectName:            projectName,
			ProjectType:            projectType,
			Summary:                summary,
			ParentLinkFieldID:      field.ID,
			SourceParentIssueID:    nonEmptyString(ref.idOrKey, parentIssue.ID),
			SourceParentIssueKey:   nonEmptyString(ref.key, parentIssue.Key),
			SourceParentSummary:    parentSummary,
			SourceParentProjectKey: parentProjectKey,
		}
		if len(parentScope) > 0 {
			if _, ok := parentScope[strings.ToUpper(strings.TrimSpace(parentProjectKey))]; !ok {
				outOfScopeRows = append(outOfScopeRows, row)
				continue
			}
		}

		rows = append(rows, row)
	}

	return rows, outOfScopeRows, findings
}

func projectScopeSet(projects []string) map[string]struct{} {
	if len(projects) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(projects))
	for _, project := range projects {
		project = strings.ToUpper(strings.TrimSpace(project))
		if project == "" {
			continue
		}
		out[project] = struct{}{}
	}
	return out
}

func issueProjectInScope(scope, projectKey string) bool {
	projects, err := normalizeIssueProjectScope(scope)
	if err != nil || len(projects) == 0 {
		return true
	}
	_, ok := projectScopeSet(projects)[strings.ToUpper(strings.TrimSpace(projectKey))]
	return ok
}

func issueTeamRowsInProjectScope(rows []IssueTeamRow, scope string) []IssueTeamRow {
	projects, err := normalizeIssueProjectScope(scope)
	if err != nil || len(projects) == 0 {
		return rows
	}
	projectSet := projectScopeSet(projects)
	out := make([]IssueTeamRow, 0, len(rows))
	for _, row := range rows {
		if _, ok := projectSet[strings.ToUpper(strings.TrimSpace(row.ProjectKey))]; ok {
			out = append(out, row)
		}
	}
	return out
}

func parentLinkRowsInProjectScope(rows []ParentLinkRow, scope string) []ParentLinkRow {
	projects, err := normalizeIssueProjectScope(scope)
	if err != nil || len(projects) == 0 {
		return rows
	}
	projectSet := projectScopeSet(projects)
	out := make([]ParentLinkRow, 0, len(rows))
	for _, row := range rows {
		if _, ok := projectSet[strings.ToUpper(strings.TrimSpace(row.ProjectKey))]; ok {
			out = append(out, row)
		}
	}
	return out
}

func issueTeamSourceIssueKeySet(rows []IssueTeamRow) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.IssueKey)
		if key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}

func parentLinkSourceIssueKeySet(rows []ParentLinkRow) map[string]struct{} {
	out := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := strings.TrimSpace(row.IssueKey)
		if key != "" {
			out[key] = struct{}{}
		}
	}
	return out
}

func issueProjectDetails(fields map[string]any) (string, string, string) {
	projectKey := ""
	projectName := ""
	projectType := ""

	if project, ok := fields["project"].(map[string]any); ok {
		if key, ok := project["key"].(string); ok {
			projectKey = key
		}
		if name, ok := project["name"].(string); ok {
			projectName = name
		}
		if typeKey, ok := project["projectTypeKey"].(string); ok {
			projectType = typeKey
		}
		if projectType == "" {
			if typeValue, ok := project["projectType"].(string); ok {
				projectType = typeValue
			}
		}
	}
	if projectType == "" {
		if typeValue, ok := fields["projectType"].(string); ok {
			projectType = typeValue
		}
	}

	return projectKey, projectName, projectType
}

type parentIssueReference struct {
	idOrKey string
	key     string
}

func extractParentIssueReference(raw any) parentIssueReference {
	ref := parentIssueReference{}
	var walk func(any)
	walk = func(value any) {
		switch v := value.(type) {
		case string:
			trimmed := strings.TrimSpace(v)
			if trimmed == "" {
				return
			}
			if ref.idOrKey == "" {
				ref.idOrKey = trimmed
			}
			if ref.key == "" && !strings.Contains(trimmed, " ") && strings.Contains(trimmed, "-") {
				ref.key = trimmed
			}
		case float64:
			if ref.idOrKey == "" {
				ref.idOrKey = strconv.FormatInt(int64(v), 10)
			}
		case map[string]any:
			if nested, ok := v["key"]; ok && ref.key == "" {
				switch n := nested.(type) {
				case string:
					ref.key = strings.TrimSpace(n)
				}
			}
			for _, key := range []string{"id", "issueId", "value"} {
				if nested, ok := v[key]; ok && ref.idOrKey == "" {
					switch n := nested.(type) {
					case string:
						ref.idOrKey = strings.TrimSpace(n)
					case float64:
						ref.idOrKey = strconv.FormatInt(int64(n), 10)
					}
				}
			}
			for _, nested := range v {
				if ref.idOrKey != "" && ref.key != "" {
					break
				}
				walk(nested)
			}
		case []any:
			for _, nested := range v {
				if ref.idOrKey != "" && ref.key != "" {
					break
				}
				walk(nested)
			}
		}
	}
	walk(raw)
	return ref
}

func scopedIssueJQL(scope, clause string) string {
	projects, err := normalizeIssueProjectScope(scope)
	if err != nil || len(projects) == 0 {
		return clause
	}
	quoted := make([]string, 0, len(projects))
	for _, project := range projects {
		quoted = append(quoted, strconv.Quote(project))
	}
	return fmt.Sprintf("project in (%s) AND (%s)", strings.Join(quoted, ","), clause)
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

func writeIssueTeamExports(cfg Config, rows []IssueTeamRow) (string, string, error) {
	outputDir, err := cleanOutputDirPath(cfg.OutputDir)
	if err != nil {
		return "", "", err
	}
	cfg.OutputDir = outputDir
	if err := ensureOutputDir(outputDir); err != nil {
		return "", "", err
	}

	detailedPath, err := writeCSVExport(
		cfg,
		"issues-with-teams.pre-migration.csv",
		[]string{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Team ID", "Team Name", "Teams Field ID"},
		detailedIssueTeamRows(rows),
	)
	if err != nil {
		return "", "", err
	}

	importPath, err := writeCSVExport(
		cfg,
		"issues-with-teams.import-ready.csv",
		[]string{"Issue Key", "Project Key", "Project Name", "Project Type", "Summary", "Team ID"},
		importIssueTeamRows(rows),
	)
	if err != nil {
		return "", "", err
	}

	return detailedPath, importPath, nil
}

func writeParentLinkExport(cfg Config, rows []ParentLinkRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.IssueID,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.ParentLinkFieldID,
			row.SourceParentIssueID,
			row.SourceParentIssueKey,
			row.SourceParentSummary,
			row.SourceParentProjectKey,
		})
	}
	return writeCSVExport(
		cfg,
		"issues-with-parent-link.pre-migration.csv",
		[]string{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Source Parent Summary", "Source Parent Project Key"},
		records,
	)
}

func writeParentLinkFieldExport(cfg Config, name string, row ParentLinkFieldRow) (string, error) {
	if strings.TrimSpace(row.FieldID) == "" {
		return "", nil
	}
	return writeCSVExport(
		cfg,
		name,
		[]string{"Field ID", "Field Name", "Schema Custom", "Schema Type"},
		[][]string{{row.FieldID, row.FieldName, row.SchemaCustom, row.SchemaType}},
	)
}

func writeParentLinkOutOfScopeExport(cfg Config, rows []ParentLinkRow) (string, error) {
	if len(rows) == 0 {
		return "", nil
	}
	records := make([][]string, 0, len(rows))
	for _, row := range rows {
		records = append(records, []string{
			row.IssueKey,
			row.IssueID,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.ParentLinkFieldID,
			row.SourceParentIssueID,
			row.SourceParentIssueKey,
			row.SourceParentSummary,
			row.SourceParentProjectKey,
			"parent issue project is outside issue project scope",
		})
	}
	return writeCSVExport(
		cfg,
		"issues-with-parent-link.out-of-scope.pre-migration.csv",
		[]string{"Issue Key", "Issue ID", "Project Key", "Project Name", "Project Type", "Summary", "Parent Link Field ID", "Source Parent Issue ID", "Source Parent Issue Key", "Source Parent Summary", "Source Parent Project Key", "Reason"},
		records,
	)
}

func detailedIssueTeamRows(rows []IssueTeamRow) [][]string {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.IssueKey,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.SourceTeamIDs,
			row.SourceTeamNames,
			row.TeamsFieldID,
		})
	}
	return out
}

func importIssueTeamRows(rows []IssueTeamRow) [][]string {
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, []string{
			row.IssueKey,
			row.ProjectKey,
			row.ProjectName,
			row.ProjectType,
			row.Summary,
			row.SourceTeamIDs,
		})
	}
	return out
}
