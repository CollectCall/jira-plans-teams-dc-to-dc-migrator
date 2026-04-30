package app

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func loadIssueTeamRowsFromExport(path string) ([]IssueTeamRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	rows := make([]IssueTeamRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 8 {
			return nil, fmt.Errorf("issue export row has %d column(s), expected 8", len(record))
		}
		rows = append(rows, IssueTeamRow{
			IssueKey:        record[0],
			ProjectKey:      record[1],
			ProjectName:     record[2],
			ProjectType:     record[3],
			Summary:         record[4],
			SourceTeamIDs:   record[5],
			SourceTeamNames: record[6],
			TeamsFieldID:    record[7],
		})
	}
	return rows, nil
}

func loadFilterTeamClauseRowsFromExport(path string) ([]FilterTeamClauseRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	rows := make([]FilterTeamClauseRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 9 {
			return nil, fmt.Errorf("filter export row has %d column(s), expected 9", len(record))
		}
		rows = append(rows, FilterTeamClauseRow{
			FilterID:       record[0],
			FilterName:     record[1],
			Owner:          record[2],
			MatchType:      record[3],
			ClauseValue:    record[4],
			SourceTeamID:   record[5],
			SourceTeamName: record[6],
			Clause:         record[7],
			JQL:            record[8],
		})
	}
	return rows, nil
}

func loadParentLinkRowsFromExport(path string) ([]ParentLinkRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	rows := make([]ParentLinkRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 11 {
			return nil, fmt.Errorf("parent link export row has %d column(s), expected 11", len(record))
		}
		rows = append(rows, ParentLinkRow{
			IssueKey:               record[0],
			IssueID:                record[1],
			ProjectKey:             record[2],
			ProjectName:            record[3],
			ProjectType:            record[4],
			Summary:                record[5],
			ParentLinkFieldID:      record[6],
			SourceParentIssueID:    record[7],
			SourceParentIssueKey:   record[8],
			SourceParentSummary:    record[9],
			SourceParentProjectKey: record[10],
		})
	}
	return rows, nil
}

func loadParentLinkFieldFromExport(path string) (*ParentLinkFieldRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	record := records[1]
	if len(record) < 2 {
		return nil, fmt.Errorf("parent link field export row has %d column(s), expected at least 2", len(record))
	}
	row := &ParentLinkFieldRow{
		FieldID:   record[0],
		FieldName: record[1],
	}
	if len(record) > 2 {
		row.SchemaCustom = record[2]
	}
	if len(record) > 3 {
		row.SchemaType = record[3]
	}
	return row, nil
}

func loadTargetIssueSnapshotRowsFromExport(path string) ([]TargetIssueSnapshotRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]TargetIssueSnapshotRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 7 {
			return nil, fmt.Errorf("target issue snapshot row has %d column(s), expected 7", len(record))
		}
		rows = append(rows, TargetIssueSnapshotRow{
			IssueKey:             record[0],
			ProjectKey:           record[1],
			ProjectName:          record[2],
			ProjectType:          record[3],
			Summary:              record[4],
			TargetTeamsFieldID:   record[5],
			CurrentTargetTeamIDs: record[6],
		})
	}
	return rows, nil
}

func loadPostMigrationIssueComparisonRowsFromExport(path string) ([]PostMigrationIssueComparisonRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]PostMigrationIssueComparisonRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 13 {
			return nil, fmt.Errorf("issue comparison row has %d column(s), expected 13", len(record))
		}
		rows = append(rows, PostMigrationIssueComparisonRow{
			IssueKey:             record[0],
			ProjectKey:           record[1],
			ProjectName:          record[2],
			ProjectType:          record[3],
			Summary:              record[4],
			SourceTeamsFieldID:   record[5],
			TargetTeamsFieldID:   record[6],
			SourceTeamIDs:        record[7],
			SourceTeamNames:      record[8],
			TargetTeamIDs:        record[9],
			CurrentTargetTeamIDs: record[10],
			Status:               record[11],
			Reason:               record[12],
		})
	}
	return rows, nil
}

func loadTargetParentLinkSnapshotRowsFromExport(path string) ([]TargetParentLinkSnapshotRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]TargetParentLinkSnapshotRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 9 {
			return nil, fmt.Errorf("target parent-link snapshot row has %d column(s), expected 9", len(record))
		}
		rows = append(rows, TargetParentLinkSnapshotRow{
			IssueKey:                record[0],
			IssueID:                 record[1],
			ProjectKey:              record[2],
			ProjectName:             record[3],
			ProjectType:             record[4],
			Summary:                 record[5],
			TargetParentLinkFieldID: record[6],
			CurrentParentIssueID:    record[7],
			CurrentParentIssueKey:   record[8],
		})
	}
	return rows, nil
}

func loadPostMigrationParentLinkComparisonRowsFromExport(path string) ([]PostMigrationParentLinkComparisonRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]PostMigrationParentLinkComparisonRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 16 {
			return nil, fmt.Errorf("parent-link comparison row has %d column(s), expected 16", len(record))
		}
		rows = append(rows, PostMigrationParentLinkComparisonRow{
			IssueKey:                record[0],
			IssueID:                 record[1],
			ProjectKey:              record[2],
			ProjectName:             record[3],
			ProjectType:             record[4],
			Summary:                 record[5],
			SourceParentLinkFieldID: record[6],
			TargetParentLinkFieldID: record[7],
			SourceParentIssueID:     record[8],
			SourceParentIssueKey:    record[9],
			TargetParentIssueID:     record[10],
			TargetParentIssueKey:    record[11],
			CurrentParentIssueID:    record[12],
			CurrentParentIssueKey:   record[13],
			Status:                  record[14],
			Reason:                  record[15],
		})
	}
	return rows, nil
}

func loadTargetFilterSnapshotRowsFromExport(path string) ([]TargetFilterSnapshotRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]TargetFilterSnapshotRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 7 {
			return nil, fmt.Errorf("target filter snapshot row has %d column(s), expected 7", len(record))
		}
		rows = append(rows, TargetFilterSnapshotRow{
			TargetFilterID:   record[0],
			TargetFilterName: record[1],
			TargetOwner:      record[2],
			Description:      record[3],
			JQL:              record[4],
			ViewURL:          record[5],
			SearchURL:        record[6],
		})
	}
	return rows, nil
}

func loadPostMigrationFilterMatchRowsFromExport(path string) ([]PostMigrationFilterMatchRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]PostMigrationFilterMatchRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 8 {
			return nil, fmt.Errorf("filter target match row has %d column(s), expected 8", len(record))
		}
		rows = append(rows, PostMigrationFilterMatchRow{
			SourceFilterID:   record[0],
			SourceFilterName: record[1],
			SourceOwner:      record[2],
			TargetFilterID:   record[3],
			TargetFilterName: record[4],
			TargetOwner:      record[5],
			Status:           record[6],
			Reason:           record[7],
		})
	}
	return rows, nil
}

func loadPostMigrationFilterComparisonRowsFromExport(path string) ([]PostMigrationFilterComparisonRow, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}
	rows := make([]PostMigrationFilterComparisonRow, 0, len(records)-1)
	for _, record := range records[1:] {
		if len(record) < 14 {
			return nil, fmt.Errorf("filter comparison row has %d column(s), expected 14", len(record))
		}
		rows = append(rows, PostMigrationFilterComparisonRow{
			SourceFilterID:     record[0],
			SourceFilterName:   record[1],
			SourceOwner:        record[2],
			SourceJQL:          record[3],
			SourceClause:       record[4],
			SourceTeamID:       record[5],
			TargetFilterID:     record[6],
			TargetFilterName:   record[7],
			TargetOwner:        record[8],
			TargetTeamID:       record[9],
			CurrentTargetJQL:   record[10],
			RewrittenTargetJQL: record[11],
			Status:             record[12],
			Reason:             record[13],
		})
	}
	return rows, nil
}

func loadTeamMappingsFromExport(path string) ([]TeamMapping, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	header := indexCSVHeader(records[0])
	required := []string{"sourceteamid", "decision"}
	if _, ok := header["migrationstatus"]; ok {
		required = []string{"sourceteamid", "migrationstatus"}
	}
	for _, key := range required {
		if _, ok := header[key]; !ok {
			return nil, fmt.Errorf("team mapping export is missing %q column", key)
		}
	}

	rows := make([]TeamMapping, 0, len(records)-1)
	for _, record := range records[1:] {
		sourceID, err := strconv.ParseInt(csvValue(record, header, "sourceteamid"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid source team ID %q: %w", csvValue(record, header, "sourceteamid"), err)
		}
		sourceTitle := csvValue(record, header, "sourcetitle")
		if sourceTitle == "" {
			sourceTitle = csvValue(record, header, "sourceteamname")
		}
		if sourceTitle == "" {
			return nil, fmt.Errorf("team mapping export row for source team %d is missing source title", sourceID)
		}
		targetTitle := csvValue(record, header, "targettitle")
		if targetTitle == "" {
			targetTitle = csvValue(record, header, "targetteamname")
		}
		if targetTitle == "" {
			targetTitle = csvValue(record, header, "destinationtitle")
		}
		decision := csvValue(record, header, "decision")
		if decision == "" {
			decision = csvValue(record, header, "migrationstatus")
		}
		targetTeamID := csvValue(record, header, "targetteamid")
		if targetTeamID == "" {
			targetTeamID = csvValue(record, header, "destinationteamid")
		}
		rows = append(rows, TeamMapping{
			SourceTeamID:    sourceID,
			SourceTitle:     sourceTitle,
			SourceShareable: parseCSVBool(csvValue(record, header, "sourceshareable")),
			TargetTeamID:    targetTeamID,
			TargetTitle:     targetTitle,
			Decision:        decision,
			Reason:          csvValue(record, header, "reason"),
			ConflictReason:  csvValue(record, header, "conflictreason"),
		})
	}
	return rows, nil
}

func loadResourcePlansFromExport(path string) ([]ResourcePlan, error) {
	records, err := readCSVRecordsFromFile(path)
	if err != nil {
		return nil, err
	}
	if len(records) <= 1 {
		return nil, nil
	}

	header := indexCSVHeader(records[0])
	for _, key := range []string{"sourceresourceid", "sourceteamid", "sourcepersonid"} {
		if _, ok := header[key]; !ok {
			return nil, fmt.Errorf("team membership mapping export is missing %q column", key)
		}
	}

	rows := make([]ResourcePlan, 0, len(records)-1)
	for _, record := range records[1:] {
		sourceResourceID, err := strconv.ParseInt(csvValue(record, header, "sourceresourceid"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid source resource ID %q: %w", csvValue(record, header, "sourceresourceid"), err)
		}
		sourceTeamID, err := strconv.ParseInt(csvValue(record, header, "sourceteamid"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid source team ID %q: %w", csvValue(record, header, "sourceteamid"), err)
		}
		sourcePersonID, err := strconv.ParseInt(csvValue(record, header, "sourcepersonid"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid source person ID %q: %w", csvValue(record, header, "sourcepersonid"), err)
		}
		status := csvValue(record, header, "status")
		if status == "" {
			status = "planned"
		}
		targetTeamID := csvValue(record, header, "targetteamid")
		if targetTeamID == "" {
			targetTeamID = csvValue(record, header, "destinationteamid")
		}
		targetTeamName := csvValue(record, header, "targetteamname")
		if targetTeamName == "" {
			targetTeamName = csvValue(record, header, "destinationteamname")
		}
		rows = append(rows, ResourcePlan{
			SourceResourceID: sourceResourceID,
			SourceTeamID:     sourceTeamID,
			SourceTeamName:   csvValue(record, header, "sourceteamname"),
			SourcePersonID:   sourcePersonID,
			SourceEmail:      csvValue(record, header, "sourceemail"),
			TargetEmail:      csvValue(record, header, "destinationemail"),
			TargetTeamID:     targetTeamID,
			TargetTeamName:   targetTeamName,
			TargetUserID:     csvValue(record, header, "destinationuserid"),
			WeeklyHours:      parseCSVFloatPtr(csvValue(record, header, "weeklyhours")),
			Status:           status,
			Reason:           csvValue(record, header, "reason"),
		})
	}
	return rows, nil
}

func indexCSVHeader(header []string) map[string]int {
	index := map[string]int{}
	for i, value := range header {
		index[normalizeCSVHeader(value)] = i
	}
	return index
}

func normalizeCSVHeader(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, "-", "")
	return value
}

func csvValue(record []string, header map[string]int, key string) string {
	idx, ok := header[key]
	if !ok || idx < 0 || idx >= len(record) {
		return ""
	}
	return strings.TrimSpace(record[idx])
}

func parseCSVBool(value string) bool {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	return err == nil && parsed
}

func parseCSVFloatPtr(value string) *float64 {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func readCSVRecordsFromFile(path string) ([][]string, error) {
	cleanPath, err := cleanInputFilePath("CSV input", path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenInRoot(filepath.Dir(cleanPath), filepath.Base(cleanPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}
