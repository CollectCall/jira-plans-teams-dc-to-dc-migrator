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
