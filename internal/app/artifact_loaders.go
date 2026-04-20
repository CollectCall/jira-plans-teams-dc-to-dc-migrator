package app

import (
	"encoding/csv"
	"fmt"
	"os"
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

func readCSVRecordsFromFile(path string) ([][]string, error) {
	file, err := os.Open(path)
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
