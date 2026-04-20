package app

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestLoadAllFiltersFallsBackToFavouriteFiltersOnSearch404(t *testing.T) {
	client, err := newJiraClient("https://example.test", "", "")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}
	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/rest/api/2/filter/search":
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
					Header:     make(http.Header),
				}, nil
			case "/rest/api/2/filter/favourite":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`[{"id":"10000","name":"Favourite Filter","jql":"Team = 42"}]`)),
					Header:     make(http.Header),
				}, nil
			default:
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
					Header:     make(http.Header),
				}, nil
			}
		}),
	}

	filters, findings, err := loadAllFilters(client, nil)
	if err != nil {
		t.Fatalf("loadAllFilters returned error: %v", err)
	}
	if len(filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(filters))
	}
	if filters[0].ID != "10000" || filters[0].Name != "Favourite Filter" {
		t.Fatalf("unexpected filter payload: %#v", filters[0])
	}
	if len(findings) != 1 || findings[0].Code != "filter_search_endpoint_unsupported" {
		t.Fatalf("expected fallback warning, got %#v", findings)
	}
}

func TestVerifyTeamFilterScriptRunnerEndpointWithClientUsesResolvedFieldID(t *testing.T) {
	client, err := newJiraClient("https://example.test/jira", "admin", "secret")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}
	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/jira/rest/api/2/field":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`[{"id":"customfield_16604","name":"Team","custom":true,"schema":{"custom":"com.atlassian.rm:team","type":"array"}}]`)),
					Header:     make(http.Header),
				}, nil
			case "/jira/rest/scriptrunner/latest/custom/findTeamFiltersDB":
				if got := req.URL.Query().Get("enabled"); got != "true" {
					t.Fatalf("expected enabled=true, got %q", got)
				}
				if got := req.URL.Query().Get("lastId"); got != "0" {
					t.Fatalf("expected lastId=0, got %q", got)
				}
				if got := req.URL.Query().Get("limit"); got != "500" {
					t.Fatalf("expected limit=500, got %q", got)
				}
				if got := req.URL.Query().Get("teamFieldId"); got != "16604" {
					t.Fatalf("expected teamFieldId=16604, got %q", got)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader(`{"meta":{"matched":0},"results":[],"parseErrors":[]}`)),
					Header:     make(http.Header),
				}, nil
			default:
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("not found")),
					Header:     make(http.Header),
				}, nil
			}
		}),
	}

	endpointURL, fieldLabel, err := verifyTeamFilterScriptRunnerEndpointWithClient(client)
	if err != nil {
		t.Fatalf("verifyTeamFilterScriptRunnerEndpointWithClient returned error: %v", err)
	}
	if fieldLabel != "Team (16604)" {
		t.Fatalf("expected resolved field label Team (16604), got %q", fieldLabel)
	}
	wantURL := "https://example.test/jira/rest/scriptrunner/latest/custom/findTeamFiltersDB?enabled=true&lastId=0&limit=500&teamFieldId=16604"
	if endpointURL != wantURL {
		t.Fatalf("expected endpoint URL %q, got %q", wantURL, endpointURL)
	}
}

func TestLoadFiltersFromSourceCSVParsesExpectedColumns(t *testing.T) {
	path := t.TempDir() + "/source-filters.csv"
	content := "Filter ID,Filter Name,Owner,JQL\n10000,Platform Filter,Jane Doe,\"project = ABC AND Team = 42\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write source filter csv: %v", err)
	}

	filters, err := loadFiltersFromSourceCSV(path)
	if err != nil {
		t.Fatalf("loadFiltersFromSourceCSV returned error: %v", err)
	}
	if len(filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(filters))
	}
	if filters[0].ID != "10000" || filters[0].Name != "Platform Filter" || filters[0].JQL != "project = ABC AND Team = 42" {
		t.Fatalf("unexpected filter row: %#v", filters[0])
	}
	if owner := filterOwnerLabel(filters[0].Owner); owner != "Jane Doe" {
		t.Fatalf("expected owner Jane Doe, got %q", owner)
	}
}

func TestScanFiltersWithSourceCSVBuildsExport(t *testing.T) {
	path := t.TempDir() + "/source-filters.csv"
	content := "Filter ID,Filter Name,Owner,JQL\n10000,Platform Filter,Jane Doe,\"project = ABC AND Team = 42\"\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write source filter csv: %v", err)
	}

	cfg := Config{
		OutputDir:        t.TempDir(),
		OutputTimestamp:  "20260419-120000",
		FilterSourceCSV:  path,
		FilterDataSource: filterDataSourceDatabaseCSV,
	}
	teams := []TeamDTO{{ID: 42, Title: "Red Team"}}

	rows, exportPath, artifact, findings, err := scanFiltersWithSourceCSV(cfg, teams)
	if err != nil {
		t.Fatalf("scanFiltersWithSourceCSV returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 matched row, got %d", len(rows))
	}
	if rows[0].SourceTeamID != "42" || rows[0].SourceTeamName != "Red Team" {
		t.Fatalf("unexpected matched row: %#v", rows[0])
	}
	if artifact == nil || exportPath == "" {
		t.Fatalf("expected export artifact, got path=%q artifact=%#v", exportPath, artifact)
	}
	foundCSVFinding := false
	for _, finding := range findings {
		if finding.Code == "source_filters_loaded_csv" {
			foundCSVFinding = true
			break
		}
	}
	if !foundCSVFinding {
		t.Fatalf("expected source_filters_loaded_csv finding, got %#v", findings)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
