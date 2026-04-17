package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPageSize     = 500
	teamsAPIMaxPageSize = 100
	jpoAPIMaxPageSize   = 100
)

type jiraClient struct {
	instanceBaseURL string
	baseURL         string
	username        string
	password        string
	httpClient      *http.Client
}

func newJiraClient(baseURL, username, password string) (*jiraClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("jira base URL is required")
	}
	return &jiraClient{
		instanceBaseURL: normalizeInstanceBaseURL(baseURL),
		baseURL:         normalizeAPIBaseURL(baseURL),
		username:        username,
		password:        password,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func normalizeInstanceBaseURL(raw string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	trimmed = strings.TrimSuffix(trimmed, "/rest/teams-api/1.0")
	return trimmed
}

func normalizeAPIBaseURL(raw string) string {
	trimmed := normalizeInstanceBaseURL(raw)
	if strings.Contains(strings.TrimRight(strings.TrimSpace(raw), "/"), "/rest/teams-api/1.0") {
		return strings.TrimRight(strings.TrimSpace(raw), "/")
	}
	return trimmed + "/rest/teams-api/1.0"
}

func (c *jiraClient) ListTeams() ([]TeamDTO, error) {
	return listPaged[TeamDTO](c, "/team")
}

func (c *jiraClient) ListPersons() ([]PersonDTO, error) {
	return listPaged[PersonDTO](c, "/person")
}

func (c *jiraClient) GetPerson(id int64) (*PersonDTO, error) {
	body, err := c.doJSON(http.MethodGet, "/person/"+strconv.FormatInt(id, 10), nil, nil)
	if err != nil {
		return nil, err
	}
	var person PersonDTO
	if err := json.Unmarshal(body, &person); err != nil {
		return nil, err
	}
	return &person, nil
}

func (c *jiraClient) ListResources() ([]ResourceDTO, error) {
	return listPaged[ResourceDTO](c, "/resource")
}

func (c *jiraClient) ListPrograms() ([]ProgramDTO, error) {
	return listJPOPaged[ProgramDTO](c, "/program")
}

func (c *jiraClient) ListPlans() ([]PlanDTO, error) {
	return listJPOPaged[PlanDTO](c, "/plan")
}

func (c *jiraClient) CreateTeam(team TeamDTO) (int64, error) {
	payload := map[string]any{
		"title":     team.Title,
		"shareable": team.Shareable,
	}
	return c.postForID("/team", payload)
}

func (c *jiraClient) CreateResource(teamID int64, jiraUserID string, weeklyHours float64) (int64, error) {
	candidates := []map[string]any{
		{
			"teamId":      teamID,
			"weeklyHours": weeklyHours,
			"person": map[string]any{
				"jiraUserId": jiraUserID,
			},
		},
		{
			"teamId":      teamID,
			"weeklyHours": weeklyHours,
			"person": map[string]any{
				"jiraUser": map[string]any{
					"jiraUserId": jiraUserID,
				},
			},
		},
	}

	var lastErr error
	for _, payload := range candidates {
		id, err := c.postForID("/resource", payload)
		if err == nil {
			return id, nil
		}
		lastErr = err
	}
	return 0, lastErr
}

func (c *jiraClient) ListFields() ([]JiraField, error) {
	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/field", nil, nil)
	if err != nil {
		return nil, err
	}
	var fields []JiraField
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func (c *jiraClient) SearchIssues(jql string, fields []string, progress func(current, total int)) ([]JiraIssue, error) {
	var all []JiraIssue
	startAt := 0
	for {
		query := url.Values{}
		query.Set("jql", jql)
		query.Set("startAt", strconv.Itoa(startAt))
		query.Set("maxResults", strconv.Itoa(defaultPageSize))
		if len(fields) > 0 {
			query.Set("fields", strings.Join(fields, ","))
		}
		body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/search", query, nil)
		if err != nil {
			return nil, err
		}
		var results JiraSearchResults
		if err := json.Unmarshal(body, &results); err != nil {
			return nil, err
		}
		if len(results.Issues) == 0 {
			break
		}
		all = append(all, results.Issues...)
		startAt += len(results.Issues)
		if progress != nil {
			progress(startAt, results.Total)
		}
		if startAt >= results.Total || len(results.Issues) < defaultPageSize {
			break
		}
	}
	return all, nil
}

func (c *jiraClient) SearchCoreUsers(queryText string) ([]CoreJiraUser, error) {
	query := url.Values{}
	query.Set("username", strings.TrimSpace(queryText))
	query.Set("includeActive", "true")
	query.Set("maxResults", "100")
	query.Set("startAt", "0")

	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/user/search", query, nil)
	if err != nil {
		return nil, err
	}
	var users []CoreJiraUser
	if err := json.Unmarshal(body, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func listPaged[T any](c *jiraClient, endpoint string) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("size", strconv.Itoa(teamsAPIMaxPageSize))

		body, err := c.doJSON(http.MethodGet, endpoint, query, nil)
		if err != nil {
			return nil, err
		}
		items, err := decodeCollection[T](body)
		if err != nil {
			return nil, fmt.Errorf("decoding %s page %d: %w", endpoint, page, err)
		}
		if len(items) == 0 {
			break
		}
		all = append(all, items...)
		if len(items) < teamsAPIMaxPageSize {
			break
		}
	}
	return all, nil
}

func (c *jiraClient) postForID(endpoint string, payload any) (int64, error) {
	body, err := c.doJSON(http.MethodPost, endpoint, nil, payload)
	if err != nil {
		return 0, err
	}
	return decodeID(body)
}

func (c *jiraClient) doJSON(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	return c.doJSONAgainstBase(c.baseURL, method, endpoint, query, payload)
}

func (c *jiraClient) doCoreJSON(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	return c.doJSONAgainstBase(c.instanceBaseURL, method, endpoint, query, payload)
}

func (c *jiraClient) doJPOJSON(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	return c.doJSONAgainstBase(c.instanceBaseURL, method, "/rest/jpo-api/1.0"+endpoint, query, payload)
}

func (c *jiraClient) doJSONAgainstBase(base, method, endpoint string, query url.Values, payload any) ([]byte, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = path.Join(u.Path, endpoint)
	if query != nil {
		u.RawQuery = query.Encode()
	}

	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s returned %d: %s", method, endpoint, resp.StatusCode, summarizeJiraError(data))
	}
	return data, nil
}

func listJPOPaged[T any](c *jiraClient, endpoint string) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("size", strconv.Itoa(jpoAPIMaxPageSize))

		body, err := c.doJPOJSON(http.MethodGet, endpoint, query, nil)
		if err != nil {
			return nil, err
		}
		items, err := decodeCollection[T](body)
		if err != nil {
			return nil, fmt.Errorf("decoding %s page %d: %w", endpoint, page, err)
		}
		if len(items) == 0 {
			break
		}
		all = append(all, items...)
		if len(items) < jpoAPIMaxPageSize {
			break
		}
	}
	return all, nil
}

func summarizeJiraError(data []byte) string {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "empty error response"
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err != nil {
		return truncateText(text, 300)
	}

	for _, key := range []string{"message", "error"} {
		if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}

	if errors, ok := payload["errorMessages"].([]any); ok && len(errors) > 0 {
		parts := make([]string, 0, len(errors))
		for _, item := range errors {
			if value, ok := item.(string); ok && strings.TrimSpace(value) != "" {
				parts = append(parts, value)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "; ")
		}
	}

	if exception, ok := payload["exception"].(map[string]any); ok {
		if value, ok := exception["message"].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}

	return truncateText(text, 300)
}

func truncateText(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max-3] + "..."
}

func decodeCollection[T any](data []byte) ([]T, error) {
	var direct []T
	if err := json.Unmarshal(data, &direct); err == nil {
		return direct, nil
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err == nil {
		for _, key := range []string{"values", "items", "content", "results", "data"} {
			if raw, ok := envelope[key]; ok {
				var nested []T
				if err := json.Unmarshal(raw, &nested); err == nil {
					return nested, nil
				}
			}
		}
	}

	var single T
	if err := json.Unmarshal(data, &single); err == nil {
		return []T{single}, nil
	}

	return nil, fmt.Errorf("unsupported collection payload")
}

func decodeID(data []byte) (int64, error) {
	var id int64
	if err := json.Unmarshal(data, &id); err == nil {
		return id, nil
	}

	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		return strconv.ParseInt(asString, 10, 64)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err == nil {
		for _, key := range []string{"id", "entityId", "value"} {
			if raw, ok := envelope[key]; ok {
				return decodeID(raw)
			}
		}
	}

	return 0, fmt.Errorf("unable to decode ID from response")
}
