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
	defaultPageSize                          = 500
	teamsAPIMaxPageSize                      = 100
	jpoAPIMaxPageSize                        = 100
	teamFilterScriptRunnerEndpointPath       = "/rest/scriptrunner/latest/custom/findTeamFiltersDB"
	teamFilterScriptRunnerScriptPath         = "scripts/sourceFindTeamFiltersDB.groovy"
	targetTeamFilterScriptRunnerEndpointPath = "/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB"
	targetTeamFilterScriptRunnerScriptPath   = "scripts/targetFindTargetTeamFiltersDB.groovy"
)

type jiraClient struct {
	instanceBaseURL string
	baseURL         string
	username        string
	password        string
	httpClient      *http.Client
}

type jiraAPIError struct {
	Method     string
	Endpoint   string
	StatusCode int
	Message    string
}

func (e *jiraAPIError) Error() string {
	switch e.StatusCode {
	case http.StatusUnauthorized:
		return fmt.Sprintf("%s %s returned 401: Jira authentication failed; check the username/password you entered for this instance", e.Method, e.Endpoint)
	case http.StatusForbidden:
		return fmt.Sprintf("%s %s returned 403: Jira authenticated the request but denied access; check the permissions for this instance", e.Method, e.Endpoint)
	}
	return fmt.Sprintf("%s %s returned %d: %s", e.Method, e.Endpoint, e.StatusCode, e.Message)
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

func (c *jiraClient) ListTeams(progress func(current, total int)) ([]TeamDTO, error) {
	return listPaged[TeamDTO](c, "/team", progress)
}

func (c *jiraClient) ListPersons(progress func(current, total int)) ([]PersonDTO, error) {
	return listPaged[PersonDTO](c, "/person", progress)
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

func (c *jiraClient) ListResources(progress func(current, total int)) ([]ResourceDTO, error) {
	return listPaged[ResourceDTO](c, "/resource", progress)
}

func (c *jiraClient) ListPrograms(progress func(current, total int)) ([]ProgramDTO, error) {
	return listJPOPaged[ProgramDTO](c, "/program", progress)
}

func (c *jiraClient) ListPlans(progress func(current, total int)) ([]PlanDTO, error) {
	return listJPOPaged[PlanDTO](c, "/plan", progress)
}

func (c *jiraClient) CreateTeam(team TeamDTO) (int64, error) {
	payload := map[string]any{
		"title":     team.Title,
		"shareable": team.Shareable,
	}
	return c.postForID("/team", payload)
}

func (c *jiraClient) CreateResource(teamID int64, jiraUserID string, weeklyHours *float64) (int64, error) {
	candidates := []map[string]any{
		createResourcePayload(teamID, jiraUserID, weeklyHours, false),
		createResourcePayload(teamID, jiraUserID, weeklyHours, true),
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

func createResourcePayload(teamID int64, jiraUserID string, weeklyHours *float64, nestedPerson bool) map[string]any {
	payload := map[string]any{
		"teamId": teamID,
	}
	if weeklyHours != nil {
		payload["weeklyHours"] = *weeklyHours
	}
	if nestedPerson {
		payload["person"] = map[string]any{
			"jiraUser": map[string]any{
				"jiraUserId": jiraUserID,
			},
		}
	} else {
		payload["person"] = map[string]any{
			"jiraUserId": jiraUserID,
		}
	}
	return payload
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

func (c *jiraClient) GetIssue(key string, fields []string) (*JiraIssue, error) {
	query := url.Values{}
	if len(fields) > 0 {
		query.Set("fields", strings.Join(fields, ","))
	}

	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/issue/"+strings.TrimSpace(key), query, nil)
	if err != nil {
		return nil, err
	}

	var issue JiraIssue
	if err := json.Unmarshal(body, &issue); err != nil {
		return nil, err
	}
	return &issue, nil
}

func (c *jiraClient) UpdateIssueFields(key string, fields map[string]any) error {
	_, err := c.doCoreJSON(http.MethodPut, "/rest/api/2/issue/"+strings.TrimSpace(key), nil, map[string]any{
		"fields": fields,
	})
	return err
}

func (c *jiraClient) SearchFilters(startAt, maxResults int) (JiraFilterSearchResults, error) {
	query := url.Values{}
	query.Set("expand", "jql,owner")
	query.Set("startAt", strconv.Itoa(startAt))
	query.Set("maxResults", strconv.Itoa(maxResults))

	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/filter/search", query, nil)
	if err != nil {
		return JiraFilterSearchResults{}, err
	}

	var results JiraFilterSearchResults
	if err := json.Unmarshal(body, &results); err != nil {
		return JiraFilterSearchResults{}, err
	}
	return results, nil
}

func (c *jiraClient) ListFavouriteFilters() ([]JiraFilter, error) {
	query := url.Values{}
	query.Set("expand", "jql,owner")

	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/filter/favourite", query, nil)
	if err != nil {
		return nil, err
	}

	var filters []JiraFilter
	if err := json.Unmarshal(body, &filters); err != nil {
		return nil, err
	}
	return filters, nil
}

func (c *jiraClient) GetFilter(id string) (*JiraFilter, error) {
	query := url.Values{}
	query.Set("expand", "jql,owner")

	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/filter/"+strings.TrimSpace(id), query, nil)
	if err != nil {
		return nil, err
	}

	var filter JiraFilter
	if err := json.Unmarshal(body, &filter); err != nil {
		return nil, err
	}
	return &filter, nil
}

type JiraFilterUpdatePayload struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	JQL         string `json:"jql"`
}

func (c *jiraClient) UpdateFilter(id string, payload JiraFilterUpdatePayload) (*JiraFilter, error) {
	body, err := c.doCoreJSON(http.MethodPut, "/rest/api/2/filter/"+strings.TrimSpace(id), nil, payload)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, nil
	}

	var filter JiraFilter
	if err := json.Unmarshal(body, &filter); err != nil {
		return nil, err
	}
	return &filter, nil
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

func verifyTeamFilterScriptRunnerEndpoint(baseURL, username, password string) (string, string, error) {
	client, err := newJiraClient(baseURL, username, password)
	if err != nil {
		return "", "", err
	}
	return verifyTeamFilterScriptRunnerEndpointWithClient(client)
}

func verifyTeamFilterScriptRunnerEndpointWithClient(client *jiraClient) (string, string, error) {
	teamFieldID, fieldLabel, err := resolveTeamsCustomFieldNumericID(client)
	if err != nil {
		return "", "", err
	}

	query := url.Values{}
	query.Set("enabled", "true")
	query.Set("lastId", "0")
	query.Set("limit", "500")
	query.Set("teamFieldId", teamFieldID)

	body, err := client.doCoreJSON(http.MethodGet, teamFilterScriptRunnerEndpointPath, query, nil)
	if err != nil {
		return "", "", err
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", "", fmt.Errorf("script runner endpoint returned a non-JSON response")
	}

	endpointURL, err := buildURL(client.instanceBaseURL, teamFilterScriptRunnerEndpointPath, query)
	if err != nil {
		return "", "", err
	}
	return endpointURL, fmt.Sprintf("%s (%s)", fieldLabel, teamFieldID), nil
}

func resolveTeamsCustomFieldNumericID(client *jiraClient) (string, string, error) {
	fields, err := client.ListFields()
	if err != nil {
		return "", "", fmt.Errorf("could not load Jira fields to resolve the Teams field ID: %w", err)
	}

	selection, findings := selectTeamsField(fields)
	if selection == nil {
		message := "could not resolve the Jira Teams field ID"
		if len(findings) > 0 && strings.TrimSpace(findings[0].Message) != "" {
			message = findings[0].Message
		}
		return "", "", fmt.Errorf("%s", message)
	}

	teamFieldID, err := extractCustomFieldNumericID(selection.Field.ID)
	if err != nil {
		return "", "", err
	}
	return teamFieldID, selection.Field.Name, nil
}

func extractCustomFieldNumericID(fieldID string) (string, error) {
	const prefix = "customfield_"
	trimmed := strings.TrimSpace(fieldID)
	if !strings.HasPrefix(trimmed, prefix) || len(trimmed) == len(prefix) {
		return "", fmt.Errorf("resolved Teams field %q is not a Jira customfield_* identifier", fieldID)
	}
	return strings.TrimPrefix(trimmed, prefix), nil
}

func listPaged[T any](c *jiraClient, endpoint string, progress func(current, total int)) ([]T, error) {
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
		if progress != nil {
			total := len(all)
			if len(items) == teamsAPIMaxPageSize {
				total = len(all) + 1
			}
			progress(len(all), total)
		}
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
		return nil, &jiraAPIError{
			Method:     method,
			Endpoint:   endpoint,
			StatusCode: resp.StatusCode,
			Message:    summarizeJiraError(data),
		}
	}
	return data, nil
}

func listJPOPaged[T any](c *jiraClient, endpoint string, progress func(current, total int)) ([]T, error) {
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
		if progress != nil {
			total := len(all)
			if len(items) == jpoAPIMaxPageSize {
				total = len(all) + 1
			}
			progress(len(all), total)
		}
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

func buildURL(base, endpoint string, query url.Values) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = path.Join(u.Path, endpoint)
	if query != nil {
		u.RawQuery = query.Encode()
	}
	return u.String(), nil
}
