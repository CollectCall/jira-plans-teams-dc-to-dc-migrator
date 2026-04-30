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
	issueKeySearchChunkSize                  = 50
	teamsAPIMaxPageSize                      = 100
	jpoAPIMaxPageSize                        = 100
	jiraMaxRetryAttempts                     = 5
	jiraInitialRetryDelay                    = 500 * time.Millisecond
	jiraMaxRetryDelay                        = 10 * time.Second
	teamFilterScriptRunnerEndpointPath       = "/rest/scriptrunner/latest/custom/findSourceTeamFiltersDB"
	teamFilterScriptRunnerScriptPath         = "scripts/sourceFindTeamFiltersDB.groovy"
	targetTeamFilterScriptRunnerEndpointPath = "/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB"
	targetTeamFilterScriptRunnerScriptPath   = "scripts/targetFindTeamFiltersDB.groovy"
)

type jiraClient struct {
	instanceBaseURL string
	baseURL         string
	username        string
	password        string
	httpClient      *http.Client
	fieldsCache     []JiraField
	fieldsLoaded    bool
}

type jiraAPIError struct {
	Method           string
	Endpoint         string
	StatusCode       int
	Message          string
	LoginReason      string
	AuthDeniedReason string
}

type jiraHTTPResponse struct {
	Body          []byte
	Status        string
	StatusCode    int
	RequestURL    string
	FinalURL      string
	ContentType   string
	ContentLength int64
	JiraUsername  string
	LoginReason   string
}

type jiraEmptyCurrentUserResponseError struct {
	Response jiraHTTPResponse
}

func (e *jiraEmptyCurrentUserResponseError) Error() string {
	resp := e.Response
	return fmt.Sprintf("GET %s returned an empty response body; expected Jira current-user JSON. HTTP status: %s. Final URL: %s. Content-Type: %s. Content-Length: %s. Check that the base URL points to Jira and that no proxy, SSO, or login page is intercepting REST API requests",
		resp.RequestURL,
		nonEmptyString(resp.Status, strconv.Itoa(resp.StatusCode)),
		nonEmptyString(resp.FinalURL, resp.RequestURL),
		nonEmptyString(resp.ContentType, "not provided"),
		formatContentLength(resp.ContentLength),
	)
}

func (e *jiraAPIError) Error() string {
	switch e.StatusCode {
	case http.StatusUnauthorized:
		return fmt.Sprintf("%s %s returned 401: Jira authentication failed; check the username/password you entered for this instance", e.Method, e.Endpoint)
	case http.StatusForbidden:
		details := []string{}
		if strings.TrimSpace(e.LoginReason) != "" {
			details = append(details, e.LoginReason)
		}
		if strings.TrimSpace(e.AuthDeniedReason) != "" {
			details = append(details, e.AuthDeniedReason)
		}
		if len(details) > 0 {
			return fmt.Sprintf("%s %s returned 403: Jira denied the request (%s): %s", e.Method, e.Endpoint, strings.Join(details, "; "), nonEmptyString(e.Message, "empty error response"))
		}
		return fmt.Sprintf("%s %s returned 403: Jira denied the request: %s", e.Method, e.Endpoint, nonEmptyString(e.Message, "empty error response"))
	}
	return fmt.Sprintf("%s %s returned %d: %s", e.Method, e.Endpoint, e.StatusCode, e.Message)
}

func newJiraClient(baseURL, username, password string) (*jiraClient, error) {
	instanceBaseURL, apiBaseURL, err := normalizeJiraBaseURLs(baseURL)
	if err != nil {
		return nil, err
	}
	return &jiraClient{
		instanceBaseURL: instanceBaseURL,
		baseURL:         apiBaseURL,
		username:        username,
		password:        password,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func normalizeJiraBaseURLs(raw string) (string, string, error) {
	instanceBaseURL, err := validatedJiraBaseURL(normalizeInstanceBaseURL(raw))
	if err != nil {
		return "", "", err
	}
	if strings.Contains(strings.TrimRight(strings.TrimSpace(raw), "/"), "/rest/teams-api/1.0") {
		apiBaseURL, err := validatedJiraBaseURL(strings.TrimRight(strings.TrimSpace(raw), "/"))
		if err != nil {
			return "", "", err
		}
		return instanceBaseURL, apiBaseURL, nil
	}
	return instanceBaseURL, instanceBaseURL + "/rest/teams-api/1.0", nil
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

func validatedJiraBaseURL(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("jira base URL is required")
	}
	if strings.ContainsAny(raw, "\x00\r\n\t") {
		return "", fmt.Errorf("invalid Jira base URL: contains control characters")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid Jira base URL: %w", err)
	}
	if !u.IsAbs() || u.Host == "" || u.Hostname() == "" {
		return "", fmt.Errorf("invalid Jira base URL: absolute http or https URL with a host is required")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return "", fmt.Errorf("invalid Jira base URL: scheme must be http or https")
	}
	if u.User != nil {
		return "", fmt.Errorf("invalid Jira base URL: embedded credentials are not allowed")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", fmt.Errorf("invalid Jira base URL: query strings are not allowed")
	}
	if u.Fragment != "" {
		return "", fmt.Errorf("invalid Jira base URL: fragments are not allowed")
	}
	return strings.TrimRight(u.String(), "/"), nil
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
	if c.fieldsLoaded {
		return append([]JiraField(nil), c.fieldsCache...), nil
	}
	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/field", nil, nil)
	if err != nil {
		return nil, err
	}
	var fields []JiraField
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	c.fieldsCache = append([]JiraField(nil), fields...)
	c.fieldsLoaded = true
	return append([]JiraField(nil), fields...), nil
}

func (c *jiraClient) ListIssueTypes() ([]JiraIssueType, error) {
	body, err := c.doCoreJSON(http.MethodGet, "/rest/api/2/issuetype", nil, nil)
	if err != nil {
		return nil, err
	}
	var issueTypes []JiraIssueType
	if err := json.Unmarshal(body, &issueTypes); err != nil {
		return nil, err
	}
	return issueTypes, nil
}

func (c *jiraClient) ListHierarchyLevels(progress func(current, total int)) ([]HierarchyLevelDTO, error) {
	return listJPOPaged[HierarchyLevelDTO](c, "/hierarchy", progress)
}

func (c *jiraClient) CurrentUser() (*CoreJiraUser, error) {
	resp, err := c.doCoreJSONResponse(http.MethodGet, "/rest/api/2/myself", nil, nil)
	if err != nil {
		return nil, err
	}
	body := resp.Body
	if strings.TrimSpace(string(body)) == "" {
		return nil, &jiraEmptyCurrentUserResponseError{Response: resp}
	}
	var user CoreJiraUser
	if err := json.Unmarshal(body, &user); err != nil {
		return nil, fmt.Errorf("decoding Jira current-user response from %s: %w", resp.RequestURL, err)
	}
	return &user, nil
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

func (c *jiraClient) SearchIssuesByKeys(keys []string, projectScope string, fields []string, progress func(current, total int)) (map[string]JiraIssue, error) {
	out := map[string]JiraIssue{}
	cleanKeys := uniqueTrimmedStrings(keys)
	total := len(cleanKeys)
	if total == 0 {
		return out, nil
	}

	processed := 0
	for _, chunk := range chunkStrings(cleanKeys, issueKeySearchChunkSize) {
		jql := scopedIssueJQL(projectScope, fmt.Sprintf("issuekey in (%s)", quoteJQLValues(chunk)))
		issues, err := c.SearchIssues(jql, fields, nil)
		if err != nil {
			return out, fmt.Errorf("searching target issues %d-%d of %d: %w", processed+1, processed+len(chunk), total, err)
		}
		for _, issue := range issues {
			key := strings.TrimSpace(issue.Key)
			if key == "" {
				continue
			}
			out[key] = issue
		}
		processed += len(chunk)
		if progress != nil {
			progress(processed, total)
		}
	}

	return out, nil
}

func uniqueTrimmedStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		key := strings.ToUpper(trimmed)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func chunkStrings(values []string, size int) [][]string {
	if size <= 0 {
		size = len(values)
	}
	chunks := make([][]string, 0, (len(values)+size-1)/size)
	for start := 0; start < len(values); start += size {
		end := start + size
		if end > len(values) {
			end = len(values)
		}
		chunks = append(chunks, values[start:end])
	}
	return chunks
}

func quoteJQLValues(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		escaped := strings.ReplaceAll(value, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		quoted = append(quoted, `"`+escaped+`"`)
	}
	return strings.Join(quoted, ", ")
}

func (c *jiraClient) GetIssue(key string, fields []string) (*JiraIssue, error) {
	return c.getIssue(key, fields, false)
}

func (c *jiraClient) GetIssueNoRetry429(key string, fields []string) (*JiraIssue, error) {
	return c.getIssue(key, fields, true)
}

func (c *jiraClient) getIssue(key string, fields []string, noRetry bool) (*JiraIssue, error) {
	query := url.Values{}
	if len(fields) > 0 {
		query.Set("fields", strings.Join(fields, ","))
	}

	endpoint := "/rest/api/2/issue/" + strings.TrimSpace(key)
	var body []byte
	var err error
	if noRetry {
		body, err = c.doCoreJSONNoRetry429(http.MethodGet, endpoint, query, nil)
	} else {
		body, err = c.doCoreJSON(http.MethodGet, endpoint, query, nil)
	}
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
	return c.updateIssueFields(key, fields, false)
}

func (c *jiraClient) UpdateIssueFieldsNoRetry429(key string, fields map[string]any) error {
	return c.updateIssueFields(key, fields, true)
}

func (c *jiraClient) updateIssueFields(key string, fields map[string]any, noRetry bool) error {
	endpoint := "/rest/api/2/issue/" + strings.TrimSpace(key)
	payload := map[string]any{
		"fields": fields,
	}
	var err error
	if noRetry {
		_, err = c.doCoreJSONNoRetry429(http.MethodPut, endpoint, nil, payload)
	} else {
		_, err = c.doCoreJSON(http.MethodPut, endpoint, nil, payload)
	}
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

		resp, err := c.doJSONResponse(http.MethodGet, endpoint, query, nil)
		if err != nil {
			return nil, err
		}
		items, err := decodeCollection[T](resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decoding %s page %d: %w; response: %s", endpoint, page, err, jiraResponseSummary(resp))
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

func (c *jiraClient) doJSONResponse(method, endpoint string, query url.Values, payload any) (jiraHTTPResponse, error) {
	return c.doJSONAgainstBaseResponse(c.baseURL, method, endpoint, query, payload)
}

func (c *jiraClient) doCoreJSON(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	return c.doJSONAgainstBase(c.instanceBaseURL, method, endpoint, query, payload)
}

func (c *jiraClient) doCoreJSONNoRetry429(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	resp, err := c.doJSONAgainstBaseResponseWithRetryPolicy(c.instanceBaseURL, method, endpoint, query, payload, jiraMaxRetryAttempts, false)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *jiraClient) doCoreJSONResponse(method, endpoint string, query url.Values, payload any) (jiraHTTPResponse, error) {
	return c.doJSONAgainstBaseResponse(c.instanceBaseURL, method, endpoint, query, payload)
}

func (c *jiraClient) doJPOJSON(method, endpoint string, query url.Values, payload any) ([]byte, error) {
	return c.doJSONAgainstBase(c.instanceBaseURL, method, "/rest/jpo-api/1.0"+endpoint, query, payload)
}

func (c *jiraClient) doJPOJSONResponse(method, endpoint string, query url.Values, payload any) (jiraHTTPResponse, error) {
	return c.doJSONAgainstBaseResponse(c.instanceBaseURL, method, "/rest/jpo-api/1.0"+endpoint, query, payload)
}

func (c *jiraClient) doJSONAgainstBase(base, method, endpoint string, query url.Values, payload any) ([]byte, error) {
	resp, err := c.doJSONAgainstBaseResponse(base, method, endpoint, query, payload)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (c *jiraClient) doJSONAgainstBaseResponse(base, method, endpoint string, query url.Values, payload any) (jiraHTTPResponse, error) {
	return c.doJSONAgainstBaseResponseWithRetryPolicy(base, method, endpoint, query, payload, jiraMaxRetryAttempts, true)
}

func (c *jiraClient) doJSONAgainstBaseResponseWithRetryPolicy(base, method, endpoint string, query url.Values, payload any, maxAttempts int, retryTooManyRequests bool) (jiraHTTPResponse, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	u, err := url.Parse(base)
	if err != nil {
		return jiraHTTPResponse{}, fmt.Errorf("invalid base URL: %w", err)
	}
	u.Path = path.Join(u.Path, endpoint)
	if query != nil {
		u.RawQuery = query.Encode()
	}
	requestURL := u.String()

	var payloadBytes []byte
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return jiraHTTPResponse{}, err
		}
		payloadBytes = encoded
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var body io.Reader
		if payloadBytes != nil {
			body = bytes.NewReader(payloadBytes)
		}

		req, err := newValidatedJiraRequest(method, *u, body)
		if err != nil {
			return jiraHTTPResponse{}, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "teams-migrator/"+currentVersion())
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.username != "" || c.password != "" {
			req.SetBasicAuth(c.username, c.password)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts && method != http.MethodPost {
				time.Sleep(retryDelay(attempt, 0))
				continue
			}
			return jiraHTTPResponse{}, err
		}

		data, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return jiraHTTPResponse{}, readErr
		}

		httpResp := jiraHTTPResponse{
			Body:          data,
			Status:        resp.Status,
			StatusCode:    resp.StatusCode,
			RequestURL:    requestURL,
			ContentType:   resp.Header.Get("Content-Type"),
			ContentLength: resp.ContentLength,
			JiraUsername:  resp.Header.Get("X-AUSERNAME"),
			LoginReason:   resp.Header.Get("X-Seraph-LoginReason"),
		}
		if resp.Request != nil && resp.Request.URL != nil {
			httpResp.FinalURL = resp.Request.URL.String()
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return httpResp, nil
		}

		apiErr := &jiraAPIError{
			Method:           method,
			Endpoint:         endpoint,
			StatusCode:       resp.StatusCode,
			Message:          summarizeJiraError(data),
			LoginReason:      resp.Header.Get("X-Seraph-LoginReason"),
			AuthDeniedReason: resp.Header.Get("X-Authentication-Denied-Reason"),
		}
		lastErr = apiErr

		if attempt < maxAttempts && shouldRetryJiraRequestWithPolicy(method, resp.StatusCode, retryTooManyRequests) {
			time.Sleep(retryDelay(attempt, retryAfterDelay(resp.Header.Get("Retry-After"))))
			continue
		}

		return jiraHTTPResponse{}, apiErr
	}

	return jiraHTTPResponse{}, lastErr
}

func newValidatedJiraRequest(method string, u url.URL, body io.Reader) (*http.Request, error) {
	parsedURL, err := validatedJiraRequestURL(u)
	if err != nil {
		return nil, err
	}
	return http.NewRequest(method, parsedURL.String(), body)
}

func validatedJiraRequestURL(u url.URL) (*url.URL, error) {
	raw := u.String()
	if strings.ContainsAny(raw, "\x00\r\n\t") {
		return nil, fmt.Errorf("invalid Jira request URL: contains control characters")
	}
	if !u.IsAbs() || u.Host == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("invalid Jira request URL: absolute http or https URL with a host is required")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("invalid Jira request URL: scheme must be http or https")
	}
	if u.User != nil {
		return nil, fmt.Errorf("invalid Jira request URL: embedded credentials are not allowed")
	}
	if u.Fragment != "" {
		return nil, fmt.Errorf("invalid Jira request URL: fragments are not allowed")
	}
	clean := u
	return &clean, nil
}

func shouldRetryJiraRequest(method string, statusCode int) bool {
	return shouldRetryJiraRequestWithPolicy(method, statusCode, true)
}

func shouldRetryJiraRequestWithPolicy(method string, statusCode int, retryTooManyRequests bool) bool {
	if method == http.MethodPost {
		return false
	}
	switch statusCode {
	case http.StatusTooManyRequests:
		return retryTooManyRequests
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > jiraMaxRetryDelay {
			return jiraMaxRetryDelay
		}
		return retryAfter
	}
	delay := jiraInitialRetryDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= jiraMaxRetryDelay {
			return jiraMaxRetryDelay
		}
	}
	return delay
}

func retryAfterDelay(value string) time.Duration {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(trimmed); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(trimmed); err == nil {
		delay := time.Until(when)
		if delay > 0 {
			return delay
		}
	}
	return 0
}

func formatContentLength(value int64) string {
	if value < 0 {
		return "not provided"
	}
	return strconv.FormatInt(value, 10)
}

func listJPOPaged[T any](c *jiraClient, endpoint string, progress func(current, total int)) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("size", strconv.Itoa(jpoAPIMaxPageSize))

		resp, err := c.doJPOJSONResponse(http.MethodGet, endpoint, query, nil)
		if err != nil {
			return nil, err
		}
		items, err := decodeCollection[T](resp.Body)
		if err != nil {
			return nil, fmt.Errorf("decoding %s page %d: %w; response: %s", endpoint, page, err, jiraResponseSummary(resp))
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

func responseSnippet(data []byte) string {
	text := strings.TrimSpace(string(data))
	if text == "" {
		return "empty response body"
	}
	text = strings.Join(strings.Fields(text), " ")
	return truncateText(text, 300)
}

func jiraResponseSummary(resp jiraHTTPResponse) string {
	return fmt.Sprintf("status=%s finalURL=%s contentType=%s contentLength=%s jiraUser=%s loginReason=%s",
		nonEmptyString(resp.Status, strconv.Itoa(resp.StatusCode)),
		nonEmptyString(resp.FinalURL, resp.RequestURL),
		nonEmptyString(resp.ContentType, "not provided"),
		formatContentLength(resp.ContentLength),
		nonEmptyString(resp.JiraUsername, "not provided"),
		nonEmptyString(resp.LoginReason, "not provided"),
	)
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

	return nil, fmt.Errorf("unsupported collection payload: %s", responseSnippet(data))
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
