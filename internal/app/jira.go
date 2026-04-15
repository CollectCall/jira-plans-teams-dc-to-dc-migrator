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

const defaultPageSize = 500

type jiraClient struct {
	baseURL    string
	token      string
	username   string
	password   string
	httpClient *http.Client
}

func newJiraClient(baseURL, token, username, password string) (*jiraClient, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("jira base URL is required")
	}
	return &jiraClient{
		baseURL:    normalizeAPIBaseURL(baseURL),
		token:      token,
		username:   username,
		password:   password,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func normalizeAPIBaseURL(raw string) string {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")
	if strings.Contains(trimmed, "/rest/teams-api/1.0") {
		return trimmed
	}
	return trimmed + "/rest/teams-api/1.0"
}

func (c *jiraClient) ListTeams() ([]TeamDTO, error) {
	return listPaged[TeamDTO](c, "/team")
}

func (c *jiraClient) ListPersons() ([]PersonDTO, error) {
	return listPaged[PersonDTO](c, "/person")
}

func (c *jiraClient) ListResources() ([]ResourceDTO, error) {
	return listPaged[ResourceDTO](c, "/resource")
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

func listPaged[T any](c *jiraClient, endpoint string) ([]T, error) {
	var all []T
	for page := 1; ; page++ {
		query := url.Values{}
		query.Set("page", strconv.Itoa(page))
		query.Set("size", strconv.Itoa(defaultPageSize))

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
		if len(items) < defaultPageSize {
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
	u, err := url.Parse(c.baseURL)
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
	} else if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
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
		return nil, fmt.Errorf("%s %s returned %d: %s", method, endpoint, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
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
