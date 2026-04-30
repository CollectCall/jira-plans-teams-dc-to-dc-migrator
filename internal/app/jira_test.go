package app

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestJiraClientSendsBasicAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			t.Fatalf("request did not include basic auth")
		}
		if username != "user" || password != "pass" {
			t.Fatalf("basic auth = %q/%q, want user/pass", username, password)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client, err := newJiraClient(server.URL, "user", "pass")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}

	if _, err := client.ListTeams(nil); err != nil {
		t.Fatalf("ListTeams returned error: %v", err)
	}
}

func TestJiraClientCachesFieldsPerClient(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/rest/api/2/field" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[{"id":"customfield_10001","name":"Team","custom":true}]`))
	}))
	defer server.Close()

	client, err := newJiraClient(server.URL, "user", "pass")
	if err != nil {
		t.Fatalf("newJiraClient returned error: %v", err)
	}
	if _, err := client.ListFields(); err != nil {
		t.Fatalf("first ListFields returned error: %v", err)
	}
	if _, err := client.ListFields(); err != nil {
		t.Fatalf("second ListFields returned error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("expected one field request, got %d", requests)
	}
}

func TestNewJiraClientValidatesBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{
			name:    "https instance URL",
			baseURL: "https://jira.example.com/jira",
		},
		{
			name:    "http instance URL for local test servers",
			baseURL: "http://127.0.0.1:2990/jira",
		},
		{
			name:    "teams API URL",
			baseURL: "https://jira.example.com/jira/rest/teams-api/1.0",
		},
		{
			name:    "missing host",
			baseURL: "https:///jira",
			wantErr: true,
		},
		{
			name:    "relative URL",
			baseURL: "/jira",
			wantErr: true,
		},
		{
			name:    "file URL",
			baseURL: "file:///etc/passwd",
			wantErr: true,
		},
		{
			name:    "embedded credentials",
			baseURL: "https://user:pass@jira.example.com/jira",
			wantErr: true,
		},
		{
			name:    "query string",
			baseURL: "https://jira.example.com/jira?target=http://metadata.google.internal",
			wantErr: true,
		},
		{
			name:    "fragment",
			baseURL: "https://jira.example.com/jira#admin",
			wantErr: true,
		},
		{
			name:    "control character",
			baseURL: "https://jira.example.com/jira\nhttps://metadata.google.internal",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newJiraClient(tt.baseURL, "user", "pass")
			if tt.wantErr && err == nil {
				t.Fatalf("newJiraClient(%q) returned nil error", tt.baseURL)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("newJiraClient(%q) returned error: %v", tt.baseURL, err)
			}
		})
	}
}

func TestNewValidatedJiraRequestPreservesURLAndBody(t *testing.T) {
	u, err := url.Parse("https://jira.example.com/jira/rest/api/2/search?jql=project%3DABC&maxResults=50")
	if err != nil {
		t.Fatalf("parse test URL: %v", err)
	}

	req, err := newValidatedJiraRequest(http.MethodPost, *u, strings.NewReader(`{"expand":["names"]}`))
	if err != nil {
		t.Fatalf("newValidatedJiraRequest returned error: %v", err)
	}

	if req.Method != http.MethodPost {
		t.Fatalf("expected POST method, got %s", req.Method)
	}
	if req.URL.String() != u.String() {
		t.Fatalf("expected URL %q, got %q", u.String(), req.URL.String())
	}
	if req.Host != u.Host {
		t.Fatalf("expected request Host %q to match URL host, got %q", u.Host, req.Host)
	}
	if req.Body == nil {
		t.Fatal("expected request body to be preserved")
	}
}
