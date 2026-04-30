package app

import (
	"bufio"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPromptForAuthPreservesProvidedCredentials(t *testing.T) {
	accountName := authFixtureValue("target", "account")
	authToken := authFixtureValue("target", "token")

	if err := promptForAuth(nil, "target", &accountName, &authToken); err != nil {
		t.Fatalf("promptForAuth returned error: %v", err)
	}

	if accountName != authFixtureValue("target", "account") {
		t.Fatalf("account name was changed to %q", accountName)
	}
	if authToken != authFixtureValue("target", "token") {
		t.Fatalf("auth token was changed to %q", authToken)
	}
}

func TestVerifyJiraCredentialsUsesCurrentUserEndpoint(t *testing.T) {
	accountName := authFixtureValue("jira", "account")
	authToken := authFixtureValue("jira", "token")
	var sawAuth bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/rest/api/2/myself" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		requestAccount, requestToken, ok := r.BasicAuth()
		sawAuth = ok && requestAccount == accountName && requestToken == authToken
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"test-account","key":"test-account-key","displayName":"Test Operator","active":true}`))
	}))
	defer server.Close()

	user, err := verifyJiraCredentials(server.URL, accountName, authToken)
	if err != nil {
		t.Fatalf("verifyJiraCredentials returned error: %v", err)
	}
	if !sawAuth {
		t.Fatal("expected basic auth credentials on current-user request")
	}
	if user.DisplayName != "Test Operator" {
		t.Fatalf("unexpected verified user: %#v", user)
	}
}

func TestVerifyJiraCredentialsReturnsAuthFailure(t *testing.T) {
	accountName := authFixtureValue("jira", "account")
	authToken := authFixtureValue("invalid", "token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/2/myself" {
			t.Fatalf("unexpected request path %s", r.URL.Path)
		}
		http.Error(w, "bad credentials", http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := verifyJiraCredentials(server.URL, accountName, authToken)
	if err == nil {
		t.Fatal("expected verification error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected 401 in verification error, got %v", err)
	}
}

func TestVerifyJiraCredentialsReturnsDecodeFailure(t *testing.T) {
	accountName := authFixtureValue("jira", "account")
	authToken := authFixtureValue("jira", "token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()

	_, err := verifyJiraCredentials(server.URL, accountName, authToken)
	if err == nil {
		t.Fatal("expected decode error")
	}
	if !strings.Contains(err.Error(), "decoding Jira current-user response") {
		t.Fatalf("expected decode context, got %v", err)
	}
}

func TestVerifyJiraCredentialsReturnsEmptyCurrentUserBodyDiagnostic(t *testing.T) {
	accountName := authFixtureValue("jira", "account")
	authToken := authFixtureValue("jira", "token")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, err := verifyJiraCredentials(server.URL, accountName, authToken)
	if err == nil {
		t.Fatal("expected empty body error")
	}
	if !strings.Contains(err.Error(), "empty response body") {
		t.Fatalf("expected empty body diagnostic, got %v", err)
	}
	if !strings.Contains(err.Error(), server.URL+"/rest/api/2/myself") {
		t.Fatalf("expected request URL in diagnostic, got %v", err)
	}
	if !strings.Contains(err.Error(), "HTTP status: 200 OK") {
		t.Fatalf("expected HTTP status in diagnostic, got %v", err)
	}
	if !strings.Contains(err.Error(), "Content-Type: application/json") {
		t.Fatalf("expected content type in diagnostic, got %v", err)
	}
	if strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Fatalf("expected friendly diagnostic instead of raw JSON EOF, got %v", err)
	}
	if !isEmptyCurrentUserVerificationResponse(err) {
		t.Fatalf("expected empty current-user response classification, got %T", err)
	}
}

func TestJiraCredentialVerificationFailureDescriptionDistinguishesTransportFromAuth(t *testing.T) {
	authErr := &jiraAPIError{StatusCode: http.StatusUnauthorized}
	if got := jiraCredentialVerificationFailureDescription("source", authErr); !strings.Contains(got, "Could not authenticate") {
		t.Fatalf("expected auth failure description, got %q", got)
	}

	transportErr := errors.New("GET /rest/api/2/myself returned an empty response body")
	if got := jiraCredentialVerificationFailureDescription("source", transportErr); !strings.Contains(got, "Could not verify") {
		t.Fatalf("expected connection verification description, got %q", got)
	}
}

func TestJiraCredentialVerificationFailureInputHelpAllowsContinueForNonAuthFailures(t *testing.T) {
	authErr := &jiraAPIError{StatusCode: http.StatusUnauthorized}
	if got := jiraCredentialVerificationFailureInputHelp(authErr); strings.Contains(got, "continue without verified connection") {
		t.Fatalf("did not expect continue option for auth failure, got %q", got)
	}

	transportErr := errors.New("GET /rest/api/2/myself returned an empty response body")
	if got := jiraCredentialVerificationFailureInputHelp(transportErr); !strings.Contains(got, "continue without verified connection") {
		t.Fatalf("expected continue option for non-auth verification failure, got %q", got)
	}
}

func TestInitIssueProjectScopeArtifactInfoShowsSavedScopeForExistingProfile(t *testing.T) {
	info := initIssueProjectScopeArtifactInfo(Config{IssueProjectScope: "ABC,DEF"}, true)

	if !strings.Contains(info, "This scope applies to issue/team and Parent Link correction flows.") {
		t.Fatalf("expected scope explanation, got %q", info)
	}
	if !strings.Contains(info, "Previously saved project scope: ABC,DEF") {
		t.Fatalf("expected saved scope in artifact info, got %q", info)
	}
}

func TestInitIssueProjectScopeArtifactInfoHidesSavedScopeForNewProfile(t *testing.T) {
	info := initIssueProjectScopeArtifactInfo(Config{IssueProjectScope: "ABC,DEF"}, false)

	if strings.Contains(info, "Previously saved project scope") {
		t.Fatalf("did not expect saved scope for new profile, got %q", info)
	}
}

func TestApplyInitSavedProfileDefaultsRestoresSavedProjectScope(t *testing.T) {
	cfg := Config{IssueProjectScope: "all"}

	applyInitSavedProfileDefaults(&cfg, SavedProfile{IssueProjectScope: "ABC,DEF"})

	if cfg.IssueProjectScope != "ABC,DEF" {
		t.Fatalf("expected saved project scope, got %q", cfg.IssueProjectScope)
	}
}

func TestApplyInitSavedProfileDefaultsPreservesExplicitProjectScope(t *testing.T) {
	cfg := Config{IssueProjectScope: "OPS", IssueProjectScopeExplicit: true}

	applyInitSavedProfileDefaults(&cfg, SavedProfile{IssueProjectScope: "ABC,DEF"})

	if cfg.IssueProjectScope != "OPS" {
		t.Fatalf("expected explicit project scope to be preserved, got %q", cfg.IssueProjectScope)
	}
}

func TestVerifyConfiguredScriptRunnerFilterEndpointCanRetryEndpoint(t *testing.T) {
	var endpointRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_16604","name":"Team","custom":true,"schema":{"custom":"com.atlassian.rm:team","type":"array"}}]`))
		case "/rest/scriptrunner/latest/custom/findSourceTeamFiltersDB":
			endpointRequests++
			if endpointRequests == 1 {
				http.Error(w, "No such data source: local", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"meta":{"matched":0},"results":[],"parseErrors":[]}`))
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{SourceBaseURL: server.URL}
	wizard := &wizardContext{
		Title:  "Test",
		Reader: bufio.NewReader(strings.NewReader("1\n\n")),
	}

	retryCredentials, err := verifyConfiguredScriptRunnerFilterEndpointWithAuth(wizard, &cfg, "admin", "secret")
	if err != nil {
		t.Fatalf("verifyConfiguredScriptRunnerFilterEndpointWithAuth returned error: %v", err)
	}
	if retryCredentials {
		t.Fatal("did not expect credential retry request")
	}
	if endpointRequests != 2 {
		t.Fatalf("expected endpoint to be called twice, got %d", endpointRequests)
	}
	if cfg.FilterScriptRunnerEndpoint == "" {
		t.Fatal("expected verified endpoint to be saved after retry")
	}
}

func TestVerifyConfiguredScriptRunnerFilterEndpointCanContinueAfterFailure(t *testing.T) {
	var endpointRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_16604","name":"Team","custom":true,"schema":{"custom":"com.atlassian.rm:team","type":"array"}}]`))
		case "/rest/scriptrunner/latest/custom/findSourceTeamFiltersDB":
			endpointRequests++
			http.Error(w, "No such data source: local", http.StatusInternalServerError)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{SourceBaseURL: server.URL}
	wizard := &wizardContext{
		Title:  "Test",
		Reader: bufio.NewReader(strings.NewReader("3\n\n")),
	}

	retryCredentials, err := verifyConfiguredScriptRunnerFilterEndpointWithAuth(wizard, &cfg, "admin", "secret")
	if err != nil {
		t.Fatalf("verifyConfiguredScriptRunnerFilterEndpointWithAuth returned error: %v", err)
	}
	if retryCredentials {
		t.Fatal("did not expect credential retry request")
	}
	if endpointRequests != 1 {
		t.Fatalf("expected endpoint to be called once, got %d", endpointRequests)
	}
	if cfg.FilterScriptRunnerEndpoint != "" {
		t.Fatalf("expected endpoint to remain unset after continuing, got %q", cfg.FilterScriptRunnerEndpoint)
	}
}

func TestVerifyConfiguredScriptRunnerFilterEndpointCanRetryCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/2/field":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"id":"customfield_16604","name":"Team","custom":true,"schema":{"custom":"com.atlassian.rm:team","type":"array"}}]`))
		case "/rest/scriptrunner/latest/custom/findSourceTeamFiltersDB":
			http.Error(w, "bad credentials", http.StatusUnauthorized)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := Config{SourceBaseURL: server.URL}
	wizard := &wizardContext{
		Title:  "Test",
		Reader: bufio.NewReader(strings.NewReader("2\n")),
	}

	retryCredentials, err := verifyConfiguredScriptRunnerFilterEndpointWithAuth(wizard, &cfg, "admin", "wrong")
	if err != nil {
		t.Fatalf("verifyConfiguredScriptRunnerFilterEndpointWithAuth returned error: %v", err)
	}
	if !retryCredentials {
		t.Fatal("expected credential retry request")
	}
	if cfg.FilterScriptRunnerEndpoint != "" {
		t.Fatalf("expected endpoint to remain unset after credential retry request, got %q", cfg.FilterScriptRunnerEndpoint)
	}
}

func authFixtureValue(parts ...string) string {
	return strings.Join(parts, "-")
}
