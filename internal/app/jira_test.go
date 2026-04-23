package app

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJiraClientSendsBasicAuthAndCookie(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			t.Fatalf("request did not include basic auth")
		}
		if username != "user" || password != "pass" {
			t.Fatalf("basic auth = %q/%q, want user/pass", username, password)
		}
		if got := r.Header.Get("Cookie"); got != "JSESSIONID=abc; atlassian.xsrf.token=token" {
			t.Fatalf("cookie header = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client, err := newJiraClientWithCookie(server.URL, "user", "pass", "JSESSIONID=abc; atlassian.xsrf.token=token")
	if err != nil {
		t.Fatalf("newJiraClientWithCookie returned error: %v", err)
	}

	if _, err := client.ListTeams(nil); err != nil {
		t.Fatalf("ListTeams returned error: %v", err)
	}
}
