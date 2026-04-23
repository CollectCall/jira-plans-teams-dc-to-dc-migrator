package app

import "testing"

func TestPromptForAuthPreservesProvidedCredentials(t *testing.T) {
	username := "target-user"
	password := "target-pass"

	if err := promptForAuth(nil, "target", &username, &password); err != nil {
		t.Fatalf("promptForAuth returned error: %v", err)
	}

	if username != "target-user" {
		t.Fatalf("username was changed to %q", username)
	}
	if password != "target-pass" {
		t.Fatalf("password was changed to %q", password)
	}
}
