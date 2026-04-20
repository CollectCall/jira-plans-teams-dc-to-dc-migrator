package main

import (
	"os"

	"github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/internal/app"
)

func main() {
	os.Exit(app.Run(os.Args[1:]))
}
