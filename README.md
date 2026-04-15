# Teams Migrator
CLI-first scaffold for migrating Jira Teams and Team Membership data between Jira instances.

The CLI can read source data from JSON exports or directly from the Jira Teams API. Target resolution and apply-mode writes use the Jira Teams API.

## Commands
- `validate`: validate configuration and local inputs
- `plan`: generate a migration plan report
- `migrate`: run a dry-run by default, or switch to apply mode with `--apply`
- `report`: re-render a previously generated JSON report as JSON or CSV

## Build
```bash
go build -o bin/teams-migrator ./cmd/teams-migrator
```

## Example
```bash
./bin/teams-migrator validate \
  --identity-mapping ./identity-mapping.csv \
  --teams-file ./teams.json \
  --persons-file ./persons.json \
  --resources-file ./resources.json \
  --target-base-url https://target.example.com/jira \
  --format json
```

## API mode
```bash
./bin/teams-migrator migrate \
  --source-base-url https://source.example.com/jira \
  --source-auth-token "$SOURCE_PAT" \
  --target-base-url https://target.example.com/jira \
  --target-auth-token "$TARGET_PAT" \
  --identity-mapping ./identity-mapping.csv \
  --apply
```

If username and password are provided, the CLI uses basic auth. Otherwise it uses `Authorization: Bearer <token>` when an auth token is set.

## Environment variables
```text
TEAMS_MIGRATOR_SOURCE_BASE_URL
TEAMS_MIGRATOR_SOURCE_AUTH_TOKEN
TEAMS_MIGRATOR_SOURCE_USERNAME
TEAMS_MIGRATOR_SOURCE_PASSWORD
TEAMS_MIGRATOR_TARGET_BASE_URL
TEAMS_MIGRATOR_TARGET_AUTH_TOKEN
TEAMS_MIGRATOR_TARGET_USERNAME
TEAMS_MIGRATOR_TARGET_PASSWORD
TEAMS_MIGRATOR_IDENTITY_MAPPING_FILE
TEAMS_MIGRATOR_TEAMS_FILE
TEAMS_MIGRATOR_PERSONS_FILE
TEAMS_MIGRATOR_RESOURCES_FILE
TEAMS_MIGRATOR_ISSUES_CSV
TEAMS_MIGRATOR_OUTPUT_DIR
TEAMS_MIGRATOR_REPORT_FORMAT
TEAMS_MIGRATOR_STRICT
TEAMS_MIGRATOR_DRY_RUN
TEAMS_MIGRATOR_REPORT_INPUT
```

## Exit codes
- `0`: success
- `1`: fatal error or validation errors
- `2`: strict mode detected warnings or errors

## Current migration behavior
- Reuses destination teams by normalized title match.
- Creates missing destination teams in apply mode.
- Resolves source users via `sourceEmail -> targetEmail -> target jiraUserId`.
- Recreates team membership through `POST /resource` in apply mode.
- Writes JSON or CSV reports to `out/`.
