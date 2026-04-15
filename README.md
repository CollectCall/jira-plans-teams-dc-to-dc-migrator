# Teams Migrator

CLI for migrating Jira Teams and Team Membership data between Jira Server/Data Center instances.

This tool is for Jira Server/Data Center to Jira Server/Data Center migrations only.
It is not intended for Jira Cloud.

## Install

### Linux
```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator config init
```

### macOS
```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator config init
```

### Windows
```powershell
irm https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.ps1 | iex
teams-migrator.exe config init
```

The release installer downloads the latest published GitHub Release for your OS and CPU and installs the binary into `~/.local/bin` on Linux/macOS by default.
If needed, add that directory to `PATH`.

## Source Install

```bash
go install github.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/cmd/teams-migrator@latest
```

Requires Go 1.26 or newer.

## Commands

- `validate`: validate configuration and local inputs
- `plan`: generate a migration plan report
- `migrate`: run a dry-run by default, or switch to apply mode with `--apply`
- `report`: re-render a previously generated JSON report as JSON or CSV
- `config init`: interactive wizard to create or update a saved profile
- `config path`: print the config file path in use
- `config show`: print saved profile config, redacted by default

## First Run

```bash
teams-migrator config init
teams-migrator validate
teams-migrator plan --profile default
teams-migrator migrate --profile default --apply
```

The CLI is interactive by default when run in a terminal. If required inputs or secrets are missing, it prompts for them.

Config defaults:
- config path: `os.UserConfigDir()/teams-migrator/config.yaml`
- override path with `--config` or `TEAMS_MIGRATOR_CONFIG_PATH`
- select a profile with `--profile` or `TEAMS_MIGRATOR_PROFILE`

## Examples

### File-based input
```bash
teams-migrator validate \
  --identity-mapping ./identity-mapping.csv \
  --teams-file ./teams.json \
  --persons-file ./persons.json \
  --resources-file ./resources.json \
  --target-base-url https://target.example.com/jira \
  --format json
```

### API mode
```bash
teams-migrator migrate \
  --source-base-url https://source.example.com/jira \
  --source-auth-token "$SOURCE_PAT" \
  --target-base-url https://target.example.com/jira \
  --target-auth-token "$TARGET_PAT" \
  --identity-mapping ./identity-mapping.csv \
  --apply
```

If username and password are provided, the CLI uses basic auth. Otherwise it uses `Authorization: Bearer <token>` when an auth token is set.
