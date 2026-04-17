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

Manual install from a downloaded release artifact:

1. Download the correct release asset from the [GitHub Releases page](https://github.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/releases):
   - `teams-migrator_<version>_linux_amd64.tar.gz`
   - `teams-migrator_<version>_linux_arm64.tar.gz`
2. Install it locally:

```bash
tar -xzf teams-migrator_<version>_linux_amd64.tar.gz
mkdir -p ~/.local/bin
install teams-migrator ~/.local/bin/teams-migrator
export PATH="$HOME/.local/bin:$PATH"
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

The release installer downloads the latest published GitHub Release for your OS and CPU and prefers a writable directory already on `PATH`.
If none is available, it falls back to a user-local bin directory and tells you what to add to `PATH`.

## Commands

- `validate`: validate configuration and local inputs
- `plan`: generate a migration plan report
- `migrate`: run a dry-run by default, or switch to apply mode with `--apply`
- `scan-filters`: scan Jira filters for `Team = {id|name}` JQL clauses that match known source teams
- `report`: re-render a previously generated JSON report as JSON or CSV
- `config init`: interactive wizard to create or update a saved profile
- `config path`: print the config file path in use
- `config show`: print saved profile config, redacted by default
- `uninstall`: remove the installed binary and local config directory

## First Run

```bash
teams-migrator config init
teams-migrator validate
teams-migrator plan --profile default
teams-migrator scan-filters --profile default
teams-migrator migrate --profile default --apply
```

The CLI is interactive by default when run in a terminal. If required inputs or secrets are missing, it prompts for them.
During interactive `validate`, `plan`, and `migrate` runs it also asks whether filters should be scanned; the default is `no`.

To update an installed binary to the latest published release:

```bash
teams-migrator self-update
```

To uninstall the binary:

```bash
teams-migrator uninstall
```

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
  --source-username "$SOURCE_USERNAME" \
  --source-password "$SOURCE_PASSWORD" \
  --target-base-url https://target.example.com/jira \
  --target-username "$TARGET_USERNAME" \
  --target-password "$TARGET_PASSWORD" \
  --identity-mapping ./identity-mapping.csv \
  --apply
```

### Filter scan POC
```bash
teams-migrator scan-filters \
  --source-base-url https://source.example.com/jira \
  --source-username "$SOURCE_USERNAME" \
  --source-password "$SOURCE_PASSWORD" \
  --teams-file ./teams.json
```

The filter scan currently performs a proof-of-concept inventory pass against Jira's filter REST API and exports filters whose JQL contains a `Team = ...` clause matching a known source team ID or team name.
It only scans filters visible to the authenticated user, so production coverage depends on the permissions of the account used for the scan.

The CLI uses basic auth for Jira API access. When credentials are not supplied through flags or environment variables, it prompts for them at runtime and does not store them in the profile.
