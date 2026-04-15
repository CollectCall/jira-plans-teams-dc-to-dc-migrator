# Teams Migrator
CLI-first scaffold for migrating Jira Teams and Team Membership data between Jira instances.

This tool is intended for Jira Server/Data Center to Jira Server/Data Center migrations only.
It is not designed for Jira Cloud migrations.

The CLI can read source data from JSON exports or directly from the Jira Teams API. Target resolution and apply-mode writes use the Jira Teams API.
When Jira base URLs are available, the tool also reads Portfolio Programs and Plans from the Jira Portfolio `jpo-api`, inspects Jira issue fields, identifies the Teams issue field, finds issues with a value in that field, and exports separate pre-migration baseline and comparison CSVs.

The CLI is interactive by default when run in a terminal. If required inputs or secrets are missing, it prompts for them instead of forcing you to predefine environment variables. Use `--no-input` if you want strict non-interactive behavior for automation.
The terminal UI auto-detects whether it should use a richer color/unicode mode or a plain fallback mode. You can override that with `TEAMS_MIGRATOR_UI=rich` or `TEAMS_MIGRATOR_UI=plain`.

It also supports saved profiles in a cross-platform config file:
- macOS/Linux/Windows default path: `os.UserConfigDir()/teams-migrator/config.yaml`
- override path with `--config` or `TEAMS_MIGRATOR_CONFIG_PATH`
- select a profile with `--profile` or `TEAMS_MIGRATOR_PROFILE`
- reusable credentials can be stored separately in an encrypted secret store next to the config file, not in the YAML file

## Commands
- `validate`: validate configuration and local inputs
- `plan`: generate a migration plan report
- `migrate`: run a dry-run by default, or switch to apply mode with `--apply`
- `report`: re-render a previously generated JSON report as JSON or CSV
- `config init`: interactive wizard to create or update a saved profile
- `config path`: print the config file path in use
- `config show`: print saved profile config, redacted by default

## Build
```bash
go build -o bin/teams-migrator ./cmd/teams-migrator
```

## Linux quick start
If the operator is on Linux and just needs the tool installed locally with the least friction:

```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator config init
```

## macOS quick start
```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator config init
```

The release installer downloads the latest published GitHub Release for your OS and CPU and installs the binary into `~/.local/bin` by default.
If that directory is not already in `PATH`, add it:

```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

On macOS with the default shell, use `~/.zshrc` instead of `~/.bashrc`.

## Windows quick start
```powershell
irm https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.ps1 | iex
teams-migrator.exe config init
```

## Source install
If you want a source-based install instead of release binaries:

```bash
go install github.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/cmd/teams-migrator@latest
```

That requires Go 1.26 or newer. If you prefer to clone the repo first, `./scripts/install-linux.sh` still installs to `~/.local/bin/teams-migrator`.

## Release process
Push a tag like `v0.1.0` and GitHub Actions will build and publish release archives for:
- Linux `amd64`, `arm64`
- macOS `amd64`, `arm64`
- Windows `amd64`, `arm64`

## First-run setup
```bash
./bin/teams-migrator config init
```

That wizard creates or updates `config.yaml` with a named profile. After that, normal commands can run with the saved defaults:

```bash
./bin/teams-migrator validate
./bin/teams-migrator plan --profile default
./bin/teams-migrator migrate --profile default --apply
```

Inspect config location and saved values:

```bash
./bin/teams-migrator config path
./bin/teams-migrator config show
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

## Interactive UX
- Prompts for missing file paths, base URLs, and auth details when running in a terminal.
- Prompts for secrets interactively instead of requiring env vars up front.
- Asks for explicit confirmation before `--apply` continues with mutating writes.
- Supports `config init` to save reusable profiles in `config.yaml`.
- Supports `config path` and `config show` so operators can find and inspect the active config without searching manually.
- Stores reusable credentials outside YAML in an encrypted secret store when you choose to save them.
- Loads saved profile values before prompting for anything still missing.
- Keeps `--no-input` available for CI or scripted usage.

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
TEAMS_MIGRATOR_UI
```

## Exit codes
- `0`: success
- `1`: fatal error or validation errors
- `2`: strict mode detected warnings or errors

## Current migration behavior
- Reuses destination teams by normalized title match.
- Creates missing destination teams in apply mode.
- Resolves source users via `sourceEmail -> targetEmail -> target jiraUserId`.
- If no identity mapping CSV is supplied, auto-resolves users where source and target emails are identical and writes `out/identity-mapping.generated.csv`.
- Writes clearly separated pre-migration CSV exports for:
  - `out/source-programs.pre-migration.csv`
  - `out/destination-programs.pre-migration.csv`
  - `out/program-mapping.pre-migration.csv`
  - `out/source-plans.pre-migration.csv`
  - `out/destination-plans.pre-migration.csv`
  - `out/plan-mapping.pre-migration.csv`
  - `out/source-teams.pre-migration.csv`
  - `out/destination-teams.pre-migration.csv`
  - `out/team-mapping.pre-migration.csv`
  - `out/source-team-memberships.pre-migration.csv`
  - `out/destination-team-memberships.pre-migration.csv`
  - `out/team-membership-mapping.pre-migration.csv`
  - `out/issues-with-teams.pre-migration.csv` when source API access is available
- Recreates team membership through `POST /resource` in apply mode.
- Writes JSON or CSV reports to `out/`.
