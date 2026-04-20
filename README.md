# Teams Migrator

This tool helps migrate teams used by Jira Advanced Roadmaps (formerly Portfolio, now called Plans in Cloud) between Jira Server/Data Center instances. It prepares source and destination exports to make manual recreation and mapping easier, migrates teams and memberships where possible, and can fix related Jira references such as Parent Links and Team IDs in saved filters. It does not support Jira Cloud.

## Install

### Linux
```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator init
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
teams-migrator init
```

### macOS
```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-plans-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator init
```

### Windows
```powershell
irm https://raw.githubusercontent.com/CollectCall/jira-plans-dc-to-dc-migrator/master/scripts/install-release.ps1 | iex
teams-migrator.exe init
```

The release installer downloads the latest published GitHub Release for your OS and CPU and prefers a writable directory already on `PATH`.
If none is available, it falls back to a user-local bin directory and tells you what to add to `PATH`.

## Important limitation

Non-shared teams must already exist in the destination plan. This tool does not create them.

## What `init` stores

`init` saves the profile information that is stable across runs:
- Jira base URLs
- file paths
- scope choices such as team scope, Parent Link corrections, and filter rewrites

Usernames and passwords are not stored in the profile. If you do not pass them with flags or environment variables, the CLI prompts for them at runtime.

## First Run

```bash
teams-migrator init
teams-migrator migrate --profile default
teams-migrator migrate --profile default --apply
```

The CLI is interactive by default when run in a terminal. If required inputs or secrets are missing, it prompts for them.
Interactive `migrate` can walk through pre-migrate, migrate, and post-migrate in one session.

## Migration phases

- `pre-migrate`: prepares the migration by collecting source and target data and writing review files. This is useful for checking what will happen before any writes, and for capturing the programs, plans, and non-shared team context you may need to recreate manually in the target first.
- `migrate`: creates the teams and memberships that the tool can safely create in the target Jira.
- `post-migrate`: fixes Jira references after the target team IDs exist, such as Parent Link issue references and Team IDs inside saved filter JQL.

## Filter prerequisite

If filter rewrites are in scope, the tool needs a source list of filters that contain team IDs.

To use ScriptRunner for that, install and publish these endpoint scripts in your Jira ScriptRunner app:
- `scripts/sourceFindTeamFiltersDB.groovy` → `/rest/scriptrunner/latest/custom/findTeamFiltersDB`
- `scripts/targetFindTeamFiltersDB.groovy` → `/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB`

The ScriptRunner endpoints require Jira admin permission and basic auth; `init` verifies them during setup when source/base URL is available.

If ScriptRunner is not available, use `--filter-source-csv` with a CSV containing `Filter ID`, `Filter Name`, `Owner`, and `JQL` (DB-derived).

## Migration details

### Teams and membership migration

This is the main migration flow. The tool:

1. Compares source teams with target teams and decides which teams already match and which ones need to be created.
2. Maps people between source and target using the identity mapping, so memberships can be attached to the right target users.
3. Creates the missing teams and memberships in the target Jira, while skipping memberships that already exist or cannot be matched safely.

It can also rewrite an external issues CSV with the mapped target team IDs by using `--issues-csv`.

### Parent Link migration

Parent Link connects an issue to its parent. When this is enabled in `init`, the tool:

1. Exports source issues that have a Parent Link.
2. Compares them with matching issues in the target Jira.
3. Updates only the target issues that still match the expected old parent, and skips anything already correct or no longer safe to change.

Keep `--issue-project-scope` limited to the projects you actually want to correct.

### Filter JQL migration

Some saved filters contain Team IDs in their JQL, such as `Team = 123`. When filter migration is enabled in `init`, the tool:

1. Builds a source list of filters that use Team IDs, from ScriptRunner or a DB-derived CSV.
2. Finds the matching filters in the target Jira and rewrites those Team IDs to the new target IDs.
3. Updates only filters that can be matched safely, and reports the rest for review.

## Other notes

- Use `--team-scope shared-only` and `--team-scope non-shared-only` if you want to split shared and non-shared team work into separate runs.
- If no identity mapping CSV is provided, the tool tries to match users by identical email address.
- Source data can come from exported JSON files or directly from the Jira APIs.
- Normal `migrate` runs write reviewable JSON and CSV reports automatically, and `report --input ...` can re-render a saved JSON report later.

Example:
```bash
teams-migrator report --input out/migrate-report.json --format csv
```

## Commands

- `init`: interactive wizard to create or update a saved profile
- `migrate`: interactive migration flow; use `--apply` to execute writes
- `report`: re-render a previously generated JSON report as JSON or CSV with `--format`
- `config show`: print saved profile config
- `version`: print the current version
- `self-update`: update the installed binary to the latest published release
- `uninstall`: remove the installed binary and local config directory

Config defaults:
- config path: `os.UserConfigDir()/teams-migrator/config.yaml`
- override path with `--config` or `TEAMS_MIGRATOR_CONFIG_PATH`
- select a profile with `--profile` or `TEAMS_MIGRATOR_PROFILE`

Useful options:
- `--filter-source-csv <path>`: required when using a DB-derived filter CSV instead of ScriptRunner
- `--phase pre-migrate|migrate|post-migrate`: run only one phase when needed
- `--no-input`: useful for scripted or non-interactive runs

To update an installed binary to the latest published release:

```bash
teams-migrator self-update
```

To uninstall the binary:

```bash
teams-migrator uninstall
```

The CLI uses basic auth for Jira API access. When credentials are not supplied through flags or environment variables, it prompts for them at runtime and does not store them in the profile.
