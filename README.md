# Jira Advanced Roadmaps Teams Migrator

[![Release](https://img.shields.io/github/v/release/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator)](https://github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/releases)
[![CI](https://img.shields.io/github/actions/workflow/status/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/ci.yml?branch=master&label=ci)](https://github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/actions/workflows/ci.yml)
[![Smoke Test](https://img.shields.io/github/actions/workflow/status/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/smoke-test.yml?branch=master&label=smoke)](https://github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/actions/workflows/smoke-test.yml)
[![License](https://img.shields.io/github/license/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator)](./LICENSE)

`teams-migrator` is a Go CLI for migrating Jira Advanced Roadmaps teams and team memberships between Jira Server or Data Center instances. It prepares reviewable artifacts first, then applies only the team, membership, and optional follow-up corrections that can be matched safely.

This project does not support Jira Cloud.

## Why this exists

Advanced Roadmaps team data is awkward to move between Jira Server/Data Center environments. This CLI is built for operators who want:

- a dry-run-first migration flow
- reviewable JSON and CSV artifacts
- support for team and membership migration
- optional Parent Link and saved filter Team ID corrections after target team IDs exist

## Project status

This repository is in an early public stage. The CLI is usable, but the supported migration paths are intentionally narrow and conservative. Expect the interface and docs to keep improving quickly while the core safety model stays strict.

## Supported environments

- Jira Server and Jira Data Center only
- Source data from Jira APIs or exported JSON files
- Target data written through Jira APIs
- Linux, macOS, and Windows release artifacts

See [docs/compatibility.md](docs/compatibility.md) for the support matrix and assumptions.

## Non-goals

- Jira Cloud migration
- creating plans or programs
- linking teams to plans
- UI automation
- broad Jira instance cloning

## Install

### Recommended: download a release artifact

Download the correct asset for your platform from the [GitHub Releases page](https://github.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/releases), verify `checksums.txt`, then install the binary locally.

#### Linux and macOS

```bash
tar -xzf teams-migrator_<version>_<os>_<arch>.tar.gz
mkdir -p ~/.local/bin
install teams-migrator ~/.local/bin/teams-migrator
export PATH="$HOME/.local/bin:$PATH"
teams-migrator version
```

#### Windows

Extract `teams-migrator_<version>_windows_<arch>.zip`, place `teams-migrator.exe` in a directory on `PATH`, then run:

```powershell
teams-migrator.exe version
```

### Convenience installer

If you are comfortable with a remote installer script, release installers are also provided:

#### Linux and macOS

```bash
curl -fsSL https://raw.githubusercontent.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/master/scripts/install-release.sh | sh
teams-migrator init
```

#### Windows

```powershell
irm https://raw.githubusercontent.com/CollectCall/jira-advanced-roadmaps-teams-dc-to-dc-migrator/master/scripts/install-release.ps1 | iex
teams-migrator.exe init
```

The installer downloads the latest published GitHub Release for your OS and CPU and prefers a writable directory already on `PATH`.

## Five-minute quickstart

1. Install a release artifact.
2. Create a profile:

```bash
teams-migrator init
```

3. Run a read-only preparation pass:

```bash
teams-migrator migrate --profile default --phase pre-migrate
```

4. Review the generated files under `out/`.
5. Apply the team and membership migration:

```bash
teams-migrator migrate --profile default --phase migrate --apply
```

6. If Parent Link or filter rewrites are in scope, run the follow-up correction phase:

```bash
teams-migrator migrate --profile default --phase post-migrate --apply
```

For a fuller walkthrough, see [docs/quickstart.md](docs/quickstart.md).

## What the output looks like

The CLI produces reviewable JSON and CSV artifacts under `out/`. A typical run gives you:

- a machine-readable report JSON with actions, findings, stats, and artifact metadata
- CSV comparisons for teams and memberships before writes happen
- post-migrate comparison CSVs for Parent Link and filter Team ID corrections when those scopes are enabled

Sample redacted artifacts:

- [examples/output/migrate-report.sample.json](examples/output/migrate-report.sample.json)
- [examples/output/team-mapping.pre-migration.sample.csv](examples/output/team-mapping.pre-migration.sample.csv)
- [examples/output/team-membership-mapping.pre-migration.sample.csv](examples/output/team-membership-mapping.pre-migration.sample.csv)
- [examples/output/filter-jql-comparison.post-migration.sample.csv](examples/output/filter-jql-comparison.post-migration.sample.csv)

See [docs/sample-output.md](docs/sample-output.md) for a brief guide to how to read them.

## Safety model

- Dry-run is the default.
- `pre-migrate` prepares review artifacts before any writes.
- `migrate --apply` creates only teams and memberships that can be matched safely.
- `post-migrate --apply` updates only references that still match the prepared expectations.
- Non-shared teams must already exist in the destination plan. This tool does not create them.

## What `init` stores

`init` saves reusable profile information such as:

- Jira base URLs
- input and output file paths
- scope choices such as team scope, Parent Link corrections, and filter rewrites

Usernames and passwords are not stored in the profile. If they are not passed by flags or environment variables, the CLI prompts for them at runtime.

## Migration phases

- `pre-migrate`: collect source and target data, analyze mappings, and write review files
- `migrate`: create teams and memberships that can be created safely
- `post-migrate`: correct Parent Link values and Team IDs in saved filters after target IDs exist

## Typical workflows

### Interactive operator flow

```bash
teams-migrator init
teams-migrator migrate --profile default
```

### Non-interactive dry run

```bash
teams-migrator migrate \
  --profile default \
  --phase pre-migrate \
  --no-input
```

### Apply one phase explicitly

```bash
teams-migrator migrate \
  --profile default \
  --phase migrate \
  --apply \
  --no-input
```

## Filter rewrite prerequisites

If filter rewrites are in scope, the tool needs a source list of filters that contain team IDs.

### ScriptRunner option

Install and publish these endpoint scripts in the Jira ScriptRunner app:

- `scripts/sourceFindTeamFiltersDB.groovy` as `/rest/scriptrunner/latest/custom/findTeamFiltersDB`
- `scripts/targetFindTeamFiltersDB.groovy` as `/rest/scriptrunner/latest/custom/findTargetTeamFiltersDB`

The endpoints require Jira admin permission and basic auth. `init` verifies them during setup when the source Jira base URL is available.

### DB-derived CSV option

If ScriptRunner is not available, use `--filter-source-csv` with a CSV containing:

- `Filter ID`
- `Filter Name`
- `Owner`
- `JQL`

Sample SQL exports are provided under `scripts/sql/`:

- `postgresql-source-filters.sql`
- `mysql-source-filters.sql`
- `sqlserver-source-filters.sql`
- `oracle-source-filters.sql`

## Commands

- `init`: interactive wizard to create or update a saved profile
- `migrate`: interactive migration flow; use `--apply` to execute writes
- `report`: re-render a previously generated JSON report as JSON or CSV with `--format`
- `config show`: print the saved profile config
- `version`: print the current version
- `self-update`: update the installed binary to the latest published release
- `uninstall`: remove the installed binary and local config directory

## Configuration defaults

- config path: `os.UserConfigDir()/teams-migrator/config.yaml`
- override with `--config` or `TEAMS_MIGRATOR_CONFIG_PATH`
- select a profile with `--profile` or `TEAMS_MIGRATOR_PROFILE`

Useful options:

- `--filter-source-csv <path>`: use a DB-derived filter CSV instead of ScriptRunner
- `--phase pre-migrate|migrate|post-migrate`: run a single phase
- `--no-input`: disable interactive prompts

## Examples and sample files

- sample profile store: [examples/config.sample.yaml](examples/config.sample.yaml)
- sample identity mapping: [examples/identity-mapping.sample.csv](examples/identity-mapping.sample.csv)
- sample issues CSV: [examples/issues.sample.csv](examples/issues.sample.csv)
- sample filter CSV: [examples/filter-source.sample.csv](examples/filter-source.sample.csv)

## Documentation

- [docs/quickstart.md](docs/quickstart.md)
- [docs/compatibility.md](docs/compatibility.md)
- [docs/sample-output.md](docs/sample-output.md)
- [docs/roadmap.md](docs/roadmap.md)
- [docs/support.md](docs/support.md)
- [docs/release-policy.md](docs/release-policy.md)
- [CHANGELOG.md](CHANGELOG.md)
- [DESIGN.md](DESIGN.md)
- [SPECIFICATIONS.md](SPECIFICATIONS.md)

## Contributing

Public contributions are welcome, especially around documentation, operator ergonomics, and narrowly scoped migration cases. Start with [CONTRIBUTING.md](CONTRIBUTING.md), then use the issue templates for bug reports or feature requests.

## Security

Please do not open public issues for credential leaks, unsafe write paths, or other security-sensitive reports. Use the process in [SECURITY.md](SECURITY.md).

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
