# Sample Output

These examples are synthetic and redacted. They show the shape of the generated artifacts without exposing real Jira data.

## Example pre-migrate report JSON

See [examples/output/migrate-report.sample.json](../examples/output/migrate-report.sample.json).

This report shows:

- command and mode metadata
- actions the tool planned or generated
- findings with severity, code, and message
- summary stats
- artifact metadata pointing to reviewable CSVs

## Example team mapping CSV

See [examples/output/team-mapping.pre-migration.sample.csv](../examples/output/team-mapping.pre-migration.sample.csv).

This is the kind of file an operator reviews before applying changes:

- which source teams match target teams
- which teams are safe to create
- which teams are blocked by conflicts or manual prerequisites

## Example team membership mapping CSV

See [examples/output/team-membership-mapping.pre-migration.sample.csv](../examples/output/team-membership-mapping.pre-migration.sample.csv).

This shows:

- source resource and team identity
- source and target email mapping
- target team and user resolution
- whether the membership is planned, skipped, or already satisfied

## Example filter rewrite comparison CSV

See [examples/output/filter-jql-comparison.post-migration.sample.csv](../examples/output/filter-jql-comparison.post-migration.sample.csv).

This is the kind of post-migrate artifact used to review Team ID rewrites in saved filters before or after apply mode.
