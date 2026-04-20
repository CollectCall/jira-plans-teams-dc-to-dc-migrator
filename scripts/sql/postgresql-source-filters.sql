-- Export candidate Jira filters for --filter-source-csv from PostgreSQL.
--
-- Expected CSV column order:
--   Filter ID, Filter Name, Owner, JQL
--
-- Notes:
-- - This is a shortlist query. The migrator will parse JQL itself and keep only
--   filters that actually contain team-ID clauses.
-- - Export the result to CSV with headers in your SQL client of choice.

SELECT
  sr.id AS "Filter ID",
  sr.filtername AS "Filter Name",
  sr.authorname AS "Owner",
  sr.reqcontent AS "JQL"
FROM searchrequest sr
WHERE sr.reqcontent IS NOT NULL
  AND sr.reqcontent <> ''
  AND (
    lower(sr.reqcontent) LIKE '%team%'
    OR lower(sr.reqcontent) LIKE '%cf[%'
  )
ORDER BY sr.id;
