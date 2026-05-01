import com.onresolve.scriptrunner.runner.rest.common.CustomEndpointDelegate
import groovy.json.JsonBuilder
import groovy.transform.BaseScript
import groovy.transform.Field

import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response
import javax.servlet.http.HttpServletRequest
import java.util.Base64

import com.atlassian.jira.component.ComponentAccessor
import com.atlassian.jira.issue.search.SearchRequestManager
import com.atlassian.jira.jql.parser.JqlQueryParser
import com.atlassian.jira.security.Permissions
import com.atlassian.jira.user.ApplicationUser

import com.atlassian.query.clause.*
import com.atlassian.query.operand.*

import com.onresolve.scriptrunner.db.DatabaseUtil
import org.ofbiz.core.entity.DelegatorInterface

@BaseScript CustomEndpointDelegate delegate

@Field static String DB_PRODUCT = null

findTargetTeamFiltersDB(httpMethod: "GET") { MultivaluedMap queryParams, String body, HttpServletRequest request ->

    long start = System.currentTimeMillis()

    // =========================
    // AUTH (EXPLICIT BASIC AUTH)
    // =========================
    def authHeader = request.getHeader("Authorization")
    if (!authHeader?.startsWith("Basic ")) {
        return Response.status(401).entity("Missing Basic Auth").build()
    }

    def encoded = authHeader.substring("Basic ".length())
    def decoded = new String(Base64.decoder.decode(encoded))
    def (username, password) = decoded.split(":", 2)

    def userManager = ComponentAccessor.getUserManager()
    ApplicationUser user = userManager.getUserByName(username)

    if (!user) {
        return Response.status(401).entity("Invalid user").build()
    }

    def permissionManager = ComponentAccessor.getPermissionManager()
    if (!permissionManager.hasPermission(Permissions.ADMINISTER, user)) {
        return Response.status(403).entity("Admin permission required").build()
    }

    if (queryParams.getFirst("enabled") != "true") {
        return Response.status(400).entity("Endpoint disabled").build()
    }

    Long lastId
    Integer limit
    String teamFieldId = queryParams.getFirst("teamFieldId")

    try {
        lastId = (queryParams.getFirst("lastId") ?: "0") as Long
        limit = (queryParams.getFirst("limit") ?: "500") as Integer
    } catch (Exception ignored) {
        return Response.status(400).entity("Invalid numeric parameters").build()
    }

    lastId = Math.max(lastId, 0)
    limit = Math.max(1, Math.min(limit, 1000))

    String filterName = queryParams.getFirst("filterName")
    String owner = queryParams.getFirst("owner")
    String ownerEmail = queryParams.getFirst("ownerEmail")

    def namesCsv = queryParams.getFirst("namesCsv")?.split(",")*.trim()
    def ownersCsv = queryParams.getFirst("ownersCsv")?.split(",")*.trim()

    def norm = { it?.toLowerCase()?.trim() }
    def filterNameNorm = norm(filterName)
    def ownerNorm = norm(owner)
    def ownerEmailNorm = norm(ownerEmail)
    def namesNorm = normalizedValues(namesCsv)
    def ownersNorm = normalizedValues(ownersCsv)

    def jqlParser = ComponentAccessor.getComponent(JqlQueryParser)
    def searchRequestManager = ComponentAccessor.getComponent(SearchRequestManager)

    def results = []
    def parseErrors = []

    int scanned = 0
    int matched = 0
    long lastScannedId = lastId

    // =========================
    // DB ACCESS (SQL + FALLBACK)
    // =========================
    boolean usedFallback = false

    try {
        if (DB_PRODUCT == null) {
            DatabaseUtil.withSql('local') { sql ->
                DB_PRODUCT = sql.connection.metaData.databaseProductName.toLowerCase()
            }
        }

        def useSqlLimit = DB_PRODUCT.contains("postgres") || DB_PRODUCT.contains("mysql")
        if (!useSqlLimit) {
            throw new UnsupportedOperationException("Database product does not support this endpoint's LIMIT query path: ${DB_PRODUCT}")
        }

        def whereParts = ["sr.id > ?"]
        def params = [lastId]

        if (filterNameNorm) {
            whereParts << "LOWER(sr.filtername) = ?"
            params << filterNameNorm
        } else if (namesNorm) {
            whereParts << "LOWER(sr.filtername) IN (${placeholders(namesNorm.size())})"
            params.addAll(namesNorm)
        }

        if (ownerNorm) {
            whereParts << "LOWER(sr.authorname) = ?"
            params << ownerNorm
        } else if (ownerEmailNorm) {
            whereParts << "LOWER(cu.email_address) = ?"
            params << ownerEmailNorm
        } else if (ownersNorm) {
            whereParts << "LOWER(sr.authorname) IN (${placeholders(ownersNorm.size())})"
            params.addAll(ownersNorm)
        }

        params << limit

        def query = """
            SELECT sr.id, sr.filtername, sr.authorname, cu.email_address AS owner_email, sr.reqcontent
            FROM searchrequest sr
            LEFT JOIN app_user au ON LOWER(sr.authorname) = LOWER(au.user_key) OR LOWER(sr.authorname) = LOWER(au.lower_user_name)
            LEFT JOIN cwd_user cu ON au.lower_user_name = cu.lower_user_name
            WHERE ${whereParts.join(" AND ")}
            ORDER BY sr.id
            LIMIT ?
        """.toString()

        DatabaseUtil.withSql('local') { sql ->

            sql.eachRow(query, params) { row ->

                scanned++

                def id = row.id as Long
                lastScannedId = id

                def name = row.filtername as String
                def author = row.authorname as String
                def authorEmail = row.owner_email as String
                def jql = row.reqcontent as String

                if (!jql) return
                if (!jqlMayContainTeamClause(jql)) return

                if (!targetJqlHasNumericTeamClause(jqlParser, jql, teamFieldId)) return

                matched++

                results << [
                    id   : id,
                    name : name,
                    owner: author,
                    ownerEmail: authorEmail,
                    jql  : jql
                ]
            }
        }

    } catch (Exception ex) {

        // Fallback (non-LIMIT DBs)
        usedFallback = true

        DelegatorInterface delegator = ComponentAccessor.getComponent(DelegatorInterface)

        def rows = delegator.findAll("SearchRequest")
                .sort { it.getLong("id") }

        for (row in rows) {

            def id = row.getLong("id")
            if (id <= lastId) continue
            if (scanned >= limit) break

            scanned++
            lastScannedId = id

            def searchRequest = searchRequestManager.getSearchRequestById(id)
            def name = searchRequest?.name
            def author = searchRequest?.owner?.name
            def authorEmail = searchRequest?.owner?.emailAddress
            def jql = searchRequest?.query?.queryString

            def nameNorm = norm(name)
            def authorNorm = norm(author)
            def authorEmailNorm = norm(authorEmail)

            if (filterNameNorm && nameNorm != filterNameNorm) continue
            if (ownerNorm && authorNorm != ownerNorm) continue
            if (ownerEmailNorm && authorEmailNorm != ownerEmailNorm) continue
            if (namesNorm && !namesNorm.contains(nameNorm)) continue
            if (ownersNorm && !ownersNorm.contains(authorNorm)) continue

            if (!jql) continue
            if (!jqlMayContainTeamClause(jql)) continue

            if (!targetJqlHasNumericTeamClause(jqlParser, jql, teamFieldId)) continue

            matched++

            results << [
                id   : id,
                name : name,
                owner: author,
                ownerEmail: authorEmail,
                jql  : jql
            ]
        }
    }

    long duration = System.currentTimeMillis() - start

    def response = [
        meta: [
            lastId         : lastId,
            nextLastId     : lastScannedId,
            scanned        : scanned,
            matched        : matched,
            parseErrorCount: parseErrors.size(),
            limit          : limit,
            dbMode         : usedFallback ? "delegator" : "sql",
            durationMs     : duration
        ],
        results: results,
        parseErrors: parseErrors
    ]

    return Response.ok(new JsonBuilder(response).toString()).build()
}

List<String> normalizedValues(Collection values) {

    if (!values) return []

    return values
        .collect { it?.toString()?.toLowerCase()?.trim() }
        .findAll { it }
        .unique()
}

String placeholders(int count) {
    return (1..count).collect { "?" }.join(", ")
}

boolean jqlMayContainTeamClause(String jql) {

    def lower = jql?.toLowerCase()
    return lower?.contains("team") || lower?.contains("cf[")
}

boolean jqlHasNumericTeamClause(String jql, String teamFieldId) {
    if (!jql) return false

    def fieldPattern = teamFieldId ?
            /(?:"?team"?|\bteam\b|"?teams"?|\bteams\b|cf\[[0-9]+\]|cf\[\Q${teamFieldId}\E\])/ :
            /(?:"?team"?|\bteam\b|"?teams"?|\bteams\b|cf\[[0-9]+\])/

    def equalsPattern = ~/(?i)(^|[^A-Za-z0-9_])${fieldPattern}\s*=\s*["']?[0-9]+["']?([^A-Za-z0-9_]|$)/
    if ((jql =~ equalsPattern).find()) return true

    def inPattern = ~/(?i)(^|[^A-Za-z0-9_])${fieldPattern}\s+in\s*\(([^)]*)\)/
    def matcher = jql =~ inPattern
    while (matcher.find()) {
        def rawValues = matcher.group(2)
        if (!rawValues) continue
        def values = rawValues.split(",")*.trim().findAll { it }
        if (values && values.every { it.replaceAll(/^["']|["']$/, "").isLong() }) {
            return true
        }
    }

    return false
}

boolean targetJqlHasNumericTeamClause(def jqlParser, String jql, String teamFieldId) {
    if (!jql) return false

    boolean regexMatch = jqlHasNumericTeamClause(jql, teamFieldId)
    if (!regexMatch) return false

    try {
        def parsed = jqlParser.parseQuery(jql)
        if (hasTeamIdClause(parsed?.whereClause, teamFieldId)) return true
    } catch (Exception e) {
        // Some Jira versions reject saved filters that still contain Team ID clauses
        // the migrator can rewrite safely, including quoted numeric IDs.
        return true
    }

    return regexMatch
}

boolean hasTeamIdClause(Clause clause, String teamFieldId) {

    if (!clause) return false

    if (clause instanceof TerminalClause) {
        def field = (clause.name ?: "").replaceAll('"', '').toLowerCase()
        def operator = clause.operator?.toString()

        boolean fieldMatch =
                (field == "team") ||
                (field == "teams") ||
                (field ==~ /cf\[[0-9]+\]/) ||
                (teamFieldId && field == "cf[${teamFieldId}]")

        boolean operatorMatch =
                (operator == "EQUALS" || operator == "IN")

        return fieldMatch && operatorMatch && clauseHasNumericTeamOperand(clause.toString())
    }

    if (clause instanceof AndClause || clause instanceof OrClause) {
        return clause.clauses.any { hasTeamIdClause(it, teamFieldId) }
    }

    if (clause instanceof NotClause) {
        return notClauseChildren(clause).any { hasTeamIdClause(it, teamFieldId) }
    }

    return false
}

List<Clause> notClauseChildren(NotClause clause) {

    for (propertyName in ["subClause", "clause", "clauses"]) {
        if (!clause.hasProperty(propertyName)) continue

        def value = clause[propertyName]
        if (!value) continue

        if (value instanceof Collection) {
            return value.findAll { it instanceof Clause } as List<Clause>
        }

        if (value instanceof Clause) {
            return [value]
        }
    }

    return []
}

boolean clauseHasNumericTeamOperand(String clauseText) {
    if (!clauseText) return false

    def equalsMatch = clauseText =~ /(?i)\s*"?[^"]+"?\s*=\s*["']?([0-9]+)["']?\s*/
    if (equalsMatch.matches()) return true

    def inMatch = clauseText =~ /(?i)\s*"?[^"]+"?\s+in\s*\((.*)\)\s*/
    if (!inMatch.matches()) return false

    def rawValues = inMatch[0][1]
    return rawValues.split(",").every { value ->
        value.trim().replaceAll(/^["']|["']$/, "").isLong()
    }
}
