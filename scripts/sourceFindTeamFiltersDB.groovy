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
import com.atlassian.query.clause.*
import com.atlassian.jira.security.Permissions
import com.onresolve.scriptrunner.db.DatabaseUtil
import org.ofbiz.core.entity.DelegatorInterface

@BaseScript CustomEndpointDelegate delegate

@Field static String DB_PRODUCT = null


findTeamFiltersDB(httpMethod: "GET") { MultivaluedMap queryParams, String body, HttpServletRequest request ->

    def startTime = System.currentTimeMillis()

    // =========================
    // AUTH
    // =========================
    def authHeader = request.getHeader("Authorization")
    if (!authHeader?.startsWith("Basic ")) {
        return Response.status(401)
            .header("WWW-Authenticate", "Basic realm=\"Jira\"")
            .build()
    }

    def username
    def password

    try {
        def decoded = new String(Base64.decoder.decode(authHeader.substring(6)))
        def parts = decoded.split(":", 2)
        if (parts.length != 2) throw new Exception()
        username = parts[0]
        password = parts[1]
    } catch (Exception ignored) {
        return Response.status(401).entity("Invalid Basic Auth").build()
    }

    def userManager = ComponentAccessor.userManager
    def loginManager = ComponentAccessor.getComponent(com.atlassian.jira.security.login.LoginManager)

    def jiraUser = userManager.getUserByName(username)
    if (!jiraUser || !loginManager.authenticate(jiraUser, password)) {
        return Response.status(401).entity("Invalid credentials").build()
    }

    def permissionManager = ComponentAccessor.permissionManager
    if (!permissionManager.hasPermission(Permissions.ADMINISTER, jiraUser)) {
        return Response.status(403).build()
    }

    if (queryParams.getFirst("enabled") != "true") {
        return Response.status(403).entity("Use ?enabled=true").build()
    }

    // =========================
    // PARAMS
    // =========================
    long lastId
    int limit
    Long teamFieldId

    try {
        lastId = (queryParams.getFirst("lastId") ?: "0") as long
        limit  = (queryParams.getFirst("limit") ?: "200") as int
        teamFieldId = (queryParams.getFirst("teamFieldId") ?: "") as Long
    } catch (Exception ignored) {
        return Response.status(400).entity("Invalid numeric parameters").build()
    }

    if (!teamFieldId) {
        return Response.status(400).entity("teamFieldId is required").build()
    }

    lastId = Math.max(lastId, 0)
    limit  = Math.max(1, Math.min(limit, 1000))

    def expectedField = "cf[" + teamFieldId + "]"

    // =========================
    // SETUP
    // =========================
    def jqlParser = ComponentAccessor.getComponent(JqlQueryParser)
    def searchRequestManager = ComponentAccessor.getComponent(SearchRequestManager)

    def results = []
    def parseErrors = []
    def scanned = 0
    def lastScannedId = lastId

    // =========================
    // DB DETECTION (cached)
    // =========================
    if (DB_PRODUCT == null) {
        DatabaseUtil.withSql('local') { sql ->
            DB_PRODUCT = sql.connection.metaData.databaseProductName.toLowerCase()
        }
    }

    def useSqlLimit = DB_PRODUCT.contains("postgres") || DB_PRODUCT.contains("mysql")

    // =========================
    // FAST PATH
    // =========================
    if (useSqlLimit) {

        DatabaseUtil.withSql('local') { sql ->

            def rows = sql.rows("""
                SELECT id
                FROM searchrequest
                WHERE id > ${lastId}
                ORDER BY id
                LIMIT ${limit}
            """)

            rows.each { row ->
                def id = (row.id as Long)
                scanned++
                lastScannedId = id
                processSingle(id, searchRequestManager, jqlParser, expectedField, results, parseErrors)
            }
        }

    } else {

        def delegator = ComponentAccessor.getComponent(DelegatorInterface)

        def page = delegator.findAll("SearchRequest")
            .findAll { it.getLong("id") > lastId }
            .sort { it.getLong("id") }
            .take(limit)

        page.each { entity ->
            def id = entity.getLong("id")
            scanned++
            lastScannedId = id
            processSingle(id, searchRequestManager, jqlParser, expectedField, results, parseErrors)
        }
    }

    def durationMs = System.currentTimeMillis() - startTime

    return Response.ok(new JsonBuilder([
        meta: [
            lastId          : lastId,
            nextLastId      : lastScannedId,
            scanned         : scanned,
            matched         : results.size(),
            parseErrorCount : parseErrors.size(),
            limit           : limit,
            dbMode          : useSqlLimit ? "sql" : "fallback",
            durationMs      : durationMs
        ],
        results: results,
        parseErrors: parseErrors
    ]).toString()).build()
}


// =========================
// PROCESS
// =========================

def processSingle(id, srm, parser, expectedField, results, parseErrors) {

    def sr = srm.getSearchRequestById(id)
    def jql = sr?.query?.queryString
    if (!jql) return

    def lower = jql.toLowerCase()
    if (!(lower.contains("team") || lower.contains("cf["))) return

    def parsed
    try {
        parsed = parser.parseQuery(jql)
    } catch (Exception e) {
        parseErrors << id
        return
    }

    if (containsTeamClause(parsed?.whereClause, expectedField)) {
        results << [
            id    : id,
            name  : sr?.name,
            owner : sr?.owner?.name,
            jql   : jql
        ]
    }
}


// =========================
// MATCH LOGIC
// =========================

boolean containsTeamClause(Clause clause, String expectedField) {

    if (!clause) return false

    if (clause instanceof TerminalClause) {
        return isTeamClauseMatch(clause, expectedField)
    }

    if (clause instanceof AndClause || clause instanceof OrClause) {
        return clause.clauses.any { containsTeamClause(it, expectedField) }
    }

    if (clause instanceof NotClause) {
        return containsTeamClause(clause.clause, expectedField)
    }

    return false
}

boolean isTeamClauseMatch(TerminalClause clause, String expectedField) {

    def field = (clause.name ?: "").replaceAll('"','').toLowerCase()
    def op = clause.operator?.toString()

    def isTeamField =
        field == "team" ||
        field == expectedField

    def isSupportedOperator =
        op == "EQUALS" || op == "IN"

    return isTeamField && isSupportedOperator
}