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


findSourceTeamFiltersDB(httpMethod: "GET") { MultivaluedMap queryParams, String body, HttpServletRequest request ->

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
    String teamFieldId

    try {
        lastId = (queryParams.getFirst("lastId") ?: "0") as long
        limit  = (queryParams.getFirst("limit") ?: "200") as int
        teamFieldId = queryParams.getFirst("teamFieldId") ?: ""
    } catch (Exception ignored) {
        return Response.status(400).entity("Invalid numeric parameters").build()
    }

    if (!teamFieldId) {
        return Response.status(400).entity("teamFieldId is required").build()
    }

    lastId = Math.max(lastId, 0)
    limit  = Math.max(1, Math.min(limit, 1000))

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
                SELECT sr.id, sr.filtername, sr.authorname, cu.email_address AS owner_email, sr.reqcontent
                FROM searchrequest sr
                LEFT JOIN app_user au ON LOWER(sr.authorname) = LOWER(au.user_key) OR LOWER(sr.authorname) = LOWER(au.lower_user_name)
                LEFT JOIN cwd_user cu ON au.lower_user_name = cu.lower_user_name
                WHERE sr.id > ${lastId}
                ORDER BY sr.id
                LIMIT ${limit}
            """)

            rows.each { row ->
                def id = (row.id as Long)
                scanned++
                lastScannedId = id
                processSingle(id, row.filtername as String, row.authorname as String, row.owner_email as String, row.reqcontent as String, jqlParser, teamFieldId, results, parseErrors)
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

            def searchRequest = searchRequestManager.getSearchRequestById(id)
            def name = searchRequest?.name
            def author = searchRequest?.owner?.name
            def ownerEmail = searchRequest?.owner?.emailAddress
            def jql = searchRequest?.query?.queryString

            processSingle(id, name, author, ownerEmail, jql, jqlParser, teamFieldId, results, parseErrors)
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

def processSingle(id, name, owner, ownerEmail, jql, parser, teamFieldId, results, parseErrors) {
    if (!jql) return

    def lower = jql.toLowerCase()
    if (!(lower.contains("team") || lower.contains("cf["))) return

    if (!sourceJqlHasNumericTeamClause(parser, jql, teamFieldId, parseErrors, id)) return

    results << [
        id    : id,
        name  : name,
        owner : owner,
        ownerEmail: ownerEmail,
        jql   : jql
    ]
}


// =========================
// MATCH LOGIC
// =========================

boolean containsTeamClause(Clause clause, String teamFieldId) {

    if (!clause) return false

    if (clause instanceof TerminalClause) {
        return isTeamClauseMatch(clause, teamFieldId)
    }

    if (clause instanceof AndClause || clause instanceof OrClause) {
        return clause.clauses.any { containsTeamClause(it, teamFieldId) }
    }

    if (clause instanceof NotClause) {
        return notClauseChildren(clause).any { containsTeamClause(it, teamFieldId) }
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

boolean isTeamClauseMatch(TerminalClause clause, String teamFieldId) {

    def field = (clause.name ?: "").replaceAll('"','').toLowerCase()
    def op = clause.operator?.toString()

    def isTeamField =
        field == "team" ||
        field == "teams" ||
        field ==~ /cf\[[0-9]+\]/ ||
        (teamFieldId && field == "cf[${teamFieldId}]")

    def isSupportedOperator =
        op == "EQUALS" || op == "IN"

    return isTeamField && isSupportedOperator && clauseHasNumericTeamOperand(clause.toString())
}

boolean sourceJqlHasNumericTeamClause(def parser, String jql, String teamFieldId, List parseErrors, Long id) {
    if (!jql) return false

    boolean regexMatch = jqlHasNumericTeamClause(jql, teamFieldId)
    if (!regexMatch) return false

    try {
        def parsed = parser.parseQuery(jql)
        if (containsTeamClause(parsed?.whereClause, teamFieldId)) return true
    } catch (Exception e) {
        // Some Jira versions reject or cannot materialize saved filters that still
        // contain Team ID clauses the migrator can safely rewrite by literal JQL.
        return true
    }

    return regexMatch
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
