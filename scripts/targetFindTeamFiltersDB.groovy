import com.onresolve.scriptrunner.runner.rest.common.CustomEndpointDelegate
import groovy.json.JsonBuilder
import groovy.transform.BaseScript

import javax.ws.rs.core.MultivaluedMap
import javax.ws.rs.core.Response
import javax.servlet.http.HttpServletRequest
import java.util.Base64

import com.atlassian.jira.component.ComponentAccessor
import com.atlassian.jira.jql.parser.JqlQueryParser
import com.atlassian.jira.security.Permissions
import com.atlassian.jira.user.ApplicationUser

import com.atlassian.query.clause.*
import com.atlassian.query.operand.*

import com.onresolve.scriptrunner.db.DatabaseUtil
import org.ofbiz.core.entity.DelegatorInterface

@BaseScript CustomEndpointDelegate delegate

findTargetTeamFiltersDB(httpMethod: "GET") { MultivaluedMap queryParams, String body, HttpServletRequest request ->

    long start = System.currentTimeMillis()

    // =========================
    // 🔒 AUTH (EXPLICIT BASIC AUTH)
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

    Long lastId = (queryParams.getFirst("lastId") ?: "0") as Long
    Integer limit = (queryParams.getFirst("limit") ?: "500") as Integer
    String teamFieldId = queryParams.getFirst("teamFieldId")

    String filterName = queryParams.getFirst("filterName")
    String owner = queryParams.getFirst("owner")

    def namesCsv = queryParams.getFirst("namesCsv")?.split(",")*.trim()
    def ownersCsv = queryParams.getFirst("ownersCsv")?.split(",")*.trim()

    def norm = { it?.toLowerCase()?.trim() }

    def jqlParser = ComponentAccessor.getComponent(JqlQueryParser)

    def results = []
    def parseErrors = []

    int scanned = 0
    int matched = 0
    long lastScannedId = lastId

    // =========================
    // 🔍 CLAUSE DETECTOR (STRICT)
    // =========================
    def hasTeamIdClause
    hasTeamIdClause = { Clause clause ->

        if (!clause) return false

        if (clause instanceof TerminalClause) {
            def field = clause.name
            def operator = clause.operator
            def operand = clause.operand

            boolean fieldMatch =
                    (field == "team") ||
                    (teamFieldId && field == "cf[${teamFieldId}]")

            boolean operatorMatch =
                    (operator == Operator.EQUALS || operator == Operator.IN)

            boolean operandIsId =
                    (operand instanceof SingleValueOperand && operand.value?.isLong()) ||
                    (operand instanceof MultiValueOperand && operand.values.every { it instanceof SingleValueOperand && it.value?.isLong() })

            return fieldMatch && operatorMatch && operandIsId
        }

        if (clause instanceof AndClause || clause instanceof OrClause) {
            return clause.clauses.any { hasTeamIdClause(it) }
        }

        if (clause instanceof NotClause) {
            return hasTeamIdClause(clause.subClause)
        }

        return false
    }

    // =========================
    // 🔍 DB ACCESS (SQL + FALLBACK)
    // =========================
    boolean usedFallback = false

    try {
        def query = """
            SELECT id, filtername, authorname, reqcontent
            FROM searchrequest
            WHERE id > ?
            ORDER BY id
            LIMIT ?
        """

        DatabaseUtil.withSql('local') { sql ->

            sql.eachRow(query, [lastId, limit]) { row ->

                scanned++

                def id = row.id as Long
                lastScannedId = id

                def name = row.filtername as String
                def author = row.authorname as String
                def jql = row.reqcontent as String

                def nameNorm = norm(name)
                def authorNorm = norm(author)

                // narrowing
                if (filterName && nameNorm != norm(filterName)) return
                if (owner && authorNorm != norm(owner)) return
                if (namesCsv && !namesCsv.collect { norm(it) }.contains(nameNorm)) return
                if (ownersCsv && !ownersCsv.collect { norm(it) }.contains(authorNorm)) return

                if (!jql) return

                try {
                    def parsed = jqlParser.parseQuery(jql)

                    if (!hasTeamIdClause(parsed?.whereClause)) return

                    matched++

                    results << [
                        id   : id,
                        name : name,
                        owner: author,
                        jql  : jql
                    ]

                } catch (Exception e) {
                    parseErrors << [id: id, name: name, error: e.message]
                }
            }
        }

    } catch (Exception ex) {

        // Fallback (non-LIMIT DBs)
        usedFallback = true

        DelegatorInterface delegator = ComponentAccessor.getComponent(DelegatorInterface)

        def rows = delegator.findList(
                "SearchRequest",
                null,
                null,
                ["id ASC"],
                null,
                false
        )

        for (row in rows) {

            def id = row.getLong("id")
            if (id <= lastId) continue
            if (scanned >= limit) break

            scanned++
            lastScannedId = id

            def name = row.getString("filtername")
            def author = row.getString("authorname")
            def jql = row.getString("reqcontent")

            def nameNorm = norm(name)
            def authorNorm = norm(author)

            if (filterName && nameNorm != norm(filterName)) continue
            if (owner && authorNorm != norm(owner)) continue
            if (namesCsv && !namesCsv.collect { norm(it) }.contains(nameNorm)) continue
            if (ownersCsv && !ownersCsv.collect { norm(it) }.contains(authorNorm)) continue

            if (!jql) continue

            try {
                def parsed = jqlParser.parseQuery(jql)

                if (!hasTeamIdClause(parsed?.whereClause)) continue

                matched++

                results << [
                    id   : id,
                    name : name,
                    owner: author,
                    jql  : jql
                ]

            } catch (Exception e) {
                parseErrors << [id: id, name: name, error: e.message]
            }
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