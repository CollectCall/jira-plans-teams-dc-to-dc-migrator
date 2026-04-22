import com.onresolve.scriptrunner.runner.rest.common.CustomEndpointDelegate
import groovy.json.JsonBuilder
import groovy.transform.BaseScript
import groovy.transform.Field

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

    def namesCsv = queryParams.getFirst("namesCsv")?.split(",")*.trim()
    def ownersCsv = queryParams.getFirst("ownersCsv")?.split(",")*.trim()

    def norm = { it?.toLowerCase()?.trim() }
    def filterNameNorm = norm(filterName)
    def ownerNorm = norm(owner)
    def namesNorm = normalizedValues(namesCsv)
    def ownersNorm = normalizedValues(ownersCsv)

    def jqlParser = ComponentAccessor.getComponent(JqlQueryParser)

    def results = []
    def parseErrors = []

    int scanned = 0
    int matched = 0
    long lastScannedId = lastId

    // =========================
    // CLAUSE DETECTOR (STRICT)
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
                    (operand instanceof SingleValueOperand && operandValueIsId(operand.value)) ||
                    (operand instanceof MultiValueOperand && operand.values.every { it instanceof SingleValueOperand && operandValueIsId(it.value) })

            return fieldMatch && operatorMatch && operandIsId
        }

        if (clause instanceof AndClause || clause instanceof OrClause) {
            return clause.clauses.any { hasTeamIdClause(it) }
        }

        if (clause instanceof NotClause) {
            return notClauseChildren(clause).any { hasTeamIdClause(it) }
        }

        return false
    }

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

        def whereParts = ["id > ?"]
        def params = [lastId]

        if (filterNameNorm) {
            whereParts << "LOWER(filtername) = ?"
            params << filterNameNorm
        } else if (namesNorm) {
            whereParts << "LOWER(filtername) IN (${placeholders(namesNorm.size())})"
            params.addAll(namesNorm)
        }

        if (ownerNorm) {
            whereParts << "LOWER(authorname) = ?"
            params << ownerNorm
        } else if (ownersNorm) {
            whereParts << "LOWER(authorname) IN (${placeholders(ownersNorm.size())})"
            params.addAll(ownersNorm)
        }

        params << limit

        def query = """
            SELECT id, filtername, authorname, reqcontent
            FROM searchrequest
            WHERE ${whereParts.join(" AND ")}
            ORDER BY id
            LIMIT ?
        """.toString()

        DatabaseUtil.withSql('local') { sql ->

            sql.eachRow(query, params) { row ->

                scanned++

                def id = row.id as Long
                lastScannedId = id

                def name = row.filtername as String
                def author = row.authorname as String
                def jql = row.reqcontent as String

                if (!jql) return
                if (!jqlMayContainTeamClause(jql)) return

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

            if (filterNameNorm && nameNorm != filterNameNorm) continue
            if (ownerNorm && authorNorm != ownerNorm) continue
            if (namesNorm && !namesNorm.contains(nameNorm)) continue
            if (ownersNorm && !ownersNorm.contains(authorNorm)) continue

            if (!jql) continue
            if (!jqlMayContainTeamClause(jql)) continue

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

boolean operandValueIsId(value) {
    return value != null && value.toString().trim().replaceAll(/^["']|["']$/, "").isLong()
}
