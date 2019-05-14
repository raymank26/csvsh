package com.github.raymank26

import java.sql.ResultSet

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine(private val sqlParser: SqlAstBuilder,
                     private val sqlPlanner: SqlPlanner) {

    @Throws(SyntaxException::class)
    fun execute(sqlStatement: String, engineContext: EngineContext): Iterator<ResultSet> {
        val parsedStatement = sqlParser.parse(sqlStatement)
        val tableName: String = SqlTableVisitor().visit(parsedStatement)
                ?: throw RuntimeException("Table is not defined")
        val indexes = loadIndexes(engineContext, tableName)
        val plan = sqlPlanner.makePlan(parsedStatement, indexes.map { it.description })

        return TODO()
    }

    fun loadIndexes(engineContext: EngineContext, tableName: String): List<IndexDescriptionAndPath> {
        return TODO()
    }
}