package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine {

    private val sqlParser: SqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner: SqlPlanner = SqlPlanner()
    private val sqlExecutor: SqlExecutor = SqlExecutor()

    @Throws(SyntaxException::class)
    fun execute(sqlStatement: String, engineContext: EngineContext): CsvContent {
        val parsedStatement = sqlParser.parse(sqlStatement)
        val tableName: String = SqlTableVisitor().visit(parsedStatement)
                ?: throw RuntimeException("Table is not defined")
        val indexes = loadIndexes(engineContext, tableName)
        val planDescriptor = sqlPlanner.makePlan(parsedStatement, indexes.map { it.description })
        return sqlExecutor.execute(engineContext, planDescriptor)
    }

    fun loadIndexes(engineContext: EngineContext, tableName: String): List<IndexDescriptionAndPath> {
        return TODO()
    }
}