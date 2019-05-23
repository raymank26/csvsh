package com.github.raymank26

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine {

    private val sqlParser: SqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner: SqlPlanner = SqlPlanner()
    private val sqlExecutor: SqlExecutor = SqlExecutor()
    private val ssExecutor = ServiceStatementsExecutor()

    fun execute(inputLine: String): ExecutorResponse {
        val parsedStatement: StatementType
        try {
            parsedStatement = sqlParser.parse(inputLine)
        } catch (e: SyntaxException) {
            return TextResponse(e.message!!)
        }
        val csvReadingFactory = ssExecutor.createCsvDatasetReaderFactory()

        return when (parsedStatement) {
            is CreateIndexType -> {
                ssExecutor.createIndex(parsedStatement.ctx, csvReadingFactory)
                VoidResponse
            }
            is DescribeStatement -> {
                val description = ssExecutor.describeTable(parsedStatement.ctx, csvReadingFactory)
                TextResponse(description.toString())
            }
            is SelectStatement -> {
                val planDescriptor = sqlPlanner.createPlan(parsedStatement.ctx, csvReadingFactory)
                val result = planDescriptor.datasetReader.use {
                    sqlExecutor.execute(planDescriptor)
                }
                TextResponse(result.toString())
            }
        }
    }
}

sealed class ExecutorResponse

data class TextResponse(val value: String) : ExecutorResponse()

object VoidResponse : ExecutorResponse()


