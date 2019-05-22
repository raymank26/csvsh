package com.github.raymank26

import org.apache.commons.csv.CSVFormat

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine {

    private val sqlParser: SqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner: SqlPlanner = SqlPlanner()
    private val sqlExecutor: SqlExecutor = SqlExecutor()
    private val ssExecutor = ServiceStatementsExecutor(CSVFormat.RFC4180)

    fun execute(inputLine: String): ExecutorResponse {
        val parsedStatement: StatementType
        try {
            parsedStatement = sqlParser.parse(inputLine)
        } catch (e: SyntaxException) {
            return TextResponse(e.message!!)
        }

        return when (parsedStatement) {
            is CreateIndexType -> {
                ssExecutor.createIndex(parsedStatement.ctx)
                VoidResponse
            }
            is DescribeStatement -> {
                val description = ssExecutor.describeTable(parsedStatement.ctx)
                TextResponse(description.toString())
            }
            is SelectStatement -> {
                val csvFactory = ssExecutor.createCsvDatasetReaderFactory()
                val planDescriptor = sqlPlanner.createPlan(parsedStatement.ctx, csvFactory)
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


