package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import java.nio.file.Path
import java.nio.file.Paths

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
                val csvPath: Path = SqlTableVisitor().visit(parsedStatement.ctx)?.let { Paths.get(it) }
                        ?: throw RuntimeException("Table is not defined")
                val indexes = ssExecutor.loadIndexes(csvPath)

                try {
                    val planDescriptor = sqlPlanner.makePlan(parsedStatement.ctx, indexes.map { it.description })
                    val fieldToIndex: Map<String, ReadOnlyIndex> = indexes.map { id -> Pair(id.description.fieldName, id.indexContent) }.toMap()
                    val result = sqlExecutor.execute(EngineContext(CsvDatasetReader(CSVFormat.RFC4180, csvPath),
                            fieldToIndex), planDescriptor)
                    TextResponse(result.toString())
                } finally {
                    indexes.forEach {
                        it.indexContent.use {  }
                    }
                }
            }
        }
    }
}

sealed class ExecutorResponse

data class TextResponse(val value: String): ExecutorResponse()

object VoidResponse : ExecutorResponse()


