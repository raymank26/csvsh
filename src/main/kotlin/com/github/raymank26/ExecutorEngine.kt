package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.RealFileSystem
import org.apache.commons.csv.CSVFormat

/**
 * Date: 2019-05-13.
 */
class ExecutorEngine {

    private val sqlParser: SqlAstBuilder = SqlAstBuilder()
    private val sqlPlanner: SqlPlanner = SqlPlanner()
    private val sqlExecutor: SqlExecutor = SqlExecutor()
    private val fileSystem: FileSystem = RealFileSystem()
    private val contentDataProvider: ContentDataProvider = CsvContentDataProvider(CSVFormat.RFC4180.withNullString("null"))
    private val ssExecutor = ServiceStatementsExecutor(DatasetMetadataProvider(fileSystem, contentDataProvider), fileSystem, contentDataProvider)

    fun execute(inputLine: String): ExecutorResponse {
        val parsedStatement: StatementType
        try {
            parsedStatement = sqlParser.parse(inputLine)
        } catch (e: SyntaxException) {
            return TextResponse(e.message!!)
        }
        val datasetReaderFactory = ssExecutor.createDatasetReaderFactory()

        return when (parsedStatement) {
            is CreateIndexType -> {
                ssExecutor.createIndex(parsedStatement.ctx, datasetReaderFactory)
                VoidResponse
            }
            is DescribeStatement -> {
                val description = ssExecutor.describeTable(parsedStatement.ctx, datasetReaderFactory)
                TextResponse(description.toString())
            }
            is SelectStatement -> {
                val planDescriptor = sqlPlanner.createPlan(parsedStatement.ctx, datasetReaderFactory)
                val result = sqlExecutor.execute(planDescriptor)
                TextResponse(result.toString())
            }
        }
    }
}

sealed class ExecutorResponse

data class TextResponse(val value: String) : ExecutorResponse()

object VoidResponse : ExecutorResponse()


