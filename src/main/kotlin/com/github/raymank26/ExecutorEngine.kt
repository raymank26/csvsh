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
    private val fileOffsetsBuilder = FileOffsetsBuilder()
    private val indexesManager = IndexesManager(fileSystem, fileOffsetsBuilder)
    private val ssExecutor = ServiceStatementsExecutor(DatasetMetadataProvider(fileSystem, contentDataProvider, indexesManager),
            fileSystem, contentDataProvider, indexesManager)

    fun execute(inputLine: String): ExecutorResponse {
        val parsedStatement: StatementType = sqlParser.parse(inputLine)
        val datasetReaderFactory = ssExecutor.createDatasetReaderFactory()

        return when (parsedStatement) {
            is CreateIndexType -> {
                ssExecutor.createIndex(parsedStatement.ctx, datasetReaderFactory)
                VoidResponse
            }
            is DescribeTable -> {
                val description = ssExecutor.describeTable(parsedStatement.ctx, datasetReaderFactory)
                if (description == null) {
                    TextResponse("No data found")
                } else {
                    CompositeResponse(listOf(
                            TextResponse("Columns:"),
                            DatasetResponse(description.columns),
                            TextResponse("Sizes:"),
                            DatasetResponse(description.sizeStatistics)))
                }
            }
            is SelectStatement -> {
                val planDescriptor = sqlPlanner.createPlan(parsedStatement.ctx, datasetReaderFactory)
                val result = sqlExecutor.execute(planDescriptor)
                DatasetResponse(result)
            }
        }
    }
}

sealed class ExecutorResponse

data class TextResponse(val value: String) : ExecutorResponse()

data class DatasetResponse(val value: DatasetResult) : ExecutorResponse()

data class CompositeResponse(val parts: List<ExecutorResponse>) : ExecutorResponse()

object VoidResponse : ExecutorResponse()


