package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.sql.SqlParser
import java.nio.file.Paths

/**
 * Date: 2019-05-18.
 */
class ServiceStatementsExecutor(private val datasetMetadataProvider: DatasetMetadataProvider,
                                private val fileSystem: FileSystem,
                                private val contentDataProvider: ContentDataProvider,
                                private val indexesManager: IndexesManager) {

    fun createIndex(ctx: SqlParser.CreateIndexContext, datasetReaderFactory: DatasetReaderFactory) {
        val csvPath = Paths.get(ctx.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val indexName = ctx.indexName().text
        val fieldName = ctx.reference().text
        indexesManager.createIndex(csvPath, indexName, fieldName, datasetReaderFactory)
    }

    fun createDatasetReaderFactory(): DatasetReaderFactory {
        return FilesystemDatasetReaderFactory(datasetMetadataProvider, fileSystem, contentDataProvider)
    }

    fun describeTable(ctx: SqlParser.DescribeContext, readerFactory: DatasetReaderFactory): DatasetResult {
        val csvPath = Paths.get(ctx.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val reader = readerFactory.getReader(csvPath)
                ?: return DatasetResult(ClosableSequence(emptySequence()), emptyList())
        val columnInfo = reader.columnInfo
        val newColumnInfo = listOf(ColumnInfo(FieldType.STRING, "columnName"), ColumnInfo(FieldType.STRING, "columnType"))
        val rows = columnInfo
                .asSequence()
                .mapIndexed { i, it -> DatasetRow(i, listOf(StringValue(it.fieldName), StringValue(it.type.name)), newColumnInfo, null) }
        return DatasetResult(ClosableSequence(rows, null), newColumnInfo)
    }
}
