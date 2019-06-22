package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.sql.SqlParser
import java.nio.file.Path
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

    fun describeTable(ctx: SqlParser.DescribeContext, readerFactory: DatasetReaderFactory): TableDescription? {
        val dataPath = Paths.get(ctx.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val reader = readerFactory.getReader(dataPath)
                ?: return null
        val columnsDataset = describeColumns(reader.columnInfo)
        val sizeStat = getAdditionalDataDescription(dataPath)
        return TableDescription(columnsDataset, sizeStat)
    }

    private fun getAdditionalDataDescription(csvPath: Path): DatasetResult {
        val indexFileSize: Long? = indexesManager.getIndexFileSize(csvPath)
        val offsetsFileSize: Long? = indexesManager.getOffsetsFileSize(csvPath)
        val columnInfo = listOf(ColumnInfo(FieldType.STRING, "file"), ColumnInfo(FieldType.DOUBLE, "Size in MB"))
        val rows = listOf(
                Pair("index", indexFileSize),
                Pair("offsets", offsetsFileSize)
        ).asSequence()
                .filter { it.second != null }
                .mapIndexed { i, value -> DatasetRow(i, listOf(StringValue(value.first), DoubleValue(value.second!!.toDouble() / 1024)), columnInfo, null) }
        return DatasetResult(ClosableSequence(rows, null), columnInfo)
    }

    private fun describeColumns(columnInfo: List<ColumnInfo>): DatasetResult {
        val newColumnInfo = listOf(ColumnInfo(FieldType.STRING, "columnName"), ColumnInfo(FieldType.STRING, "columnType"))
        val rows = columnInfo
                .asSequence()
                .mapIndexed { i, it -> DatasetRow(i, listOf(StringValue(it.fieldName), StringValue(it.type.name)), newColumnInfo, null) }
        return DatasetResult(ClosableSequence(rows, null), newColumnInfo)
    }
}
