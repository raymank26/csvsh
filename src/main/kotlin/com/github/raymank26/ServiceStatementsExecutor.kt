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
        val dataPath = Paths.get(ctx.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val indexName = ctx.indexName().text
        val fieldName = ctx.reference().text
        indexesManager.createIndex(dataPath, indexName, fieldName, datasetReaderFactory)
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
        val indexes = getIndexesDescription(dataPath)

        return TableDescription(columnsDataset, sizeStat, indexes)
    }

    private fun getIndexesDescription(dataPath: Path): DatasetResult {
        val indexes = indexesManager.listIndexes(dataPath)
        val columnInfo = listOf(ColumnInfo(FieldType.STRING, "indexName"), ColumnInfo(FieldType.STRING, "fieldName"))
        val rows = ClosableSequence(indexes.mapIndexed { i, it ->
            DatasetRow(i, listOf(StringValue(it.name), StringValue(it.fieldName)), columnInfo, null)
        }.asSequence(), null)
        return DatasetResult(rows, columnInfo)
    }

    private fun getAdditionalDataDescription(dataPath: Path): DatasetResult {
        val dataFileSize = datasetMetadataProvider.getSize(dataPath)
        val indexFileSize: Long? = indexesManager.getIndexFileSize(dataPath)
        val offsetsFileSize: Long? = indexesManager.getOffsetsFileSize(dataPath)
        val columnInfo = listOf(ColumnInfo(FieldType.STRING, "file"), ColumnInfo(FieldType.DOUBLE, "Size in MB"))
        val rows = listOf(
                Pair("data", dataFileSize),
                Pair("index", indexFileSize),
                Pair("offsets", offsetsFileSize)
        ).asSequence()
                .filter { it.second != null }
                .mapIndexed { i, value ->
                    val formatting = "%.2f".format((value.second!!.toDouble() / 1024 / 1024))
                    DatasetRow(i, listOf(StringValue(value.first), StringValue(formatting)), columnInfo, null)
                }
        return DatasetResult(ClosableSequence(rows, null), columnInfo)
    }

    private fun describeColumns(columnInfo: List<ColumnInfo>): DatasetResult {
        val newColumnInfo = listOf(ColumnInfo(FieldType.STRING, "columnName"), ColumnInfo(FieldType.STRING, "columnType"))
        val rows = columnInfo
                .asSequence()
                .mapIndexed { i, it -> DatasetRow(i, listOf(StringValue(it.fieldName), StringValue(it.type.name)), newColumnInfo, null) }
        return DatasetResult(ClosableSequence(rows, null), newColumnInfo)
    }

    fun dropIndex(ctx: SqlParser.DropIndexContext) {
        val csvPath = Paths.get(ctx.table().IDENTIFIER_Q().text.drop(1).dropLast(1))
        val indexName = ctx.indexName().text
        indexesManager.dropIndex(csvPath, indexName)
    }
}
