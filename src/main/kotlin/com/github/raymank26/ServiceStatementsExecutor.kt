package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.sql.SqlParser
import com.google.common.collect.ImmutableMap
import org.mapdb.BTreeMap
import org.mapdb.DBMaker
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

private val MAPDB_SERIALIZERS: Map<FieldType, GroupSerializer<*>> = ImmutableMap.of(
        FieldType.INTEGER, Serializer.INTEGER,
        FieldType.FLOAT, Serializer.FLOAT,
        FieldType.STRING, Serializer.STRING
)

/**
 * Date: 2019-05-18.
 */
class ServiceStatementsExecutor(private val datasetMetadataProvider: DatasetMetadataProvider,
                                private val fileSystem: FileSystem,
                                private val contentDataProvider: ContentDataProvider) {

    fun createIndex(ctx: SqlParser.CreateIndexContext, datasetReaderFactory: DatasetReaderFactory) {
        val csvPath = Paths.get(ctx.table().text)
        val indexName = ctx.indexName().text
        val fieldName = ctx.reference().text
        val indexFile = csvPathToIndexFile(csvPath)
        val csvReader = datasetReaderFactory.getReader(csvPath)
                ?: throw PlannerException("No csv found for path = $csvPath")
        val fields: List<ColumnInfo> = csvReader.columnInfo

        val columnInfo: ColumnInfo = fields.find { it.fieldName == fieldName }!!

        val keySerializer: GroupSerializer<*> = MAPDB_SERIALIZERS[columnInfo.type] ?: throw IllegalStateException()
        @Suppress("UNCHECKED_CAST")
        val indexContent: BTreeMap<Any, IntArray> = DBMaker.fileDB(indexFile).make()
                .treeMap("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer as GroupSerializer<Any>, Serializer.INT_ARRAY)
                .createOrOpen()

        indexContent.use {
            indexContent.clear()
            csvReader.getIterator().use { iterator ->
                iterator.forEach { row ->
                    val fieldValue: Any = row.getCell(fieldName).asValue ?: return@forEach
                    indexContent.compute(fieldValue) { _: Any, positions: IntArray? ->
                        if (positions == null) {
                            IntArray(1).apply { this[0] = row.rowNum }
                        } else {
                            val newPositions = IntArray(positions.size + 1)
                            System.arraycopy(positions, 0, newPositions, 0, positions.size)
                            newPositions[positions.size + 1] = row.rowNum
                            newPositions
                        }
                    }

                }
            }
        }
    }

    fun createDatasetReaderFactory(): DatasetReaderFactory {
        return FilesystemDatasetReaderFactory(datasetMetadataProvider, fileSystem, contentDataProvider)
    }

    private fun csvPathToIndexFile(csvPath: Path): File {
        return csvPath.parent.resolve("indexContent").toFile()
    }

    fun describeTable(ctx: SqlParser.DescribeContext, readerFactory: DatasetReaderFactory): List<ColumnInfo> {
        val csvPath = Paths.get(ctx.table().text)
        return readerFactory.getReader(csvPath)?.columnInfo
                ?: throw PlannerException("No csv found for path = $csvPath")
    }
}
