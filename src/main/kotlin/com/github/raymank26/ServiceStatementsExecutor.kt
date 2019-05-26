package com.github.raymank26

import com.github.raymank26.sql.SqlParser
import org.mapdb.BTreeMap
import org.mapdb.DBMaker
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Date: 2019-05-18.
 */
class ServiceStatementsExecutor {

    fun createIndex(ctx: SqlParser.CreateIndexContext, datasetReaderFactory: DatasetReaderFactory) {
        val csvPath = Paths.get(ctx.table().text)
        val indexName = ctx.indexName().text
        val fieldName = ctx.reference().text
        val indexFile = csvPathToIndexFile(csvPath)
        val csvReader = datasetReaderFactory.getReader(csvPath)
                ?: throw PlannerException("No csv found for path = $csvPath")
        val fields: List<ColumnInfo> = csvReader.columnInfo

        val columnInfo: ColumnInfo = fields.find { it.fieldName == fieldName }!!

        val keySerializer: Any
        keySerializer = when (columnInfo.type) {
            FieldType.INTEGER -> {
                Serializer.INTEGER
            }
            FieldType.FLOAT -> {
                Serializer.FLOAT
            }
            FieldType.STRING -> {
                Serializer.STRING
            }
        }
        @Suppress("UNCHECKED_CAST")
        val indexContent: BTreeMap<Any, IntArray> = DBMaker.fileDB(indexFile).make()
                .treeMap("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer as GroupSerializer<Any>, Serializer.INT_ARRAY)
                .createOrOpen()

        indexContent.use {
            indexContent.clear()
            csvReader.getIterator().use { iterator ->
                iterator.forEach { row ->
                    val fieldValue: Any = row.getCell(fieldName).asValue
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

    fun createCsvDatasetReaderFactory(): DatasetReaderFactory {
        return CsvDatasetReaderFactory(indexesLoader = this::loadIndexes)
    }

    private fun loadIndexes(csvPath: Path): List<IndexDescriptionAndPath> {
        val indexFile = csvPathToIndexFile(csvPath)
        if (!indexFile.exists()) {
            return emptyList()
        }
        val db = DBMaker.fileDB(indexFile).readOnly().make()
        val result = mutableListOf<IndexDescriptionAndPath>()
        for (name in db.getAllNames()) {
            val (indexName, fieldName, fieldTypeMark) = name.split("|")

            val byteMark = fieldTypeMark.toByte()
            val serializer = when (FieldType.MARK_TO_FIELD_TYPE[byteMark]) {
                FieldType.INTEGER -> Serializer.INTEGER
                FieldType.FLOAT -> Serializer.FLOAT
                FieldType.STRING -> Serializer.STRING
                else -> throw RuntimeException("Unable to get field type by mark = $byteMark")
            }
            val tm = db.treeMap(name, serializer, Serializer.INT_ARRAY).open()
            @Suppress("UNCHECKED_CAST")
            val readOnlyIndex = MapDBReadonlyIndex(tm as BTreeMap<in Any, IntArray>, FieldType.INTEGER)
            result.add(IndexDescriptionAndPath(IndexDescription(indexName, fieldName), readOnlyIndex))
        }
        return result
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
