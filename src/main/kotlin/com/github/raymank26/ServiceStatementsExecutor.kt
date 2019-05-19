package com.github.raymank26

import com.github.raymank26.sql.SqlParser
import org.apache.commons.csv.CSVFormat
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
class ServiceStatementsExecutor(private val csvFormat: CSVFormat) {

    fun createIndex(ctx: SqlParser.CreateIndexContext) {
        val csvPath = Paths.get(ctx.table().text)
        val indexName = ctx.indexName().text
        val fieldName = ctx.reference().text
        val indexFile = csvPathToIndexFile(csvPath)
        val csvReader = CsvDatasetReader(csvFormat, csvPath)
        val fields: Map<String, ColumnInfo> = csvReader.getColumnInfo()

        val columnInfo: ColumnInfo = requireNotNull(fields[fieldName])

        val keySerializer: Any
        val keyToPos: (String) -> Any
        when (columnInfo.type) {
            FieldType.INTEGER -> {
                keySerializer = Serializer.INTEGER
                keyToPos = { str: String -> str.toInt() }
            }
            FieldType.FLOAT -> {
                keySerializer = Serializer.FLOAT
                keyToPos = { str: String -> str.toFloat() }
            }
            FieldType.STRING -> {
                keySerializer = Serializer.STRING
                keyToPos = { str: String -> str }
            }
        }
        @Suppress("UNCHECKED_CAST")
        val indexContent: BTreeMap<Any, IntArray> = DBMaker.fileDB(indexFile).make()
                .treeMap("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer as GroupSerializer<Any>, Serializer.INT_ARRAY)
                .createOrOpen()

        indexContent.use {
            indexContent.clear()

            csvReader.read({ row ->
                val fieldValue: Any = keyToPos(row.columns[columnInfo.position])
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
            }, null)
        }
    }

    fun loadIndexes(csvPath: Path): List<IndexDescriptionAndPath> {
        val indexFile = csvPathToIndexFile(csvPath)
        if (!indexFile.exists()) {
            return emptyList()
        }
        val db = DBMaker.fileDB(indexFile).readOnly().make()
        val result = mutableListOf<IndexDescriptionAndPath>()
        for (name in db.getAllNames()) {
            val (indexName, fieldName, fieldTypeMark) = name.split("|")
            val fieldType = FieldType.MARK_TO_FIELD_TYPE[fieldTypeMark.toByte()] ?:
                throw RuntimeException("Unable to get fieldType")
            val readOnlyIndex: ReadOnlyIndex = when (fieldType) {
                FieldType.INTEGER -> {
                    val tm = db.treeMap(name, Serializer.INTEGER, Serializer.INT_ARRAY).open()
                    MapDBReadonlyIndex(tm, FieldType.INTEGER)
                }
                FieldType.FLOAT -> {
                    val tm = db.treeMap(name, Serializer.FLOAT, Serializer.INT_ARRAY).open()
                    MapDBReadonlyIndex(tm, FieldType.FLOAT)
                }
                FieldType.STRING -> {
                    val tm = db.treeMap(name, Serializer.STRING, Serializer.INT_ARRAY).open()
                    MapDBReadonlyIndex(tm, FieldType.FLOAT)
                }
            }
            result.add(IndexDescriptionAndPath(IndexDescription(indexName, fieldName), readOnlyIndex))
        }
        return result
    }

    private fun csvPathToIndexFile(csvPath: Path): File {
        return csvPath.parent.resolve("indexContent").toFile()
    }

    fun describeTable(ctx: SqlParser.DescribeContext): Map<String, ColumnInfo> {
        return CsvDatasetReader(csvFormat, Paths.get(ctx.table().text)).getColumnInfo()
    }
}
