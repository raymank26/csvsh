package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.getFilenameWithoutExtension
import com.google.common.collect.ImmutableMap
import org.mapdb.BTreeMap
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import java.nio.file.Path

private val MAPDB_SERIALIZERS: Map<FieldType, GroupSerializer<*>> = ImmutableMap.of(
        FieldType.INTEGER, Serializer.INTEGER,
        FieldType.FLOAT, Serializer.FLOAT,
        FieldType.STRING, Serializer.STRING
)

/**
 * Date: 2019-06-13.
 */
class IndexesManager(private val fileSystem: FileSystem) {

    fun loadIndexes(csvPath: Path): List<IndexDescriptionAndPath> {
        val indexFile: Path = getIndexPath(csvPath)
        if (!fileSystem.isFileExists(indexFile)) {
            return emptyList()
        }
        return fileSystem.getDB(indexFile).use { db ->
            val result = mutableListOf<IndexDescriptionAndPath>()
            for (name in db.getAllNames()) {
                val (indexName, fieldName, fieldTypeMark) = name.split("|")

                val byteMark = fieldTypeMark.toByte()
                val serializer = requireNotNull(MAPDB_SERIALIZERS[FieldType.MARK_TO_FIELD_TYPE[byteMark]])
                val tm = db.treeMap(name, serializer, Serializer.INT_ARRAY).open()
                @Suppress("UNCHECKED_CAST")
                val readOnlyIndex = MapDBReadonlyIndex(tm as BTreeMap<in Any, IntArray>, FieldType.INTEGER)
                result.add(IndexDescriptionAndPath(IndexDescription(indexName, fieldName), readOnlyIndex))
            }
            result
        }
    }

    fun createIndex(dataPath: Path, indexName: String, fieldName: String, datasetReaderFactory: DatasetReaderFactory) {
        val indexFile = getIndexPath(dataPath)
        val csvReader = datasetReaderFactory.getReader(dataPath)
                ?: throw PlannerException("No csv found for path = $dataPath")
        val fields: List<ColumnInfo> = csvReader.columnInfo

        val columnInfo: ColumnInfo = fields.find { it.fieldName == fieldName }!!

        val keySerializer: GroupSerializer<*> = MAPDB_SERIALIZERS[columnInfo.type] ?: throw IllegalStateException()
        @Suppress("UNCHECKED_CAST")
        val indexContent: BTreeMap<Any, IntArray> = fileSystem.getDB(indexFile)
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

    private fun getIndexPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".index")
    }

}