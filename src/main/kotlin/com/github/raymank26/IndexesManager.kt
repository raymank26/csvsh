package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.getFilenameWithoutExtension
import com.google.common.collect.ImmutableMap
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import java.nio.file.Path

private val MAPDB_SERIALIZERS: Map<FieldType, GroupSerializer<*>> = ImmutableMap.of(
        FieldType.LONG, Serializer.LONG,
        FieldType.DOUBLE, Serializer.DOUBLE,
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
            var readDb: DB? = null
            val result = mutableListOf<IndexDescriptionAndPath>()
            for (name in db.getAllNames()) {
                val (indexName, fieldName, fieldTypeMark) = name.split("|")

                val byteMark = fieldTypeMark.toByte()
                val serializer = requireNotNull(MAPDB_SERIALIZERS[FieldType.MARK_TO_FIELD_TYPE[byteMark]])
                val dbReader = lazy {
                    val newDb = if (readDb == null || readDb!!.isClosed()) {
                        readDb = fileSystem.getDB(indexFile)
                        readDb!!
                    } else {
                        readDb!!
                    }
                    val tm = newDb.treeMap(name, serializer, Serializer.INT_ARRAY).open()
                    @Suppress("UNCHECKED_CAST")
                    MapDBReadonlyIndex(tm as BTreeMap<in Any, LongArray>, FieldType.LONG)
                }
                result.add(IndexDescriptionAndPath(IndexDescription(indexName, fieldName), dbReader))
            }
            result
        }
    }

    fun createIndex(dataPath: Path, indexName: String, fieldName: String, datasetReaderFactory: DatasetReaderFactory) {
        val indexFile = getIndexPath(dataPath)
        val csvReader = datasetReaderFactory.getReader(dataPath)
                ?: throw PlannerException("No csv found for path = $dataPath")
        val fields: List<ColumnInfo> = csvReader.columnInfo

        val columnInfo: ColumnInfo = fields.find { it.fieldName == fieldName }
                ?: throw ExecutorException("Unable to find fieldName = $fieldName in dataset with columns = $fields")

        val keySerializer: GroupSerializer<*> = MAPDB_SERIALIZERS[columnInfo.type] ?: throw IllegalStateException()
        @Suppress("UNCHECKED_CAST")
        val indexContent: DB.TreeMapSink<Any, LongArray> = fileSystem.getDB(indexFile)
                .treeMap("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer as GroupSerializer<Any>, Serializer.LONG_ARRAY)
                .createFromSink()

        csvReader.getIterator()
                .transform { data -> data.groupBy { it.getCell(fieldName) }.asSequence().filter { it.key.asValue != null }.sortedBy { it.key } }
                .forEach { entry ->
                    indexContent.put(entry.key.asValue!!, entry.value.map { it.offset!! }.toLongArray())
                }
        indexContent.create().close()
    }

    private fun getIndexPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".index")
    }

}