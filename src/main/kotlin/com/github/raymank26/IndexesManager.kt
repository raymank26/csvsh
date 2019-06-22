package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.Md5Hash
import com.github.raymank26.file.Md5HashConverter
import com.github.raymank26.file.getFilenameWithoutExtension
import com.google.common.collect.ImmutableMap
import org.mapdb.BTreeMap
import org.mapdb.DB
import org.mapdb.HTreeMap
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
class IndexesManager(private val fileSystem: FileSystem, private val offsetsBuilder: FileOffsetsBuilder) {

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

        val offsetsPath = dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".offsets")

        var offsetsDB: HTreeMap<Long, Long>? = loadOffsets(offsetsPath, csvReader.md5Hash)
        if (offsetsDB == null) {
            offsetsDB = persistOffsets(csvReader, offsetsPath)
        }

        offsetsDB.use {
            csvReader.getIterator()
                    .transform { data -> data.groupBy { it.getCell(fieldName) }.asSequence().filter { it.key.asValue != null }.sortedBy { it.key } }
                    .forEach { entry ->
                        indexContent.put(entry.key.asValue!!, entry.value.map { offsetsDB[it.characterOffset!!]!! }.toLongArray())
                    }
            indexContent.create().close()
        }
    }

    private fun loadOffsets(offsetsPath: Path, dataContentMd5: Md5Hash): HTreeMap<Long, Long>? {
        if (!fileSystem.isFileExists(offsetsPath)) {
            return null
        }
        val db = fileSystem.getDB(offsetsPath)
        val md5: Md5Hash = db.atomicString("contentMd5").open().get().let { Md5HashConverter.INSTANCE.deserialize(it) }
        if (md5 != dataContentMd5) {
            db.close()
            return null
        }
        return db
                .hashMap("offsets", Serializer.LONG, Serializer.LONG)
                .open()
    }

    private fun persistOffsets(csvReader: DatasetReader, offsetsPath: Path): HTreeMap<Long, Long> {
        val offsets: List<DatasetOffset> = csvReader.getNavigableReader().use {
            csvReader.getIterator()
            offsetsBuilder.buildOffsets(it, csvReader.getIterator().map { it.characterOffset!! }.toList())
        }
        val db = fileSystem.getDB(offsetsPath)
        val offsetsMap = db
                .hashMap("offsets", Serializer.LONG, Serializer.LONG)
                .create()
        for (offset in offsets) {
            offsetsMap[offset.charPosition] = offset.byteOffset
        }
        db.atomicString("contentMd5").create().set(Md5HashConverter.INSTANCE.serialize(csvReader.md5Hash))
        return offsetsMap
    }

    private fun getIndexPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".index")
    }
}