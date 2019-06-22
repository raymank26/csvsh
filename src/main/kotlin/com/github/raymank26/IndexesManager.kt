package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.Md5Hash
import com.github.raymank26.file.Md5HashConverter
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
class IndexesManager(private val fileSystem: FileSystem, private val offsetsBuilder: FileOffsetsBuilder) {

    fun loadIndexes(dataPath: Path): List<IndexDescriptionAndPath> {
        val indexFile: Path = getIndexPath(dataPath)
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

    fun getIndexFileSize(dataPath: Path): Long? {
        val indexPath = getIndexPath(dataPath)
        if (!fileSystem.isFileExists(indexPath)) {
            return null
        }
        return fileSystem.getSize(indexPath)
    }

    fun getOffsetsFileSize(dataPath: Path): Long? {
        val offsetsPath = getOffsetsPath(dataPath)
        if (!fileSystem.isFileExists(offsetsPath)) {
            return null
        }
        return fileSystem.getSize(offsetsPath)
    }

    fun createIndex(dataPath: Path, indexName: String, fieldName: String, datasetReaderFactory: DatasetReaderFactory) {
        val indexFile = getIndexPath(dataPath)
        val dataReader = datasetReaderFactory.getReader(dataPath)
                ?: throw PlannerException("No data found for path = $dataPath")
        val fields: List<ColumnInfo> = dataReader.columnInfo

        val columnInfo: ColumnInfo = fields.find { it.fieldName == fieldName }
                ?: throw ExecutorException("Unable to find fieldName = $fieldName in dataset with columns = $fields")

        val keySerializer: GroupSerializer<*> = MAPDB_SERIALIZERS[columnInfo.type] ?: throw IllegalStateException()
        @Suppress("UNCHECKED_CAST")
        val indexContent: DB.TreeMapSink<Any, LongArray> = fileSystem.getDB(indexFile)
                .treeMap("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer as GroupSerializer<Any>, Serializer.LONG_ARRAY)
                .createFromSink()

        val offsetsPath = getOffsetsPath(dataPath)

        var offsetsDB: BTreeMap<Long, Long>? = loadOffsets(offsetsPath, dataReader.contentHash)
        if (offsetsDB == null) {
            offsetsDB = persistOffsets(dataReader, offsetsPath)
        }

        offsetsDB.use {
            dataReader.getIterator()
                    .transform { data -> data.groupBy { it.getCell(fieldName) }.asSequence().filter { it.key.asValue != null }.sortedBy { it.key } }
                    .forEach { entry ->
                        indexContent.put(entry.key.asValue!!, entry.value.map { offsetsDB[it.characterOffset!!]!! }.toLongArray())
                    }
            indexContent.create().close()
        }
    }

    private fun getOffsetsPath(dataPath: Path) =
            dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".offsets")

    private fun loadOffsets(offsetsPath: Path, dataContentMd5: Md5Hash): BTreeMap<Long, Long>? {
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
                .treeMap("offsets", Serializer.LONG, Serializer.LONG)
                .open()
    }

    private fun persistOffsets(dataReader: DatasetReader, offsetsPath: Path): BTreeMap<Long, Long> {
        val offsets: List<DatasetOffset> = dataReader.getNavigableReader().use {
            offsetsBuilder.buildOffsets(it, dataReader.getIterator().map { v -> v.characterOffset!! }.toList())
        }
        val db = fileSystem.getDB(offsetsPath)
        val offsetsMap: BTreeMap<Long, Long> = db
                .treeMap("offsets", Serializer.LONG, Serializer.LONG)
                .createFrom(offsets.asSequence().map { Pair(it.charPosition, it.byteOffset) }.iterator())

        db.atomicString("contentMd5").create().set(Md5HashConverter.INSTANCE.serialize(dataReader.contentHash))
        return offsetsMap
    }

    private fun getIndexPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".index")
    }
}