package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.github.raymank26.file.getFilenameWithoutExtension
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Ordering
import com.google.common.primitives.Doubles
import com.google.common.primitives.Longs
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.KeyRange
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.Path

interface FieldSerializer {
    fun serialize(sqlValueAtom: SqlValueAtom): ByteBuffer
    fun deserialize(buffer: ByteBuffer): SqlValueAtom
    fun comparator(): Comparator<ByteBuffer>
    fun fieldType(): FieldType
}

private val BYTE_BUFFER_LONG_CMP = Comparator<ByteBuffer> { o1, o2 ->
    val a = o1.getLong(0)
    val b = o2.getLong(0)
    Longs.compare(a, b)
}

private val BYTE_BUFFER_DOUBLE_CMP = Comparator<ByteBuffer> { o1, o2 ->
    val a = o1.getDouble(0)
    val b = o2.getDouble(0)
    Doubles.compare(a, b)
}

object LongSerializer : FieldSerializer {
    override fun serialize(sqlValueAtom: SqlValueAtom): ByteBuffer {
        val res = ByteBuffer.allocateDirect(8)
        res.putLong((sqlValueAtom as LongValue).value!!)
        res.flip()
        return res
    }

    override fun deserialize(buffer: ByteBuffer): SqlValueAtom {
        return LongValue(buffer.getLong(0))
    }

    override fun comparator(): Comparator<ByteBuffer> {
        return BYTE_BUFFER_LONG_CMP
    }

    override fun fieldType(): FieldType {
        return FieldType.LONG
    }
}

object StringSerializer : FieldSerializer {
    override fun serialize(sqlValueAtom: SqlValueAtom): ByteBuffer {

        val chars = (sqlValueAtom as StringValue).value!!.toByteArray()
        val res = ByteBuffer.allocateDirect(chars.size)
        res.put(chars)
        res.flip()
        return res
    }

    override fun deserialize(buffer: ByteBuffer): SqlValueAtom {
        val size = buffer.limit()
        val bytes = ByteArray(size)
        buffer.get(bytes)
        return StringValue(bytes.toString(Charset.defaultCharset()))
    }

    override fun comparator(): Comparator<ByteBuffer> {
        return Ordering.natural()
    }

    override fun fieldType(): FieldType {
        return FieldType.STRING
    }
}

object DoubleSerializer : FieldSerializer {
    override fun serialize(sqlValueAtom: SqlValueAtom): ByteBuffer {
        val res = ByteBuffer.allocateDirect(8)
        res.putDouble((sqlValueAtom as DoubleValue).value!!)
        res.flip()
        return res
    }

    override fun deserialize(buffer: ByteBuffer): SqlValueAtom {
        return DoubleValue(buffer.getDouble(0))
    }

    override fun comparator(): Comparator<ByteBuffer> {
        return BYTE_BUFFER_DOUBLE_CMP
    }

    override fun fieldType(): FieldType {
        return FieldType.DOUBLE
    }
}

private val INDEX_SERIALIZERS: Map<FieldType, FieldSerializer> = ImmutableMap.of(
        FieldType.LONG, LongSerializer,
        FieldType.DOUBLE, DoubleSerializer,
        FieldType.STRING, StringSerializer
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
        return fileSystem.getDB(indexFile).use { env ->
            var readDb: Env<ByteBuffer>? = null
            val result = mutableListOf<IndexDescriptionAndPath>()
            for (index: IndexMeta in env.listIndexes()) {
                val (name, fieldName, serializer) = index

                val dbReader = lazy {
                    val newDb = if (readDb == null) {
                        readDb = fileSystem.getDB(indexFile)
                        readDb!!
                    } else {
                        readDb!!
                    }
                    val dbi = newDb.openDbi(index.dbiName, serializer.comparator(), DbiFlags.MDB_DUPSORT)
                    MapDBReadonlyIndex(dbi, newDb, serializer)
                }
                result.add(IndexDescriptionAndPath(IndexDescription(name, fieldName), dbReader))
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

        val keySerializer: FieldSerializer = INDEX_SERIALIZERS[columnInfo.type] ?: throw IllegalStateException()
        fileSystem.getDB(indexFile).use { env ->

            var offsetsMap: Map<Long, Long>? = loadOffsets(env)
            if (offsetsMap == null) {
                offsetsMap = persistOffsets(dataReader, env)
            }
            val indexContent = env
                    .openDbi("$indexName|$fieldName|${columnInfo.type.mark}", keySerializer.comparator(), DbiFlags.MDB_DUPSORT, DbiFlags.MDB_CREATE)
            env.txnWrite().use { tx ->
                indexContent.drop(tx)
                dataReader.getIterator().forEach { item ->
                    val sqlAtom = item.getCell(fieldName)
                    if (sqlAtom.asValue == null) {
                        return@forEach
                    }
                    val byteOffset = offsetsMap[item.characterOffset]!!
                    indexContent.put(tx, keySerializer.serialize(sqlAtom), serializeLong(byteOffset))
                }
                tx.commit()
            }
        }
    }

    private fun getOffsetsPath(dataPath: Path) =
            dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".offsets")

    private fun loadOffsets(env: Env<ByteBuffer>): Map<Long, Long>? {
        if (!env.dbiNames.map { it.toString(Charset.defaultCharset()) }.contains("offsets")) {
            return null
        }
        val loadedOffsets = env.openDbi("offsets")
        val resOffsets = mutableMapOf<Long, Long>()
        env.txnRead().use { tx ->
            loadedOffsets.iterate(tx, KeyRange.all()).use { cursor ->
                for (keyVal in cursor.iterable()) {
                    resOffsets[keyVal.key().getLong(0)] = keyVal.`val`().getLong(0)
                }
            }
        }
        return resOffsets
    }

    private fun persistOffsets(dataReader: DatasetReader, env: Env<ByteBuffer>): Map<Long, Long> {
        val offsets: List<DatasetOffset> = dataReader.getNavigableReader().use {
            offsetsBuilder.buildOffsets(it, dataReader.getIterator().map { v -> v.characterOffset!! }.toList())
        }
        val offsetsDb = env.openDbi("offsets", DbiFlags.MDB_CREATE)
        val offsetsRes = mutableMapOf<Long, Long>()
        env.txnWrite().use { tx ->
            for (offset in offsets) {

                offsetsDb.put(tx, serializeLong(offset.charPosition), serializeLong(offset.byteOffset))
                offsetsRes[offset.charPosition] = offset.byteOffset
            }
            tx.commit()
        }
        return offsetsRes
    }

    private fun serializeLong(value: Long): ByteBuffer {
        val bb = ByteBuffer.allocateDirect(8)
        bb.putLong(value)
        bb.flip()
        return bb
    }

    private fun getIndexPath(dataPath: Path): Path {
        return dataPath.parent.resolve(getFilenameWithoutExtension(dataPath) + ".index")
    }

    fun dropIndex(dataPath: Path, indexNameToDelete: String) {
        val indexFile: Path = getIndexPath(dataPath)
        if (!fileSystem.isFileExists(indexFile)) {
            throw PlannerException("Unable to locate index for table = $dataPath")
        }
        fileSystem.getDB(indexFile).use { env ->
            for (indexMeta in env.listIndexes()) {
                if (indexMeta.name != indexNameToDelete) {
                    continue
                }

                val indexContent = env
                        .openDbi(indexMeta.dbiName, indexMeta.serializer.comparator(), DbiFlags.MDB_DUPSORT)

                env.txnWrite().use { tx ->
                    indexContent.drop(tx, true)
                    tx.commit()
                }
            }
        }
    }

    fun listIndexes(dataPath: Path): List<IndexMeta> {
        val indexFile: Path = getIndexPath(dataPath)
        if (!fileSystem.isFileExists(indexFile)) {
            throw PlannerException("Unable to locate index for table = $dataPath")
        }
        return fileSystem.getDB(indexFile).use { env ->
            env.listIndexes()
        }
    }
}

private fun Env<ByteBuffer>.listIndexes(): List<IndexMeta> {
    val res = mutableListOf<IndexMeta>()
    for (name in this.dbiNames.map { it.toString(Charset.defaultCharset()) }) {
        if (!name.contains("|")) {
            continue
        }
        val (indexName, fieldName, fieldTypeMark) = name.split("|")
        val byteMark = fieldTypeMark.toByte()
        val fieldType = FieldType.MARK_TO_FIELD_TYPE[byteMark]
        val serializer = requireNotNull(INDEX_SERIALIZERS[fieldType])
        res.add(IndexMeta(indexName, fieldName, serializer, name))
    }
    return res
}

data class IndexMeta(val name: String, val fieldName: String, val serializer: FieldSerializer, val dbiName: String)
