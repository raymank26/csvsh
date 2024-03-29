package com.github.raymank26.csvsh

import com.github.raymank26.csvsh.executor.ContentDataProvider
import com.github.raymank26.csvsh.executor.DatasetMetadataProvider
import com.github.raymank26.csvsh.file.FileSystem
import com.github.raymank26.csvsh.file.Md5Hash
import com.github.raymank26.csvsh.file.NavigableReader
import com.github.raymank26.csvsh.index.IndexDescriptionAndPath
import java.nio.file.Path

/**
 * Date: 2019-05-17.
 */
interface DatasetReaderFactory {
    fun getReader(path: Path): DatasetReader?
}

class FilesystemDatasetReaderFactory(
        private val datasetMetadataProvider: DatasetMetadataProvider,
        private val fileSystem: FileSystem,
        private val contentDataProvider: ContentDataProvider) : DatasetReaderFactory {

    override fun getReader(path: Path): DatasetReader? {
        val metadata = datasetMetadataProvider.getOrCreate(path)
        return DatasetReaderImpl(contentDataProvider, { fileSystem.getNavigableReader(path) }, metadata)
    }
}

data class DatasetResult(val rows: ClosableSequence<DatasetRow>, val columnInfo: List<ColumnInfo>)

data class DatasetRow(val rowNum: Int,
                      val columns: List<SqlValueAtom>,
                      val columnInfo: List<ColumnInfo>,
                      val characterOffset: Long?) {

    private val fieldNameToInfo: Map<String, Pair<ColumnInfo, Int>> = columnInfo.withIndex().associate {
        Pair(it.value.fieldName, Pair(it.value, it.index))
    }

    fun getCell(fieldName: String): SqlValueAtom {
        return fieldNameToInfo[fieldName]?.let {
            columns[it.second]
        } ?: fieldNotFound(fieldName)
    }

    fun getCellType(fieldName: String): FieldType {
        return getCell(fieldName).type
    }

    fun getColumnInfo(fieldName: String): ColumnInfo? {
        return fieldNameToInfo[fieldName]?.first
    }
}

private fun fieldNotFound(name: String): Nothing {
    throw ExecutorException("Field \"$name\" is not found")
}

interface DatasetReader {
    val columnInfo: List<ColumnInfo>
    val availableIndexes: List<IndexDescriptionAndPath>
    val contentHash: Md5Hash

    fun getIterator(): ClosableSequence<DatasetRow>

    fun getIterator(offsets: List<Long>): ClosableSequence<DatasetRow>

    fun getNavigableReader(): NavigableReader
}

class ClosableSequence<T>(private val sequence: Sequence<T>, private val resource: AutoCloseable?) : AutoCloseable {

    constructor(sequence: Sequence<T>) : this(sequence, null)

    override fun close() {
        resource?.close()
    }

    fun forEach(f: (T) -> Unit) {
        resource.use {
            sequence.forEach(f)
        }
    }

    fun toList(): List<T> {
        return resource.use {
            sequence.asSequence().toList()
        }
    }

    fun <U> transform(f: (Sequence<T>) -> Sequence<U>): ClosableSequence<U> {
        return ClosableSequence(f(sequence), resource)
    }

    fun withClosable(closable: AutoCloseable): ClosableSequence<T> {
        return ClosableSequence(sequence, AutoCloseable {
            closable.close()
            resource?.close()
        })
    }

    fun <U> map(f: (T) -> U): ClosableSequence<U> {
        return transform { it.map(f) }
    }

    fun filter(f: (T) -> Boolean): ClosableSequence<T> {
        return transform { it.filter(f) }
    }

    fun take(n: Int): ClosableSequence<T> {
        return transform { it.take(n) }
    }
}

fun createSqlAtom(value: String?, type: FieldType): SqlValueAtom {
    return when (type) {
        FieldType.LONG -> LongValue(value?.toLong())
        FieldType.DOUBLE -> DoubleValue(value?.toDouble())
        FieldType.STRING -> StringValue(value)
    }
}

data class ColumnInfo(val type: FieldType, val fieldName: String)

