package com.github.raymank26

import com.github.raymank26.file.FileSystem
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
        return DatasetReaderImpl(contentDataProvider, { fileSystem.getReader(path) }, metadata.columnInfos, metadata.indexes)
    }
}

data class DatasetResult(val rows: ClosableSequence<DatasetRow>, val columnInfo: List<ColumnInfo>)

data class DatasetRow(val rowNum: Int,
                      val columns: List<SqlValueAtom>,
                      val columnInfo: List<ColumnInfo>) {

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

    fun getIterator(): ClosableSequence<DatasetRow>
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

class AggregateFunction(private var initial: SqlValueAtom, private val agg: (SqlValueAtom, SqlValueAtom) -> SqlValueAtom) {
    fun process(value: SqlValueAtom) {
        initial = agg.invoke(initial, value)
    }

    fun getResult(): SqlValueAtom {
        return initial
    }
}

typealias AggregateFunctionFactory = () -> AggregateFunction

object Aggregates {
    val SUM_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(0), SqlValueAtom::plus) }
    val SUM_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(0.toDouble()), SqlValueAtom::plus) }
    val COUNT_ANY: AggregateFunctionFactory = { AggregateFunction(LongValue(0)) { a, _ -> a plus LongValue(1) } }
    val MAX_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(null), SqlValueAtom::max) }
    val MIN_INT: AggregateFunctionFactory = { AggregateFunction(LongValue(null), SqlValueAtom::min) }
    val MAX_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(null), SqlValueAtom::max) }
    val MIN_FLOAT: AggregateFunctionFactory = { AggregateFunction(DoubleValue(null), SqlValueAtom::min) }
    val MAX_STRING: AggregateFunctionFactory = { AggregateFunction(StringValue(null), SqlValueAtom::max) }
    val MIN_STRING: AggregateFunctionFactory = { AggregateFunction(StringValue(null), SqlValueAtom::min) }
}
