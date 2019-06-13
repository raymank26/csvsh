package com.github.raymank26

import com.github.raymank26.file.FileSystem
import com.google.common.collect.Iterators
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

data class DatasetResult(val rows: List<DatasetRow>, val columnInfo: List<ColumnInfo>)

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

    fun getIterator(): ClosableIterator<DatasetRow>
}

class ClosableIterator<T>(private val iterator: Iterator<T>, private val resource: AutoCloseable?) : Iterator<T>, AutoCloseable {
    override fun hasNext(): Boolean {
        return iterator.hasNext()
    }

    override fun next(): T {
        return iterator.next()
    }

    override fun close() {
        resource?.close()
    }

    fun toList(): List<T?> {
        return iterator.asSequence().toList()
    }

    fun <T2> map(f: (T) -> T2): ClosableIterator<T2> {
        return ClosableIterator(Iterators.transform(iterator) { a -> a?.let { f(a) } }, resource)
    }
}

fun createSqlAtom(value: String?, type: FieldType): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(value?.toInt())
        FieldType.FLOAT -> FloatValue(value?.toFloat())
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
    val SUM_INT: AggregateFunctionFactory = { AggregateFunction(IntValue(0), SqlValueAtom::plus) }
    val SUM_FLOAT: AggregateFunctionFactory = { AggregateFunction(FloatValue(0.toFloat()), SqlValueAtom::plus) }
    val COUNT_ANY: AggregateFunctionFactory = { AggregateFunction(IntValue(0)) { a, _ -> a plus IntValue(1) } }
    val MAX_INT: AggregateFunctionFactory = { AggregateFunction(IntValue(Int.MIN_VALUE), SqlValueAtom::max) }
    val MIN_INT: AggregateFunctionFactory = { AggregateFunction(IntValue(Int.MAX_VALUE), SqlValueAtom::min) }
    val MAX_FLOAT: AggregateFunctionFactory = { AggregateFunction(FloatValue(Float.MIN_VALUE), SqlValueAtom::max) }
    val MIN_FLOAT: AggregateFunctionFactory = { AggregateFunction(FloatValue(Float.MAX_VALUE), SqlValueAtom::min) }
}
