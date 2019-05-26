package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import java.io.BufferedReader
import java.io.FileReader
import java.io.Reader
import java.nio.file.Path
import java.util.Collections

/**
 * Date: 2019-05-17.
 */
interface DatasetReaderFactory {
    fun getReader(path: Path): DatasetReader?
}

class CsvDatasetReaderFactory(private val indexesLoader: (Path) -> List<IndexDescriptionAndPath>) : DatasetReaderFactory {
    override fun getReader(path: Path): DatasetReader? {
        if (!path.toFile().exists()) {
            return null
        }
        return CsvDatasetReader(CSVFormat.RFC4180, path, indexesLoader(path))
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

    fun <T2> map(f: (T) -> T2): ClosableIterator<T2> {
        if (!hasNext()) {
            return ClosableIterator(Collections.emptyIterator<T2>(), resource)
        }
        return ClosableIterator(iterator.asSequence().map { v -> f(v) }.iterator(), resource)
    }
}

class CsvDatasetReader(private val csvFormat: CSVFormat,
                       private val readerFactory: () -> Reader,
                       override val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {

    override val columnInfo = getColumnInfoInner()

    constructor(csvFormat: CSVFormat, csvPath: Path, availableIndexes: List<IndexDescriptionAndPath>) :
            this(csvFormat, { BufferedReader(FileReader(csvPath.toFile())) }, availableIndexes)

    override fun getIterator(): ClosableIterator<DatasetRow> {
        if (columnInfo.isEmpty()) {
            return ClosableIterator(Collections.emptyIterator(), null)
        }
        return readInner(1, null).map { csvRecord ->
            DatasetRow(csvRecord.recordNumber.toInt(), columnInfo.mapIndexed { i, col -> createSqlAtom(csvRecord[i], col.type) }, columnInfo)
        }
    }

    private fun readInner(skip: Int, limit: Int?): ClosableIterator<CSVRecord> {
        val reader = readerFactory()
        return ClosableIterator(CSVParser(reader, csvFormat)
                .iterator()
                .asSequence()
                .drop(skip)
                .apply { if (limit != null) this.take(limit) }
                .iterator(), reader)
    }

    private fun getColumnInfoInner(): List<ColumnInfo> {
        return this.readInner(0, 1).use { headerIterator ->
            if (!headerIterator.hasNext()) {
                return@use emptyList()
            }
            val headerRecord: CSVRecord = headerIterator.next()
            val header: List<String> = (0 until headerRecord.size()).map { headerRecord.get(it) }

            val result = mutableListOf<ColumnInfo>()
            this.readInner(1, 1).use use2@{ valueIterator ->
                if (!valueIterator.hasNext()) {
                    return@use2 header.map { ColumnInfo(FieldType.STRING, it) }
                }
                valueIterator.next().forEachIndexed { i, value ->
                    val fieldType: FieldType = when {
                        value.contains('.') && value.toFloatOrNull() != null -> FieldType.FLOAT
                        value.toIntOrNull() != null -> FieldType.INTEGER
                        else -> FieldType.STRING
                    }
                    result.add(ColumnInfo(fieldType, header[i]))
                }
                result
            }
        }
    }
}

fun createSqlAtom(value: String, type: FieldType): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(value.toInt())
        FieldType.FLOAT -> FloatValue(value.toFloat())
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
