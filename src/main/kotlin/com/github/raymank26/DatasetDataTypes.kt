package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.BufferedReader
import java.io.FileReader
import java.nio.file.Path

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
                      val columns: List<String>,
                      val columnInfo: List<ColumnInfo>) {

    private val fieldNameToInfo = lazy { columnInfo.associateBy { it.fieldName } }

    fun getCell(fieldName: String): String? {
        return fieldNameToInfo.value[fieldName]?.let {
            columns[it.position]
        }
    }

    fun getCellTyped(fieldName: String): Comparable<Any>? {
        @Suppress("UNCHECKED_CAST")
        return when (getCellType(fieldName)) {
            FieldType.INTEGER -> getCell(fieldName)?.toInt() as? Comparable<Any>
            FieldType.FLOAT -> getCell(fieldName)?.toFloat() as? Comparable<Any>
            FieldType.STRING -> getCell(fieldName) as? Comparable<Any>
            null -> null
        }
    }

    fun getCellType(fieldName: String): FieldType? {
        return fieldNameToInfo.value[fieldName]?.type
    }

    fun getColumnInfo(fieldName: String): ColumnInfo? {
        return fieldNameToInfo.value[fieldName]
    }
}

interface DatasetReader : AutoCloseable {
    val columnInfo: List<ColumnInfo>
    val availableIndexes: List<IndexDescriptionAndPath>

    fun read(handle: (row: DatasetRow) -> Unit, limit: Int?)
}

class CsvDatasetReader(private val csvFormat: CSVFormat,
                       private val csvPath: Path,
                       override val availableIndexes: List<IndexDescriptionAndPath>) : DatasetReader {

    override val columnInfo = getColumnInfoInner()

    override fun read(handle: (csvRow: DatasetRow) -> Unit, limit: Int?) {
        readInner(limit, true, handle)
    }

    private fun readInner(limit: Int?, skipHeader: Boolean, handle: (csvRow: DatasetRow) -> Unit) {
        var headerSkipped = skipHeader
        val columnNames = columnInfo.map { it.fieldName }
        CSVParser(BufferedReader(FileReader(csvPath.toFile())), csvFormat).use {
            val linesIterator = it.iterator()

            var rowNum = 0
            while (linesIterator.hasNext()) {
                if (!headerSkipped) {
                    linesIterator.next()
                    headerSkipped = true
                }
                val csvLine = linesIterator.next()
                val columns = mutableListOf<String>()
                for (header in columnNames) {
                    val rowValue = csvLine.get(header)
                    columns.add(rowValue)
                }
                handle(DatasetRow(rowNum++, columns, columnInfo))
                if (limit != null && limit == rowNum) {
                    return@use
                }
            }
        }
    }

    private fun getColumnInfoInner(): List<ColumnInfo> {
        var row: DatasetRow? = null
        this.read({
            row = it
        }, limit = 1)
        if (row == null) {
            return emptyList()
        }
        val result = mutableListOf<ColumnInfo>()
        row!!.columns.forEachIndexed { i, columnName ->
            val fieldType: FieldType = when {
                columnName.contains('.') && columnName.toFloatOrNull() != null -> FieldType.FLOAT
                columnName.toIntOrNull() != null -> FieldType.INTEGER
                else -> FieldType.STRING
            }
            result.add(ColumnInfo(fieldType, columnName, i))
        }
        return result
    }

    override fun close() {
    }
}

data class ColumnInfo(val type: FieldType, val fieldName: String, val position: Int)

interface AggregateFunction<in T, out T2> {
    fun process(value: T)
    fun getResult(): T2
    fun toText(): String {
        return getResult().toString()
    }
}

class AggregateFunctionImpl<in T, out T2>(private var initial: T2, private val agg: (T2, T) -> T2) : AggregateFunction<T, T2> {

    override fun process(value: T) {
        initial = agg(initial, value)
    }

    override fun getResult(): T2 {
        return initial
    }
}

typealias AggregateFunctionFactory<T1, T2> = () -> AggregateFunction<T1, T2>

object Aggregates {
    val SUM_INT: AggregateFunctionFactory<Int, Int> = { AggregateFunctionImpl(0, { a, b -> a + b }) }
    val SUM_FLOAT: AggregateFunctionFactory<Float, Float> = { AggregateFunctionImpl(0.0.toFloat(), { a, b -> a + b }) }
    val COUNT_ANY: AggregateFunctionFactory<Any, Int> = { AggregateFunctionImpl(0, { a, _ -> a + 1 }) }
    val MAX_INT: AggregateFunctionFactory<Int, Int> = { AggregateFunctionImpl(Int.MIN_VALUE, { a, b -> Math.max(a, b) }) }
    val MIN_INT: AggregateFunctionFactory<Int, Int> = { AggregateFunctionImpl(Int.MAX_VALUE, { a, b -> Math.min(a, b) }) }
    val MAX_FLOAT: AggregateFunctionFactory<Float, Float> = { AggregateFunctionImpl(Float.MIN_VALUE, { a, b -> Math.max(a, b) }) }
    val MIN_FLOAT: AggregateFunctionFactory<Float, Float> = { AggregateFunctionImpl(Float.MAX_VALUE, { a, b -> Math.min(a, b) }) }
}
