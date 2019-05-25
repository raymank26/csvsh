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
                      val columns: List<SqlValueAtom>,
                      val columnInfo: List<ColumnInfo>) {

    private val fieldNameToInfo = lazy { columnInfo.associateBy { it.fieldName } }

    fun getCell(fieldName: String): SqlValueAtom {
        return fieldNameToInfo.value[fieldName]?.let {
            columns[it.position]
        } ?: fieldNotFound(fieldName)
    }

    fun getCellType(fieldName: String): FieldType? {
        return getCell(fieldName).type
    }

    fun getColumnInfo(fieldName: String): ColumnInfo? {
        return fieldNameToInfo.value[fieldName]
    }
}

private fun fieldNotFound(name: String): Nothing {
    throw ExecutorException("Field \"$name\" is not found")
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
        readInner(limit, true, { rowNum, columns -> DatasetRow(rowNum, columns.mapIndexed { index, s -> createSqlAtom(s, columnInfo[index].type) }, columnInfo) }, handle)
    }

    private fun <T> readInner(limit: Int?, skipHeader: Boolean, mapper: (Int, List<String>) -> T, handle: (csvRow: T) -> Unit) {
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
                handle(mapper(rowNum++, columns))
                if (limit != null && limit == rowNum) {
                    return@use
                }
            }
        }
    }

    private fun getColumnInfoInner(): List<ColumnInfo> {
        var row: List<String>? = null
        this.readInner(1, true, { _, columns -> columns }, { row = it })
        if (row == null) {
            return emptyList()
        }
        val result = mutableListOf<ColumnInfo>()
        row!!.forEachIndexed { i, columnName ->
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

fun createSqlAtom(value: String, type: FieldType): SqlValueAtom {
    return when (type) {
        FieldType.INTEGER -> IntValue(value.toInt())
        FieldType.FLOAT -> FloatValue(value.toFloat())
        FieldType.STRING -> StringValue(value)
    }
}


data class ColumnInfo(val type: FieldType, val fieldName: String, val position: Int)

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
