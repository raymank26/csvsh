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

data class DatasetResult(val headers: List<String>, val rows: List<DatasetRow>)
data class DatasetRow(val rowNum: Int, val columns: List<String>)

interface DatasetReader : AutoCloseable {
    fun read(handle: (row: DatasetRow) -> Unit, limit: Int?)
    fun getColumnNames(): List<String> {
        return getColumnInfo().keys.toList()
    }
    fun getColumnInfo(): Map<String, ColumnInfo>
    fun availableIndexes(): List<IndexDescriptionAndPath>
}

class CsvDatasetReader(private val csvFormat: CSVFormat,
                       private val csvPath: Path) : DatasetReader {

    private val columnNameField: List<String>

    private val columnInfoField: Map<String, ColumnInfo>
    init {
        columnInfoField = getColumnInfoInner()
        columnNameField = getColumnNames()
    }

    override fun read(handle: (csvRow: DatasetRow) -> Unit, limit: Int?) {
        readInner(limit, true, handle)
    }

    override fun getColumnNames(): List<String> {
        return columnNameField
    }

    override fun getColumnInfo(): Map<String, ColumnInfo> {
        return columnInfoField
    }

    override fun availableIndexes(): List<IndexDescriptionAndPath> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    private fun readInner(limit: Int?, skipHeader: Boolean, handle: (csvRow: DatasetRow) -> Unit) {
        var headerSkipped = skipHeader
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
                for (header in columnNameField) {
                    val rowValue = csvLine.get(header)
                    columns.add(rowValue)
                }
                handle(DatasetRow(rowNum++, columns))
                if (limit != null && limit == rowNum) {
                    return@use
                }
            }
        }
    }

    private fun getColumnInfoInner(): Map<String, ColumnInfo> {
        var row: DatasetRow? = null
        this.read({
            row = it
        }, 1)
        if (row == null) {
            return emptyMap()
        }
        val result = mutableMapOf<String, ColumnInfo>()
        row!!.columns.forEachIndexed { i, column ->
            val fieldType: FieldType = when {
                column.contains('.') && column.toFloatOrNull() != null -> FieldType.FLOAT
                column.toIntOrNull() != null -> FieldType.INTEGER
                else -> FieldType.STRING
            }
            result[columnNameField[i]] = ColumnInfo(fieldType, i)
        }
        return result
    }

    override fun close() {
    }
}

data class ColumnInfo(val type: FieldType, val position: Int)