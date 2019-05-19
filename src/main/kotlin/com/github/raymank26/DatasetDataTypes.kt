package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.BufferedReader
import java.io.FileReader
import java.lang.RuntimeException
import java.nio.file.Path

/**
 * Date: 2019-05-17.
 */

data class DatasetResult(val headers: List<String>, val rows: List<DatasetRow>)
data class DatasetRow(val rowNum: Int, val columns: List<String>)

interface DatasetReader {
    fun read(handle: (row: DatasetRow) -> Unit, limit: Int?)
    fun getColumnNames(): List<String>
    fun getColumnInfo(): Map<String, ColumnInfo>
}

class CsvDatasetReader(private val csvFormat: CSVFormat,
                       private val csvPath: Path) : DatasetReader {

    private val columnNameField: List<String>
    private val columnInfoField: Map<String, ColumnInfo>

    init {
        columnNameField = getColumnNamesInner()
        columnInfoField = getColumnInfoInner()
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

    private fun getColumnNamesInner(): List<String> {
        var header: DatasetRow? = null
        readInner(1, false) { row: DatasetRow ->
            header = row
        }
        if (header == null) {
            throw RuntimeException("Csv is empty")
        }
        return header!!.columns
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
}

data class ColumnInfo(val type: FieldType, val position: Int)