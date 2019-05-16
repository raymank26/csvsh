package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import java.io.BufferedReader
import java.io.FileReader
import java.nio.file.Path

/**
 * Date: 2019-05-17.
 */

data class DatasetResult(val headers: List<String>, val rows: List<DatasetRow>)
data class DatasetRow(val rowNum: Int, val columns: List<String>)

interface DatasetReader {
    fun read(handle: (row: DatasetRow) -> Unit)
    fun getColumnNames(): List<String>
}

class CsvDatasetReader(private val csvFormat: CSVFormat,
                       private val csvPath: Path,
                       private val csvColumns: List<String>) : DatasetReader {

    override fun read(handle: (csvRow: DatasetRow) -> Unit) {
        CSVParser(BufferedReader(FileReader(csvPath.toFile())), csvFormat).use {
            val linesIterator = it.iterator()
            linesIterator.next() // skip header

            var rowNum = 0
            while (linesIterator.hasNext()) {
                val csvLine = linesIterator.next()
                val columns = mutableListOf<String>()
                for (header in csvColumns) {
                    val rowValue = csvLine.get(header)
                    columns.add(rowValue)
                }
                handle(DatasetRow(rowNum++, columns))
            }
        }
    }

    override fun getColumnNames(): List<String> {
        return csvColumns
    }
}
