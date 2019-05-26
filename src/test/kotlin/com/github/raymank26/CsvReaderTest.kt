package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.StringReader
import kotlin.test.assertEquals

/**
 * Date: 2019-05-26.
 */
class CsvReaderTest {

    @Test
    fun testDatasetReader() {
        val input = """
            a,b,c
            foobar,2,3.0
            baz,6,4.4
            on,8,-1.7
        """.trimIndent()
        val csvDatasetReader = CsvDatasetReader(CSVFormat.RFC4180, { StringReader(input) }, emptyList())
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.INTEGER, "b"), ColumnInfo(FieldType.FLOAT, "c")),
                csvDatasetReader.columnInfo)
        val rows = csvDatasetReader.getIterator().asSequence().toList()
        assertEquals(3, rows.size)
    }
}