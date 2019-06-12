package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.StringReader
import kotlin.test.assertEquals

/**
 * Date: 2019-05-26.
 */
class CsvReaderTest {
    private val testInput = """
            a,b,c
            null,2,3.0
            baz,null,4.4
            on,8,-1.7
        """.trimIndent()

    @Test
    fun testDatasetReader() {
        val dataProvider = CsvContentDataProvider(CSVFormat.RFC4180.withNullString("null"))
        val header = dataProvider.header(StringReader(testInput))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(StringReader(testInput)).asSequence().toList()
        assertEquals(3, content.size)
        assertEquals(null, content[0][0])

//        val csvDatasetReader = DatasetReaderImpl(CSVFormat.RFC4180, { StringReader(input) }, emptyList())
//        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.INTEGER, "b"), ColumnInfo(FieldType.FLOAT, "c")),
//                csvDatasetReader.columnInfo)
//        val rows = csvDatasetReader.getIterator().asSequence().toList()
//        assertEquals(3, rows.size)
    }
}
