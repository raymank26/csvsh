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

    private val dataProvider = CsvContentDataProvider(CSVFormat.RFC4180.withNullString("null"))

    @Test
    fun testDatasetReader() {
        val header = dataProvider.header(StringReader(testInput))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(StringReader(testInput)).asSequence().toList()
        assertEquals(3, content.size)
        assertEquals(null, content[0][0])
        assertEquals(null, content[1][1])
    }

    @Test
    fun testEmptyReader() {
        val input = ""
        val header = dataProvider.header(StringReader(input))
        assertEquals(emptyList(), header)
        val content = dataProvider.get(StringReader(input)).asSequence().toList()
        assertEquals(emptyList(), content)
    }

    @Test
    fun testHeaderOnly() {
        val input = "a,b,c"
        val header = dataProvider.header(StringReader(input))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(StringReader(input)).asSequence().toList()
        assertEquals(emptyList(), content)
    }
}
