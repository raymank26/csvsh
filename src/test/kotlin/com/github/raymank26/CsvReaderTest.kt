package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.Test
import java.io.StringReader
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

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

    @Test
    fun testMap() {
        val content: List<List<String?>?> = dataProvider.get(StringReader(testInput))
                .map { it.plus("1") }
                .map { it.plus("2") }
                .toList()
        assertEquals(listOf(null, "2", "3.0", "1", "2"), content[0])
    }

    @Test
    fun testFieldInferring() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))
        val metadataProvider = DatasetMetadataProvider(fileSystem, dataProvider)
        val data = metadataProvider.getOrCreate(dataPath)
        val metadata = fileSystem.outputMapping[Paths.get("/path/content.meta")]?.toString()
        assertNotNull(metadata)
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.INTEGER, "b"), ColumnInfo(FieldType.FLOAT, "c")), data.columnInfos)
    }
}
