package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.Test
import org.mapdb.DBMaker
import org.mapdb.Serializer
import java.io.StringReader
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertFalse

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
        val content = dataProvider.get(StringReader(testInput)).toList()
        assertEquals(3, content.size)
        assertEquals(null, content[0][0])
        assertEquals(null, content[1][1])
    }

    @Test
    fun testEmptyReader() {
        val input = ""
        val header = dataProvider.header(StringReader(input))
        assertEquals(emptyList(), header)
        val content = dataProvider.get(StringReader(input)).toList()
        assertEquals(emptyList(), content)
    }

    @Test
    fun testHeaderOnly() {
        val input = "a,b,c"
        val header = dataProvider.header(StringReader(input))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(StringReader(input)).toList()
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
        val metadata = metadataProvider.getOrCreate(dataPath)
        println(fileSystem.outputMapping[Paths.get("/path/content.meta")])
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.INTEGER, "b"), ColumnInfo(FieldType.FLOAT, "c")),
                metadata.columnInfos)
    }

    @Test
    fun testFieldInferringAllNull() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, """
            a,b,c
            null,null,null
        """.trimIndent())))
        val metadataProvider = DatasetMetadataProvider(fileSystem, dataProvider)
        val metadata = metadataProvider.getOrCreate(dataPath)
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.STRING, "b"), ColumnInfo(FieldType.STRING, "c")),
                metadata.columnInfos)
    }

    @Test
    fun testMetadataUsed() {
        val dataPath = Paths.get("/path/content.csv")
        val metaPath = Paths.get("/path/content.meta")
        val inMemory = InMemoryFileSystem(mutableMapOf(
                Pair(dataPath, testInput),
                Pair(metaPath, """
                    columns=3,a;1,b;2,c
                    md5=d53b4a5133cd8a0d86151986735a3feb
                """.trimIndent())
        ))

        val metadataProvider = DatasetMetadataProvider(inMemory, dataProvider)

        metadataProvider.getOrCreate(dataPath)

        assertFalse(inMemory.outputMapping.containsKey(metaPath))
    }

    @Test
    fun testIndexLoaded() {
        val dataPath = Paths.get("/path/content.csv")
        val indexPath = Paths.get("/path/content.index")
        val memoryDb = DBMaker.memoryDB().make()
        memoryDb.treeMap("bIndex|b|${FieldType.INTEGER.mark}", Serializer.INTEGER, Serializer.INT_ARRAY)
                .createOrOpen()

        memoryDb.treeMap("cIndex|c|${FieldType.FLOAT.mark}", Serializer.FLOAT, Serializer.INT_ARRAY)
                .createOrOpen()

        val inMemory = InMemoryFileSystem(mapOf(
                Pair(dataPath, testInput)
        ), mapOf(
                Pair(indexPath, memoryDb)
        ))
        val metadataProvider = DatasetMetadataProvider(inMemory, dataProvider)

        val metadata = metadataProvider.getOrCreate(dataPath)
        assertEquals(listOf(IndexDescription("bIndex", "b"), IndexDescription("cIndex", "c")), metadata.indexes.map { it.description })
        println(metadata)
    }
}
