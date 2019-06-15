package com.github.raymank26

import org.apache.commons.csv.CSVFormat
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Paths
import kotlin.test.assertEquals
import kotlin.test.assertFalse

private val FILE_DB_PATH = Paths.get("/tmp/csv_test")

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
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))
        val header = dataProvider.header(fileSystem.getNavigableReader(dataPath))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(fileSystem.getNavigableReader(dataPath)).toList()
        assertEquals(3, content.size)
        assertEquals(null, content[0][0])
        assertEquals(null, content[1][1])
    }

    @Test
    fun testEmptyReader() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, "")))
        val header = dataProvider.header(fileSystem.getNavigableReader(dataPath))
        assertEquals(emptyList(), header)
        val content = dataProvider.get(fileSystem.getNavigableReader(dataPath)).toList()
        assertEquals(emptyList(), content)
    }

    @Test
    fun testHeaderOnly() {
        val headerContent = "a,b,c"
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, headerContent)))
        val header = dataProvider.header(fileSystem.getNavigableReader(dataPath))
        assertEquals(listOf("a", "b", "c"), header)
        val content = dataProvider.get(fileSystem.getNavigableReader(dataPath)).toList()
        assertEquals(emptyList(), content)
    }

    @Test
    fun testMap() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))
        val content: List<List<String?>?> = dataProvider.get(fileSystem.getNavigableReader(dataPath))
                .map { it.plus("1") }
                .map { it.plus("2") }
                .toList()
        assertEquals(listOf(null, "2", "3.0", "1", "2"), content[0])
    }

    @Test
    fun testFieldInferring() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))
        val indexesManager = IndexesManager(fileSystem)
        val metadataProvider = DatasetMetadataProvider(fileSystem, dataProvider, indexesManager)
        val metadata = metadataProvider.getOrCreate(dataPath)
        println(fileSystem.outputMapping[Paths.get("/path/content.meta")])
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.LONG, "b"), ColumnInfo(FieldType.DOUBLE, "c")),
                metadata.columnInfos)
    }

    @Test
    fun testFieldInferringAllNull() {
        val dataPath = Paths.get("/path/content.csv")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, """
            a,b,c
            null,null,null
        """.trimIndent())))
        val indexesManager = IndexesManager(fileSystem)
        val metadataProvider = DatasetMetadataProvider(fileSystem, dataProvider, indexesManager)
        val metadata = metadataProvider.getOrCreate(dataPath)
        assertEquals(listOf(ColumnInfo(FieldType.STRING, "a"), ColumnInfo(FieldType.STRING, "b"), ColumnInfo(FieldType.STRING, "c")),
                metadata.columnInfos)
    }

    @Test
    fun testMetadataUsed() {
        val dataPath = Paths.get("/path/content.csv")
        val metaPath = Paths.get("/path/content.meta")
        val inMemoryFS = InMemoryFileSystem(mutableMapOf(
                Pair(dataPath, testInput),
                Pair(metaPath, """
                    columns=3,a;1,b;2,c
                    md5=d53b4a5133cd8a0d86151986735a3feb
                """.trimIndent())
        ))

        val indexesManager = IndexesManager(inMemoryFS)

        val metadataProvider = DatasetMetadataProvider(inMemoryFS, dataProvider, indexesManager)

        metadataProvider.getOrCreate(dataPath)

        assertFalse(inMemoryFS.outputMapping.containsKey(metaPath))
    }

    @Test
    fun testIndexLoaded() {
        val dataPath = Paths.get("/path/content.csv")
        val indexPath = Paths.get("/path/content.index")

        val inMemoryFS = InMemoryFileSystem(mapOf(
                Pair(dataPath, testInput)
        ), mapOf(
                Pair(indexPath, FILE_DB_PATH)
        ))
        val indexesManager = IndexesManager(inMemoryFS)
        val metadataProvider = DatasetMetadataProvider(inMemoryFS, dataProvider, indexesManager)
        val readerFactory = FilesystemDatasetReaderFactory(metadataProvider, inMemoryFS, dataProvider)
        indexesManager.createIndex(dataPath, "aIndex", "a", readerFactory)
        indexesManager.createIndex(dataPath, "bIndex", "b", readerFactory)
        indexesManager.createIndex(dataPath, "cIndex", "c", readerFactory)

        val metadata = metadataProvider.getOrCreate(dataPath)
        assertEquals(
                listOf(IndexDescription("aIndex", "a"), IndexDescription("bIndex", "b"), IndexDescription("cIndex", "c")),
                metadata.indexes.map { it.description })
        println(metadata)
    }

    @Test
    fun testOffsets() {
        val dataPath = Paths.get("/path/content.csv")
        val inMemoryFS = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))

        val indexesManager = IndexesManager(inMemoryFS)
        val metadataProvider = DatasetMetadataProvider(inMemoryFS, dataProvider, indexesManager)
        val readerFactory = FilesystemDatasetReaderFactory(metadataProvider, inMemoryFS, dataProvider)
        val reader = readerFactory.getReader(dataPath)!!

        val rows = reader.getIterator(listOf(6, 17, 30)).toList()
        assertEquals(3, rows.size)
    }

    @Before
    @After
    fun cleanupFileDb() {
        if (Files.exists(FILE_DB_PATH)) {
            Files.delete(FILE_DB_PATH)
        }
    }
}
