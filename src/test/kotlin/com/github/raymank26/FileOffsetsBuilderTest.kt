package com.github.raymank26

import com.github.raymank26.index.FileOffsetsBuilder
import com.github.raymank26.planner.dataProvider
import java.nio.file.Paths
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-06-18.
 */
class FileOffsetsBuilderTest {

    private val testInput = """
        a,b,c
        Какой-то,здесь,юникод
        А строк,здесь,три
        12,8,3
    """.trimIndent()

    private val fileOffsetBuilder = FileOffsetsBuilder()

    @Test
    fun testOffset() {
        val dataPath = Paths.get("/test/file")
        val fileSystem = InMemoryFileSystem(mapOf(Pair(dataPath, testInput)))
        val data = dataProvider.get(fileSystem.getNavigableReader(dataPath)).toList()
        val charPositions = data.map { it.firstCharacterPosition }.toList()
        assertEquals(3, charPositions.size)

        val offsets: List<DatasetOffset> = fileSystem.getNavigableReader(dataPath).use { navigableReader ->
            fileOffsetBuilder.buildOffsets(navigableReader, charPositions)
        }
        assertEquals(3, offsets.size)

        val rowsByOffsets = dataProvider.get(fileSystem.getNavigableReader(dataPath), offsets.map { it.byteOffset }).toList()
                .map { it.columns }
        assertEquals(data.map { it.columns }, rowsByOffsets)
    }
}