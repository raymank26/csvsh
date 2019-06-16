package com.github.raymank26

import org.junit.Test
import java.nio.file.Paths
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils() {

    @Test
    fun simple() {
        val dataset = executeSelect("SELECT a FROM '/test/input.csv' WHERE b = 1", getDefaultDatasetFactory())
        val datasetList = dataset.rows.toList()
        assertEquals(1, datasetList.size)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))
    }

    @Test
    fun testLike() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a LIKE '%ba%' AND c < 3.2 AND b <= 2", getDefaultDatasetFactory())
        val datasetList = dataset.rows.toList()
        assertEquals(3, datasetList.size)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))
    }

    @Test
    fun testLikeWithIndexes() {
        indexesManager.createIndex(Paths.get("/test/input.csv"), "bIndex", "b", readerFactory)
        indexesManager.createIndex(Paths.get("/test/input.csv"), "cIndex", "c", readerFactory)
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a LIKE '%ba%' AND c < 3.2 AND b <= 2", getDefaultDatasetFactory())
        val datasetList = dataset.rows.toList()
        assertEquals(3, datasetList.size)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))

    }

    @Test
    fun testGroupBy() {
        val dataset = executeSelect("SELECT a, SUM(b), MIN(b), MAX(b), COUNT(b), SUM(c) FROM '/test/input.csv' GROUP BY a ORDER BY SUM(b) DESC", getDefaultDatasetFactory())
        val datasetList = dataset.rows.toList()
        assertEquals(5, datasetList.size)
        println(prettifyDataset(dataset.copy(ClosableSequence(datasetList.asSequence()))))
    }
}