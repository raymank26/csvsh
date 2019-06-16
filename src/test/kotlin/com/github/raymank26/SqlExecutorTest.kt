package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils {

    @Test
    fun simple() {
        val dataset = executeSelect("SELECT a FROM '/test/input.csv' WHERE b = 1", getDefaultDatasetFactory())
        assertEquals(1, dataset.rows.toList().size)
    }

    @Test
    fun testLike() {
        val dataset = executeSelect("SELECT * FROM '/test/input.csv' WHERE a LIKE '%ba%' AND c < 3.2 AND b <= 2", getDefaultDatasetFactory())
        println(prettifyDataset(dataset))
    }

    @Test
    fun testGroupBy() {
        val dataset = executeSelect("SELECT a, SUM(b), MIN(b), MAX(b), COUNT(b), SUM(c) FROM '/test/input.csv' GROUP BY a ORDER BY SUM(b) DESC", getDefaultDatasetFactory())
        println(prettifyDataset(dataset))
    }
}