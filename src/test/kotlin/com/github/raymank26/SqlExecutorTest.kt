package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils {

    @Test
    fun simple() {
        val dataset = executeSelect("SELECT a FROM 'a' WHERE b = 1", getDefaultDatasetFactory())
        assertEquals(1, dataset.rows.size)
    }

    @Test
    fun testIn() {
        val dataset = executeSelect("SELECT a FROM 'a' WHERE a LIKE '%ob%'", getDefaultDatasetFactory())
        assertEquals(1, dataset.rows.size)
    }

    @Test
    fun testGroupBy() {
        val dataset = executeSelect("SELECT a, SUM(b), MIN(b), MAX(b), COUNT(b) FROM 'a' GROUP BY a ORDER BY SUM(b) desc", getDefaultDatasetFactory())
        println(prettifyDataset(dataset))
    }
}