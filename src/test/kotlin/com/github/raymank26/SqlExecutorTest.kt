package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils {

    @Test
    fun simple() {
        val executeSelect = executeSelect("SELECT a FROM 'a' WHERE b = 1", getDefaultDatasetFactory())
        assertEquals(1, executeSelect.rows.size)
    }

    @Test
    fun testIn() {
        val executeSelect = executeSelect("SELECT a FROM 'a' WHERE a LIKE '%ob%'", getDefaultDatasetFactory())
        assertEquals(1, executeSelect.rows.size)
    }
}