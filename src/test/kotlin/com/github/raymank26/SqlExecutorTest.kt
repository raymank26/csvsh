package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils {

    @Test
    fun simple() {
        val executeSelect = executeSelect("SELECT field1 FROM 'a' WHERE field2 = 1", getDefaultDatasetFactory())
        assertEquals(1, executeSelect.rows.size)
    }

    @Test
    fun testIn() {
        val executeSelect = executeSelect("SELECT field1 FROM 'a' WHERE field1 LIKE '%ob%'", getDefaultDatasetFactory())
        assertEquals(1, executeSelect.rows.size)
    }
}