package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : SqlTestUtils {
    private val dataset = InMemoryDatasetReader(
            columnNamesField = listOf("field1", "field2"),
            columnInfoField = mutableMapOf(
                    Pair("field1", ColumnInfo(FieldType.STRING, 0)),
                    Pair("field2", ColumnInfo(FieldType.INTEGER, 1))
            ),
            datasetRows = listOf(DatasetRow(0, listOf("foobar", "1")), DatasetRow(1, listOf("baz", "10")))
    )
    @Test
    fun simple() {
        val executeSelect = executeSelect("SELECT field1 FROM 'a' WHERE field2 = 1", dataset)
        assertEquals(1, executeSelect.rows.size)
    }

    @Test
    fun testIn() {
        val executeSelect = executeSelect("SELECT field1 FROM 'a' WHERE field1 LIKE '%ob%'", dataset)
        assertEquals(1, executeSelect.rows.size)
    }
}