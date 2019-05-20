package com.github.raymank26

import org.junit.Test
import kotlin.test.assertEquals

/**
 * Date: 2019-05-20.
 */
class SqlExecutorTest : BaseSqlTest() {
    private val dataset = InMemoryDatasetReader(
            columnNamesField = listOf("field1", "field2"),
            columnInfoField = mutableMapOf(
                    Pair("field1", ColumnInfo(FieldType.STRING, 0)),
                    Pair("field2", ColumnInfo(FieldType.INTEGER, 1))
            ),
            datasetRows = listOf(DatasetRow(0, listOf("foobar", "1")), DatasetRow(1, listOf("baz", "10")))
    )
    @Test
    fun simpleExpression() {
        assertEquals(1, executeSelect("SELECT * FROM 'a' WHERE field2 = 1", dataset).rows.size)
    }
}