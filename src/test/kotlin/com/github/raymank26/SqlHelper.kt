package com.github.raymank26

import kotlin.test.fail

/**
 * Date: 2019-05-20.
 */
private val sqlAstBuilder = SqlAstBuilder()
private val sqlPlanner = SqlPlanner()
private val sqlExecutor = SqlExecutor()

private val availableIndexes = listOf(
        IndexDescriptionAndPath(IndexDescription(name = "aIndex", fieldName = "a"), InMemoryEmptyIndex<String>(FieldType.STRING)),
        IndexDescriptionAndPath(IndexDescription(name = "bIndex", fieldName = "b"), InMemoryEmptyIndex<Int>(FieldType.INTEGER))
)

private val dataset = InMemoryDatasetReader(
        columnInfoField = mutableMapOf(
                Pair("a", ColumnInfo(FieldType.STRING, 0)),
                Pair("b", ColumnInfo(FieldType.INTEGER, 1))
        ),
        datasetRows = listOf(DatasetRow(0, listOf("foobar", "1")), DatasetRow(1, listOf("baz", "10"))),
        availableIndexes = availableIndexes
)

private val datasetFactory = InMemoryDatasetFactory(dataset)

interface SqlTestUtils {

    fun getDefaultDatasetFactory(): DatasetReaderFactory {
        return datasetFactory
    }

    fun testParser(statement: String) {
        sqlAstBuilder.parse(statement)
    }

    fun makePlan(sql: String, datasetReaderFactory: DatasetReaderFactory = getDefaultDatasetFactory()): SqlPlan {
        return sqlPlanner.createPlan((sqlAstBuilder.parse(sql) as SelectStatement).ctx, datasetReaderFactory)
    }

    fun executeSelect(sql: String, datasetReaderFactory: DatasetReaderFactory): DatasetResult {
        val plan = makePlan(sql, datasetReaderFactory)
        return TODO()
//        return sqlExecutor.execute(EngineContext(TODO(), emptyMap()), plan)
    }

    fun testFailure(r: () -> Unit) {
        try {
            r()
            fail("Passed function has been executed normally, but failure has been expected")
        } catch (_: Exception) {
        }
    }
}

