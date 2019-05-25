package com.github.raymank26

import java.util.NavigableMap
import java.util.TreeMap
import kotlin.test.fail

/**
 * Date: 2019-05-20.
 */
private val sqlAstBuilder = SqlAstBuilder()
private val sqlPlanner = SqlPlanner()
private val sqlExecutor = SqlExecutor()

private val columnInfos = listOf(
        ColumnInfo(FieldType.STRING, "a", 0),
        ColumnInfo(FieldType.INTEGER, "b", 1)
)
val datasetRows = createDataset(listOf(
        listOf("foobar", "1"),
        listOf("baz", "10"),
        listOf("baz", "11"),
        listOf("bazz", "2")
), columnInfos)

fun createDataset(listOf: List<List<String>>, columnInfo: List<ColumnInfo>): List<DatasetRow> {
    return listOf.mapIndexed { rowNum, columns ->
        val sqlColumns = columns.mapIndexed { i, column ->
            createSqlAtom(column, columnInfo[i].type)
        }
        return@mapIndexed DatasetRow(rowNum, sqlColumns, columnInfo)
    }
}

private val availableIndexes = listOf(
        createIndexBy(datasetRows, IndexDescription(name = "aIndex", fieldName = "a")),
        createIndexBy(datasetRows, IndexDescription(name = "bIndex", fieldName = "b"))
)

private val datasetReader = InMemoryDatasetReader(
        columnInfo = columnInfos,
        datasetRows = datasetRows,
        availableIndexes = availableIndexes
)

private fun createIndexBy(rows: List<DatasetRow>, indexDescription: IndexDescription): IndexDescriptionAndPath {
    if (rows.isEmpty()) {
        throw RuntimeException("Empty rows passed")
    }
    val index = TreeMap<Any, MutableList<Int>>()
    for (row in rows) {
        val key = row.getCell(indexDescription.fieldName).asValue
        index.compute(key) { _, prev ->
            return@compute if (prev == null) {
                mutableListOf(row.rowNum)
            } else {
                prev.add(row.rowNum)
                prev
            }
        }
    }
    val fieldType = rows.first().getCellType(indexDescription.fieldName) ?: throw RuntimeException("Field not found")
    return IndexDescriptionAndPath(indexDescription, InMemoryIndex(fieldType, index as NavigableMap<Any, List<Int>>))
}

private val datasetFactory = InMemoryDatasetFactory(datasetReader)

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
        return plan.datasetReader.use {
            sqlExecutor.execute(plan)
        }
    }

    fun testFailure(r: () -> Unit) {
        try {
            r()
            fail("Passed function has been executed normally, but failure has been expected")
        } catch (_: Exception) {
        }
    }
}

